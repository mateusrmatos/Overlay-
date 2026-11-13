"""Nucleo do protocolo (sans-I/O) - Marco 1 & 2 (Flood + Rotas).

Ideia central: este modulo NAO abre sockets, NAO dorme, NAO le o relogio.
Recebe EVENTOS (arrancar, chegou um pacote, expirou um temporizador) e devolve ACOES
(enviar um datagrama, armar/cancelar um temporizador, escrever uma linha de log).

    eventos:  start(now) | on_packet(now, src, data) | on_timer(now, key)
    acoes:    Send(dst, data) | SetTimer(key, delay) | CancelTimer(key) | Log(text)
"""
from collections import namedtuple

from . import wire

# ------------------------------------------------------------------ acoes
Send = namedtuple("Send", "dst data")
SetTimer = namedtuple("SetTimer", "key delay")
CancelTimer = namedtuple("CancelTimer", "key")
Log = namedtuple("Log", "text")

# ------------------------------------------------------------------ parametros
REGISTER_RTO0, REGISTER_RTO_MAX, REGISTER_TRIES = 0.5, 4.0, 10
HELLO_RTO0, HELLO_RTO_MAX, HELLO_TRIES = 0.2, 4.0, 5

HELLO_INTERVAL, HELLO_DEAD_AFTER = 1.0, 3.0
RTT_ALPHA = 0.125

# Flood periodico de rotas (Servidor -> Rede) e tempo de expiracao (3 periodos)
ADVERT_INTERVAL = 2.0
ROUTE_STALE_AFTER = 6.0

# estados do no
INIT, REGISTERING, JOINED, ORPHAN = "INIT", "REGISTERING", "JOINED", "ORPHAN"


def _seq_mais_recente(a, b):
    """True se o numero de sequencia de 16 bits `a` for mais recente do que `b`."""
    return 0 < ((a - b) & 0xFFFF) < 0x8000


class Pending:
    """Uma mensagem fiavel a aguardar confirmacao."""
    __slots__ = ("dst", "data", "tries", "rto", "rto_max", "max_tries", "kind", "peer")

    def __init__(self, dst, data, rto, rto_max, max_tries, kind, peer=None):
        self.dst, self.data = dst, data
        self.tries, self.rto, self.rto_max, self.max_tries = 1, rto, rto_max, max_tries
        self.kind, self.peer = kind, peer


class Vizinho:
    """O que um no sabe de um vizinho: so isto. Nunca o grafo completo."""
    __slots__ = ("name", "addr", "alive", "last_heard", "rtt", "srtt")

    def __init__(self, name, addr):
        self.name, self.addr = name, addr
        self.alive, self.last_heard = False, None
        self.rtt = self.srtt = None

    def __repr__(self):
        return f"Vizinho({self.name}, {self.addr}, {'vivo' if self.alive else 'sem contacto'})"


class Rota:
    """Entrada na tabela de encaminhamento para um fluxo (stream).
    Mantem a rota principal (origem, custo) e caminhos alternativos conhecidos."""
    __slots__ = (
        "stream", "server", "origem", "custo", "seq", "last_updated",
        "destinos", "alternativas", "local", "consumo_local",
    )

    def __init__(self, stream, server, origem, custo, seq, now=0.0, local=False):
        self.stream = stream
        self.server = server
        self.origem = origem
        self.custo = custo
        self.seq = seq
        self.last_updated = now
        self.destinos = set()
        self.alternativas = {}
        self.local = local
        self.consumo_local = False

    @property
    def tem_montante(self):
        return self.local or self.origem is not None

    @property
    def tem_jusante(self):
        return self.consumo_local or len(self.destinos) > 0

    @property
    def ativa(self):
        """A rota esta ativa se temos fonte (origem ou local) e interessados (destinos ou cliente local)."""
        return self.tem_montante and self.tem_jusante

    def __repr__(self):
        est = "ATIVA" if self.ativa else ("INATIVA" if self.tem_montante else "SEM_ORIGEM")
        dsts = list(sorted(self.destinos))
        if self.consumo_local:
            dsts.append("LOCAL")
        dst_str = ",".join(dsts) or "-"
        orig_str = "LOCAL" if self.local else str(self.origem)
        c_str = f"{self.custo:g}" if self.custo != float("inf") else "inf"
        return (f"Rota(fluxo={self.stream}, srv={self.server}, origem={orig_str}, "
                f"custo={c_str}, destinos=[{dst_str}], estado={est})")


class Node:
    def __init__(self, name, boot_addr, hello_interval=HELLO_INTERVAL, dead_after=HELLO_DEAD_AFTER,
                 advert_interval=ADVERT_INTERVAL, route_stale_after=ROUTE_STALE_AFTER,
                 streams=(), watch=()):
        self.name = name
        self.boot_addr = tuple(boot_addr)
        self.hello_interval, self.dead_after = hello_interval, dead_after
        self.advert_interval = advert_interval
        self.route_stale_after = route_stale_after
        self.state = INIT
        self.vizinhos = {}        # nome -> Vizinho
        self.rotas = {}           # stream -> Rota (Tabela de Rotas)
        self.pending = {}         # msg_id -> Pending (mensagens fiaveis)
        self.probes = {}          # msg_id -> (nome, t_envio): HELLO periodico
        self. desejadas = set(watch)  # streams que este no (cliente) quer consumir
        self._next_id = 1
        for s in streams:
            self.registar_stream_local(s)

    # ------------------------------------------------------------ utilitarios
    def _new_id(self):
        for _ in range(0x10000):
            mid = self._next_id
            self._next_id = self._next_id % 0xFFFF + 1
            if mid not in self.pending and mid not in self.probes:
                return mid
        raise RuntimeError("sem msg_id livres")

    def _vizinho_por_addr(self, src):
        """Identifica o Vizinho pelo IP de origem do pacote UDP."""
        ip = src[0]
        for v in self.vizinhos.values():
            if v.addr[0] == ip:
                return v
        return None

    def _send(self, acts, dst, data):
        """Unico ponto por onde sai um datagrama. Depois de JOINED/ORPHAN, o bootstrapper nao e contactado."""
        if tuple(dst) == self.boot_addr and self.state in (JOINED, ORPHAN):
            raise AssertionError(f"{self.name}: tentativa de contactar o bootstrapper depois do arranque")
        acts.append(Send(tuple(dst), data))

    def _send_reliable(self, acts, dst, msg_id, data, rto0, rto_max, max_tries, kind, peer=None):
        self.pending[msg_id] = Pending(tuple(dst), data, rto0, rto_max, max_tries, kind, peer)
        self._send(acts, dst, data)
        acts.append(SetTimer(("rtx", msg_id), rto0))

    def _settle(self, acts, msg_id):
        p = self.pending.pop(msg_id, None)
        if p is not None:
            acts.append(CancelTimer(("rtx", msg_id)))
        return p

    # ------------------------------------------------------------ eventos
    def start(self, now):
        acts = []
        self.state = REGISTERING
        mid = self._new_id()
        data = wire.enc_name_msg(wire.T_REGISTER, mid, self.name)
        acts.append(Log(f"a arrancar; REGISTER ao bootstrapper {self.boot_addr[0]}:{self.boot_addr[1]}"))
        self._send_reliable(acts, self.boot_addr, mid, data,
                            REGISTER_RTO0, REGISTER_RTO_MAX, REGISTER_TRIES, "REGISTER")
        return acts

    def on_packet(self, now, src, data):
        acts = []
        try:
            tipo, mid, payload = wire.decode(data)
            handler = {
                wire.T_NEIGHBOURS: self._on_neighbours,
                wire.T_HELLO: self._on_hello,
                wire.T_ACK: self._on_ack,
                wire.T_ADVERT: self._on_advert,
                wire.T_ACTIVATE: self._on_activate,
                wire.T_DEACTIVATE: self._on_deactivate,
            }.get(tipo)
            if handler is None:
                acts.append(Log(f"ignorei {wire.nome_do_tipo(tipo)} de {src[0]} (ainda nao implementado)"))
            else:
                handler(acts, now, src, mid, payload)
        except wire.WireError as e:
            acts.append(Log(f"datagrama invalido de {src[0]}: {e}"))
        return acts

    def on_timer(self, now, key):
        acts = []
        if key[0] == "rtx":
            self._on_rtx(acts, key[1])
        elif key[0] == "tick":
            self._on_tick(acts, now)
        elif key[0] == "advert":
            self._on_advert_timer(acts, now)
        return acts

    # ------------------------------------------------------------ retransmissao
    def _on_rtx(self, acts, mid):
        p = self.pending.get(mid)
        if p is None:
            return
        if p.tries >= p.max_tries:
            del self.pending[mid]
            if p.kind == "REGISTER":
                self.state = ORPHAN
                acts.append(Log(f"REGISTER sem resposta apos {p.tries} tentativas: desisto"))
            else:
                acts.append(Log(f"{p.kind} para {p.peer} sem ACK apos {p.tries} tentativas "
                                f"(o vizinho pode ainda nao ter arrancado; continuo a sondar "
                                f"a cada {self.hello_interval:g}s)"))
            return
        p.tries += 1
        p.rto = min(p.rto * 2, p.rto_max)
        self._send(acts, p.dst, p.data)
        acts.append(SetTimer(("rtx", mid), p.rto))
        acts.append(Log(f"retransmito {p.kind} #{p.tries} (proximo timeout {p.rto:.1f}s)"))

    # ------------------------------------------------------------ handlers Marco 1
    def _on_neighbours(self, acts, now, src, mid, payload):
        if self._settle(acts, mid) is None or self.state != REGISTERING:
            return
        entries = wire.dec_neighbours(payload)
        self.state = JOINED
        acts.append(Log(f"JOINED; vizinhos: " + (", ".join(f"{n}@{ip}" for n, ip, _ in entries) or "(nenhum)")))
        if not entries:
            acts.append(Log(f"AVISO: JOINED sem vizinhos. O bootstrapper conhece o nome {self.name!r}? "
                            f"Tem de ser igual ao do cenario, maiusculas incluidas."))
        for nome, ip, porta in entries:
            if nome in self.vizinhos:
                self.vizinhos[nome].addr = (ip, porta)
            else:
                self.vizinhos[nome] = Vizinho(nome, (ip, porta))
            hid = self._new_id()
            data = wire.enc_name_msg(wire.T_HELLO, hid, self.name)
            self._send_reliable(acts, (ip, porta), hid, data,
                                HELLO_RTO0, HELLO_RTO_MAX, HELLO_TRIES, "HELLO", peer=nome)
        acts.append(SetTimer(("tick",), self.hello_interval))
        if any(r.local for r in self.rotas.values()):
            acts.append(SetTimer(("advert",), self.advert_interval))

    def _on_hello(self, acts, now, src, mid, payload):
        nome = wire.dec_name_msg(payload)
        acts.append(Send(tuple(src), wire.enc_ack(mid)))
        v = self.vizinhos.get(nome)
        if v is None:
            v = self.vizinhos[nome] = Vizinho(nome, tuple(src))
        era_morto = not v.alive
        if era_morto:
            voltou = v.last_heard is not None
            v.alive = True
            acts.append(Log(f"HELLO de {nome} ({src[0]}): vizinho {'voltou' if voltou else 'vivo'}"))
        v.last_heard = now
        if era_morto:
            self._enviar_rotas_conhecidas_a(acts, v)

    def _on_ack(self, acts, now, src, mid, payload):
        sonda = self.probes.pop(mid, None)
        if sonda is not None:
            self._on_sonda_ack(acts, now, src, *sonda)
            return
        p = self._settle(acts, mid)
        if p is None:
            return
        if p.kind == "HELLO":
            v = self.vizinhos[p.peer]
            era_morto = not v.alive
            if era_morto:
                v.alive = True
                acts.append(Log(f"ACK de {p.peer} ({src[0]}): vizinho vivo ({p.tries} tentativa(s))"))
            v.last_heard = now
            if era_morto:
                self._enviar_rotas_conhecidas_a(acts, v)

    # ------------------------------------------------------------ handlers Marco 2 (Flood e Rotas)
    def _on_advert(self, acts, now, src, mid, payload):
        """Recebe um anuncio de rota (ADVERT), atualiza a tabela e propaga se melhorou (Flood controlado)."""
        stream, server, seq, cost = wire.dec_advert(payload)
        v = self._vizinho_por_addr(src)
        if v is None:
            return
        if not v.alive:
            v.alive = True
            acts.append(Log(f"ADVERT de {v.name} ({src[0]}): vizinho vivo"))
        v.last_heard = now

        melhorou = self.atualizar_rota(acts, stream, server, v.name, cost, seq, now=now)
        if stream in self.desejadas:
            self.pedir_stream(acts, stream)
        if melhorou:
            self._propagar_advert(acts, self.rotas[stream], exceto=v.name)

    def _on_activate(self, acts, now, src, mid, payload):
        """Recebe pedido de ativacao de um vizinho a jusante."""
        stream = wire.dec_stream_msg(payload)
        v = self._vizinho_por_addr(src)
        if v is None:
            return
        v.last_heard = now
        self.adicionar_destino_rota(acts, stream, v.name)

    def _on_deactivate(self, acts, now, src, mid, payload):
        """Recebe cancelamento de stream de um vizinho a jusante."""
        stream = wire.dec_stream_msg(payload)
        v = self._vizinho_por_addr(src)
        if v is None:
            return
        v.last_heard = now
        self.remover_destino_rota(acts, stream, v.name)

    def _on_advert_timer(self, acts, now):
        """Disparo periodico no Servidor: incrementa seq e inunda ADVERT de todas as streams locais."""
        locais = [r for r in self.rotas.values() if r.local]
        if not locais:
            return
        acts.append(SetTimer(("advert",), self.advert_interval))
        for r in locais:
            r.seq = ((r.seq or 0) + 1) & 0xFFFF
            if r.seq == 0:
                r.seq = 1
            r.last_updated = now
            self._propagar_advert(acts, r, exceto=None)

    def _propagar_advert(self, acts, r, exceto=None):
        """Inundacao controlada: envia ADVERT com o custo deste no a todos os vizinhos vivos,
        exceto ao que nos enviou o anuncio (`exceto`) e ao proprio servidor da stream."""
        if not r.tem_montante or r.seq is None or r.custo == float("inf"):
            return
        for v in self.vizinhos.values():
            if not v.alive or v.name == exceto or v.name == r.server:
                continue
            mid = self._new_id()
            self._send(acts, v.addr, wire.enc_advert(mid, r.stream, r.server, r.seq, r.custo))

    def _enviar_rotas_conhecidas_a(self, acts, v):
        """Quando um vizinho fica vivo, envia-lhe imediatamente as rotas validas conhecidas."""
        for r in self.rotas.values():
            if not r.tem_montante or r.custo == float("inf") or v.name == r.origem or v.name == r.server:
                continue
            if r.local and (r.seq is None or r.seq == 0):
                r.seq = 1
            if r.seq is not None:
                mid = self._new_id()
                self._send(acts, v.addr, wire.enc_advert(mid, r.stream, r.server, r.seq, r.custo))

    # ------------------------------------------------------------ monitorizacao dos vizinhos
    def _on_tick(self, acts, now):
        acts.append(SetTimer(("tick",), self.hello_interval))
        self.probes = {m: pr for m, pr in self.probes.items() if now - pr[1] < self.dead_after}
        for v in self.vizinhos.values():
            if v.alive and v.last_heard is not None and now - v.last_heard > self.dead_after:
                self._vizinho_caiu(acts, now, v)
            if any(p.kind == "HELLO" and p.peer == v.name for p in self.pending.values()):
                continue
            mid = self._new_id()
            self.probes[mid] = (v.name, now)
            self._send(acts, v.addr, wire.enc_name_msg(wire.T_HELLO, mid, self.name))
        self._expirar_rotas(acts, now)

    def _on_sonda_ack(self, acts, now, src, nome, t_envio):
        v = self.vizinhos.get(nome)
        if v is None:
            return
        v.rtt = now - t_envio
        v.srtt = v.rtt if v.srtt is None else (1 - RTT_ALPHA) * v.srtt + RTT_ALPHA * v.rtt
        era_morto = not v.alive
        if era_morto:
            voltou = v.last_heard is not None
            v.alive = True
            acts.append(Log(f"ACK de {nome} ({src[0]}): vizinho {'voltou' if voltou else 'vivo'}"))
        v.last_heard = now
        if era_morto:
            self._enviar_rotas_conhecidas_a(acts, v)

    def _vizinho_caiu(self, acts, now, v):
        acts.append(Log(f"vizinho {v.name} CAIU (sem sinal ha {now - v.last_heard:.1f}s)"))
        v.alive = False
        v.rtt = v.srtt = None
        self._remover_vizinho_das_rotas(acts, v.name)

    # ------------------------------------------------------------ tabela de rotas
    def custo_ligacao(self, nome_vizinho):
        return 1.0

    def _enviar_controlo_rota(self, acts, tipo, nome_vizinho, stream):
        if nome_vizinho is None:
            return
        v = self.vizinhos.get(nome_vizinho)
        if v is None:
            return
        mid = self._new_id()
        self._send(acts, v.addr, wire.enc_stream_msg(tipo, mid, stream))

    def registar_stream_local(self, stream):
        r = self.rotas.get(stream)
        if r is None:
            r = self.rotas[stream] = Rota(stream, self.name, None, 0.0, 0, local=True)
        else:
            r.local = True
            r.server = self.name
            r.origem = None
            r.custo = 0.0
        return r

    def pedir_stream(self, acts, stream):
        self.desejadas.add(stream)
        r = self.rotas.get(stream)
        if r is None or not r.tem_montante:
            return False
        estava_ativa = r.ativa
        r.consumo_local = True
        if not estava_ativa and r.ativa and not r.local:
            acts.append(Log(f"rota {stream} passou a ATIVA (consumo local via {r.origem})"))
            self._enviar_controlo_rota(acts, wire.T_ACTIVATE, r.origem, stream)
        return not estava_ativa and r.ativa

    def deixar_stream(self, acts, stream):
        self.desejadas.discard(stream)
        r = self.rotas.get(stream)
        if r is None or not r.consumo_local:
            return False
        estava_ativa = r.ativa
        r.consumo_local = False
        if estava_ativa and not r.ativa and not r.local:
            acts.append(Log(f"rota {stream} passou a INATIVA (fim de consumo local)"))
            self._enviar_controlo_rota(acts, wire.T_DEACTIVATE, r.origem, stream)
        return estava_ativa and not r.ativa

    def atualizar_rota(self, acts, stream, server, vizinho_origem, custo_anunciado, seq, now=0.0):
        if vizinho_origem not in self.vizinhos or server == self.name or custo_anunciado == float("inf"):
            return False

        novo_custo = custo_anunciado + self.custo_ligacao(vizinho_origem)
        r = self.rotas.get(stream)

        # Caso 1: Fluxo totalmente novo para este no
        if r is None:
            r = self.rotas[stream] = Rota(stream, server, vizinho_origem, novo_custo, seq, now=now)
            acts.append(Log(f"rota criada: {r}"))
            return True

        if r.local:
            return False

        seq_novo = r.seq is None or _seq_mais_recente(seq, r.seq)
        seq_igual = r.seq is not None and seq == r.seq

        if not seq_novo and not seq_igual:
            return False

        # Caso 2: Nova ronda de anuncio do servidor (seq mais recente)
        if seq_novo:
            if not (vizinho_origem == r.origem or r.origem is None or novo_custo < r.custo):
                r.alternativas[vizinho_origem] = (novo_custo, seq)
                return False

            origem_antiga = r.origem
            tinha_jusante = r.tem_jusante
            r.alternativas = {
                v: (c, s) for v, (c, s) in r.alternativas.items()
                if s is not None and (s == seq or _seq_mais_recente(s, seq))
            }
            r.alternativas.pop(vizinho_origem, None)
            r.server = server
            r.seq = seq
            r.last_updated = now
            r.origem = vizinho_origem
            r.custo = novo_custo
            acts.append(Log(f"rota atualizada (seq={seq}): {r}"))
            if tinha_jusante and vizinho_origem != origem_antiga:
                self._enviar_controlo_rota(acts, wire.T_ACTIVATE, vizinho_origem, stream)
                if origem_antiga is not None:
                    self._enviar_controlo_rota(acts, wire.T_DEACTIVATE, origem_antiga, stream)
            return True

        # Caso 3: Mesmo seq (anuncio que chegou por outro caminho na mesma inundacao)
        if vizinho_origem == r.origem:
            r.last_updated = now
            if novo_custo < r.custo:
                r.custo = novo_custo
                return True
            return False

        if r.origem is None or novo_custo < r.custo:
            origem_antiga = r.origem
            tinha_jusante = r.tem_jusante
            if origem_antiga is not None and r.custo != float("inf"):
                r.alternativas[origem_antiga] = (r.custo, r.seq)
            r.origem = vizinho_origem
            r.custo = novo_custo
            r.last_updated = now
            r.alternativas.pop(vizinho_origem, None)
            acts.append(Log(f"melhor rota encontrada para {stream} via {vizinho_origem} (custo {novo_custo:g})"))
            if tinha_jusante and vizinho_origem != origem_antiga:
                self._enviar_controlo_rota(acts, wire.T_ACTIVATE, vizinho_origem, stream)
                if origem_antiga is not None:
                    self._enviar_controlo_rota(acts, wire.T_DEACTIVATE, origem_antiga, stream)
            return True

        # Caso 4: Caminho mais longo ou igual -> guarda-se como rota alternativa de backup
        antigo = r.alternativas.get(vizinho_origem)
        if antigo is None or novo_custo < antigo[0]:
            r.alternativas[vizinho_origem] = (novo_custo, seq)
        return False

    def adicionar_destino_rota(self, acts, stream, vizinho_destino):
        r = self.rotas.get(stream)
        if r is None:
            return False
        estava_ativa = r.ativa
        r.destinos.add(vizinho_destino)
        ativou_agora = not estava_ativa and r.ativa
        if ativou_agora:
            acts.append(Log(f"rota {stream} passou a ATIVA -> destinos: {sorted(r.destinos)}"))
            if not r.local:
                self._enviar_controlo_rota(acts, wire.T_ACTIVATE, r.origem, stream)
        else:
            acts.append(Log(f"rota {stream} adicionou destino {vizinho_destino} -> destinos: {sorted(r.destinos)}"))
        return ativou_agora

    def remover_destino_rota(self, acts, stream, vizinho_destino):
        r = self.rotas.get(stream)
        if r is None or vizinho_destino not in r.destinos:
            return False
        estava_ativa = r.ativa
        r.destinos.discard(vizinho_destino)
        desativou_agora = estava_ativa and not r.ativa
        if desativou_agora:
            acts.append(Log(f"rota {stream} passou a INATIVA (sem clientes)"))
            if not r.local:
                self._enviar_controlo_rota(acts, wire.T_DEACTIVATE, r.origem, stream)
        return desativou_agora

    def _remover_vizinho_das_rotas(self, acts, nome_viz):
        for r in self.rotas.values():
            r.alternativas.pop(nome_viz, None)
            if nome_viz in r.destinos:
                self.remover_destino_rota(acts, r.stream, nome_viz)

            if r.origem == nome_viz:
                tinha_jusante = r.tem_jusante
                validas = {v: (c, s) for v, (c, s) in r.alternativas.items()
                           if v in self.vizinhos and self.vizinhos[v].alive and c != float("inf")}
                if validas:
                    melhor_v = min(validas, key=lambda v: validas[v][0])
                    r.origem, (r.custo, r.seq) = melhor_v, r.alternativas.pop(melhor_v)
                    acts.append(Log(f"failover da rota {r.stream}: nova origem {r.origem} (custo {r.custo:g})"))
                    if tinha_jusante:
                        self._enviar_controlo_rota(acts, wire.T_ACTIVATE, r.origem, r.stream)
                else:
                    r.origem, r.custo = None, float("inf")
                    acts.append(Log(f"rota {r.stream} perdeu a origem {nome_viz} e nao tem alternativas!"))

    def _expirar_rotas(self, acts, now):
        if self.route_stale_after is None:
            return
        for r in self.rotas.values():
            if r.local or r.seq is None:
                continue
            if now - r.last_updated > self.route_stale_after:
                r.seq = None
                r.alternativas.clear()
                acts.append(Log(f"rota {r.stream} expirou por falta de anuncios (pronta para re-sincronizar seq)"))


class Bootstrapper:
    """Servidor de arranque, SEM ESTADO por no: so le uma tabela estatica (nome -> vizinhos)
    e responde a cada REGISTER."""

    def __init__(self, tabela):
        self.tabela = tabela

    def start(self, now):
        return [Log(f"bootstrapper pronto; conhece {len(self.tabela)} no(s)")]

    def on_timer(self, now, key):
        return []

    def on_packet(self, now, src, data):
        acts = []
        try:
            tipo, mid, payload = wire.decode(data)
            if tipo != wire.T_REGISTER:
                acts.append(Log(f"ignorei {wire.nome_do_tipo(tipo)} de {src[0]}"))
                return acts
            nome = wire.dec_name_msg(payload)
        except wire.WireError as e:
            acts.append(Log(f"datagrama invalido de {src[0]}: {e}"))
            return acts
        lista = self.tabela.get(nome)
        if lista is None:
            acts.append(Log(f"REGISTER de no desconhecido {nome!r} ({src[0]}): lista vazia; "
                            f"nomes conhecidos: {', '.join(sorted(self.tabela))}"))
            lista = []
        else:
            acts.append(Log(f"REGISTER de {nome} ({src[0]}): {len(lista)} vizinho(s)"))
        acts.append(Send(tuple(src), wire.enc_neighbours(mid, lista)))
        return acts
