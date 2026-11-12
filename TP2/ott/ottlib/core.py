"""Nucleo do protocolo (sans-I/O) - Marco 1 & 2.

Ideia central: este modulo NAO abre sockets, NAO dorme, NAO le o relogio.
Recebe EVENTOS (arrancar, chegou um pacote, expirou um temporizador) e devolve ACOES
(enviar um datagrama, armar/cancelar um temporizador, escrever uma linha de log).

    eventos:  start(now) | on_packet(now, src, data) | on_timer(now, key)
    acoes:    Send(dst, data) | SetTimer(key, delay) | CancelTimer(key) | Log(text)

Quem executa as acoes decide se e rede real (runtime.py, asyncio) ou rede simulada
(sim.py, tempo virtual). A logica e exatamente a mesma nos dois casos; por isso os
testes na simulacao testam o codigo que depois corre no CORE.

Enderecos (dst, src) sao tuplos (ip_str, porta).
Compativel com Python 3.10.
"""
from collections import namedtuple

from . import wire

# ------------------------------------------------------------------ acoes
Send = namedtuple("Send", "dst data")
SetTimer = namedtuple("SetTimer", "key delay")
CancelTimer = namedtuple("CancelTimer", "key")
Log = namedtuple("Log", "text")

# ------------------------------------------------------------------ parametros
# REGISTER: o bootstrapper pode estar mais lento ou a rede perder pacotes.
# Recuo exponencial 0.5, 1, 2, 4, 4, 4 ... (limitado a 4 s) ate MAX tentativas.
REGISTER_RTO0, REGISTER_RTO_MAX, REGISTER_TRIES = 0.5, 4.0, 10
# HELLO: vizinhos estao a 1 salto, por isso o RTO inicial e curto.
# 0.2, 0.4, 0.8, 1.6, 3.2 s  (cerca de 6 s no total) ate MAX tentativas.
HELLO_RTO0, HELLO_RTO_MAX, HELLO_TRIES = 0.2, 4.0, 5

# HELLO periodico (monitorizacao dos vizinhos). A cada HELLO_INTERVAL o no sonda cada vizinho
# com um HELLO SEM retransmissao; se nao ouvir nada dele durante HELLO_DEAD_AFTER, declara-o
# caido. A deteccao demora entre DEAD_AFTER e DEAD_AFTER + INTERVAL.
HELLO_INTERVAL, HELLO_DEAD_AFTER = 1.0, 3.0
RTT_ALPHA = 0.125     # peso da amostra nova no RTT suavizado (o mesmo valor do TCP)

# estados do no
INIT, REGISTERING, JOINED, ORPHAN = "INIT", "REGISTERING", "JOINED", "ORPHAN"


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
        self.rtt = self.srtt = None     # ultimo RTT medido e RTT suavizado (segundos)

    def __repr__(self):
        return f"Vizinho({self.name}, {self.addr}, {'vivo' if self.alive else 'sem contacto'})"


# === [PONTO A: CLASSE ROTA ADICIONADA AQUI] ===
class Rota:
    """Entrada na tabela de encaminhamento para um fluxo (stream).
    Mantem a rota principal (origem, custo) e caminhos alternativos conhecidos."""
    __slots__ = ("stream", "server", "origem", "custo", "seq", "destinos", "alternativas")

    def __init__(self, stream, server, origem, custo, seq):
        self.stream = stream              # ex: "video1"
        self.server = server              # ex: "S1" (servidor que gera a stream)
        self.origem = origem              # nome do vizinho upstream (ex: "N1"), ou None se invalida
        self.custo = custo                # metrica acumulada ate ao servidor
        self.seq = seq                    # ultimo num de sequencia visto do servidor
        self.destinos = set()             # nomes dos vizinhos downstream que pediram o fluxo
        self.alternativas = {}            # {nome_vizinho: (custo, seq)} para failover rapido

    @property
    def ativa(self):
        """A rota esta ativa se temos por onde receber e alguem a quem entregar."""
        return self.origem is not None and len(self.destinos) > 0

    def __repr__(self):
        est = "ATIVA" if self.ativa else ("INATIVA" if self.origem else "SEM_ORIGEM")
        dsts = ",".join(sorted(self.destinos)) or "-"
        c_str = f"{self.custo:g}" if self.custo != float("inf") else "inf"
        return (f"Rota(fluxo={self.stream}, srv={self.server}, origem={self.origem}, "
                f"custo={c_str}, destinos=[{dsts}], estado={est})")


class Node:
    def __init__(self, name, boot_addr, hello_interval=HELLO_INTERVAL, dead_after=HELLO_DEAD_AFTER):
        self.name = name
        self.boot_addr = tuple(boot_addr)
        self.hello_interval, self.dead_after = hello_interval, dead_after
        self.state = INIT
        self.vizinhos = {}        # nome -> Vizinho
        self.rotas = {}           # stream -> Rota (Tabela de Rotas)  <-- [PONTO B AQUI]
        self.pending = {}         # msg_id -> Pending (mensagens fiaveis)
        self.probes = {}          # msg_id -> (nome, t_envio): HELLO periodico
        self._next_id = 1

    # ------------------------------------------------------------ utilitarios
    def _new_id(self):
        for _ in range(0x10000):
            mid = self._next_id
            self._next_id = self._next_id % 0xFFFF + 1     # 1..65535 (0 fica livre)
            if mid not in self.pending and mid not in self.probes:
                return mid
        raise RuntimeError("sem msg_id livres")

    def _send(self, acts, dst, data):
        """Unico ponto por onde sai um datagrama. Aqui vive a regra do enunciado:
        depois de o no ter entrado na rede, o bootstrapper NAO volta a ser contactado."""
        if tuple(dst) == self.boot_addr and self.state in (JOINED, ORPHAN):
            raise AssertionError(f"{self.name}: tentativa de contactar o bootstrapper depois do arranque")
        acts.append(Send(tuple(dst), data))

    def _send_reliable(self, acts, dst, msg_id, data, rto0, rto_max, max_tries, kind, peer=None):
        self.pending[msg_id] = Pending(tuple(dst), data, rto0, rto_max, max_tries, kind, peer)
        self._send(acts, dst, data)
        acts.append(SetTimer(("rtx", msg_id), rto0))

    def _settle(self, acts, msg_id):
        """Mensagem confirmada: deixa de estar pendente e cancela o temporizador."""
        p = self.pending.pop(msg_id, None)
        if p is not None:
            acts.append(CancelTimer(("rtx", msg_id)))
        return p

    # ------------------------------------------------------------ eventos
    def start(self, now):
        """Arranque (ou rearranque): um unico REGISTER ao bootstrapper."""
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
        return acts

    # ------------------------------------------------------------ retransmissao
    def _on_rtx(self, acts, mid):
        p = self.pending.get(mid)
        if p is None:
            return                              # entretanto confirmada
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

    # ------------------------------------------------------------ handlers
    def _on_neighbours(self, acts, now, src, mid, payload):
        """Resposta do bootstrapper. O msg_id repetido do REGISTER serve de confirmacao,
        por isso nao ha ACK de volta (o bootstrapper nao guarda estado)."""
        if self._settle(acts, mid) is None or self.state != REGISTERING:
            return                              # duplicado ou fora de horas: idempotente
        entries = wire.dec_neighbours(payload)
        self.state = JOINED
        acts.append(Log(f"JOINED; vizinhos: " + (", ".join(f"{n}@{ip}" for n, ip, _ in entries) or "(nenhum)")))
        if not entries:
            acts.append(Log(f"AVISO: JOINED sem vizinhos. O bootstrapper conhece o nome {self.name!r}? "
                            f"Tem de ser igual ao do cenario, maiusculas incluidas."))
        for nome, ip, porta in entries:
            if nome in self.vizinhos:
                # ja o aprendemos por um HELLO dele: nao perder o estado, so fixar o endereco do bootstrapper
                self.vizinhos[nome].addr = (ip, porta)
            else:
                self.vizinhos[nome] = Vizinho(nome, (ip, porta))
            hid = self._new_id()
            data = wire.enc_name_msg(wire.T_HELLO, hid, self.name)
            self._send_reliable(acts, (ip, porta), hid, data,
                                HELLO_RTO0, HELLO_RTO_MAX, HELLO_TRIES, "HELLO", peer=nome)
        acts.append(SetTimer(("tick",), self.hello_interval))     # arranca a monitorizacao

    def _on_hello(self, acts, now, src, mid, payload):
        """Um vizinho diz quem e. Respondemos sempre com ACK (o ACK pode ter-se perdido
        e o vizinho repetir o HELLO) e aprendemos o vizinho so da primeira vez."""
        nome = wire.dec_name_msg(payload)
        acts.append(Send(tuple(src), wire.enc_ack(mid)))
        v = self.vizinhos.get(nome)
        if v is None:
            # vizinho que o bootstrapper nao nos indicou (ex.: no novo): aprendemo-lo aqui
            v = self.vizinhos[nome] = Vizinho(nome, tuple(src))
        if not v.alive:
            voltou = v.last_heard is not None
            v.alive = True
            acts.append(Log(f"HELLO de {nome} ({src[0]}): vizinho {'voltou' if voltou else 'vivo'}"))
        v.last_heard = now

    def _on_ack(self, acts, now, src, mid, payload):
        sonda = self.probes.pop(mid, None)
        if sonda is not None:                   # ACK a um HELLO periodico
            self._on_sonda_ack(acts, now, src, *sonda)
            return
        p = self._settle(acts, mid)
        if p is None:
            return                              # ACK repetido
        if p.kind == "HELLO":
            v = self.vizinhos[p.peer]
            if not v.alive:
                v.alive = True
                acts.append(Log(f"ACK de {p.peer} ({src[0]}): vizinho vivo ({p.tries} tentativa(s))"))
            v.last_heard = now

    # ------------------------------------------------------------ monitorizacao dos vizinhos
    def _on_tick(self, acts, now):
        """A cada HELLO_INTERVAL: (1) detetar vizinhos calados, (2) sondar todos os vizinhos."""
        acts.append(SetTimer(("tick",), self.hello_interval))
        self.probes = {m: pr for m, pr in self.probes.items() if now - pr[1] < self.dead_after}
        for v in self.vizinhos.values():
            if v.alive and v.last_heard is not None and now - v.last_heard > self.dead_after:
                self._vizinho_caiu(acts, now, v)
            # Sondar tambem os que nao estao vivos: e assim que se deteta o regresso de um vizinho
            # (e que um HELLO fiavel desistido de arranque ganha nova oportunidade).
            if any(p.kind == "HELLO" and p.peer == v.name for p in self.pending.values()):
                continue                        # o HELLO fiavel de arranque ainda esta em curso
            mid = self._new_id()
            self.probes[mid] = (v.name, now)
            self._send(acts, v.addr, wire.enc_name_msg(wire.T_HELLO, mid, self.name))

    def _on_sonda_ack(self, acts, now, src, nome, t_envio):
        """ACK a um HELLO periodico: prova de vida e amostra limpa de RTT (um unico envio, sem
        retransmissoes, por isso sem ambiguidade; o HELLO de arranque nao serve para isto)."""
        v = self.vizinhos.get(nome)
        if v is None:
            return
        v.rtt = now - t_envio
        v.srtt = v.rtt if v.srtt is None else (1 - RTT_ALPHA) * v.srtt + RTT_ALPHA * v.rtt
        if not v.alive:
            voltou = v.last_heard is not None
            v.alive = True
            acts.append(Log(f"ACK de {nome} ({src[0]}): vizinho {'voltou' if voltou else 'vivo'}"))
        v.last_heard = now

    def _vizinho_caiu(self, acts, now, v):
        """Um vizinho deixou de dar sinal. Invalida/recalcula rotas que dependiam dele."""
        acts.append(Log(f"vizinho {v.name} CAIU (sem sinal ha {now - v.last_heard:.1f}s)"))
        v.alive = False
        v.rtt = v.srtt = None
        self._remover_vizinho_das_rotas(acts, v.name)

    # === [PONTO C: METODOS DA TABELA DE ROTAS ADICIONADOS AQUI DENTRO DE NODE] ===
    def custo_ligacao(self, nome_vizinho):
        """Metrica da ligacao ate ao vizinho.
        Usa 1.0 (contagem de saltos) por omissao, ou podera usar v.srtt quando quiserem."""
        return 1.0

    def atualizar_rota(self, acts, stream, server, vizinho_origem, custo_anunciado, seq):
        """Avalia um anuncio de rota recebido de `vizinho_origem`.
        Devolve True se a rota principal melhorou/mudou (sinal para fazer flood a seguir!)
        ou False se foi ignorada ou guardada apenas como alternativa."""
        if vizinho_origem not in self.vizinhos:
            return False

        novo_custo = custo_anunciado + self.custo_ligacao(vizinho_origem)
        r = self.rotas.get(stream)

        # Caso 1: Fluxo totalmente novo para este no
        if r is None:
            r = self.rotas[stream] = Rota(stream, server, vizinho_origem, novo_custo, seq)
            acts.append(Log(f"rota criada: {r}"))
            return True

        # Ignorar anuncios antigos (seq menor)
        if seq < r.seq:
            return False

        # Caso 2: Nova ronda de anuncio do servidor (seq mais recente)
        if seq > r.seq:
            r.alternativas.clear()
            r.server = server
            r.seq = seq
            r.origem = vizinho_origem
            r.custo = novo_custo
            acts.append(Log(f"rota atualizada (seq={seq}): {r}"))
            return True

        # Caso 3: Mesmo seq (anuncio que chegou por outro caminho na mesma inundacao)
        if vizinho_origem == r.origem:
            if novo_custo < r.custo:
                r.custo = novo_custo
                return True
            return False

        # Se o novo caminho for estritamente melhor que a origem atual, promovemos!
        if r.origem is None or novo_custo < r.custo:
            if r.origem is not None:
                r.alternativas[r.origem] = (r.custo, r.seq)
            r.origem = vizinho_origem
            r.custo = novo_custo
            r.alternativas.pop(vizinho_origem, None)
            acts.append(Log(f"melhor rota encontrada para {stream} via {vizinho_origem} (custo {novo_custo:g})"))
            return True

        # Caso 4: Caminho mais longo ou igual -> guarda-se como rota alternativa de backup
        antigo = r.alternativas.get(vizinho_origem)
        if antigo is None or novo_custo < antigo[0]:
            r.alternativas[vizinho_origem] = (novo_custo, seq)
        return False

    def adicionar_destino_rota(self, acts, stream, vizinho_destino):
        """Um vizinho a jusante pediu para receber a stream (ACTIVATE)."""
        r = self.rotas.get(stream)
        if r is None:
            return False
        estava_ativa = r.ativa
        r.destinos.add(vizinho_destino)
        if not estava_ativa and r.ativa:
            acts.append(Log(f"rota {stream} passou a ATIVA -> destinos: {sorted(r.destinos)}"))
        return not estava_ativa and r.ativa

    def remover_destino_rota(self, acts, stream, vizinho_destino):
        """Um vizinho a jusante deixou de querer a stream (DEACTIVATE ou caiu)."""
        r = self.rotas.get(stream)
        if r is None or vizinho_destino not in r.destinos:
            return False
        estava_ativa = r.ativa
        r.destinos.discard(vizinho_destino)
        if estava_ativa and not r.ativa:
            acts.append(Log(f"rota {stream} passou a INATIVA (sem clientes)"))
        return estava_ativa and not r.ativa

    def _remover_vizinho_das_rotas(self, acts, nome_viz):
        """Limpa um vizinho caido das tabelas e ativa rotas alternativas se existirem."""
        for r in self.rotas.values():
            r.alternativas.pop(nome_viz, None)
            if nome_viz in r.destinos:
                self.remover_destino_rota(acts, r.stream, nome_viz)

            if r.origem == nome_viz:
                validas = {v: (c, s) for v, (c, s) in r.alternativas.items()
                           if v in self.vizinhos and self.vizinhos[v].alive}
                if validas:
                    melhor_v = min(validas, key=lambda v: validas[v][0])
                    r.origem, (r.custo, r.seq) = melhor_v, r.alternativas.pop(melhor_v)
                    acts.append(Log(f"failover da rota {r.stream}: nova origem {r.origem} (custo {r.custo:g})"))
                else:
                    r.origem, r.custo = None, float("inf")
                    acts.append(Log(f"rota {r.stream} perdeu a origem {nome_viz} e nao tem alternativas!"))
    # === [FIM DO PONTO C - A CLASSE NODE TERMINA AQUI] ===


class Bootstrapper:
    """Servidor de arranque, SEM ESTADO por no: so le uma tabela estatica (nome -> vizinhos)
    e responde a cada REGISTER. Nao envia nada por iniciativa propria, nao guarda quem
    se registou, nao participa no overlay."""

    def __init__(self, tabela):
        self.tabela = tabela        # {nome: [(nome_viz, ip, porta), ...]}

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
