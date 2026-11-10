"""Nucleo do protocolo (sans-I/O) - Marco 1.

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
    __slots__ = ("name", "addr", "alive", "last_heard")

    def __init__(self, name, addr):
        self.name, self.addr = name, addr
        self.alive, self.last_heard = False, None

    def __repr__(self):
        return f"Vizinho({self.name}, {self.addr}, {'vivo' if self.alive else 'sem contacto'})"


class Node:
    def __init__(self, name, boot_addr):
        self.name = name
        self.boot_addr = tuple(boot_addr)
        self.state = INIT
        self.vizinhos = {}        # nome -> Vizinho
        self.pending = {}         # msg_id -> Pending
        self._next_id = 1

    # ------------------------------------------------------------ utilitarios
    def _new_id(self):
        for _ in range(0x10000):
            mid = self._next_id
            self._next_id = self._next_id % 0xFFFF + 1     # 1..65535 (0 fica livre)
            if mid not in self.pending:
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
                                f"(o vizinho pode ainda nao ter arrancado)"))
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
        for nome, ip, porta in entries:
            self.vizinhos[nome] = Vizinho(nome, (ip, porta))
            hid = self._new_id()
            data = wire.enc_name_msg(wire.T_HELLO, hid, self.name)
            self._send_reliable(acts, (ip, porta), hid, data,
                                HELLO_RTO0, HELLO_RTO_MAX, HELLO_TRIES, "HELLO", peer=nome)

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
            v.alive = True
            acts.append(Log(f"HELLO de {nome} ({src[0]}): vizinho vivo"))
        v.last_heard = now

    def _on_ack(self, acts, now, src, mid, payload):
        p = self._settle(acts, mid)
        if p is None:
            return                              # ACK repetido
        if p.kind == "HELLO":
            v = self.vizinhos[p.peer]
            if not v.alive:
                v.alive = True
                acts.append(Log(f"ACK de {p.peer} ({src[0]}): vizinho vivo ({p.tries} tentativa(s))"))
            v.last_heard = now


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
            acts.append(Log(f"REGISTER de no desconhecido {nome!r} ({src[0]}): lista vazia"))
            lista = []
        else:
            acts.append(Log(f"REGISTER de {nome} ({src[0]}): {len(lista)} vizinho(s)"))
        acts.append(Send(tuple(src), wire.enc_neighbours(mid, lista)))
        return acts
