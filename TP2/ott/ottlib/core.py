"""Nucleo do protocolo (sans-I/O) - Marco 1, 2 & Encaminhamento de Dados.

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

# Intervalo de emissao de pacotes DATA de teste quando uma stream local esta ATIVA
DATA_INTERVAL = 1.0

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
    Mantem a rota principal (origem, custo), caminhos alternativos e contadores de dados."""
    __slots__ = (
        "stream", "server", "origem", "custo", "seq", "last_updated",
        "destinos", "alternativas", "local", "consumo_local",
        "frames_enviadas", "frames_recebidas", "ultima_frame",
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
        self.frames_enviadas = 0
        self.frames_recebidas = 0
        self.ultima_frame = None

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
                 data_interval=DATA_INTERVAL, streams=(), watch=(), player_addr=None):
        self.name = name
        self.boot_addr = tuple(boot_addr)
        self.hello_interval, self.dead_after = hello_interval, dead_after
        self.advert_interval = advert_interval
        self.route_stale_after = route_stale_after
        self.data_interval = data_interval
        self.player_addr = tuple(player_addr) if
