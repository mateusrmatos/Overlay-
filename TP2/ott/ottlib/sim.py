"""Simulador de rede em TEMPO VIRTUAL - Marco 1.

Faz o papel do CORE + sockets, mas sem relogio real: ha uma fila de eventos ordenada por
instante e o "tempo" salta de evento em evento. Um minuto de protocolo simula-se em
milissegundos, e com uma semente fixa o resultado e sempre o mesmo (testes reprodutiveis).

Regras da rede simulada:
 - um no so consegue falar com vizinhos de ligacao direta (como no CORE: ligacoes diretas)
   e com o bootstrapper (rede de gestao);
 - cada ligacao tem atraso e probabilidade de perda proprios;
 - um no "morto" nao recebe nem tem temporizadores.
"""
import heapq
import random

from . import core, wire
from .topology import Topology

BOOT = "BOOT"


class Sim:
    def __init__(self, topo, seed=1, mgmt_delay=0.001, mgmt_loss=0.0, verbose=False):
        self.topo = topo if isinstance(topo, Topology) else Topology(topo)
        self.rng = random.Random(seed)
        self.now = 0.0
        self._q, self._seq = [], 0
        self.logic = {}                # nome -> Node (ou None se morto)
        self.timers = {}               # (dono, chave) -> ficha
        self._tok = 0
        self.mgmt = {"delay": mgmt_delay, "loss": mgmt_loss}
        self.trace = []                # (t, origem, destino, tipo, msg_id, estado)
        self.logs = []                 # (t, dono, texto)
        self.verbose = verbose
        self._boot = core.Bootstrapper(self.topo.tabela_vizinhos())
        self.logic[BOOT] = None        # so existe depois de start_boot()

    # ---------------------------------------------------------------- agendamento
    def _push(self, t, kind, *args):
        self._seq += 1
        heapq.heappush(self._q, (t, self._seq, kind, args))

    def start_boot(self, at=0.0):
        self._push(at, "start", BOOT)

    def start(self, nome, at=0.0):
        self._push(at, "start", nome)

    def kill(self, nome, at):
        self._push(at, "kill", nome)

    def restart(self, nome, at):
        self._push(at, "start", nome)

    def start_all(self, at=0.0, spread=0.0):
        """Arranca o bootstrapper e todos os nos (opcionalmente espalhados no tempo)."""
        self.start_boot(at)
        for i, n in enumerate(self.topo.nodes):
            self.start(n, at + 0.01 + i * spread)

    # ---------------------------------------------------------------- execucao
    def run(self, until):
        while self._q and self._q[0][0] <= until:
            t, _, kind, args = heapq.heappop(self._q)
            self.now = t
            getattr(self, "_ev_" + kind)(*args)
        self.now = max(self.now, until)

    def _ev_start(self, nome):
        if nome == BOOT:
            logic = self.logic[BOOT] = self._boot
        else:
            self._cancel_all(nome)
            info = self.topo.nodes[nome]
            logic = self.logic[nome] = core.Node(
                nome, self.topo.boot_addr,
                streams=info.get("streams", ()),
                watch=info.get("watch", ()),
            )
        self._exec(nome, logic.start(self.now))

    def _ev_kill(self, nome):
        self.logic[nome] = None
        self._cancel_all(nome)
        self._log(nome, "*** MORREU ***")

    def _ev_deliver(self, dono, src, data):
        logic = self.logic.get(dono)
        if logic is None:
            self._trace_update(dono, data, "destino morto")
            return
        self._exec(dono, logic.on_packet(self.now, src, data))

    def _ev_timer(self, dono, key, ficha):
        if self.timers.get((dono, key)) != ficha:
            return                                  # cancelado ou rearmado
        del self.timers[(dono, key)]
        logic = self.logic.get(dono)
        if logic is not None:
            self._exec(dono, logic.on_timer(self.now, key))

    # ---------------------------------------------------------------- acoes
    def _exec(self, dono, acts):
        for a in acts:
            if isinstance(a, core.Send):
                self._send(dono, a.dst, a.data)
            elif isinstance(a, core.SetTimer):
                self._tok += 1
                self.timers[(dono, a.key)] = self._tok
                self._push(self.now + a.delay, "timer", dono, a.key, self._tok)
            elif isinstance(a, core.CancelTimer):
                self.timers.pop((dono, a.key), None)
            elif isinstance(a, core.Log):
                self._log(dono, a.text)

    def _cancel_all(self, dono):
        for k in [k for k in self.timers if k[0] == dono]:
            del self.timers[k]

    def _props(self, de, para):
        if BOOT in (de, para):
            return self.mgmt
        l = self.topo.link_between(de, para)
        if l is None:
            return None
        return {"delay": l.get("delay_ms", 10) / 1000.0, "loss": l.get("loss", 0.0)}

    def _src_addr(self, de, para):
        if de == BOOT:
            return self.topo.boot_addr
        if para == BOOT:
            return (self.topo.nodes[de]["mgmt"], self.topo.port)
        return (self.topo.addr_towards(de, para), self.topo.port)

    def _send(self, de, dst, data):
        para = self.topo.owner.get(dst[0])
        tipo, mid = data[1], int.from_bytes(data[2:4], "big")
        if para is None or para == de:
            self._trace(de, str(dst[0]), tipo, mid, "sem rota")
            return
        p = self._props(de, para)
        if p is None:
            self._trace(de, para, tipo, mid, "sem ligacao")
            return
        if self.rng.random() < p["loss"]:
            self._trace(de, para, tipo, mid, "perdido")
            return
        self._trace(de, para, tipo, mid, "enviado")
        self._push(self.now + p["delay"], "deliver", para, self._src_addr(de, para), data)

    # ---------------------------------------------------------------- registo
    def _trace(self, de, para, tipo, mid, estado):
        self.trace.append((self.now, de, para, tipo, mid, estado))

    def _trace_update(self, dono, data, estado):
        self.trace.append((self.now, "?", dono, data[1], int.from_bytes(data[2:4], "big"), estado))

    def _log(self, dono, texto):
        self.logs.append((self.now, dono, texto))
        if self.verbose:
            print(f"{self.now:8.3f}s [{dono}] {texto}")

    # ---------------------------------------------------------------- consultas (para testes)
    def pacotes(self, de=None, para=None, tipo=None, estado=None):
        r = []
        for t, d, p, ty, mid, st in self.trace:
            if de is not None and d != de:
                continue
            if para is not None and p != para:
                continue
            if tipo is not None and ty != tipo:
                continue
            if estado is not None and st != estado:
                continue
            r.append((t, d, p, ty, mid, st))
        return r

    def formata_trace(self, limite=None):
        linhas = []
        for t, d, p, ty, mid, st in self.trace[:limite]:
            marca = "" if st == "enviado" else f"   <-- {st}"
            linhas.append(f"{t:8.3f}s  {d:>5} -> {p:<5} {wire.nome_do_tipo(ty):<10} id={mid}{marca}")
        return "\n".join(linhas)
