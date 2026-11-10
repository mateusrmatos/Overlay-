"""Executor de rede real (asyncio + UDP) - Marco 1.

Faz para o mundo real o que o sim.py faz para o mundo simulado: pega nas ACOES devolvidas
pelo nucleo (core.py) e executa-as com um socket UDP e com loop.call_later.
Nao contem logica de protocolo.

Um unico socket UDP por no, ligado a 0.0.0.0:porta. Todas as mensagens saem por ele, por isso
a porta de origem e a porta de escuta e as respostas voltam ao mesmo sitio.
"""
import asyncio
import time

from . import core, wire


class Runtime(asyncio.DatagramProtocol):
    def __init__(self, logic, name, trace=False):
        self.logic, self.name, self.trace = logic, name, trace
        self.transport = None
        self.timers = {}
        self.t0 = time.monotonic()

    # --------------------------------------------------------------- relogio e log
    def now(self):
        return time.monotonic() - self.t0

    def say(self, text):
        print(f"{self.now():8.3f}s [{self.name}] {text}", flush=True)

    # --------------------------------------------------------------- asyncio
    def connection_made(self, transport):
        self.transport = transport
        self.run(self.logic.start(self.now()))

    def datagram_received(self, data, addr):
        if self.trace:
            try:
                t, mid, _ = wire.decode(data)
                self.say(f"<- {wire.nome_do_tipo(t)} id={mid} de {addr[0]}:{addr[1]}")
            except wire.WireError:
                self.say(f"<- lixo ({len(data)} B) de {addr[0]}:{addr[1]}")
        self.run(self.logic.on_packet(self.now(), addr, data))

    def error_received(self, exc):
        # ex.: ICMP "destino inalcancavel" - o UDP nao garante nada; a retransmissao trata disto
        if self.trace:
            self.say(f"erro de socket: {exc}")

    def _fire(self, key):
        self.timers.pop(key, None)
        self.run(self.logic.on_timer(self.now(), key))

    # --------------------------------------------------------------- acoes
    def run(self, acts):
        loop = asyncio.get_running_loop()
        for a in acts:
            if isinstance(a, core.Send):
                if self.trace:
                    try:
                        t, mid, _ = wire.decode(a.data)
                        self.say(f"-> {wire.nome_do_tipo(t)} id={mid} para {a.dst[0]}:{a.dst[1]}")
                    except wire.WireError:
                        pass
                self.transport.sendto(a.data, a.dst)
            elif isinstance(a, core.SetTimer):
                old = self.timers.pop(a.key, None)
                if old:
                    old.cancel()
                self.timers[a.key] = loop.call_later(a.delay, self._fire, a.key)
            elif isinstance(a, core.CancelTimer):
                h = self.timers.pop(a.key, None)
                if h:
                    h.cancel()
            elif isinstance(a, core.Log):
                self.say(a.text)


async def serve(logic, name, bind_ip, port, trace=False):
    loop = asyncio.get_running_loop()
    rt = Runtime(logic, name, trace)
    await loop.create_datagram_endpoint(lambda: rt, local_addr=(bind_ip, port))
    await asyncio.Event().wait()          # corre ate Ctrl+C
