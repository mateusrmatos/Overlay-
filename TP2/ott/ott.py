#!/usr/bin/env python3
"""ott - overlay OTT (TP2 ESR). Marco 1: arranque com bootstrapper + HELLO.

  Bootstrapper (no B da rede de gestao):
      python3 ott.py bootstrap --topology cenarios/cenario1.json [--port 9000] [--trace]

  Um no (S1, N1, C1, ... ):
      python3 ott.py node --name N1 --boot 172.16.0.254:9000 [--port 5000] [--trace]

  Simulacao em tempo virtual (sem rede, sem CORE):
      python3 ott.py sim --topology cenarios/cenario1.json [--spread 0.5] [--loss-gestao 0.3]
      python3 ott.py sim --topology cenarios/cenario1.json --kill N1 --kill-at 10 --restart-at 25 --ate 40
"""
import argparse
import asyncio
import ipaddress
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ottlib import core, runtime, wire      # noqa: E402
from ottlib.sim import Sim                  # noqa: E402
from ottlib.topology import Topology        # noqa: E402


def parse_addr(s):
    """'IP:porta' (porta opcional, 9000 por omissao). Valida, para apanhar erros como
    '172.16.0.254.9000:9000' (ponto em vez de dois pontos)."""
    ip, _, port = s.partition(":")
    try:
        ipaddress.IPv4Address(ip)
        p = int(port) if port else 9000
        if not 1 <= p <= 65535:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError(f"endereco invalido {s!r}: use IP:porta, ex. 172.16.0.254:9000")
    return ip, p


def node_name(s):
    """Nome de no valido para o protocolo (1 a 31 caracteres ASCII)."""
    try:
        wire.enc_name_msg(wire.T_REGISTER, 0, s)
    except wire.WireError:
        raise argparse.ArgumentTypeError(f"nome invalido {s!r}: use 1 a 31 caracteres ASCII, ex. N1")
    return s


def cmd_bootstrap(a):
    topo = Topology.load(a.topology)
    logic = core.Bootstrapper(topo.tabela_vizinhos())
    port = a.port or topo.boot_addr[1]
    asyncio.run(runtime.serve(logic, "BOOT", a.bind, port, a.trace))


def cmd_node(a):
    logic = core.Node(a.name, a.boot)
    asyncio.run(runtime.serve(logic, a.name, a.bind, a.port, a.trace))


def cmd_sim(a):
    topo = Topology.load(a.topology)
    s = Sim(topo, seed=a.seed, mgmt_loss=a.loss_gestao, verbose=True)
    s.start_all(spread=a.spread)
    if a.kill:
        s.kill(a.kill, a.kill_at)
        if a.restart_at is not None:
            s.restart(a.kill, a.restart_at)
    s.run(a.ate)
    print("\n--- trace de pacotes ---")
    print(s.formata_trace())


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bootstrap")
    b.add_argument("--topology", required=True)
    b.add_argument("--port", type=int, default=0)
    b.add_argument("--bind", default="0.0.0.0")
    b.add_argument("--trace", action="store_true")
    b.set_defaults(fn=cmd_bootstrap)

    n = sub.add_parser("node")
    n.add_argument("--name", required=True, type=node_name)
    n.add_argument("--boot", required=True, type=parse_addr, help="IP:porta do bootstrapper")
    n.add_argument("--port", type=int, default=5000)
    n.add_argument("--bind", default="0.0.0.0", help="so para testes locais (loopback)")
    n.add_argument("--trace", action="store_true")
    n.set_defaults(fn=cmd_node)

    s = sub.add_parser("sim")
    s.add_argument("--topology", required=True)
    s.add_argument("--spread", type=float, default=0.5, help="intervalo entre arranques (s)")
    s.add_argument("--loss-gestao", type=float, default=0.0)
    s.add_argument("--seed", type=int, default=1)
    s.add_argument("--ate", type=float, default=15.0)
    s.add_argument("--kill", metavar="NO", help="mata este no durante a simulacao")
    s.add_argument("--kill-at", type=float, default=10.0, help="instante da morte (s)")
    s.add_argument("--restart-at", type=float, default=None, help="instante em que o no ressuscita (s)")
    s.set_defaults(fn=cmd_sim)

    a = p.parse_args()
    try:
        a.fn(a)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
