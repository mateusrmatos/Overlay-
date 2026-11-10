#!/usr/bin/env python3
"""ott - overlay OTT (TP2 ESR). Marco 1: arranque com bootstrapper + HELLO.

  Bootstrapper (no B da rede de gestao):
      python3 ott.py bootstrap --topology cenarios/cenario1.json [--port 9000] [--trace]

  Um no (S1, N1, C1, ... ):
      python3 ott.py node --name N1 --boot 172.16.0.254:9000 [--port 5000] [--trace]

  Simulacao em tempo virtual (sem rede, sem CORE):
      python3 ott.py sim --topology cenarios/cenario1.json [--spread 0.5] [--loss-gestao 0.3]
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ottlib import core, runtime            # noqa: E402
from ottlib.sim import Sim                  # noqa: E402
from ottlib.topology import Topology        # noqa: E402


def parse_addr(s):
    ip, _, port = s.partition(":")
    return ip, int(port or 9000)


def cmd_bootstrap(a):
    topo = Topology.load(a.topology)
    logic = core.Bootstrapper(topo.tabela_vizinhos())
    port = a.port or topo.boot_addr[1]
    asyncio.run(runtime.serve(logic, "BOOT", a.bind, port, a.trace))


def cmd_node(a):
    logic = core.Node(a.name, parse_addr(a.boot))
    asyncio.run(runtime.serve(logic, a.name, a.bind, a.port, a.trace))


def cmd_sim(a):
    topo = Topology.load(a.topology)
    s = Sim(topo, seed=a.seed, mgmt_loss=a.loss_gestao, verbose=True)
    s.start_all(spread=a.spread)
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
    n.add_argument("--name", required=True)
    n.add_argument("--boot", required=True, help="IP:porta do bootstrapper")
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
    s.set_defaults(fn=cmd_sim)

    a = p.parse_args()
    try:
        a.fn(a)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
