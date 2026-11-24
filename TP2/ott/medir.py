#!/usr/bin/env python3
"""medir.py - metricas do protocolo na simulacao (tempo virtual).

Serve para comparar versoes do protocolo (baseline vs extras) com numeros reproduziveis.

    python3 medir.py --topology cenarios/cenario3.json --serve O1:video1 \\
        --watch O4:video1 --watch O8:video1 --watch O9:video1 --kill O3 --kill-at 40

Mede, para um cenario:
  - convergencia:   instante em que cada cliente recebe o 1.o pacote DATA;
  - regime:         pacotes por segundo por tipo (controlo vs dados), numa janela antes da falha;
  - failover:       por cliente, quanto tempo ficou sem dados depois de morrer um no
                    (ou "SEM RECUPERACAO" se nunca mais recebeu);
  - bootstrapper:   quantos pacotes trocou com o bootstrapper depois do arranque (tem de ser 0).
"""
import argparse
import collections
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ottlib import wire                      # noqa: E402
from ottlib.sim import Sim, BOOT             # noqa: E402
from ottlib.topology import Topology         # noqa: E402

ARRANQUE_ATE = 10.0      # depois deste instante nao deve haver contactos com o bootstrapper


def par(s):
    no, sep, stream = s.partition(":")
    if not sep:
        raise argparse.ArgumentTypeError(f"use NO:STREAM, ex. S1:video1 (recebi {s!r})")
    return no, stream


def instantes_dados(sim, cliente):
    """Instantes em que o cliente recebeu um pacote DATA (vem dos logs do no)."""
    return [t for t, dono, txt in sim.logs if dono == cliente and txt.startswith("DATA [")]


def medir(topology, serve, watch, kill=None, kill_at=40.0, ate=None, janela=20.0, seed=1,
          loss_gestao=0.0, node_opts=None):
    topo = Topology.load(topology) if isinstance(topology, str) else topology
    for no, st in serve:
        topo.nodes[no].setdefault("streams", []).append(st)
    for no, st in watch:
        topo.nodes[no].setdefault("watch", []).append(st)
    ate = ate if ate is not None else (kill_at + 30.0 if kill else 60.0)
    s = Sim(topo, seed=seed, mgmt_loss=loss_gestao, node_opts=node_opts)
    s.start_all(spread=0.1)
    if kill:
        s.kill(kill, kill_at)
    s.run(ate)

    fim_regime = kill_at if kill else ate
    ini_regime = max(ARRANQUE_ATE, fim_regime - janela)
    cont = collections.Counter(wire.nome_do_tipo(p[3]) for p in s.trace
                               if ini_regime <= p[0] < fim_regime and p[5] == "enviado")
    dur = fim_regime - ini_regime
    por_s = {k: v / dur for k, v in sorted(cont.items())}
    dados = por_s.get("DATA", 0.0)
    res = {
        "topologia": os.path.basename(topology) if isinstance(topology, str) else "?",
        "nos": len(topo.nodes), "ligacoes": len(topo.links),
        "regime_pacotes_por_s": round(sum(por_s.values()), 1),
        "regime_controlo_por_s": round(sum(por_s.values()) - dados, 1),
        "regime_por_tipo": {k: round(v, 1) for k, v in por_s.items()},
        "bootstrapper_apos_arranque": len([p for p in s.trace
                                           if BOOT in (p[1], p[2]) and p[0] > ARRANQUE_ATE]),
        "clientes": {},
    }
    for no, _ in watch:
        ts = instantes_dados(s, no)
        c = {"primeiro_dado": round(ts[0], 2) if ts else None}
        if kill:
            # Pacotes que ja iam a caminho chegam ate ~0.3 s depois da morte: so contam como
            # "recuperou" os que chegam depois disso. O buraco e o MAIOR intervalo entre dois
            # dados consecutivos a partir da morte (nao o intervalo a volta do instante da morte).
            if no == kill:
                c["failover"] = "(no morto)"
            elif not any(t > kill_at + 0.3 for t in ts):
                c["failover"] = "SEM RECUPERACAO"
            else:
                janela_ts = [t for t in ts if t >= kill_at - 0.5]
                c["failover"] = round(max(b - a for a, b in zip(janela_ts, janela_ts[1:])), 2)
        res["clientes"][no] = c
    return res


def imprime(res, kill):
    print(f"\n== {res['topologia']}  (N={res['nos']}, E={res['ligacoes']})")
    print(f"regime: {res['regime_pacotes_por_s']} pacotes/s no total, "
          f"{res['regime_controlo_por_s']} de controlo  ->  {res['regime_por_tipo']}")
    print(f"bootstrapper depois de t={ARRANQUE_ATE:g}s: {res['bootstrapper_apos_arranque']} pacotes")
    for no, c in res["clientes"].items():
        linha = f"  {no:>4}: 1.o dado em t={c['primeiro_dado']}s"
        if kill:
            f = c["failover"]
            linha += f"   sem dados durante: {f if isinstance(f, str) else str(f) + ' s'}"
        print(linha)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topology", required=True)
    p.add_argument("--serve", action="append", type=par, required=True)
    p.add_argument("--watch", action="append", type=par, required=True)
    p.add_argument("--kill", help="no a matar")
    p.add_argument("--kill-at", type=float, default=40.0)
    p.add_argument("--ate", type=float)
    p.add_argument("--janela", type=float, default=20.0, help="janela de regime (s) antes da falha")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--loss-gestao", type=float, default=0.0)
    p.add_argument("--json", action="store_true")
    p.add_argument("--data-interval", type=float, default=1.0,
                   help="intervalo entre pacotes DATA (s); 0.05 = video a 20 fps")
    p.add_argument("--extra", action="append", default=[], choices=["keepalive"],
                   help="liga um extra: keepalive = dados como keepalive")
    a = p.parse_args()
    opts = {"data_interval": a.data_interval}
    if "keepalive" in a.extra:
        opts["keepalive_dados"] = True
    res = medir(a.topology, a.serve, a.watch, a.kill, a.kill_at, a.ate, a.janela, a.seed,
                a.loss_gestao, opts)
    if a.json:
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        imprime(res, a.kill)


if __name__ == "__main__":
    main()
