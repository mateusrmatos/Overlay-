"""Testes do extra "dados como keepalive" (comparacao com a baseline na simulacao).

    cd ott && python3 -m unittest discover -s tests -v
"""
import collections
import os
import sys
import unittest

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
sys.path.insert(0, RAIZ)

from ottlib import wire                          # noqa: E402
from ottlib.sim import Sim, BOOT                 # noqa: E402
from ottlib.topology import Topology             # noqa: E402

CLIENTES3 = ("C1", "C2", "C3", "C4", "C5")


def cenario(n, serve, watch, loss=0.0):
    t = Topology.load(os.path.join(RAIZ, "cenarios", f"cenario{n}.json"))
    for l in t.links:
        l["loss"] = loss
    t.nodes[serve]["streams"] = ["video1"]
    for c in watch:
        t.nodes[c]["watch"] = ["video1"]
    return t


def corre(t, extra, ate, kill=None, kill_at=40.0, seed=1):
    s = Sim(t, seed=seed, node_opts={"data_interval": 0.05, "keepalive_dados": extra})
    s.start_all(spread=0.1)
    if kill:
        s.kill(kill, kill_at)
    s.run(ate)
    return s


def controlo_por_s(s, ini, fim):
    n = sum(1 for p in s.trace if ini <= p[0] < fim and p[5] == "enviado" and p[3] != wire.T_DATA)
    return n / (fim - ini)


def buraco_max(s, cliente, kill_at):
    ts = [t for t, d, txt in s.logs if d == cliente and txt.startswith("DATA [")]
    if not any(t > kill_at + 0.3 for t in ts):
        return float("inf")
    w = [t for t in ts if t >= kill_at - 0.5]
    return max(b - a for a, b in zip(w, w[1:]))


class TestKeepalive(unittest.TestCase):
    def test_menos_trafego_de_controlo_que_a_baseline(self):
        t = lambda: cenario(3, "S1", CLIENTES3)
        base = controlo_por_s(corre(t(), False, 60), 20, 60)
        extra = controlo_por_s(corre(t(), True, 60), 20, 60)
        self.assertLess(extra, 0.5 * base, f"baseline={base:.1f}/s extra={extra:.1f}/s")

    def test_failover_bem_mais_rapido(self):
        t = lambda: cenario(3, "S1", CLIENTES3)
        base = buraco_max(corre(t(), False, 70, kill="O3"), "C4", 40.0)
        extra = buraco_max(corre(t(), True, 70, kill="O3"), "C4", 40.0)
        self.assertGreater(base, 3.0)          # baseline: ~3 s de silencio + granularidade
        self.assertLess(extra, 1.5)            # extra: suspeita + confirmacao
        self.assertLess(extra, base / 2)

    def test_no_isolado_continua_sem_recuperacao(self):
        """O O4 so tem o O3 como vizinho: morte catastrofica, nenhuma versao o salva
        (e o caso do extra do bootstrapper)."""
        s = corre(cenario(3, "S1", CLIENTES3), True, 70, kill="O3")
        self.assertEqual(buraco_max(s, "C1", 40.0), float("inf"))

    def test_sem_falsas_mortes_numa_rede_sem_perdas(self):
        for extra in (False, True):
            s = corre(cenario(3, "S1", CLIENTES3), extra, 120)
            falsas = [x for x in s.logs if "CAIU" in x[2]]
            self.assertEqual(falsas, [], f"extra={extra}")

    def test_aresta_sem_dados_mantem_o_hello(self):
        """So o C1 vê o video: a aresta N1-C2 nao tem dados e tem de continuar a ser sondada."""
        s = corre(cenario(1, "S1", ("C1",)), True, 60)
        hellos = [p for p in s.pacotes(de="N1", para="C2", tipo=wire.T_HELLO) if p[0] > 10]
        self.assertGreaterEqual(len(hellos), 45)            # ~1 por segundo em 50 s
        ativa = [p for p in s.pacotes(de="N1", para="C1", tipo=wire.T_HELLO) if p[0] > 10]
        self.assertLessEqual(len(ativa), 8)                  # aresta com dados: esparso (~1 por 10 s)

    def test_destino_morto_expira_pelo_soft_state(self):
        """O pai nao recebe nada do filho (so lhe envia video): quem apaga o filho morto e o
        prazo do ACTIVATE (3 periodos), nao o silencio."""
        s = corre(cenario(1, "S1", ("C1", "C2")), True, 60, kill="C1", kill_at=20.0)
        self.assertNotIn("C1", s.logic["N1"].rotas["video1"].destinos)
        self.assertIn("C2", s.logic["N1"].rotas["video1"].destinos)
        n = len([x for x in s.logs if x[1] == "N1" and "expirou" in x[2] and "destino C1" in x[2]
                 or x[1] == "N1" and "C1 CAIU" in x[2]])
        self.assertGreaterEqual(n, 1)

    def test_o_extra_nao_contacta_o_bootstrapper(self):
        s = corre(cenario(3, "S1", CLIENTES3), True, 70, kill="O3")
        depois = [p for p in s.trace if BOOT in (p[1], p[2]) and p[0] > 10]
        self.assertEqual(depois, [])


if __name__ == "__main__":
    unittest.main()
