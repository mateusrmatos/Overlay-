"""Cenario 3 do enunciado (Figura 1): underlay do CORE + overlay de 12 nos e 14 ligacoes.

A topologia vem do XML exportado do CORE (cenarios/cenario3_core.xml) atraves do importador;
estes testes fixam o que ela tem de garantir.
"""
import os
import sys
import unittest

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
sys.path.insert(0, AQUI)
sys.path.insert(0, RAIZ)

from ottlib import core                                   # noqa: E402
from ottlib.importar import importar, le_arestas          # noqa: E402
from ottlib.sim import Sim                                # noqa: E402
from ottlib.topology import Topology                      # noqa: E402
from test_keepalive import buraco_max                     # noqa: E402

CEN = os.path.join(RAIZ, "cenarios")
NOS = {"S1", "S2", "O2", "O3", "O5", "O6", "O7", "C1", "C2", "C3", "C4", "C5"}
CLIENTES = ("C1", "C2", "C3", "C4", "C5")
INF = float("inf")


def topo():
    d = importar(os.path.join(CEN, "cenario3_core.xml"), le_arestas(os.path.join(CEN, "fig1_overlay.txt")))
    return Topology(d)


def corre(serve, watch, ate=30, kill=None, kill_at=30.0, extra=True):
    t = topo()
    for no, streams in serve.items():
        t.nodes[no]["streams"] = list(streams)
    for no, streams in watch.items():
        t.nodes[no]["watch"] = list(streams)
    s = Sim(t, node_opts={"data_interval": 0.05, "keepalive_dados": extra})
    s.start_all(spread=0.1)
    if kill:
        s.kill(kill, kill_at)
    s.run(ate)
    return s


class TestImportacao(unittest.TestCase):
    def test_nos_ligacoes_e_bootstrapper(self):
        t = topo()
        self.assertEqual(set(t.nodes), NOS)
        self.assertEqual(len(t.links), 14)                       # 12 do enunciado + 2 do S2
        self.assertEqual(t.boot_addr, ("10.0.20.21", 9000))
        self.assertNotIn("B", t.nodes)

    def test_um_unico_ip_por_no(self):
        t = topo()
        ips = [v["mgmt"] for v in t.nodes.values()]
        self.assertEqual(len(set(ips)), len(ips))
        for l in t.links:
            self.assertEqual(l["a_addr"], t.nodes[l["a"]]["mgmt"])
            self.assertEqual(l["b_addr"], t.nodes[l["b"]]["mgmt"])

    def test_ligacao_virtual_atravessa_o_underlay_como_diz_o_enunciado(self):
        """O enunciado: a ligacao virtual O3-O5 e suportada por n9 <-> n5 <-> n2."""
        l = topo().link_between("O3", "O5")
        self.assertEqual(l["via"], ["n9", "n5", "n2"])

    def test_aresta_inexistente_ou_no_sem_ligacoes_e_erro(self):
        with self.assertRaises(ValueError):
            importar(os.path.join(CEN, "cenario3_core.xml"), [("S1", "X9")])
        with self.assertRaises(ValueError):
            importar(os.path.join(CEN, "cenario3_core.xml"), [("S1", "O2")])   # os outros ficam sem ligacoes


class TestComportamento(unittest.TestCase):
    def test_todos_entram_e_conhecem_os_vizinhos(self):
        s = corre({}, {}, ate=30)
        esperado = {n: {v for v, _, _ in vs} for n, vs in s.topo.tabela_vizinhos().items()}
        for n in NOS:
            self.assertEqual(s.logic[n].state, core.JOINED, n)
            self.assertEqual({v.name for v in s.logic[n].vizinhos.values() if v.alive}, esperado[n], n)

    def test_arvore_do_enunciado(self):
        """Enunciado: melhor caminho S1->C1 = O4-O3-O2-O1 e S1->C2 = O8-O7-O2-O1; O2 replica para O3 e O7."""
        s = corre({"S1": ["video1"]}, {c: ["video1"] for c in CLIENTES}, ate=25)
        pai = {n: s.logic[n].rotas["video1"].origem for n in NOS - {"S1", "S2"}}
        self.assertEqual((pai["C1"], pai["O3"], pai["O2"]), ("O3", "O2", "S1"))
        self.assertEqual((pai["C2"], pai["O7"]), ("O7", "O2"))
        self.assertEqual(s.logic["O2"].rotas["video1"].destinos, {"O3", "O7"})   # um so pacote por ligacao

    def test_dois_servidores_com_dois_streams_cada(self):
        s = corre({"S1": ["video1", "video2"], "S2": ["video3", "video4"]},
                  {"C1": ["video1", "video3"], "C2": ["video2", "video4"], "C3": ["video1", "video4"],
                   "C4": ["video2", "video3"], "C5": ["video1", "video2", "video3", "video4"]}, ate=25)
        for c, streams in (("C1", ("video1", "video3")), ("C2", ("video2", "video4")), ("C3", ("video1", "video4")),
                           ("C4", ("video2", "video3")), ("C5", ("video1", "video2", "video3", "video4"))):
            for st in streams:
                self.assertGreater(s.logic[c].rotas[st].frames_recebidas, 50, f"{c} {st}")

    def test_morre_O7_C3_recupera_e_C2_fica_isolado(self):
        s = corre({"S1": ["video1"]}, {c: ["video1"] for c in CLIENTES}, ate=80, kill="O7")
        self.assertLess(buraco_max(s, "C3", 30.0), 5.0)           # S1-O2-O3-O5-O6-C3
        self.assertEqual(buraco_max(s, "C2", 30.0), INF)          # C2 so tem O7
        for c in ("C1", "C4", "C5"):
            self.assertLess(buraco_max(s, c, 30.0), 0.5, c)       # nem notam

    def test_O2_e_ponto_unico_de_falha_do_S1_mas_nao_do_S2(self):
        s = corre({"S1": ["video1"], "S2": ["video3"]}, {"C1": ["video1"], "C4": ["video3"]}, ate=80, kill="O2")
        self.assertEqual(buraco_max(s, "C1", 30.0), INF)          # S1 so tem O2: sem caminho alternativo
        self.assertLess(buraco_max(s, "C4", 30.0), 0.5)           # S2 nao passa por O2


if __name__ == "__main__":
    unittest.main()
