"""Cenario base (cenario5): 1 servidor, 4 nos, 4 clientes, 13 ligacoes, com varios caminhos.

Cada falha mostra um comportamento diferente. Estes testes fixam a tabela que vai para o relatorio.
"""
import os
import sys
import unittest

AQUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AQUI)
sys.path.insert(0, os.path.dirname(AQUI))

from test_keepalive import cenario, corre, buraco_max       # noqa: E402

CL = ("C1", "C2", "C3", "C4")
INF = float("inf")


def gaps(extra, kill):
    s = corre(cenario(5, "S1", CL), extra, 75, kill=kill)
    return {c: buraco_max(s, c, 40.0) for c in CL if c != kill}


class TestCenarioBase(unittest.TestCase):
    def test_arvore_inicial_usa_os_caminhos_de_menor_atraso(self):
        s = corre(cenario(5, "S1", CL), True, 20)
        pai = {n: s.logic[n].rotas["video1"].origem for n in ("N1", "N2", "N3", "N4", *CL)}
        self.assertEqual(pai["N1"], "S1")
        self.assertEqual(pai["N3"], "N1")          # S1-N1-N3 (20 ms) e nao S1-N2-N3 (40 ms)
        self.assertEqual(pai["C1"], "N3")
        self.assertEqual(pai["C3"], "N4")

    def test_morre_N1_todos_recuperam_por_N2(self):
        for extra in (False, True):
            g = gaps(extra, "N1")
            for c in CL:
                self.assertLess(g[c], 5.0, f"extra={extra} {c}")
        # sem sucessor viavel (condicao DUAL) o N3 espera pelo proximo ADVERT (2 s): ~2 s com o extra
        self.assertLess(gaps(True, "N1")["C1"], 3.0)

    def test_morre_N2_so_o_C3_sente(self):
        g = gaps(True, "N2")
        self.assertLess(g["C3"], 1.5)
        for c in ("C1", "C2", "C4"):
            self.assertLess(g[c], 0.2, c)            # sem interrupcao

    def test_morre_N3_C1_e_C2_isolados_C4_recupera(self):
        g = gaps(True, "N3")
        self.assertEqual(g["C1"], INF)
        self.assertEqual(g["C2"], INF)
        self.assertLess(g["C4"], 3.0)
        self.assertLess(g["C3"], 0.2)

    def test_morre_N4_so_o_C3_fica_isolado(self):
        g = gaps(True, "N4")
        self.assertEqual(g["C3"], INF)
        for c in ("C1", "C2", "C4"):
            self.assertLess(g[c], 0.2, c)

    def test_extra_recupera_pelo_menos_1_5x_mais_depressa(self):
        b, e = gaps(False, "N1"), gaps(True, "N1")
        self.assertGreater(b["C1"], 1.5 * e["C1"])

    def test_regressao_sem_ciclo_N2_N3_quando_morre_N1(self):
        """Com a metrica por atraso, N2 e N3 tinham-se um ao outro como alternativa (custo 30 cada, stale) e
        ficavam presos a apontar um para o outro. A condicao de viabilidade (DUAL) e a expiracao que limpa a
        origem impedem isto: no fim ninguem aponta para quem aponta para si, e todos os clientes voltam a receber."""
        s = corre(cenario(5, "S1", CL), True, 75, kill="N1")
        pai = {n: s.logic[n].rotas["video1"].origem for n in ("N2", "N3", "N4")}
        for a, b in (("N2", "N3"), ("N3", "N4"), ("N2", "N4")):
            self.assertFalse(pai[a] == b and pai[b] == a, f"ciclo {a}<->{b}: {pai}")
        for c in ("C1", "C2", "C3", "C4"):
            self.assertLess(buraco_max(s, c, 40.0), 5.0, c)


if __name__ == "__main__":
    unittest.main()
