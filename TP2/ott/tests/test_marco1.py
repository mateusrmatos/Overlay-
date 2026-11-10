"""Testes do Marco 1 (correm na simulacao em tempo virtual, sem rede).

    cd ott && python3 -m unittest discover -s tests -v
"""
import os
import sys
import unittest

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
sys.path.insert(0, RAIZ)

from ottlib import core, wire                       # noqa: E402
from ottlib.sim import Sim, BOOT                    # noqa: E402
from ottlib.topology import Topology                # noqa: E402


def topo(n):
    return Topology.load(os.path.join(RAIZ, "cenarios", f"cenario{n}.json"))


def vizinhos_vivos(sim, nome):
    return {v.name for v in sim.logic[nome].vizinhos.values() if v.alive}


def vizinhos_esperados(t, nome):
    return {n for n, _, _ in t.tabela_vizinhos()[nome]}


class TestWire(unittest.TestCase):
    def test_register_ida_e_volta(self):
        d = wire.enc_name_msg(wire.T_REGISTER, 7, "N1")
        tipo, mid, pl = wire.decode(d)
        self.assertEqual((tipo, mid, wire.dec_name_msg(pl)), (wire.T_REGISTER, 7, "N1"))

    def test_neighbours_ida_e_volta(self):
        lst = [("S1", "10.0.0.20", 5000), ("C2", "10.0.1.20", 5000)]
        tipo, mid, pl = wire.decode(wire.enc_neighbours(9, lst))
        self.assertEqual(wire.dec_neighbours(pl), lst)

    def test_lista_vazia(self):
        _, _, pl = wire.decode(wire.enc_neighbours(1, []))
        self.assertEqual(wire.dec_neighbours(pl), [])

    def test_lixo_nao_rebenta(self):
        for lixo in (b"", b"\x01", b"\x09\x01\x00\x01", wire.encode(wire.T_NEIGHBOURS, 1, b"\x05")):
            with self.assertRaises(wire.WireError):
                t, m, pl = wire.decode(lixo)
                wire.dec_neighbours(pl)

    def test_nome_invalido(self):
        with self.assertRaises(wire.WireError):
            wire.enc_name_msg(wire.T_REGISTER, 1, "")
        with self.assertRaises(wire.WireError):
            wire.enc_name_msg(wire.T_REGISTER, 1, "x" * 32)


class TestArranque(unittest.TestCase):
    def test_todos_os_cenarios_entram_e_conhecem_os_vizinhos(self):
        for n in (1, 2, 3):
            with self.subTest(cenario=n):
                t = topo(n)
                s = Sim(t)
                s.start_all(spread=0.3)
                s.run(30)
                for nome in t.nodes:
                    self.assertEqual(s.logic[nome].state, core.JOINED, nome)
                    self.assertEqual(vizinhos_vivos(s, nome), vizinhos_esperados(t, nome), nome)

    def test_bootstrapper_recebe_exatamente_um_register_por_no(self):
        t = topo(3)
        s = Sim(t)
        s.start_all(spread=0.3)
        s.run(30)
        regs = s.pacotes(para=BOOT, tipo=wire.T_REGISTER)
        self.assertEqual(len(regs), len(t.nodes))
        self.assertEqual({r[1] for r in regs}, set(t.nodes))

    def test_bootstrapper_nao_trabalha_depois_do_arranque(self):
        """Depois do ultimo NEIGHBOURS enviado, o bootstrapper nao envia nem recebe nada."""
        t = topo(2)
        s = Sim(t)
        s.start_all(spread=0.3)
        s.run(60)
        todos = s.pacotes(de=BOOT) + s.pacotes(para=BOOT)
        ultimo_boot = max(p[0] for p in todos)
        # tudo o que acontece depois e trafego entre nos
        depois = [p for p in s.trace if p[0] > ultimo_boot]
        self.assertTrue(depois, "devia haver HELLOs depois do ultimo contacto com o bootstrapper")
        self.assertTrue(all(BOOT not in (p[1], p[2]) for p in depois))
        # e nenhum no e acordado ou contactado depois de estar JOINED (a asserção em core._send)
        self.assertLess(ultimo_boot, 0.3 * len(t.nodes) + 1)

    def test_no_tardio_e_aprendido_pelos_vizinhos(self):
        """C2 arranca 20 s depois: o HELLO de C2 chega ao N1 (que ja desistiu de C2) e ambos se conhecem."""
        t = topo(1)
        s = Sim(t)
        s.start_boot()
        for n in ("S1", "N1", "C1"):
            s.start(n, 0.1)
        s.start("C2", 20.0)
        s.run(40)
        self.assertEqual(vizinhos_vivos(s, "N1"), {"S1", "C1", "C2"})
        self.assertEqual(vizinhos_vivos(s, "C2"), {"N1"})

    def test_gestao_com_40pc_de_perda_ainda_entra(self):
        for seed in range(1, 21):
            t = topo(1)
            s = Sim(t, seed=seed, mgmt_loss=0.4)
            s.start_all(spread=0.2)
            s.run(60)
            for nome in t.nodes:
                self.assertEqual(s.logic[nome].state, core.JOINED, f"seed {seed} {nome}")

    def test_overlay_com_30pc_de_perda_ainda_se_conhece(self):
        falhas = 0
        for seed in range(1, 31):
            t = topo(1)
            for l in t.links:
                l["loss"] = 0.3
            s = Sim(t, seed=seed)
            s.start_all(spread=0.2)
            s.run(60)
            for nome in t.nodes:
                if vizinhos_vivos(s, nome) != vizinhos_esperados(t, nome):
                    falhas += 1
        # com 5 tentativas e 30 % de perda por sentido a falha de um par e rara mas possivel (~2 %)
        self.assertLessEqual(falhas, 3, f"{falhas} nos incompletos em 30 execucoes")

    def test_restart_faz_um_unico_register_extra(self):
        t = topo(1)
        s = Sim(t)
        s.start_all(spread=0.2)
        s.kill("N1", at=10.0)
        s.restart("N1", at=15.0)
        s.run(40)
        regs_n1 = s.pacotes(de="N1", para=BOOT, tipo=wire.T_REGISTER)
        self.assertEqual(len(regs_n1), 2)                     # um no arranque, um no rearranque
        self.assertEqual(s.logic["N1"].state, core.JOINED)
        self.assertEqual(vizinhos_vivos(s, "N1"), vizinhos_esperados(t, "N1"))

    def test_register_idempotente(self):
        """O mesmo REGISTER duas vezes da a mesma resposta (o bootstrapper nao guarda estado)."""
        t = topo(1)
        b = core.Bootstrapper(t.tabela_vizinhos())
        pkt = wire.enc_name_msg(wire.T_REGISTER, 5, "N1")
        r1 = [a for a in b.on_packet(0, ("1.1.1.1", 5000), pkt) if isinstance(a, core.Send)]
        r2 = [a for a in b.on_packet(1, ("1.1.1.1", 5000), pkt) if isinstance(a, core.Send)]
        self.assertEqual(r1[0].data, r2[0].data)

    def test_no_desconhecido_recebe_lista_vazia(self):
        b = core.Bootstrapper(topo(1).tabela_vizinhos())
        pkt = wire.enc_name_msg(wire.T_REGISTER, 5, "X9")
        send = [a for a in b.on_packet(0, ("1.1.1.1", 5000), pkt) if isinstance(a, core.Send)][0]
        _, _, pl = wire.decode(send.data)
        self.assertEqual(wire.dec_neighbours(pl), [])

    def test_guarda_contra_contacto_posterior_ao_bootstrapper(self):
        n = core.Node("N1", ("172.16.0.254", 9000))
        n.state = core.JOINED
        with self.assertRaises(AssertionError):
            n._send([], ("172.16.0.254", 9000), b"x")

    def test_register_sem_bootstrapper_desiste(self):
        t = topo(1)
        s = Sim(t)                     # bootstrapper nunca arranca
        s.start("S1", 0.0)
        s.run(120)
        self.assertEqual(s.logic["S1"].state, core.ORPHAN)
        n = len(s.pacotes(de="S1", para=BOOT, tipo=wire.T_REGISTER))
        self.assertEqual(n, core.REGISTER_TRIES)

    def test_bootstrapper_morre_nos_ja_entrados_nao_dependem_dele(self):
        t = topo(2)
        s = Sim(t)
        s.start_all(spread=0.2)
        s.kill(BOOT, at=20.0)
        s.run(120)
        for nome in t.nodes:
            self.assertEqual(vizinhos_vivos(s, nome), vizinhos_esperados(t, nome), nome)

    def test_datagramas_lixo_sao_ignorados(self):
        n = core.Node("N1", ("172.16.0.254", 9000))
        for lixo in (b"", b"\xff\xff\xff\xff\xff", wire.encode(wire.T_NEIGHBOURS, 1, b"\x09")):
            acts = n.on_packet(0, ("9.9.9.9", 1), lixo)
            self.assertTrue(all(isinstance(a, core.Log) for a in acts))


if __name__ == "__main__":
    unittest.main()
