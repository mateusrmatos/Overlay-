"""Topologia: um JSON descreve nos, ligacoes e enderecos.

Formato:
{
  "overlay_port": 5000,
  "boot": {"addr": "172.16.0.254", "port": 9000},
  "nodes": {"S1": {"mgmt": "172.16.0.1"}, ...},
  "links": [ {"a": "S1", "b": "N1", "a_addr": "10.0.0.20", "b_addr": "10.0.0.1",
              "delay_ms": 10, "loss": 0.0}, ... ]
}

- "mgmt" e o endereco do no na rede de gestao (a unica por onde fala com o bootstrapper).
- a_addr/b_addr sao os enderecos de cada ponta da ligacao (a rede de overlay, ligacoes diretas).
- delay_ms/loss so interessam ao simulador (no CORE quem os impoe e o netem).

Este mesmo ficheiro serve o bootstrapper real (le a tabela de vizinhos) e o simulador.
"""
import json


class Topology:
    def __init__(self, data):
        self.port = data.get("overlay_port", 5000)
        self.boot_addr = (data["boot"]["addr"], data["boot"].get("port", 9000))
        self.nodes = {n: dict(v) for n, v in data["nodes"].items()}
        self.links = [dict(l) for l in data["links"]]
        for l in self.links:
            for n in (l["a"], l["b"]):
                if n not in self.nodes:
                    raise ValueError(f"ligacao refere no desconhecido: {n}")
        # dono de cada endereco IP (para o simulador encaminhar)
        self.owner = {self.boot_addr[0]: "BOOT"}
        for n, v in self.nodes.items():
            self.owner[v["mgmt"]] = n
        for l in self.links:
            self.owner[l["a_addr"]] = l["a"]
            self.owner[l["b_addr"]] = l["b"]

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as f:
            return cls(json.load(f))

    def tabela_vizinhos(self):
        """{nome: [(vizinho, ip_do_vizinho_nessa_ligacao, porta)]}  -  o que o bootstrapper serve."""
        t = {n: [] for n in self.nodes}
        for l in self.links:
            t[l["a"]].append((l["b"], l["b_addr"], self.port))
            t[l["b"]].append((l["a"], l["a_addr"], self.port))
        return t

    def link_between(self, x, y):
        for l in self.links:
            if {l["a"], l["b"]} == {x, y}:
                return l
        return None

    def addr_towards(self, src, dst):
        """Endereco que `src` usa como origem quando fala com o vizinho `dst`."""
        l = self.link_between(src, dst)
        if l is None:
            return None
        return l["a_addr"] if l["a"] == src else l["b_addr"]


def from_edges(edges, boot_ip="172.16.0.254", boot_port=9000, port=5000, delay_ms=10, loss=0.0):
    """Gera uma topologia a partir de uma lista de arestas, com enderecos automaticos
    (ligacao k -> rede 10.0.k.0/24, pontas .1 e .2; gestao 172.16.0.n). Util para os cenarios
    2 e 3; no CORE usam-se os enderecos reais que o CORE atribuir."""
    nomes = []
    for a, b in edges:
        for n in (a, b):
            if n not in nomes:
                nomes.append(n)
    nodes = {n: {"mgmt": f"172.16.0.{i + 1}"} for i, n in enumerate(nomes)}
    links = [{"a": a, "b": b, "a_addr": f"10.0.{k}.1", "b_addr": f"10.0.{k}.2",
              "delay_ms": delay_ms, "loss": loss} for k, (a, b) in enumerate(edges)]
    return {"overlay_port": port, "boot": {"addr": boot_ip, "port": boot_port},
            "nodes": nodes, "links": links}
