"""Importador de cenarios do CORE (XML) -> topologia JSON do overlay.

O CORE descreve o UNDERLAY (routers, switches, hosts, ligacoes, IPs, atrasos). O OVERLAY
(quais hosts sao vizinhos) nao se desenha no CORE: vem de um ficheiro de arestas.

Para cada aresta do overlay, o importador calcula o caminho mais curto no underlay
(so por routers e switches, nunca atraves de outro host) e fica com:
    delay_ms = soma dos atrasos das ligacoes do caminho (netem do CORE; sem netem usa base_ms por ligacao)
    loss     = 1 - produto(1 - perda de cada ligacao)
    via      = nos do underlay atravessados (informativo)

Cada host overlay tem UM so endereco IP; e esse que vai em mgmt, a_addr e b_addr.
O bootstrapper e o host chamado "B" (nao e no overlay).
"""
import heapq
import xml.etree.ElementTree as ET

HOSTS = ("PC", "host")


def le_arestas(path):
    """Ficheiro de texto: uma aresta 'A-B' por linha; '#' comeca um comentario."""
    r = []
    with open(path, encoding="utf-8") as f:
        for linha in f:
            linha = linha.split("#", 1)[0].strip()
            if linha:
                a, sep, b = linha.partition("-")
                if not sep or not a.strip() or not b.strip():
                    raise ValueError(f"aresta invalida: {linha!r} (use A-B)")
                r.append((a.strip(), b.strip()))
    return r


def _le_xml(xml_path):
    root = ET.parse(xml_path).getroot()
    nome, tipo = {}, {}
    for sec in ("devices", "networks"):
        for d in root.find(sec) or []:
            nome[d.get("id")] = d.get("name")
            tipo[d.get("name")] = d.get("type")
    ips, viz = {}, {n: [] for n in nome.values()}
    for l in root.find("links"):
        a, b = nome[l.get("node1")], nome[l.get("node2")]
        for lado, dono in (("iface1", a), ("iface2", b)):
            i = l.find(lado)
            if i is not None and i.get("ip4"):
                ips.setdefault(dono, set()).add(i.get("ip4"))
        o = l.find("options")
        atraso_us = float(o.get("delay", 0)) if o is not None else 0.0
        perda = float(o.get("loss", 0.0)) / 100.0 if o is not None else 0.0
        viz[a].append((b, atraso_us, perda))
        viz[b].append((a, atraso_us, perda))
    return tipo, ips, viz


def _caminho(viz, tipo, base_ms, a, b):
    """Dijkstra por atraso; so atravessa routers/switches. Devolve (atraso_ms, perda, [nos intermedios])."""
    dist, prev = {a: 0.0}, {}
    fila = [(0.0, a)]
    while fila:
        d, u = heapq.heappop(fila)
        if d > dist.get(u, float("inf")):
            continue
        if u == b:
            break
        if u != a and tipo.get(u) in HOSTS:
            continue                                   # um host nao encaminha trafego alheio
        for v, atraso_us, perda in viz[u]:
            w = atraso_us / 1000.0 if atraso_us > 0 else base_ms
            if d + w < dist.get(v, float("inf")):
                dist[v] = d + w
                prev[v] = (u, perda)
                heapq.heappush(fila, (d + w, v))
    if b not in dist:
        raise ValueError(f"sem caminho no underlay entre {a} e {b}")
    nos, ok, x = [], 1.0, b
    while x != a:
        x, perda = prev[x][0], prev[x][1]
        ok *= 1.0 - perda
        if x != a:
            nos.append(x)
    return dist[b], 1.0 - ok, nos[::-1]


def importar(xml_path, arestas, base_ms=1.0, porta=5000, boot_nome="B", boot_porta=9000):
    tipo, ips, viz = _le_xml(xml_path)
    hosts = [n for n, t in tipo.items() if t in HOSTS]
    for h in hosts:
        if len(ips.get(h, ())) != 1:
            raise ValueError(f"o host {h} devia ter exatamente 1 endereco IP, tem {sorted(ips.get(h, ()))}")
    if boot_nome not in hosts:
        raise ValueError(f"nao ha nenhum host chamado {boot_nome} (bootstrapper)")
    ip = {h: next(iter(ips[h])) for h in hosts}
    nos = {h: {"mgmt": ip[h]} for h in hosts if h != boot_nome}
    links = []
    for a, b in arestas:
        for n in (a, b):
            if n not in nos:
                raise ValueError(f"aresta {a}-{b} refere {n}, que nao e um no overlay do XML")
        atraso, perda, via = _caminho(viz, tipo, base_ms, a, b)
        links.append({"a": a, "b": b, "a_addr": ip[a], "b_addr": ip[b],
                      "delay_ms": round(atraso, 3), "loss": round(perda, 4), "via": via})
    sem_vizinhos = [n for n in nos if not any(n in (l["a"], l["b"]) for l in links)]
    if sem_vizinhos:
        raise ValueError(f"nos overlay sem nenhuma aresta: {sem_vizinhos}")
    return {"overlay_port": porta, "boot": {"addr": ip[boot_nome], "port": boot_porta},
            "nodes": nos, "links": links}
