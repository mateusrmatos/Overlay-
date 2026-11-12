"""Formato das mensagens (binario, big-endian) - Marco 1 & 2.

Cada datagrama UDP de controlo comeca por um cabecalho de 4 bytes:

    +---------+--------+---------------+
    | versao  | tipo   | msg_id        |
    | 1 byte  | 1 byte | 2 bytes       |
    +---------+--------+---------------+

- versao:  permite evoluir o protocolo sem partir nos antigos.
- tipo:    que mensagem e (ver T_* abaixo).
- msg_id:  identifica a mensagem para o ACK e para detetar repeticoes (idempotencia).
           Um ACK leva o msg_id da mensagem que confirma.

Um "nome" e um byte de comprimento seguido de ASCII (1 a 31 caracteres).
Um IPv4 ocupa 4 bytes e uma porta 2 bytes.
"""
import socket
import struct

VERSION = 1

# --- tipos usados no Marco 1
T_REGISTER = 1     # no -> bootstrapper:  "sou o N1, quem sao os meus vizinhos?"
T_NEIGHBOURS = 2   # bootstrapper -> no:  lista de vizinhos (nome, ip, porta)
T_HELLO = 3        # no -> vizinho:       "sou o N1" (fiavel: espera ACK)
T_ACK = 4          # confirmacao de uma mensagem fiavel

# --- tipos usados no Marco 2 (Rotas) e seguintes
T_ADVERT = 5       # anuncio de rota (stream, servidor, seq, metrica)
T_QUERY = 6
T_REPLY = 7
T_ACTIVATE = 8     # pedido de ativacao de fluxo (downstream -> upstream)
T_DEACTIVATE = 9   # cancelamento de fluxo (downstream -> upstream)
T_PROBE = 10
T_ECHO = 11
T_DATA = 12

NOMES = {
    T_REGISTER: "REGISTER", T_NEIGHBOURS: "NEIGHBOURS", T_HELLO: "HELLO", T_ACK: "ACK",
    T_ADVERT: "ADVERT", T_QUERY: "QUERY", T_REPLY: "REPLY", T_ACTIVATE: "ACTIVATE",
    T_DEACTIVATE: "DEACTIVATE", T_PROBE: "PROBE", T_ECHO: "ECHO", T_DATA: "DATA",
}

HDR = struct.Struct("!BBH")
_ENTRY_TAIL = struct.Struct("!4sH")    # ip (4 bytes) + porta (2 bytes)
_ADVERT_TAIL = struct.Struct("!HI")    # seq (2 bytes) + custo escalado em milesimas (4 bytes)


class WireError(ValueError):
    """Datagrama mal formado (curto, versao errada, campos truncados)."""


def nome_do_tipo(t):
    return NOMES.get(t, f"TIPO{t}")


def _pack_name(name):
    try:
        raw = name.encode("ascii")
    except UnicodeEncodeError:
        raise WireError(f"nome nao-ASCII: {name!r}")
    if not 1 <= len(raw) <= 31:
        raise WireError(f"nome invalido: {name!r}")
    return bytes([len(raw)]) + raw


def _unpack_name(buf, off):
    if off >= len(buf):
        raise WireError("nome truncado")
    n = buf[off]
    end = off + 1 + n
    if n == 0 or end > len(buf):
        raise WireError("nome truncado")
    try:
        return buf[off + 1:end].decode("ascii"), end
    except UnicodeDecodeError:
        raise WireError("nome nao-ASCII no pacote")


def encode(tipo, msg_id, payload=b""):
    return HDR.pack(VERSION, tipo, msg_id & 0xFFFF) + payload


def decode(data):
    """Devolve (tipo, msg_id, payload) ou levanta WireError."""
    if len(data) < HDR.size:
        raise WireError("datagrama mais curto do que o cabecalho")
    ver, tipo, msg_id = HDR.unpack_from(data)
    if ver != VERSION:
        raise WireError(f"versao desconhecida: {ver}")
    return tipo, msg_id, data[HDR.size:]


# ---------------------------------------------------------------- REGISTER / HELLO
def enc_name_msg(tipo, msg_id, name):
    """REGISTER e HELLO so levam o nome de quem envia."""
    return encode(tipo, msg_id, _pack_name(name))


def dec_name_msg(payload):
    name, end = _unpack_name(payload, 0)
    if end != len(payload):
        raise WireError("bytes a mais")
    return name


# ---------------------------------------------------------------- NEIGHBOURS
def enc_neighbours(msg_id, entries):
    """entries: lista de (nome, ip_str, porta)."""
    if len(entries) > 255:
        raise WireError("vizinhos a mais")
    body = bytes([len(entries)])
    for name, ip, port in entries:
        body += _pack_name(name) + _ENTRY_TAIL.pack(socket
