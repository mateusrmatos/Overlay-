"""Formato das mensagens (binario, big-endian) - Marco 1.

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

# --- reservados para os marcos seguintes (nao usados ainda)
T_ADVERT = 5
T_QUERY = 6
T_REPLY = 7
T_ACTIVATE = 8
T_DEACTIVATE = 9
T_PROBE = 10
T_ECHO = 11
T_DATA = 12

NOMES = {
    T_REGISTER: "REGISTER", T_NEIGHBOURS: "NEIGHBOURS", T_HELLO: "HELLO", T_ACK: "ACK",
    T_ADVERT: "ADVERT", T_QUERY: "QUERY", T_REPLY: "REPLY", T_ACTIVATE: "ACTIVATE",
    T_DEACTIVATE: "DEACTIVATE", T_PROBE: "PROBE", T_ECHO: "ECHO", T_DATA: "DATA",
}

HDR = struct.Struct("!BBH")
_ENTRY_TAIL = struct.Struct("!4sH")   # ip (4 bytes) + porta (2 bytes)


class WireError(ValueError):
    """Datagrama mal formado (curto, versao errada, campos truncados)."""


def nome_do_tipo(t):
    return NOMES.get(t, f"TIPO{t}")


def _pack_name(name):
    raw = name.encode("ascii")
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
    return buf[off + 1:end].decode("ascii"), end


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
        body += _pack_name(name) + _ENTRY_TAIL.pack(socket.inet_aton(ip), port)
    return encode(T_NEIGHBOURS, msg_id, body)


def dec_neighbours(payload):
    if not payload:
        raise WireError("lista vazia sem contador")
    count, off, out = payload[0], 1, []
    for _ in range(count):
        name, off = _unpack_name(payload, off)
        if off + _ENTRY_TAIL.size > len(payload):
            raise WireError("entrada truncada")
        ip, port = _ENTRY_TAIL.unpack_from(payload, off)
        off += _ENTRY_TAIL.size
        out.append((name, socket.inet_ntoa(ip), port))
    if off != len(payload):
        raise WireError("bytes a mais")
    return out


# ---------------------------------------------------------------- ACK
def enc_ack(msg_id):
    return encode(T_ACK, msg_id)
