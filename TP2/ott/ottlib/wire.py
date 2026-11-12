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
cat << 'EOF' > /home/core/Overlay-/TP2/ott/ottlib/wire.py
"""Formato das mensagens (binario, big-endian) - Marco 1 & 2."""
import socket
import struct

VERSION = 1

T_REGISTER = 1
T_NEIGHBOURS = 2
T_HELLO = 3
T_ACK = 4

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
_ENTRY_TAIL = struct.Struct("!4sH")
_ADVERT_TAIL = struct.Struct("!HI")


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
    if len(data) < HDR.size:
        raise WireError("datagrama mais curto do que o cabecalho")
    ver, tipo, msg_id = HDR.unpack_from(data)
    if ver != VERSION:
        raise WireError(f"versao desconhecida: {ver}")
    return tipo, msg_id, data[HDR.size:]


def enc_name_msg(tipo, msg_id, name):
    return encode(tipo, msg_id, _pack_name(name))


def dec_name_msg(payload):
    name, end = _unpack_name(payload, 0)
    if end != len(payload):
        raise WireError("bytes a mais")
    return name


def enc_neighbours(msg_id, entries):
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


def enc_ack(msg_id):
    return encode(T_ACK, msg_id)


def enc_advert(msg_id, stream, server, seq, cost):
    if cost < 0:
        raise WireError(f"custo negativo: {cost}")
    c_int = min(0xFFFFFFFF, int(round(cost * 1000)))
    body = _pack_name(stream) + _pack_name(server) + _ADVERT_TAIL.pack(seq & 0xFFFF, c_int)
    return encode(T_ADVERT, msg_id, body)


def dec_advert(payload):
    stream, off = _unpack_name(payload, 0)
    server, off = _unpack_name(payload, off)
    if off + _ADVERT_TAIL.size != len(payload):
        raise WireError("ADVERT truncado ou com bytes a mais")
    seq, c_int = _ADVERT_TAIL.unpack_from(payload, off)
    return stream, server, seq, c_int / 1000.0


def enc_stream_msg(tipo, msg_id, stream):
    return encode(tipo, msg_id, _pack_name(stream))


def dec_stream_msg(payload):
    return dec_name_msg(payload)
EOF
