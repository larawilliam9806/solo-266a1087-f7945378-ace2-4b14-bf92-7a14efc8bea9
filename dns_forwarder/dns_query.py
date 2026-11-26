"""dnslib 底层查询与 TTL 解析。"""
from __future__ import annotations

import socket
from dataclasses import dataclass

from dnslib import QTYPE, RCODE, RR, DNSQuestion, DNSRecord

from .cache import DEFAULT_NEGATIVE_TTL
from .models import normalize_name

# 单次 UDP 查询超时（秒）。上游的"答复延迟"是 sleep 之后才回包，
# 所以这里要大于演示允许设置的最大延迟。
DEFAULT_QUERY_TIMEOUT = 10.0


class DnsQueryError(Exception):
    """网络层 / 解析层错误（超时、拒连、坏包等）。"""


@dataclass
class DnsReply:
    rcode: str                       # "NOERROR" / "NXDOMAIN" / ...
    addresses: tuple[str, ...]
    ttl: int                         # 按 DNS 规则计算出的可缓存 TTL
    raw_ttls: tuple[int, ...]        # 报文里实际带的 TTL，便于页面展示


def build_query(name: str) -> DNSRecord:
    qname = normalize_name(name)
    q = DNSRecord()
    q.add_question(DNSQuestion(qname, QTYPE.A))
    q.header.rd = 1
    return q


def _send_udp(q: DNSRecord, host: str, port: int, timeout: float) -> DNSRecord:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        try:
            sock.sendto(q.pack(), (host, port))
            data, _ = sock.recvfrom(65535)
        finally:
            sock.close()
    except socket.timeout as exc:
        raise DnsQueryError(f"查询 {host}:{port} 超时（{timeout}s）") from exc
    except OSError as exc:
        raise DnsQueryError(f"查询 {host}:{port} 失败: {exc}") from exc

    try:
        reply = DNSRecord.parse(data)
    except Exception as exc:  # dnslib 抛的异常类型比较杂
        raise DnsQueryError(f"无法解析上游回包: {exc}") from exc
    return reply


def _positive_ttl(records: list[RR]) -> int:
    # RFC 2181：多条 RR 时取最小 TTL
    return min(int(rr.ttl) for rr in records) if records else DEFAULT_NEGATIVE_TTL


def _negative_ttl(reply: DNSRecord) -> int:
    # RFC 2308：NXDOMAIN/NODATA 的 TTL 取权威区 SOA 的 MINIMUM 字段，
    # 且不得超过 SOA 记录自身的 TTL
    soas = [rr for rr in reply.auth if rr.rtype == QTYPE.SOA]
    if soas:
        soa = soas[0]
        minimum = int(soa.rdata.times[4])
        return max(0, min(int(soa.ttl), minimum))
    return DEFAULT_NEGATIVE_TTL


def query_a(
    name: str, host: str, port: int, timeout: float = DEFAULT_QUERY_TIMEOUT
) -> DnsReply:
    """向单个上游发一次 A 查询并解析结果。"""
    q = build_query(name)
    reply = _send_udp(q, host, port, timeout)

    rcode = RCODE.get(reply.header.rcode)
    answers = [rr for rr in reply.rr if rr.rtype == QTYPE.A]
    raw_ttls = tuple(int(rr.ttl) for rr in answers)

    if rcode == "NXDOMAIN":
        return DnsReply("NXDOMAIN", (), _negative_ttl(reply), raw_ttls)

    if rcode != "NOERROR":
        raise DnsQueryError(f"上游返回错误码 {rcode}")

    if not answers:
        # NOERROR 但没有 A 记录 = NODATA，同样按 RFC 2308 做否定缓存
        return DnsReply("NODATA", (), _negative_ttl(reply), raw_ttls)

    addresses = tuple(str(rr.rdata) for rr in answers)
    return DnsReply("OK", addresses, _positive_ttl(answers), raw_ttls)
