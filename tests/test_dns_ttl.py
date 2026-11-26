"""dns_query 的 TTL 解析规则（不经过网络，直接构造报文）。"""
from __future__ import annotations

from dnslib import A, QTYPE, RCODE, RR, DNSRecord, SOA

from dns_forwarder.dns_query import _negative_ttl, _positive_ttl


def test_positive_ttl_takes_minimum():
    records = [
        RR("h.test", QTYPE.A, ttl=60, rdata=A("10.0.0.1")),
        RR("h.test", QTYPE.A, ttl=20, rdata=A("10.0.0.2")),
    ]
    assert _positive_ttl(records) == 20


def _nx_reply(soa_ttl: int, minimum: int) -> DNSRecord:
    q = DNSRecord.question("nx.test")
    reply = q.reply()
    reply.header.rcode = RCODE.NXDOMAIN
    reply.add_auth(
        RR(
            "mock.internal",
            QTYPE.SOA,
            ttl=soa_ttl,
            rdata=SOA(
                "ns1.mock.internal",
                "admin.mock.internal",
                (1, 7200, 3600, 1209600, minimum),
            ),
        )
    )
    return reply


def test_negative_ttl_uses_soa_minimum_capped_by_soa_ttl():
    # RFC 2308：取 SOA TTL 与 minimum 的较小者
    assert _negative_ttl(_nx_reply(soa_ttl=60, minimum=30)) == 30
    assert _negative_ttl(_nx_reply(soa_ttl=15, minimum=30)) == 15


def test_negative_ttl_fallback_without_soa():
    q = DNSRecord.question("nx.test")
    reply = q.reply()
    reply.header.rcode = RCODE.NXDOMAIN
    from dns_forwarder.cache import DEFAULT_NEGATIVE_TTL

    assert _negative_ttl(reply) == DEFAULT_NEGATIVE_TTL
