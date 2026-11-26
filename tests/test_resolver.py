"""Resolver 并发语义：等待期间规则变化 -> 丢弃旧回包并按现行规则重查。"""
from __future__ import annotations

import asyncio
import socket

import pytest

from dns_forwarder.cache import DnsCache
from dns_forwarder.resolver import Resolver
from dns_forwarder.rules import RuleStore
from dns_forwarder.upstream import Upstream, UpstreamRegistry

from .fakes import FakeUpstream


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def harness():
    port_a, port_b = _free_port(), _free_port()
    fake_a = FakeUpstream("127.0.0.1", port_a, ip_base=10, delay=0.6)
    fake_b = FakeUpstream("127.0.0.1", port_b, ip_base=172, delay=0.0)
    registry = UpstreamRegistry(
        [
            Upstream("upstream-a", "A", "127.0.0.1", port_a, 1),
            Upstream("upstream-b", "B", "127.0.0.1", port_b, 2),
        ]
    )
    rules = RuleStore("upstream-a")
    cache = DnsCache()
    resolver = Resolver(rules, cache, registry)
    yield fake_a, fake_b, rules, cache, resolver
    resolver.shutdown()
    fake_a.stop()
    fake_b.stop()


def test_resolve_basic_and_cache_hit(harness):
    fake_a, fake_b, rules, cache, resolver = harness

    result = asyncio.run(resolver.resolve("host.corp.local"))
    assert result["status"] == "OK"
    assert result["upstream_id"] == "upstream-a"
    assert result["addresses"] == ["10.0.0.1"]
    assert result["cached_hit"] is False
    assert fake_a.query_count == 1

    # 第二次：TTL 内缓存命中，不再问上游
    again = asyncio.run(resolver.resolve("host.corp.local"))
    assert again["cached_hit"] is True
    assert again["upstream_id"] == "upstream-a"
    assert fake_a.query_count == 1
    assert cache.stats()["hits"] == 1


def test_nxdomain_cached_with_soa_minimum_ttl(harness):
    fake_a, fake_b, rules, cache, resolver = harness
    fake_a.nx_on_calls.add(1)

    result = asyncio.run(resolver.resolve("nx.corp.local"))
    assert result["status"] == "NXDOMAIN"
    assert result["addresses"] == []
    assert result["ttl"] == 30  # SOA minimum

    again = asyncio.run(resolver.resolve("nx.corp.local"))
    assert again["cached_hit"] is True
    assert again["status"] == "NXDOMAIN"
    assert fake_a.query_count == 1  # 否定缓存生效，未再回源


def test_stale_reply_discarded_when_rule_changes_midflight(harness):
    fake_a, fake_b, rules, cache, resolver = harness

    async def scenario():
        task = asyncio.create_task(resolver.resolve("host.corp.local"))
        await asyncio.sleep(0.15)  # 查询已发出、正在等 A 的延迟回包
        rules.set_rule("", "upstream-b")  # 等待期间把默认上游切到 B
        return await task

    result = asyncio.run(scenario())

    # A 确实答了，但回包被丢弃；最终结果来自现行规则指向的 B
    assert fake_a.query_count == 1
    assert fake_b.query_count == 1
    assert result["stale_discarded"] == 1
    assert result["cached_hit"] is False
    assert result["upstream_id"] == "upstream-b"
    assert result["addresses"] == ["172.0.0.1"]

    # 陈旧回包没有污染缓存：缓存里只存 B 的结果
    entry = cache.get("host.corp.local")
    assert entry is not None and entry.upstream_id == "upstream-b"


def test_change_back_still_discards_old_reply(harness):
    """改到 B 又改回 A：版本号已变，A 的旧回包照样作废。"""
    fake_a, fake_b, rules, cache, resolver = harness

    async def scenario():
        task = asyncio.create_task(resolver.resolve("host.corp.local"))
        await asyncio.sleep(0.15)
        rules.set_rule("", "upstream-b")
        rules.set_rule("", "upstream-a")  # 又改回，但版本继续递增
        return await task

    result = asyncio.run(scenario())

    assert fake_a.query_count == 2  # 第一次的回包丢弃，按现行规则重查了一次
    assert fake_b.query_count == 0
    assert result["stale_discarded"] == 1
    assert result["upstream_id"] == "upstream-a"
    assert result["addresses"] == ["10.0.0.2"]  # 第二次查询的答复，不是第一次的 .1


def test_longer_suffix_inserted_midflight(harness):
    fake_a, fake_b, rules, cache, resolver = harness

    async def scenario():
        task = asyncio.create_task(resolver.resolve("host.dc.corp.local"))
        await asyncio.sleep(0.15)
        rules.set_rule("dc.corp.local", "upstream-b")
        return await task

    result = asyncio.run(scenario())
    assert result["stale_discarded"] == 1
    assert result["upstream_id"] == "upstream-b"
    assert result["suffix"] == "dc.corp.local"
    assert fake_a.query_count == 1 and fake_b.query_count == 1


def test_concurrent_queries_and_edits_do_not_block_each_other(harness):
    fake_a, fake_b, rules, cache, resolver = harness
    fake_b.delay = 0.4
    rules.set_rule("slow.test", "upstream-b")

    async def scenario():
        # 两个慢查询挂着，期间改规则 + 查别的域名，事件循环不能被堵住
        t1 = asyncio.create_task(resolver.resolve("a.slow.test"))
        t2 = asyncio.create_task(resolver.resolve("b.slow.test"))
        await asyncio.sleep(0.1)
        rules.set_rule("other.test", "upstream-a")  # 等待期间可改规则
        quick = await resolver.resolve("quick.other.test")  # 可查别的域名
        return await t1, await t2, quick

    r1, r2, quick = asyncio.run(scenario())
    assert quick["upstream_id"] == "upstream-a"
    assert quick["cached_hit"] is False
    assert r1["upstream_id"] == "upstream-b" and r2["upstream_id"] == "upstream-b"
    assert fake_b.query_count == 2


def test_no_reeviction_when_upstream_unchanged(harness):
    fake_a, fake_b, rules, cache, resolver = harness
    # 先在默认 A 下产生缓存
    asyncio.run(resolver.resolve("host.corp.local"))
    assert cache.get("host.corp.local") is not None

    # 同上游重复保存：无变化，缓存保留
    assert rules.set_rule("", "upstream-a") is False
    assert cache.get("host.corp.local") is not None
