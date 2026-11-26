"""DnsCache：TTL 过期、命中计数、精确剔除。"""
from __future__ import annotations

import time

from dns_forwarder.cache import CacheEntry, DnsCache


def _entry(name: str, upstream: str, suffix: str = "", ttl: int = 60) -> CacheEntry:
    now = time.monotonic()
    return CacheEntry(
        key=name,
        status="OK",
        addresses=("10.0.0.1",),
        ttl=ttl,
        upstream_id=upstream,
        suffix=suffix,
        expire_at=now + ttl,
        created_at=now,
    )


def test_put_get_and_hit_count():
    cache = DnsCache()
    cache.put(_entry("a.test", "upstream-a"))
    assert cache.get("a.test") is not None
    assert cache.get("a.test") is not None
    assert cache.stats()["hits"] == 2
    assert cache.get("missing.test") is None


def test_ttl_expiry_is_lazy():
    cache = DnsCache()
    cache.put(_entry("short.test", "upstream-a", ttl=1))
    time.sleep(1.1)
    assert cache.get("short.test") is None
    assert cache.stats()["entries"] == 0


def test_evict_only_matching_predicate():
    cache = DnsCache()
    cache.put(_entry("keep.a", "upstream-a", suffix="a.local"))
    cache.put(_entry("drop.b", "upstream-b", suffix="b.local"))
    cache.put(_entry("also-keep.a", "upstream-a", suffix="a.local"))

    victims = cache.evict(lambda e: e.upstream_id == "upstream-b")
    assert victims == ["drop.b"]
    assert cache.get("keep.a") is not None
    assert cache.get("also-keep.a") is not None
    assert cache.get("drop.b") is None


def test_nxdomain_entry_cached_like_positive():
    cache = DnsCache()
    now = time.monotonic()
    cache.put(
        CacheEntry(
            key="nx.test",
            status="NXDOMAIN",
            addresses=(),
            ttl=30,
            upstream_id="upstream-a",
            suffix="",
            expire_at=now + 30,
            created_at=now,
        )
    )
    got = cache.get("nx.test")
    assert got is not None and got.status == "NXDOMAIN" and got.addresses == ()


def test_clear_and_remove():
    cache = DnsCache()
    cache.put(_entry("x.test", "upstream-a"))
    assert cache.remove("x.test") is True
    assert cache.remove("x.test") is False
    cache.put(_entry("y.test", "upstream-a"))
    assert cache.clear() == 1
