"""异步解析编排。

关键并发语义：
- 阻塞的 UDP 查询放在线程池里执行，事件循环不被阻塞，
  等待回包期间页面照常改规则、查别的域名；
- 回包后先用 RuleStore.is_stale() 校验等待期间命中规则是否被动过。
  被动过（哪怕又改回原样）就丢弃回包，按现行规则重新查询；
- 陈旧回包绝不写入缓存；只有现行规则下的回包才按 TTL 入缓存。
"""
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from .cache import CacheEntry, DnsCache
from .dns_query import DnsQueryError, DnsReply, query_a
from .models import normalize_name
from .rules import Rule, RuleStore
from .upstream import UpstreamRegistry

MAX_REATTEMPTS = 6  # 等待期间规则反复变化时的重查上限，防止极端情况下无限循环


class Resolver:
    def __init__(
        self,
        rules: RuleStore,
        cache: DnsCache,
        upstreams: UpstreamRegistry,
        max_workers: int = 32,
        query_timeout: float | None = None,
    ):
        self.rules = rules
        self.cache = cache
        self.upstreams = upstreams
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="dns-query"
        )
        self._timeout = query_timeout

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)

    async def resolve(self, name: str) -> dict:
        key = normalize_name(name)
        started = time.monotonic()

        # 1. 缓存优先（惰性过期，命中即返回）
        cached = self.cache.get(key)
        if cached is not None:
            return self._result(
                key, cached, cached_hit=True, stale_discarded=0, elapsed=time.monotonic() - started
            )

        stale_discarded = 0
        last_error: str | None = None

        # 2. 规则 -> 上游 -> 查询；回包陈旧则丢弃重查
        for _ in range(MAX_REATTEMPTS):
            rule: Rule = self.rules.match(key)
            upstream = self.upstreams.get(rule.upstream_id)

            try:
                reply: DnsReply = await asyncio.get_running_loop().run_in_executor(
                    self._pool,
                    lambda: query_a(
                        key, upstream.host, upstream.dns_port, self._timeout
                    )
                    if self._timeout
                    else query_a(key, upstream.host, upstream.dns_port),
                )
            except DnsQueryError as exc:
                last_error = str(exc)
                break

            # 3. 等待期间规则被动过？丢包，按现行规则重来
            if self.rules.is_stale(key, rule.suffix, rule.version):
                stale_discarded += 1
                continue

            # 4. 现行规则下的有效回包：按 TTL 入缓存
            now = time.monotonic()
            entry = CacheEntry(
                key=key,
                status=reply.rcode,
                addresses=reply.addresses,
                ttl=max(1, reply.ttl),
                upstream_id=upstream.id,
                suffix=rule.suffix,
                expire_at=now + max(1, reply.ttl),
                created_at=now,
            )
            self.cache.put(entry)
            return self._result(
                key, entry, cached_hit=False, stale_discarded=stale_discarded,
                elapsed=time.monotonic() - started, raw_ttls=reply.raw_ttls,
            )

        if stale_discarded and last_error is None:
            last_error = "规则在等待期间多次变更，重查次数达到上限"
        return {
            "name": key,
            "cached_hit": False,
            "stale_discarded": stale_discarded,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            "status": "ERROR",
            "error": last_error or "解析失败",
        }

    @staticmethod
    def _result(
        key: str,
        entry: CacheEntry,
        cached_hit: bool,
        stale_discarded: int,
        elapsed: float,
        raw_ttls: tuple[int, ...] | None = None,
    ) -> dict:
        body = entry.to_dict()
        body.update(
            {
                "name": key,
                "cached_hit": cached_hit,
                "stale_discarded": stale_discarded,
                "elapsed_ms": round(elapsed * 1000, 1),
                "raw_ttls": list(raw_ttls or []),
            }
        )
        return body
