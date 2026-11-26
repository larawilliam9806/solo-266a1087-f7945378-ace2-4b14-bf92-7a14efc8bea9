"""异步解析编排（统一经 QueryScheduler 回源）。

关键语义：
- **缓存命中直接返回**，不进调度器、不占回源槽位；
- 所有回源查询（网页试查和 /api/resolve 共用同一个 Resolver）都进
  QueryScheduler：全局回源并发上限可配置，每上游 FIFO，多上游有空槽
  轮流分配，单上游有待发可独占空槽（详见 scheduler.py）；
- 阻塞的 UDP 查询放在线程池里执行，事件循环不被阻塞，等待回包期间
  页面照常改规则、查别的域名；
- 规则真实变更时通过 RuleStore 监听器把**尚未发出**的查询按新路由
  重新归队（保留原排队先后），在途查询不动，其槽位一直占用到本次
  UDP 调用结束；
- 回包后用 RuleStore.is_stale() 校验：等待期间命中规则被动过
  （哪怕又改回原样）就丢弃回包，按**当前路由**重新排队（队尾）；
- 陈旧回包绝不写入缓存；只有现行规则下的回包才按 TTL 入缓存。
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor

from .cache import CacheEntry, DnsCache
from .dns_query import DnsQueryError, DnsReply, query_a
from .models import normalize_name
from .rules import Rule, RuleStore
from .scheduler import DEFAULT_MAX_IN_FLIGHT, QueryScheduler
from .upstream import UpstreamRegistry

MAX_REATTEMPTS = 6  # 等待期间规则反复变化时的重查上限，防止极端情况下无限循环


class Resolver:
    def __init__(
        self,
        rules: RuleStore,
        cache: DnsCache,
        upstreams: UpstreamRegistry,
        max_in_flight: int | None = None,
        query_timeout: float | None = None,
        executor: ThreadPoolExecutor | None = None,
    ):
        self.rules = rules
        self.cache = cache
        self.upstreams = upstreams
        if max_in_flight is None:
            max_in_flight = int(os.getenv("UPSTREAM_MAX_CONCURRENCY", DEFAULT_MAX_IN_FLIGHT))
        self._timeout = query_timeout
        self.scheduler = QueryScheduler(
            upstreams.ids(), max_in_flight=max_in_flight, executor=executor
        )
        # 规则一变，立刻把待发查询按新路由重排（在途的不动）
        rules.add_listener(self._on_rules_changed)

    def shutdown(self) -> None:
        self.scheduler.shutdown()

    # ---------- 规则变更 -> 待发查询即时重排 ----------
    def _on_rules_changed(self) -> None:
        loop = self.scheduler.loop  # 还没有任何查询时 loop 尚未绑定
        if loop is None:
            return
        loop.call_soon_threadsafe(self._reroute_pending)

    def _reroute_pending(self) -> None:
        self.scheduler.reroute(lambda name: self.rules.match(name).upstream_id)

    # ---------- 解析 ----------
    async def resolve(self, name: str) -> dict:
        key = normalize_name(name)
        started = time.monotonic()

        # 1. 缓存优先（惰性过期，命中即返回，不占回源槽位）
        cached = self.cache.get(key)
        if cached is not None:
            return self._result(
                key, cached, cached_hit=True, stale_discarded=0,
                elapsed=time.monotonic() - started,
            )

        stale_discarded = 0
        last_error: str | None = None

        # 2. 规则 -> 上游 -> 进调度器回源；回包陈旧则丢弃重查（重排到队尾）
        for _ in range(MAX_REATTEMPTS):
            first_rule: Rule = self.rules.match(key)
            timeout = self._timeout
            # 规则快照在查询 *实际发出* 的瞬间记录（见 make_query），
            # 这样入队后、发送前发生的改路重排不会把合法回包误判成陈旧
            dispatched: dict = {}

            def make_query(uid: str):
                # 由调度器在发送瞬间调用：按当时的上游 uid 构造 UDP 查询
                rule_now: Rule = self.rules.match(key)
                up = self.upstreams.get(uid)
                dispatched["suffix"] = rule_now.suffix
                dispatched["version"] = rule_now.version
                dispatched["upstream_id"] = uid

                def do_query() -> DnsReply:
                    if timeout is not None:
                        return query_a(key, up.host, up.dns_port, timeout)
                    return query_a(key, up.host, up.dns_port)

                return do_query

            try:
                reply: DnsReply = await self.scheduler.submit(
                    key, first_rule.upstream_id, make_query
                )
            except DnsQueryError as exc:
                last_error = str(exc)
                break

            # 3. 等待期间规则被动过？丢包，按现行路由重新排队
            sent_suffix = dispatched.get("suffix")
            sent_version = dispatched.get("version")
            if sent_suffix is None or self.rules.is_stale(
                key, sent_suffix, sent_version
            ):
                stale_discarded += 1
                continue

            # 4. 现行规则下的有效回包：按 TTL 入缓存
            sent_upstream = self.upstreams.get(dispatched["upstream_id"])
            now = time.monotonic()
            entry = CacheEntry(
                key=key,
                status=reply.rcode,
                addresses=reply.addresses,
                ttl=max(1, reply.ttl),
                upstream_id=sent_upstream.id,
                suffix=sent_suffix,
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

    def stats(self) -> dict:
        return self.scheduler.stats()

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
