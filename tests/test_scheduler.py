"""QueryScheduler 调度语义测试 + Resolver 集成测试。

覆盖需求：
- 回源总并发上限可配置，缓存命中不占槽位；
- 单上游有待发时可用满空槽；两台都有待发时空槽轮流分配，慢上游不饿死快上游；
- 同一上游 FIFO；
- 改规则后待发查询按新路由即时重排，保留原始排队先后；在途查询不动、
  槽位占用到本次 UDP 调用结束；
- 陈旧重查按当前路由排到队尾；
- /api/scheduler 暴露各上游排队数/在途数。
"""
from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest

from dns_forwarder.cache import DnsCache
from dns_forwarder.resolver import Resolver
from dns_forwarder.rules import RuleStore
from dns_forwarder.scheduler import QueryScheduler
from dns_forwarder.upstream import Upstream, UpstreamRegistry

from .fakes import FakeUpstream


# ---------------------------------------------------------------------
# 纯调度器测试：用脚本化的阻塞函数代替真实 UDP
# ---------------------------------------------------------------------
class ScriptedQueries:
    """每个上游一个固定延迟；记录每次实际开始执行的（上游, 域名, 时刻）。"""

    def __init__(self, delays: dict[str, float]):
        self.delays = delays
        self.started: dict[str, list[tuple[str, float]]] = {u: [] for u in delays}
        self._lock = threading.Lock()

    def make_func(self, uid: str, name: str):
        """固定上游的阻塞函数（不经重排的场景用）。"""
        def _f():
            with self._lock:
                self.started[uid].append((name, time.monotonic()))
            time.sleep(self.delays[uid])
            return uid, name

        return _f

    def make_factory(self, name: str):
        """发送瞬间按调度器给出的上游 uid 构建阻塞函数。"""
        def factory(uid: str):
            def _f():
                with self._lock:
                    self.started[uid].append((name, time.monotonic()))
                time.sleep(self.delays[uid])
                return uid, name

            return _f

        return factory

    def start_names(self, uid: str) -> list[str]:
        return [n for n, _ in self.started[uid]]


async def _gather(scheduler: QueryScheduler, plan: list[tuple[str, str]], script: ScriptedQueries):
    tasks = [
        asyncio.create_task(scheduler.submit(name, uid, script.make_factory(name)))
        for uid, name in plan
    ]
    return await asyncio.gather(*tasks)


def test_single_upstream_fills_all_free_slots():
    script = ScriptedQueries({"a": 0.3})
    sched = QueryScheduler(["a"], max_in_flight=3)
    try:
        async def scenario():
            task = asyncio.create_task(
                _gather(sched, [("a", f"n{i}") for i in range(4)], script)
            )
            await asyncio.sleep(0.1)
            stats = sched.stats()
            assert stats["max_in_flight"] == 3 and stats["slots_used"] == 3
            assert stats["upstreams"][0]["in_flight"] == 3
            assert stats["upstreams"][0]["queued"] == 1
            await task

        asyncio.run(scenario())
    finally:
        sched.shutdown()

    # 同一上游 FIFO：开始执行顺序 == 提交顺序
    assert script.start_names("a") == ["n0", "n1", "n2", "n3"]


def test_two_upstreams_share_free_slots_round_robin():
    # 两边都够慢，保证 0.1s 后断言时首批 4 个任务都在途
    script = ScriptedQueries({"a": 0.4, "b": 0.4})
    sched = QueryScheduler(["a", "b"], max_in_flight=4)
    try:
        async def scenario():
            task = asyncio.create_task(
                _gather(
                    sched,
                    [("a", "a1"), ("a", "a2"), ("b", "b1"), ("b", "b2")],
                    script,
                )
            )
            await asyncio.sleep(0.1)
            stats = {u["upstream_id"]: u for u in sched.stats()["upstreams"]}
            # 空槽轮流：4 个槽位两边各拿 2，而不是慢的 A 先占满
            assert stats["a"]["in_flight"] == 2
            assert stats["b"]["in_flight"] == 2
            await task

        asyncio.run(scenario())
    finally:
        sched.shutdown()


def test_slow_upstream_backlog_does_not_starve_fast_upstream():
    """慢上游 A 的队列没排完，快上游 B 的新查询也轮得到发送。"""
    script = ScriptedQueries({"a": 0.3, "b": 0.05})
    sched = QueryScheduler(["a", "b"], max_in_flight=2)
    try:
        async def scenario():
            task = asyncio.create_task(
                _gather(
                    sched,
                    [("a", "a1"), ("a", "a2"), ("a", "a3"), ("a", "a4")],
                    script,
                )
            )
            await asyncio.sleep(0.1)  # a1/a2 在途，a3/a4 排队
            # B 新来一个查询
            b_task = asyncio.create_task(
                sched.submit("b1", "b", script.make_factory("b1"))
            )
            results = await asyncio.gather(task, b_task)
            return results

        asyncio.run(scenario())
    finally:
        sched.shutdown()

    a_starts = dict(script.started["a"])
    b_starts = dict(script.started["b"])
    # 空槽轮流：b1 在 a4 之前发出（若 A 独占空槽，b1 要等到 a4 之后）
    assert b_starts["b1"] < a_starts["a4"]
    # a3 先于 b1（先排先发，二者在同一轮空槽里按游标顺序）
    assert a_starts["a3"] < b_starts["b1"]
    # FIFO
    assert script.start_names("a") == ["a1", "a2", "a3", "a4"]


def test_reroute_moves_pending_and_preserves_order():
    script = ScriptedQueries({"a": 0.3, "b": 0.05})
    sched = QueryScheduler(["a", "b"], max_in_flight=2)
    try:
        async def scenario():
            task = asyncio.create_task(
                _gather(
                    sched,
                    [("a", "n1"), ("a", "n2"), ("a", "n3"), ("a", "n4")],
                    script,
                )
            )
            await asyncio.sleep(0.1)
            stats = {u["upstream_id"]: u for u in sched.stats()["upstreams"]}
            # n1/n2 已在途，n3/n4 还在 A 队列
            assert stats["a"]["in_flight"] == 2
            assert stats["a"]["queued_names"] == ["n3", "n4"]

            # 改路：n3 -> B，n4 留 A。在途的 n1/n2 不动
            sched.reroute(lambda name: "b" if name == "n3" else "a")
            stats = {u["upstream_id"]: u for u in sched.stats()["upstreams"]}
            assert stats["a"]["in_flight"] == 2 and stats["b"]["in_flight"] == 0
            assert stats["a"]["queued_names"] == ["n4"]
            assert stats["b"]["queued_names"] == ["n3"]

            # 再改一次：待发的两个全去 B，原始先后（n3 在 n4 前）保留
            sched.reroute(lambda name: "b")
            stats = {u["upstream_id"]: u for u in sched.stats()["upstreams"]}
            assert stats["a"]["queued"] == 0
            assert stats["b"]["queued_names"] == ["n3", "n4"]

            await task

        asyncio.run(scenario())
    finally:
        sched.shutdown()

    # n3/n4 最终都由 B 执行且保持原序；n1/n2 仍由 A 执行（在途不迁移）
    assert script.start_names("a") == ["n1", "n2"]
    assert script.start_names("b") == ["n3", "n4"]


def test_reroute_on_idle_scheduler_is_safe():
    sched = QueryScheduler(["a", "b"], max_in_flight=2)
    try:
        # 还没绑定事件循环时改路不应报错（没有任何待发查询）
        sched.reroute(lambda name: "b")

        async def scenario():
            sched.reroute(lambda name: "b")  # 绑定后空队列改路
            assert sched.stats()["slots_used"] == 0

        asyncio.run(scenario())
    finally:
        sched.shutdown()


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        QueryScheduler([], max_in_flight=2)
    with pytest.raises(ValueError):
        QueryScheduler(["a"], max_in_flight=0)


def test_submit_unknown_upstream_rejected():
    sched = QueryScheduler(["a"], max_in_flight=1)
    try:
        async def scenario():
            with pytest.raises(KeyError):
                await sched.submit("x.test", "nope", lambda uid: (lambda: None))

        asyncio.run(scenario())
    finally:
        sched.shutdown()


def test_factory_error_releases_slot_and_keeps_dispatcher_alive():
    """构建查询函数抛错：异常传给该任务、槽位释放，后续任务不受影响。"""
    script = ScriptedQueries({"a": 0.05})
    sched = QueryScheduler(["a"], max_in_flight=1)
    try:
        async def scenario():
            def boom(_uid):
                raise RuntimeError("build failed")

            bad = asyncio.create_task(sched.submit("bad.test", "a", boom))
            with pytest.raises(RuntimeError, match="build failed"):
                await bad
            # dispatcher 仍存活：再提交一个正常任务可以正常完成
            ok = await sched.submit("ok.test", "a", script.make_factory("ok.test"))
            assert ok == ("a", "ok.test")
            assert sched.stats()["slots_used"] == 0

        asyncio.run(scenario())
    finally:
        sched.shutdown()


# ---------------------------------------------------------------------
# Resolver 集成：走真实 FakeUpstream UDP
# ---------------------------------------------------------------------
def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def harness():
    port_a, port_b = _free_port(), _free_port()
    fake_a = FakeUpstream("127.0.0.1", port_a, ip_base=10, delay=0.3)
    fake_b = FakeUpstream("127.0.0.1", port_b, ip_base=172, delay=0.05)
    registry = UpstreamRegistry(
        [
            Upstream("upstream-a", "A", "127.0.0.1", port_a, 1),
            Upstream("upstream-b", "B", "127.0.0.1", port_b, 2),
        ]
    )
    rules = RuleStore("upstream-a")
    cache = DnsCache()
    resolver = Resolver(rules, cache, registry, max_in_flight=2)
    yield fake_a, fake_b, rules, cache, resolver
    resolver.shutdown()
    fake_a.stop()
    fake_b.stop()


def test_configurable_in_flight_limit(harness):
    fake_a, fake_b, rules, cache, resolver = harness

    async def scenario():
        tasks = [
            asyncio.create_task(resolver.resolve(f"h{i}.corp.local")) for i in range(4)
        ]
        await asyncio.sleep(0.1)
        stats = resolver.stats()
        assert stats["max_in_flight"] == 2
        assert stats["slots_used"] == 2  # 上限生效，没有发出第 3 个
        return await asyncio.gather(*tasks)

    results = asyncio.run(scenario())
    assert all(r["status"] == "OK" for r in results)
    assert fake_a.query_count == 4


def test_cache_hit_does_not_take_slot(harness):
    fake_a, fake_b, rules, cache, resolver = harness

    async def scenario():
        first = await resolver.resolve("cached.corp.local")
        assert first["cached_hit"] is False
        again = await resolver.resolve("cached.corp.local")
        assert again["cached_hit"] is True
        # 缓存命中后没有任何在途/排队
        stats = resolver.stats()
        assert stats["slots_used"] == 0
        assert all(u["queued"] == 0 and u["in_flight"] == 0 for u in stats["upstreams"])

    asyncio.run(scenario())
    assert fake_a.query_count == 1


def test_rule_change_reroutes_pending_queries_immediately(harness):
    """等待改规则：未发出的查询随即走新上游，在途的槽位继续占用到回包。"""
    fake_a, fake_b, rules, cache, resolver = harness
    rules.set_rule("b.test", "upstream-b")

    async def scenario():
        # 4 个默认走 A 的查询先排队（上限 2，两个在途两个待发）
        tasks = [
            asyncio.create_task(resolver.resolve(f"h{i}.corp.local")) for i in range(4)
        ]
        await asyncio.sleep(0.1)
        mid = resolver.stats()
        assert mid["slots_used"] == 2
        queued_a_before = mid["upstreams"][0]["queued"]
        assert queued_a_before == 2

        # 把默认上游切到 B：待发的两个应立即转到 B 队列，在途的留在 A
        rules.set_rule("", "upstream-b")
        await asyncio.sleep(0.05)
        stats = {u["upstream_id"]: u for u in resolver.stats()["upstreams"]}
        assert stats["upstream-a"]["in_flight"] == 2  # 在途不迁移、槽位不释放
        assert stats["upstream-a"]["queued"] == 0
        assert stats["upstream-b"]["queued"] == 2

        return await asyncio.gather(*tasks)

    results = asyncio.run(scenario())
    # 前两个来自 A（在途旧回包被判陈旧丢弃，重查到 B —— 见下一条测试），
    # 这里只校验待发的两个确实由 B 回答
    assert fake_b.query_count >= 2
    assert all(r["status"] == "OK" for r in results)


def test_stale_reattempt_queues_at_tail_under_current_route(harness):
    """陈旧回包丢弃后，重查按当前路由重新排队（在 B 的队尾）。"""
    fake_a, fake_b, rules, cache, resolver = harness

    async def scenario():
        t1 = asyncio.create_task(resolver.resolve("first.corp.local"))
        t2 = asyncio.create_task(resolver.resolve("second.corp.local"))
        await asyncio.sleep(0.1)  # 两个都在 A 在途
        # 切到 B：两个旧回包都会判陈旧，重查都排到 B（B 原本无队列）
        rules.set_rule("", "upstream-b")
        r1, r2 = await asyncio.gather(t1, t2)
        return r1, r2

    r1, r2 = asyncio.run(scenario())
    assert r1["stale_discarded"] == 1 and r2["stale_discarded"] == 1
    assert r1["upstream_id"] == "upstream-b" and r2["upstream_id"] == "upstream-b"
    assert fake_a.query_count == 2 and fake_b.query_count == 2
    # 陈旧回包未污染缓存
    assert cache.get("first.corp.local").upstream_id == "upstream-b"
