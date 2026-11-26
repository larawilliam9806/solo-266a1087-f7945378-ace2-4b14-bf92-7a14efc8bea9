"""回源查询的统一调度器。

网页试查和 /api/resolve 共用同一个 Resolver，也就共用本调度器，语义：

- **全局回源并发上限** ``max_in_flight`` 可配置：所有上游的在途 UDP 查询
  合计不超过该上限，缓存命中的查询不进调度器、不占槽位；
- **每上游一条 FIFO 队列**：同一上游先排先发；
- **槽位分配**：
  - 只有一台上游有待发查询时，它可以用满全部空槽；
  - 两台（或多台）都有待发查询时，空出的槽位按上游顺序**轮流**分配，
    慢上游的队列没排完，快上游的新查询也轮得到发送；
- **改路重排**：``reroute()`` 把所有尚未发出的查询按新路由重新归队，
  归队时保留各自最初的排队先后（全局序号），在途查询不动；
- **槽位占用到本次 UDP 调用结束**：查询一旦发出就占着槽位，直到回包
  （或超时/报错）为止，期间即使规则改路也不提前释放；
- 陈旧回包的重查由上层重新 ``submit()``，自然排到当前路由队列的队尾。

实现上所有可变状态都只在事件循环线程内访问（dispatcher 任务 +
executor 完成回调），因此不需要额外加锁；跨线程的规则变更通知通过
``loop.call_soon_threadsafe`` 切回循环线程执行。
"""
from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable

DEFAULT_MAX_IN_FLIGHT = 4


@dataclass
class _Job:
    """一次待发/在途的 UDP 查询。"""

    seq: int                      # 全局入队序号，重排时用于保留原始先后
    name: str                     # 查询域名
    upstream_id: str              # 当前路由到的上游（重排只改待发任务的该字段）
    # 发送瞬间按 *当前* upstream_id 构建零参阻塞调用；这样改路重排后
    # 真正发出去的查询指向新上游，而不是入队时闭包的旧地址
    func_factory: Callable[[str], Callable[[], object]]
    fut: asyncio.Future           # 上层 await 的结果 future


class QueryScheduler:
    def __init__(
        self,
        upstream_ids: list[str],
        max_in_flight: int = DEFAULT_MAX_IN_FLIGHT,
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        if not upstream_ids:
            raise ValueError("至少需要一个上游")
        if max_in_flight < 1:
            raise ValueError("回源并发上限必须 >= 1")
        self._upstream_ids = list(upstream_ids)
        self.max_in_flight = max_in_flight
        self._pool = executor or ThreadPoolExecutor(
            max_workers=max_in_flight, thread_name_prefix="dns-query"
        )
        # 以下都是事件循环线程内的状态，随 bind_loop 初始化/重置
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None
        self._events: asyncio.Queue | None = None
        self._queues: dict[str, deque[_Job]] = {}
        self._inflight: dict[str, int] = {}
        self._slots_used = 0
        self._cursor = 0  # 轮流分配游标，只在多上游都有待发时推进
        self._seq = 0

    # ---------- 生命周期 ----------
    def bind_loop(self) -> None:
        """确保 dispatcher 任务运行在当前事件循环里。

        每个事件循环只启动一个；换了循环（测试里反复 asyncio.run）就
        重新初始化整套循环内状态。
        """
        loop = asyncio.get_running_loop()
        if self._loop is loop and self._task is not None:
            return
        self._loop = loop
        self._events = asyncio.Queue()
        self._queues = {uid: deque() for uid in self._upstream_ids}
        self._inflight = {uid: 0 for uid in self._upstream_ids}
        self._slots_used = 0
        self._cursor = 0
        self._task = loop.create_task(self._run())

    def shutdown(self) -> None:
        task, loop = self._task, self._loop
        if task is not None and loop is not None and loop.is_running():
            loop.call_soon_threadsafe(task.cancel)
        self._pool.shutdown(wait=False)

    @property
    def loop(self):
        """当前 dispatcher 绑定的事件循环；尚无查询时为 None。"""
        return self._loop

    # ---------- 提交与重排 ----------
    async def submit(
        self,
        name: str,
        upstream_id: str,
        func_factory: Callable[[str], Callable[[], object]],
    ):
        """把一次查询排入对应上游的 FIFO 队列，等待调度并返回结果。

        ``func_factory(uid)`` 在任务真正被发送时按当时的上游 id 构建
        阻塞调用，保证改路重排后查询发往新上游。
        """
        if self._loop is None or self._task is None:
            self.bind_loop()
        assert self._events is not None
        if upstream_id not in self._queues:
            raise KeyError(f"未知上游: {upstream_id}")
        self._seq += 1
        job = _Job(
            seq=self._seq,
            name=name,
            upstream_id=upstream_id,
            func_factory=func_factory,
            fut=asyncio.get_running_loop().create_future(),
        )
        self._queues[upstream_id].append(job)
        self._events.put_nowait(None)
        return await job.fut

    def reroute(self, route_fn: Callable[[str], str]) -> None:
        """按新路由把所有**待发**查询重新归队（须在事件循环线程执行）。

        先按最初的全局序号取出全部待发任务，逐个改判上游再按原顺序
        放回各上游队列，因此跨上游的相对先后也保持不变；在途任务不在
        任何队列里，不受影响。
        """
        if self._events is None:
            return
        pending = sorted(
            (job for q in self._queues.values() for job in q),
            key=lambda j: j.seq,
        )
        for q in self._queues.values():
            q.clear()
        for job in pending:
            job.upstream_id = route_fn(job.name)
            if job.upstream_id not in self._queues:
                # 路由指向了已注销的上游，退回第一个上游，避免任务丢失
                job.upstream_id = self._upstream_ids[0]
            self._queues[job.upstream_id].append(job)
        self._events.put_nowait(None)

    # ---------- 调度主循环 ----------
    async def _run(self) -> None:
        assert self._events is not None
        while True:
            await self._events.get()
            self._pump()

    def _pump(self) -> None:
        """尽量按规则把待发任务发出去，直到槽位用尽或没有待发任务。"""
        while self._slots_used < self.max_in_flight:
            uid = self._pick_upstream()
            if uid is None:
                return
            job = self._queues[uid].popleft()  # 同一上游 FIFO
            self._inflight[uid] += 1
            self._slots_used += 1
            try:
                # 发送瞬间按当前路由上游构建阻塞调用
                func = job.func_factory(uid)
            except BaseException as exc:  # 构建失败不能拖死整个 dispatcher
                self._inflight[uid] -= 1
                self._slots_used -= 1
                if not job.fut.done():
                    job.fut.set_exception(exc)
                continue
            future = self._loop.run_in_executor(self._pool, func)  # type: ignore[union-attr]
            future.add_done_callback(lambda f, j=job, u=uid: self._on_done(j, u, f))

    def _pick_upstream(self) -> str | None:
        """选下一个该发的上游；没有待发任务返回 None。"""
        nonempty = [uid for uid in self._upstream_ids if self._queues[uid]]
        if not nonempty:
            return None
        if len(nonempty) == 1:
            # 只有一台有待发：它用满空槽，游标不动
            return nonempty[0]
        # 多台都有待发：从游标起轮流，保证每台都轮得到
        n = len(self._upstream_ids)
        for _ in range(n):
            cand = self._upstream_ids[self._cursor % n]
            self._cursor += 1
            if self._queues[cand]:
                return cand
        return None  # 理论上不可达

    def _on_done(self, job: _Job, uid: str, future: asyncio.Future) -> None:
        # executor 完成回调在事件循环线程执行
        self._inflight[uid] -= 1
        self._slots_used -= 1  # 槽位占用到本次 UDP 调用真正结束
        if not job.fut.done():
            exc = future.exception()
            if exc is not None:
                job.fut.set_exception(exc)
            else:
                job.fut.set_result(future.result())
        assert self._events is not None
        self._events.put_nowait(None)  # 空出槽位，立即再调度

    # ---------- 可观测性 ----------
    def stats(self) -> dict:
        return {
            "max_in_flight": self.max_in_flight,
            "slots_used": self._slots_used,
            "free_slots": max(0, self.max_in_flight - self._slots_used),
            "upstreams": [
                {
                    "upstream_id": uid,
                    "queued": len(self._queues.get(uid, ())),
                    "in_flight": self._inflight.get(uid, 0),
                    "queued_names": [j.name for j in self._queues.get(uid, ())],
                }
                for uid in self._upstream_ids
            ],
        }
