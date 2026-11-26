"""FastAPI 入口：页面 + 规则/上游/缓存/解析 API。"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .cache import DnsCache
from .models import name_matches_suffix, normalize_name, normalize_suffix
from .resolver import Resolver
from .rules import RuleStore
from .upstream import Upstream, UpstreamRegistry

STATIC_DIR = Path(__file__).parent / "static"


class RuleBody(BaseModel):
    suffix: str = Field("", description="后缀；空串或 * 表示默认规则")
    upstream_id: str


class DelayBody(BaseModel):
    delay_seconds: float = Field(ge=0, le=30)


def _effective_upstream(snapshot: dict[str, tuple[str, int]], name: str) -> str:
    """在某一版规则快照上做最长匹配，返回上游 id。"""
    candidates = [s for s in snapshot if name_matches_suffix(name, s)]
    best = max(candidates, key=len)
    return snapshot[best][0]


def _build_upstreams() -> list[Upstream]:
    def one(prefix: str, default_id: str, default_host: str) -> Upstream:
        uid = os.getenv(f"{prefix}_ID", default_id)
        host = os.getenv(f"{prefix}_HOST", default_host)
        return Upstream(
            id=uid,
            label=os.getenv(f"{prefix}_LABEL", uid),
            host=host,
            dns_port=int(os.getenv(f"{prefix}_DNS_PORT", "53")),
            control_port=int(os.getenv(f"{prefix}_CONTROL_PORT", "8080")),
        )

    return [
        one("UPSTREAM1", "upstream-a", "upstream-a"),
        one("UPSTREAM2", "upstream-b", "upstream-b"),
    ]


def create_app(
    upstreams: UpstreamRegistry | None = None,
    rules: RuleStore | None = None,
    cache: DnsCache | None = None,
) -> FastAPI:
    if upstreams is None:
        upstreams = UpstreamRegistry(_build_upstreams())
    default_id = os.getenv("DEFAULT_UPSTREAM_ID", upstreams.list()[0].id)
    if rules is None:
        rules = RuleStore(default_id)
    if cache is None:
        cache = DnsCache()
    resolver = Resolver(rules, cache, upstreams)

    app = FastAPI(title="内网 DNS 转发演示", version="1.0.0")
    app.state.rules = rules
    app.state.cache = cache
    app.state.upstreams = upstreams
    app.state.resolver = resolver

    # ---------------- 页面 ----------------
    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # ---------------- 上游 ----------------
    @app.get("/api/upstreams")
    async def list_upstreams() -> list[dict]:
        result = []
        loop = asyncio.get_running_loop()
        for u in upstreams.list():
            item = {"id": u.id, "label": u.label, "address": u.address}
            try:
                item["delay_seconds"] = await loop.run_in_executor(
                    None, upstreams.fetch_delay, u.id
                )
            except Exception as exc:  # 控制口不可用时不拖垮整页
                item["delay_seconds"] = None
                item["delay_error"] = str(exc)
            result.append(item)
        return result

    @app.get("/api/upstreams/{upstream_id}/delay")
    async def get_delay(upstream_id: str) -> dict:
        loop = asyncio.get_running_loop()
        try:
            delay = await loop.run_in_executor(None, upstreams.fetch_delay, upstream_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(502, f"读取上游延迟失败: {exc}") from exc
        return {"upstream_id": upstream_id, "delay_seconds": delay}

    @app.post("/api/upstreams/{upstream_id}/delay")
    async def set_delay(upstream_id: str, body: DelayBody) -> dict:
        loop = asyncio.get_running_loop()
        try:
            delay = await loop.run_in_executor(
                None, upstreams.set_delay, upstream_id, body.delay_seconds
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(502, f"设置上游延迟失败: {exc}") from exc
        return {"upstream_id": upstream_id, "delay_seconds": delay}

    # ---------------- 规则 ----------------
    @app.get("/api/rules")
    def list_rules() -> list[dict]:
        return [
            {
                "suffix": r.suffix or "*",
                "upstream_id": r.upstream_id,
                "version": r.version,
                "is_default": r.suffix == "",
            }
            for r in rules.list_rules()
        ]

    @app.put("/api/rules")
    async def upsert_rule(body: RuleBody) -> dict:
        try:
            suffix = normalize_suffix(body.suffix)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if body.upstream_id not in upstreams.ids():
            raise HTTPException(400, f"未知上游: {body.upstream_id}")

        before = rules.snapshot()
        changed = rules.set_rule(suffix, body.upstream_id)
        evicted: list[str] = []
        if changed:
            after = rules.snapshot()
            # 只清掉"实际换了上游"的域名的缓存；上游没变的保留
            evicted = cache.evict(
                lambda e: _effective_upstream(before, e.key)
                != _effective_upstream(after, e.key)
            )
        return {
            "changed": changed,
            "evicted": evicted,
            "rules": [
                {
                    "suffix": r.suffix or "*",
                    "upstream_id": r.upstream_id,
                    "version": r.version,
                    "is_default": r.suffix == "",
                }
                for r in rules.list_rules()
            ],
        }

    @app.delete("/api/rules")
    async def delete_rule(suffix: str = Query(..., description="要删除的后缀")) -> dict:
        try:
            norm = normalize_suffix(suffix)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if norm == "":
            raise HTTPException(400, "默认规则不可删除")

        before = rules.snapshot()
        if not rules.delete_rule(norm):
            raise HTTPException(404, f"规则不存在: {suffix}")
        after = rules.snapshot()
        evicted = cache.evict(
            lambda e: _effective_upstream(before, e.key)
            != _effective_upstream(after, e.key)
        )
        return {"deleted": norm, "evicted": evicted}

    # ---------------- 缓存 ----------------
    @app.get("/api/cache")
    def list_cache() -> dict:
        import time as _time

        mono = _time.monotonic()
        return {
            "stats": cache.stats(),
            "entries": [e.to_dict(mono) for e in cache.list_entries()],
        }

    @app.delete("/api/cache")
    def clear_cache() -> dict:
        return {"cleared": cache.clear()}

    # ---------------- 调度状态（各上游排队数 / 在途数 / 全局槽位） ----------------
    @app.get("/api/scheduler")
    def scheduler_status() -> dict:
        return resolver.stats()

    # ---------------- 试查 ----------------
    @app.get("/api/resolve")
    async def resolve(name: str = Query(..., description="要查询的域名")) -> dict:
        try:
            normalize_name(name)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return await resolver.resolve(name)

    return app


app = create_app()
