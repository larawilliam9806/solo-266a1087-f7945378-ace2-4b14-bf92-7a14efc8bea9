"""API 级测试：规则变更只清实际换了上游的域名缓存 + 端到端试查。"""
from __future__ import annotations

import socket
import threading
import time

import pytest
from fastapi.testclient import TestClient

from dns_forwarder.main import create_app
from dns_forwarder.upstream import Upstream, UpstreamRegistry

from .fakes import FakeUpstream


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def client():
    port_a, port_b = _free_port(), _free_port()
    fake_a = FakeUpstream("127.0.0.1", port_a, ip_base=10)
    fake_b = FakeUpstream("127.0.0.1", port_b, ip_base=172)
    registry = UpstreamRegistry(
        [
            Upstream("upstream-a", "A", "127.0.0.1", port_a, 1),
            Upstream("upstream-b", "B", "127.0.0.1", port_b, 2),
        ]
    )
    app = create_app(upstreams=registry)
    with TestClient(app) as c:
        yield c, fake_a, fake_b, app
    fake_a.stop()
    fake_b.stop()


def test_resolve_endpoint_and_cache_listing(client):
    c, fake_a, fake_b, app = client
    r = c.get("/api/resolve", params={"name": "git.corp.local"})
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "OK" and data["cached_hit"] is False
    assert data["addresses"] == ["10.0.0.1"]

    r2 = c.get("/api/resolve", params={"name": "git.corp.local"})
    assert r2.json()["cached_hit"] is True

    listing = c.get("/api/cache").json()
    assert listing["stats"]["entries"] == 1
    assert listing["entries"][0]["name"] == "git.corp.local"


def test_rule_change_evicts_only_affected_domains(client):
    c, fake_a, fake_b, app = client
    # 默认走 A：造两条缓存
    c.get("/api/resolve", params={"name": "host1.corp.local"})
    c.get("/api/resolve", params={"name": "host2.corp.local"})
    # corp.local 后缀指到 B：host1/host2 有效上游变了，应被精确剔除；
    # 再在 B 下重新查询产生缓存
    r = c.put("/api/rules", json={"suffix": "corp.local", "upstream_id": "upstream-b"})
    assert sorted(r.json()["evicted"]) == ["host1.corp.local", "host2.corp.local"]
    c.get("/api/resolve", params={"name": "host1.corp.local"})
    c.get("/api/resolve", params={"name": "app.corp.local"})
    c.get("/api/resolve", params={"name": "db.corp.local"})
    # 另一个后缀留在 A
    c.put("/api/rules", json={"suffix": "other.local", "upstream_id": "upstream-a"})
    c.get("/api/resolve", params={"name": "keep.other.local"})

    assert c.get("/api/cache").json()["stats"]["entries"] == 4

    # 把 corp.local 从 B 换成 A：
    # - corp.local 下的 3 个域名有效上游都变了（host1/app/db 直接切 A）
    # - other.local 的缓存上游没变，必须保留
    r = c.put("/api/rules", json={"suffix": "corp.local", "upstream_id": "upstream-a"})
    body = r.json()
    assert body["changed"] is True
    assert sorted(body["evicted"]) == sorted(
        ["host1.corp.local", "app.corp.local", "db.corp.local"]
    )
    remaining = {e["name"] for e in c.get("/api/cache").json()["entries"]}
    assert remaining == {"keep.other.local"}


def test_saving_same_upstream_changes_nothing(client):
    c, fake_a, fake_b, app = client
    c.get("/api/resolve", params={"name": "x.test"})
    r = c.put("/api/rules", json={"suffix": "", "upstream_id": "upstream-a"})
    assert r.json()["changed"] is False
    assert c.get("/api/cache").json()["stats"]["entries"] == 1


def test_delete_rule_evicts_fallback_affected_only(client):
    c, fake_a, fake_b, app = client
    c.put("/api/rules", json={"suffix": "corp.local", "upstream_id": "upstream-b"})
    c.get("/api/resolve", params={"name": "a.corp.local"})
    c.get("/api/resolve", params={"name": "b.other.local"})  # 默认 A

    r = c.delete("/api/rules", params={"suffix": "corp.local"})
    assert r.json()["evicted"] == ["a.corp.local"]
    remaining = {e["name"] for e in c.get("/api/cache").json()["entries"]}
    assert remaining == {"b.other.local"}


def test_validation_errors(client):
    c, *_ = client
    assert c.get("/api/resolve", params={"name": "bad name!!"}).status_code == 400
    assert (
        c.put("/api/rules", json={"suffix": "x.local", "upstream_id": "nope"}).status_code
        == 400
    )
    assert c.delete("/api/rules", params={"suffix": ""}).status_code == 400
    assert c.delete("/api/rules", params={"suffix": "gone.local"}).status_code == 404


def test_index_page_served(client):
    c, *_ = client
    r = c.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


def test_scheduler_endpoint_reports_queued_and_inflight(monkeypatch):
    """网页轮询的 /api/scheduler：在途数、排队数、全局槽位。"""
    port_a, port_b = _free_port(), _free_port()
    fake_a = FakeUpstream("127.0.0.1", port_a, ip_base=10, delay=0.4)
    fake_b = FakeUpstream("127.0.0.1", port_b, ip_base=172, delay=0.05)
    registry = UpstreamRegistry(
        [
            Upstream("upstream-a", "A", "127.0.0.1", port_a, 1),
            Upstream("upstream-b", "B", "127.0.0.1", port_b, 2),
        ]
    )
    monkeypatch.setenv("UPSTREAM_MAX_CONCURRENCY", "2")
    app = create_app(upstreams=registry)
    with TestClient(app) as c:
        # 初始空闲
        stats = c.get("/api/scheduler").json()
        assert stats["max_in_flight"] > 0 and stats["slots_used"] == 0
        by_id = {u["upstream_id"]: u for u in stats["upstreams"]}
        assert by_id["upstream-a"]["queued"] == 0
        assert by_id["upstream-a"]["in_flight"] == 0

        # 并发 4 个试查（共享同一套调度），在途中观察一次
        results = []
        errors = []

        def worker(i):
            try:
                results.append(c.get("/api/resolve", params={"name": f"q{i}.corp.local"}))
            except Exception as exc:  # pragma: no cover - 仅防线程崩溃
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.15)
        mid = c.get("/api/scheduler").json()
        by_id = {u["upstream_id"]: u for u in mid["upstreams"]}
        assert mid["max_in_flight"] == 2
        assert mid["slots_used"] == 2
        assert by_id["upstream-a"]["in_flight"] == 2
        assert by_id["upstream-a"]["queued"] == 2
        assert len(by_id["upstream-a"]["queued_names"]) == 2
        total_busy = sum(u["in_flight"] + u["queued"] for u in mid["upstreams"])
        assert total_busy == 4
        for t in threads:
            t.join()
        assert not errors
        assert all(r.status_code == 200 for r in results)

        # 全部结束后槽位归还
        done = c.get("/api/scheduler").json()
        assert done["slots_used"] == 0
        assert all(u["in_flight"] == 0 and u["queued"] == 0 for u in done["upstreams"])

    fake_a.stop()
    fake_b.stop()
