"""上游注册表。

上游是本演示用的 mock DNS 服务（见 mock_upstream/），每个上游同时监听：
- UDP 53：DNS 服务（答复延迟可通过 HTTP 控制口调整）；
- HTTP 8080：GET/POST /delay 读取或修改答复延迟（秒，支持小数）。
"""
from __future__ import annotations

import json
import threading
import urllib.request
from dataclasses import dataclass


@dataclass
class Upstream:
    id: str
    label: str
    host: str
    dns_port: int
    control_port: int

    @property
    def address(self) -> str:
        return f"{self.host}:{self.dns_port}"

    def _control_url(self, path: str) -> str:
        return f"http://{self.host}:{self.control_port}{path}"


class UpstreamRegistry:
    def __init__(self, upstreams: list[Upstream]):
        if not upstreams:
            raise ValueError("至少需要一个上游")
        self._lock = threading.RLock()
        self._items: dict[str, Upstream] = {u.id: u for u in upstreams}

    def get(self, upstream_id: str) -> Upstream:
        with self._lock:
            if upstream_id not in self._items:
                raise KeyError(f"未知上游: {upstream_id}")
            return self._items[upstream_id]

    def list(self) -> list[Upstream]:
        with self._lock:
            return list(self._items.values())

    def ids(self) -> list[str]:
        return [u.id for u in self.list()]

    # ---------- 延迟控制（同步阻塞调用，由上层丢进线程池） ----------
    def fetch_delay(self, upstream_id: str) -> float:
        u = self.get(upstream_id)
        with urllib.request.urlopen(u._control_url("/delay"), timeout=3) as resp:
            data = json.loads(resp.read().decode())
        return float(data["delay_seconds"])

    def set_delay(self, upstream_id: str, delay_seconds: float) -> float:
        if delay_seconds < 0:
            raise ValueError("延迟不能为负")
        u = self.get(upstream_id)
        body = json.dumps({"delay_seconds": float(delay_seconds)}).encode()
        req = urllib.request.Request(
            u._control_url("/delay"),
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
        return float(data["delay_seconds"])
