"""DNS 缓存。

按 DNS TTL 规则缓存两类结果：
- 成功的 A 记录（取应答记录里的最小 TTL）；
- NXDOMAIN / NODATA（取权威区 SOA 的 MINIMUM TTL，无 SOA 时用兜底值）。

条目在后台不主动清理，读取时惰性过期；规则变更时由上层按
"实际换了上游的域名" 精确剔除。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

# RFC 2308 要求 NXDOMAIN 也有 TTL；上游没给 SOA 时的兜底值
DEFAULT_NEGATIVE_TTL = 30


@dataclass
class CacheEntry:
    key: str                      # 归一化域名
    status: str                   # "OK" | "NXDOMAIN" | "NODATA"
    addresses: tuple[str, ...]    # OK 时的 A 记录地址
    ttl: int                      # 生效 TTL（秒）
    upstream_id: str
    suffix: str                   # 命中的规则后缀
    expire_at: float              # monotonic 过期时刻
    created_at: float = field(default_factory=time.monotonic)

    def is_expired(self, now: float | None = None) -> bool:
        return (now or time.monotonic()) >= self.expire_at

    def remaining_ttl(self, now: float | None = None) -> int:
        return max(0, int(round(self.expire_at - (now or time.monotonic()))))

    def to_dict(self, now: float | None = None) -> dict:
        now = now or time.monotonic()
        return {
            "name": self.key,
            "status": self.status,
            "addresses": list(self.addresses),
            "ttl": self.ttl,
            "remaining_ttl": self.remaining_ttl(now),
            "upstream_id": self.upstream_id,
            "suffix": self.suffix,
        }


class DnsCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._store: dict[str, CacheEntry] = {}
        self.hits = 0

    def get(self, key: str) -> CacheEntry | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                del self._store[key]
                return None
            self.hits += 1
            return entry

    def put(self, entry: CacheEntry) -> None:
        with self._lock:
            self._store[entry.key] = entry

    def remove(self, key: str) -> bool:
        with self._lock:
            return self._store.pop(key, None) is not None

    def evict(self, predicate) -> list[str]:
        """剔除所有满足 predicate(entry) 的条目，返回被剔除的域名。"""
        with self._lock:
            victims = [key for key, entry in self._store.items() if predicate(entry)]
            for key in victims:
                del self._store[key]
            return victims

    def clear(self) -> int:
        with self._lock:
            n = len(self._store)
            self._store.clear()
            return n

    def list_entries(self) -> list[CacheEntry]:
        with self._lock:
            now = time.monotonic()
            alive = [e for e in self._store.values() if not e.is_expired(now)]
            return sorted(alive, key=lambda e: e.key)

    def stats(self) -> dict:
        with self._lock:
            now = time.monotonic()
            alive = sum(1 for e in self._store.values() if not e.is_expired(now))
            return {"entries": alive, "hits": self.hits}
