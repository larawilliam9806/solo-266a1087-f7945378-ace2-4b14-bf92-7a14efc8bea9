"""后缀 -> 上游 规则表。

- 空后缀 "" 表示默认规则，始终存在且不可删除。
- 查找使用最长后缀匹配。
- 每个后缀持有一个单调递增的 version：只有当该后缀的上游 *实际变更*
  （含新建、删除、改到别的上游）时才 +1。查询发出后若版本发生过变化，
  说明等待期间命中的规则被动过，旧回包必须丢弃。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from .models import name_matches_suffix, normalize_suffix


@dataclass(frozen=True)
class Rule:
    suffix: str
    upstream_id: str
    version: int  # 该后缀规则的版本号，变更一次 +1


class RuleStore:
    def __init__(self, default_upstream_id: str):
        self._lock = threading.RLock()
        # suffix -> (upstream_id, version)
        self._rules: dict[str, tuple[str, int]] = {"": (default_upstream_id, 1)}
        # 已删除后缀的版本墓碑：删了又建回时版本继续递增，
        # 保证删除前发出的查询不会把新规则误判成"没动过"
        self._tombstones: dict[str, int] = {}

    # ---------- 读取 ----------
    def match(self, name: str) -> Rule:
        """按最长后缀匹配选择上游。"""
        with self._lock:
            candidates = [
                suffix for suffix in self._rules if name_matches_suffix(name, suffix)
            ]
            # 最长的后缀优先；"" 长度为 0 自然排最后
            best = max(candidates, key=len)
            upstream_id, version = self._rules[best]
            return Rule(best, upstream_id, version)

    def list_rules(self) -> list[Rule]:
        with self._lock:
            return [
                Rule(suffix, upstream_id, version)
                for suffix, (upstream_id, version) in sorted(
                    self._rules.items(), key=lambda kv: (-len(kv[0]), kv[0])
                )
            ]

    def snapshot(self) -> dict[str, tuple[str, int]]:
        with self._lock:
            return dict(self._rules)

    # ---------- 变更 ----------
    def set_rule(self, suffix: str, upstream_id: str) -> bool:
        """新增/修改一条规则。

        返回 True 表示上游确实变了（调用方应据此清理缓存），
        False 表示指向的上游没变化，什么都不做。
        """
        suffix = normalize_suffix(suffix)
        with self._lock:
            current = self._rules.get(suffix)
            if current is not None and current[0] == upstream_id:
                return False
            old_version = current[1] if current is not None else self._tombstones.get(suffix, 0)
            self._rules[suffix] = (upstream_id, old_version + 1)
            self._tombstones.pop(suffix, None)
            return True

    def delete_rule(self, suffix: str) -> bool:
        """删除一条规则。默认规则不可删除。返回是否实际删除。"""
        suffix = normalize_suffix(suffix)
        if suffix == "":
            raise ValueError("默认规则不可删除")
        with self._lock:
            if suffix not in self._rules:
                return False
            _, version = self._rules.pop(suffix)
            self._tombstones[suffix] = version
            return True

    def is_stale(self, name: str, suffix: str, version: int) -> bool:
        """判断查询发出后，该域名命中的规则是否被动过。

        两种情况都算陈旧：
        1. 原后缀的规则版本变了（改上游 / 删掉又建回 / 被删除）；
        2. 现在最长匹配落到了 *另一个* 后缀上（中途插入了更长的规则，
           或更长的规则被删除导致回退）。
        """
        with self._lock:
            current = self._rules.get(suffix)
            if current is None or current[1] != version:
                return True
            return self.match(name).suffix != suffix
