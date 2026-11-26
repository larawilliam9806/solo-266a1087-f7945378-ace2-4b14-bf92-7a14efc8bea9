"""域名归一化与校验。"""
from __future__ import annotations

import re

_LABEL_RE = re.compile(r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$")
# 下划线（如 _service._tcp 之类）在真实环境中也常见，这里放行
_LABEL_RE_LOOSE = re.compile(r"^[A-Za-z0-9_-]{1,63}$")


def normalize_name(name: str) -> str:
    """把域名归一化为不带尾点的小写形式。"""
    if name is None:
        raise ValueError("域名不能为空")
    n = name.strip().lower().rstrip(".")
    if not n:
        raise ValueError("域名不能为空")
    if len(n) > 253:
        raise ValueError("域名过长")
    labels = n.split(".")
    for label in labels:
        if not _LABEL_RE.match(label) and not _LABEL_RE_LOOSE.match(label):
            raise ValueError(f"非法域名标签: {label!r}")
    return n


def normalize_suffix(suffix: str) -> str:
    """归一化后缀。空串代表默认规则（根）。"""
    if suffix is None:
        return ""
    s = suffix.strip().lower().rstrip(".")
    if s == "" or s == "*":
        return ""
    return normalize_name(s)


def name_matches_suffix(name: str, suffix: str) -> bool:
    """name 是否属于 suffix 之下（含完全相等）。suffix 为空表示默认命中。"""
    if suffix == "":
        return True
    return name == suffix or name.endswith("." + suffix)
