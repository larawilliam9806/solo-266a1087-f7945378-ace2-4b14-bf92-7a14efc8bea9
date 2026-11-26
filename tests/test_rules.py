"""RuleStore：最长匹配、版本号、陈旧判定。"""
from __future__ import annotations

import pytest

from dns_forwarder.rules import RuleStore


def test_longest_suffix_match_and_default():
    store = RuleStore("a")
    assert store.match("anything.test").suffix == ""
    assert store.match("anything.test").upstream_id == "a"

    assert store.set_rule("corp.local", "b") is True
    assert store.set_rule("dc.corp.local", "a") is True

    assert store.match("host.dc.corp.local").suffix == "dc.corp.local"
    assert store.match("host.corp.local").suffix == "corp.local"
    assert store.match("host.other.local").suffix == ""
    # 后缀本身也算命中
    assert store.match("corp.local").suffix == "corp.local"


def test_version_bumps_only_on_real_change():
    store = RuleStore("a")
    assert store.match("x.test").version == 1

    # 指向同一个上游：无变化，版本不动
    assert store.set_rule("", "a") is False
    assert store.match("x.test").version == 1

    # 真实变更：版本 +1
    assert store.set_rule("", "b") is True
    assert store.match("x.test").version == 2

    # 新建规则版本从 1 开始
    assert store.set_rule("z.test", "a") is True
    assert store.match("h.z.test").version == 1


def test_delete_restores_longest_match_and_marks_stale():
    store = RuleStore("a")
    store.set_rule("corp.local", "b")
    rule = store.match("host.corp.local")
    assert rule.suffix == "corp.local" and rule.version == 1

    assert store.delete_rule("corp.local") is True
    # 原后缀规则已不存在 -> 陈旧
    assert store.is_stale("host.corp.local", rule.suffix, rule.version) is True
    # 现在回退到默认规则
    assert store.match("host.corp.local").suffix == ""

    assert store.delete_rule("corp.local") is False
    with pytest.raises(ValueError):
        store.delete_rule("")


def test_stale_detection_versions_and_suffix_switch():
    store = RuleStore("a")
    rule = store.match("host.corp.local")  # 默认 v1

    # 没动过 -> 新鲜
    assert store.is_stale("host.corp.local", rule.suffix, rule.version) is False

    # 默认规则换上游：版本变化 -> 陈旧（即便后来又改回，版本也不会回到旧值）
    store.set_rule("", "b")
    assert store.is_stale("host.corp.local", "", 1) is True
    store.set_rule("", "a")
    assert store.is_stale("host.corp.local", "", 1) is True
    assert store.is_stale("host.corp.local", "", 3) is False

    # 中途插入更长的规则：后缀变了 -> 陈旧
    base = store.match("host.corp.local")
    store.set_rule("corp.local", "b")
    assert store.is_stale("host.corp.local", base.suffix, base.version) is True

    # 更长规则被删除、回退到默认：同样陈旧
    long_rule = store.match("host.corp.local")
    store.delete_rule("corp.local")
    assert store.is_stale("host.corp.local", long_rule.suffix, long_rule.version) is True


def test_re_add_suffix_keeps_old_versions_stale():
    store = RuleStore("a")
    store.set_rule("corp.local", "b")
    r1 = store.match("h.corp.local")
    store.delete_rule("corp.local")
    store.set_rule("corp.local", "b")  # 同样的上游，但版本继续递增
    r2 = store.match("h.corp.local")
    assert r2.version == r1.version + 1
    assert store.is_stale("h.corp.local", r1.suffix, r1.version) is True
