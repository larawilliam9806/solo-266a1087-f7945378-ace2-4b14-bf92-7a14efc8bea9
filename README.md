# 内网 DNS 转发演示（FastAPI + dnslib）

一个面向运维场景的 DNS 转发小工具：网页上维护 **后缀 → 上游** 规则，试查 A 记录，
页面展示应答地址或 NXDOMAIN、命中的上游以及缓存命中情况。配套两个**可在线调整答复延迟**
的本地 mock 上游，一条 `docker compose up` 即可启动整套环境。

## 功能与语义

| 需求 | 实现方式 |
| --- | --- |
| 最长后缀匹配选上游 | `RuleStore.match()`，空后缀 `*` 为默认规则，始终兜底 |
| TTL 缓存地址 | A 记录取应答 RR 的最小 TTL（RFC 2181），惰性过期 |
| TTL 缓存 NXDOMAIN | 取权威区 SOA 的 MINIMUM（且不超过 SOA 自身 TTL，RFC 2308）；无 SOA 时兜底 30s；NODATA 同样否定缓存 |
| 改规则只清受影响缓存 | 比对改动前后每个缓存域名的**有效上游**，只剔除真正换了上游的条目 |
| 等待期间可改规则、可查别的域名 | 阻塞 UDP 查询放线程池（`run_in_executor`），事件循环不阻塞 |
| 等待期间规则变过（含改回原样）则丢弃旧回包 | 每个后缀带单调递增版本号；回包后 `is_stale()` 校验，陈旧即丢弃并按现行规则重查，且绝不写缓存 |
| 可调答复延迟的本地上游 | mock 上游带 HTTP 控制口 `GET/POST /delay`，页面直接调 |

### 陈旧回包判定（关键设计）

每条规则后缀持有一个 **version**，仅当该后缀的上游**真实变更**时 +1（新建、改上游、
删除均算；指向同一上游的重复保存不产生任何变化）。删除后缀时保留版本墓碑，
删了又建回版本继续递增。查询发出时记录 `(命中后缀, version)`，回包后：

1. 该后缀已不存在或版本对不上 → 陈旧；
2. 重新做最长匹配，命中的后缀变了（中途插入/删除了更长规则）→ 陈旧。

因此"改到 B 又改回 A"时版本号已递增，A 的旧回包照样作废，保证结果只来自现行规则。

## 目录结构

```
dns_forwarder/
  main.py        FastAPI 入口（页面 + 规则/上游/缓存/解析 API）
  rules.py       规则表：最长匹配、版本号、陈旧判定
  cache.py       TTL 缓存（正向 + 否定）、惰性过期、精确剔除
  resolver.py    异步解析编排：线程池查询、陈旧丢弃重查
  dns_query.py   dnslib UDP 查询、RCODE/TTL 解析
  upstream.py    上游注册表与延迟控制口客户端
  static/index.html  单页前端（无构建步骤）
mock_upstream/
  server.py      mock DNS 上游（dnslib）+ HTTP 延迟控制口
tests/           26 个 pytest 用例（规则/缓存/TTL/陈旧丢弃/精确清缓存/API）
docker-compose.yml
Dockerfile
```

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 页面：http://localhost:8000
# 上游 A 控制口：localhost:8301 ，上游 B：localhost:8302
# 上游 DNS（可选调试）：A=localhost:5301/udp，B=localhost:5302/udp
```

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 两个 mock 上游（各开一个终端）
UPSTREAM_ID=upstream-a IP_BASE=10  python -m mock_upstream.server
UPSTREAM_ID=upstream-b IP_BASE=172 python -m mock_upstream.server

# Web 服务（第三个终端）
UPSTREAM1_HOST=127.0.0.1 UPSTREAM1_DNS_PORT=53 UPSTREAM1_CONTROL_PORT=8080 \
UPSTREAM2_HOST=127.0.0.1 UPSTREAM2_DNS_PORT=53 UPSTREAM2_CONTROL_PORT=8080 \
uvicorn dns_forwarder.main:app --port 8000
```

> 本地裸跑时两个上游不能占用同一组端口，可用 `DNS_PORT` / `CONTROL_PORT`
> 环境变量错开，并相应调整 Web 侧的 `UPSTREAM*_*_PORT`。

## 页面操作说明

1. **上游与延迟**：给 upstream-a / upstream-b 设置答复延迟（0–30 秒，支持小数）；
2. **后缀规则**：输入后缀（留空表示默认规则）选择上游后保存，可删除非默认规则，
   保存后提示本次精确清除了哪些域名的缓存；
3. **试查 A 记录**：普通域名返回 A 记录，`nx.` 开头的域名 mock 上游回 NXDOMAIN。
   结果区显示状态、地址、命中上游与后缀、缓存命中/回源、TTL 与剩余时间、耗时，
   以及等待期间被丢弃的陈旧回包数量；
4. **缓存**：每 3 秒自动刷新，展示 TTL 倒计时进度条，可手动清空。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/upstreams` | 上游列表及当前延迟 |
| GET/POST | `/api/upstreams/{id}/delay` | 读取/设置答复延迟 |
| GET | `/api/rules` | 规则列表（含版本号） |
| PUT | `/api/rules` | 新增/修改规则，返回精确剔除的缓存域名 |
| DELETE | `/api/rules?suffix=` | 删除非默认规则 |
| GET | `/api/resolve?name=` | 试查 A 记录 |
| GET | `/api/cache` | 缓存条目与统计 |
| DELETE | `/api/cache` | 清空缓存 |

## 测试

```bash
pip install pytest httpx
pytest
```

重点用例：

- `test_stale_reply_discarded_when_rule_changes_midflight`：在途改规则，旧回包丢弃、按新规则重查；
- `test_change_back_still_discards_old_reply`：改过去又改回，旧回包仍作废；
- `test_longer_suffix_inserted_midflight`：在途插入更长后缀规则触发重选上游；
- `test_concurrent_queries_and_edits_do_not_block_each_other`：慢查询在途时 API 不阻塞；
- `test_rule_change_evicts_only_affected_domains`：只清实际换了上游的域名缓存；
- `test_nxdomain_cached_with_soa_minimum_ttl`：NXDOMAIN 按 SOA MINIMUM 否定缓存。
