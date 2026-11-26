# 内网 DNS 转发演示（FastAPI + dnslib）

一个面向运维场景的 DNS 转发小工具：网页上维护 **后缀 → 上游** 规则，试查 A 记录，
页面展示应答地址或 NXDOMAIN、命中的上游以及缓存命中情况。配套两个**可在线调整答复延迟**
的本地 mock 上游，一条 `docker compose up` 即可启动整套环境。

## 功能与语义

| 需求 | 实现方式 |
| --- | --- |
| 网页试查与 `/api/resolve` 共用一套调度 | 两者都走同一个 `Resolver` → `QueryScheduler`，缓存与并发状态全局共享 |
| 回源总并发上限可配置 | 环境变量 `UPSTREAM_MAX_CONCURRENCY`（默认 4），所有上游的在途 UDP 查询合计受限；`QueryScheduler(max_in_flight=...)` 也可直接构造 |
| 缓存命中直接返回 | 命中即返回，不进调度器、不占回源槽位 |
| 单上游有待发时用满空槽 | `_pick_upstream()`：仅一个上游队列非空时连续把空槽分给它 |
| 两台都有待发时空槽轮流分配 | 游标在两台上游间轮转，慢上游队列没排完，快上游的新查询也轮得到发送 |
| 同一上游先排先发 | 每上游一条 FIFO 队列，按全局入队顺序 `popleft()` |
| 改规则即时重排待发查询 | `RuleStore` 变更监听 → `QueryScheduler.reroute()`：待发查询按新路由重新归队，按原始全局序号保留排队先后；在途查询不动 |
| 在途查询占槽到本次调用结束 | 槽位在 UDP 查询发出时占用，回包/超时/报错后才释放，期间改路不提前释放 |
| 陈旧回包丢弃、重查排队尾 | 规则版本在查询**实际发出时**快照；回包判陈旧即丢弃，按当前路由重新 `submit()`，自然排到队尾，且绝不写缓存 |
| 页面观察队列变化 | 上游表实时显示各上游**排队数 / 在途数**（悬停排队数可见待发域名），每秒刷新；配合 mock 上游的延迟设置即可观察 |
| 最长后缀匹配选上游 | `RuleStore.match()`，空后缀 `*` 为默认规则，始终兜底 |
| TTL 缓存地址 | A 记录取应答 RR 的最小 TTL（RFC 2181），惰性过期 |
| TTL 缓存 NXDOMAIN | 取权威区 SOA 的 MINIMUM（且不超过 SOA 自身 TTL，RFC 2308）；无 SOA 时兜底 30s；NODATA 同样否定缓存 |
| 改规则只清受影响缓存 | 比对改动前后每个缓存域名的**有效上游**，只剔除真正换了上游的条目 |
| 等待期间可改规则、可查别的域名 | 阻塞 UDP 查询放线程池，dispatcher 事件驱动，事件循环不阻塞 |
| 可调答复延迟的本地上游 | mock 上游带 HTTP 控制口 `GET/POST /delay`，页面直接调 |

### 调度器工作方式（关键设计）

`QueryScheduler` 为每个上游维护一条 FIFO 队列，外加 `max_in_flight` 个全局槽位，
由单个事件驱动的 dispatcher 任务在槽位空出或新任务入队时反复 `_pump()`：

1. 只有一台上游的队列非空 → 该上游连续取走所有空槽（用满上限）；
2. 两台（或多台）队列都非空 → 从轮转游标开始选下一个非空上游，空槽轮流分给两台，
   任何一台都不会被饿死；
3. 改规则时 `reroute()` 把所有**待发**任务按全局序号取出、按新路由重新归位，
   跨上游的相对先后也保持不变；已经发出的在途查询不迁移，槽位占用到回包为止；
4. 查询函数在**发送瞬间**按当时的上游地址构建，因此重排后真正发出的 UDP 查询
   指向新上游；规则版本快照也在发送时记录，避免把重排后的合法回包误判为陈旧。

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
  main.py        FastAPI 入口（页面 + 规则/上游/缓存/解析/调度状态 API）
  rules.py       规则表：最长匹配、版本号、陈旧判定、变更监听
  cache.py       TTL 缓存（正向 + 否定）、惰性过期、精确剔除
  scheduler.py   统一回源调度：全局并发槽位、每上游 FIFO、轮流分配、改路重排
  resolver.py    异步解析编排：缓存直返、经调度器回源、陈旧丢弃重查
  dns_query.py   dnslib UDP 查询、RCODE/TTL 解析
  upstream.py    上游注册表与延迟控制口客户端
  static/index.html  单页前端（无构建步骤）
mock_upstream/
  server.py      mock DNS 上游（dnslib）+ HTTP 延迟控制口
tests/           pytest 用例（规则/缓存/TTL/陈旧丢弃/精确清缓存/调度公平性/重排/API）
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

1. **上游与延迟**：给 upstream-a / upstream-b 设置答复延迟（0–30 秒，支持小数）。
   表格中的**排队 / 在途**两列每秒刷新，悬停排队数可查看待发域名，底部显示
   回源并发上限、已用/空闲槽位；
2. **观察调度**：把 upstream-a 延迟调大（如 3s），连续试查 5 个不同域名，
   可以看到前 `UPSTREAM_MAX_CONCURRENCY` 个进入在途、其余在 A 队列排队；
   此时再给 upstream-b 发一个查询，空出的槽位会轮流分给 B，B 不必等 A 的队列排完。
   排队期间修改后缀规则，待发查询会立刻按新路由归队（排队先后不变）；
3. **后缀规则**：输入后缀（留空表示默认规则）选择上游后保存，可删除非默认规则，
   保存后提示本次精确清除了哪些域名的缓存；
4. **试查 A 记录**：普通域名返回 A 记录，`nx.` 开头的域名 mock 上游回 NXDOMAIN。
   结果区显示状态、地址、命中上游与后缀、缓存命中/回源、TTL 与剩余时间、耗时，
   以及等待期间被丢弃的陈旧回包数量；
5. **缓存**：每 3 秒自动刷新，展示 TTL 倒计时进度条，可手动清空。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/upstreams` | 上游列表及当前延迟 |
| GET/POST | `/api/upstreams/{id}/delay` | 读取/设置答复延迟 |
| GET | `/api/rules` | 规则列表（含版本号） |
| PUT | `/api/rules` | 新增/修改规则，返回精确剔除的缓存域名 |
| DELETE | `/api/rules?suffix=` | 删除非默认规则 |
| GET | `/api/resolve?name=` | 试查 A 记录（与页面共用同一调度器） |
| GET | `/api/scheduler` | 全局槽位用量 + 各上游排队数/在途数/待发域名 |
| GET | `/api/cache` | 缓存条目与统计 |
| DELETE | `/api/cache` | 清空缓存 |

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `UPSTREAM_MAX_CONCURRENCY` | `4` | 回源总并发上限（所有上游在途 UDP 查询合计） |
| `UPSTREAM1_*` / `UPSTREAM2_*` | — | 两个上游的 id / host / DNS 端口 / 控制口端口 |
| `DEFAULT_UPSTREAM_ID` | 第一个上游 | 默认规则（空后缀）指向的上游 |

## 测试

```bash
pip install pytest httpx
pytest
```

重点用例：

- `test_slow_upstream_backlog_does_not_starve_fast_upstream`：慢上游队列没排完，快上游的新查询照样轮得到发送；
- `test_two_upstreams_share_free_slots_round_robin`：两台都有待发时空槽轮流分配；
- `test_single_upstream_fills_all_free_slots`：只有一台有待发时用满空槽，且同上游 FIFO；
- `test_reroute_moves_pending_and_preserves_order`：改规则后待发查询按新路由归队、保留原排队先后，在途查询不迁移；
- `test_rule_change_reroutes_pending_queries_immediately`：端到端验证改规则即时重排、在途槽位不提前释放；
- `test_stale_reattempt_queues_at_tail_under_current_route`：陈旧回包丢弃后按当前路由排队尾重查；
- `test_cache_hit_does_not_take_slot` / `test_configurable_in_flight_limit`：缓存命中不占槽、并发上限可配置；
- `test_stale_reply_discarded_when_rule_changes_midflight`：在途改规则，旧回包丢弃、按新规则重查；
- `test_change_back_still_discards_old_reply`：改过去又改回，旧回包仍作废；
- `test_longer_suffix_inserted_midflight`：在途插入更长后缀规则触发重选上游；
- `test_concurrent_queries_and_edits_do_not_block_each_other`：慢查询在途时 API 不阻塞；
- `test_rule_change_evicts_only_affected_domains`：只清实际换了上游的域名缓存；
- `test_nxdomain_cached_with_soa_minimum_ttl`：NXDOMAIN 按 SOA MINIMUM 否定缓存。
