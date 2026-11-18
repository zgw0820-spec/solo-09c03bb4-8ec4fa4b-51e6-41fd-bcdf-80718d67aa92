# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；每个请求在同一只读事务内
先用 `EXPLAIN (FORMAT JSON)` 估算执行成本，单条或当前在途查询的累计估算
成本超过上限时**返回估算值并拒绝执行原 SQL**；计划获取失败或总期限耗尽
同样不执行。超时立即取消数据库语句，确认事务结束才复用连接，无法确认则
弃置连接并补建；迟到结果不会改写超时回执。后续查询始终拿到属于自己的
结果。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队、EXPLAIN 预检与执行**的总预算（单调时钟），
从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，附 `estimated_cost`/`cost_limit`/`plan_ms`，`rows` 为本次查询自身结果 |
| 单条或累计估算成本超限 | 429 | `error.code=cost_limit_exceeded`，`status=rejected`、`estimated_cost`、`in_flight_cost`，`executed=false`，**原 SQL 未执行** |
| 计划获取失败 | 503 | `error.code=plan_failed`、`sqlstate`，`executed=false`，**原 SQL 未执行** |
| 总期限在预检阶段耗尽 | 504 | `error.code=query_timeout`、`phase=plan`，`executed=false`，**原 SQL 未执行** |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`、`phase=execute`，附 `cancelled`、`connection_discarded` |
| 已准入的 SQL 执行报错 | 400 | `error.code=sql_error`，`executed=true`、`phase=execute`、`sqlstate`；事务已回滚、额度已释放 |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数，以及成本额度快照
（`admission.limit`、`in_flight_cost`、`in_flight_count`、累计释放/拒绝数）。

## 关键设计

### 1. 容量恒为 2，全部阶段共用总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
排队、EXPLAIN 预检与执行共用同一预算；任一阶段耗尽都不会继续发送原 SQL。

### 2. 执行成本准入（`app/admission.py` + `app/executor.py`）

1. 取得连接后 `BEGIN READ ONLY`，**在同一只读事务内**对原 SQL 执行
   `EXPLAIN (FORMAT JSON)`（无 `ANALYZE`，只计划不执行），取计划顶层
   `Total Cost` 作为估算值。
2. 将估算值交给 `AdmissionLedger.reserve()`：单条估算超过
   `MAX_QUERY_COST`，或**该值与所有在途查询累计估算之和**超过上限，即
   返回 `429` 与估算值，原 SQL 不发送；该检查与记账之间没有 `await`，
   因此并发请求不会同时看到额度而超额放行。
3. 已准入的查询在**执行与事务收尾期间持续占用**额度；成功 `COMMIT`、
   超时取消/ROLLBACK、SQL 错误、连接弃置、请求被取消等所有出口都在
   `finally` 中释放，不永久占额。
4. 计划获取失败（如未知表、语法错误）→ `503 plan_failed`；预检耗时耗尽
   总期限 → `504 query_timeout (phase=plan)`；两者都不执行原 SQL。

### 3. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行（预检与执行同一事务），
   会话默认 `default_transaction_read_only=on`，并设置局部
   `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 4. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，修正共享状态中的“已放弃”标记并调用 libpq `PQcancel`；
   预检阶段尚未发送原 SQL 的任务会观察到该标记，**不会在回执决定后
   再启动查询**。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`；
   成本额度在事务收尾完成后才释放。

### 5. 连接级故障的弃置补建与无界感知重试

后端被带外终止（如 `pg_terminate_backend`，甚至两条同时被终止）时，
连接级错误触发弃置并由池补建；只读语句在同一总预算内重试（最多三次
尝试，足以跨过两条失效后端），客户端无感知，且每次重试都重新预检、
重新占用额度。若补建暂时失败，池临时缩水并在下一次获取时懒建补回。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn（MAX_QUERY_COST=5000）
python -m tests.selftest         # HTTP 端到端自测（80 项）
python tests/test_protocol.py    # 预检/准入/超时/取消协议直测（40 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/admission/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_DSN` | `postgresql://fault_ro:fault_ro@db:5432/faultdb` | 只读角色 DSN |
| `POOL_SIZE` | `2` | 固定物理连接数 |
| `MAX_QUERY_COST` | `50000` | 成本上限（PostgreSQL 计划成本单位），同时约束单条与在途累计；Compose/本地自测设为 `5000` |
| `STATEMENT_TIMEOUT_GRACE_MS` | `1250` | 服务器兜底 `statement_timeout` 相对总期限的宽限 |
| `MAX_SQL_CHARS` | `10000` | SQL 长度上限 |
| `CONNECT_TIMEOUT` | `10` | 建连超时（秒） |

