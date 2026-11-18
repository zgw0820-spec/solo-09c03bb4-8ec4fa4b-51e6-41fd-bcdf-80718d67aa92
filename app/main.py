"""FastAPI application: a two-connection read-only fault-log query API.

Run locally::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Swagger UI is served at ``/docs``, ReDoc at ``/redoc``.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import settings
from .errors import InvalidDeadlineError, QueryServiceError
from .executor import QueryExecutor
from .pool import QueryConnectionPool
from .schemas import (
    ErrorResponse,
    HealthResponse,
    QueryRequest,
    QueryResponse,
)
from .validation import validate_single_select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")

pool = QueryConnectionPool(settings)
executor = QueryExecutor(pool, settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Eagerly open both connections at startup; close them on shutdown.
    await pool.open()
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(
    title="机房设备故障日志只读查询 API",
    description=(
        "值班分析员共用的只读 SQL 查询入口。固定两条 PostgreSQL 连接，"
        "提交单条 SELECT 与总截止时间；每个请求在同一只读事务内先用 "
        "`EXPLAIN (FORMAT JSON)` 估算成本，单条或当前在途查询累计估算成本"
        "超过 `MAX_QUERY_COST` 时直接拒绝且不执行原 SQL；计划获取失败或"
        "总期限耗尽同样不执行。排队、预检与执行共用一个总期限，超时立即"
        "取消语句，确认事务结束才复用连接，无法确认则弃置并补建。"
    ),
    version="2.0.0",
    lifespan=lifespan,
)


@app.exception_handler(QueryServiceError)
async def _service_error_handler(_: Request, exc: QueryServiceError):
    extra = {k: v for k, v in exc.extra.items() if k != "detail"}
    return JSONResponse(
        status_code=exc.http_status,
        content={"error": {"code": exc.code, "message": str(exc)}, **extra},
    )


@app.exception_handler(Exception)
async def _unexpected_handler(_: Request, exc: Exception):
    logger.exception("unhandled error")
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": f"{type(exc).__name__}: {exc}",
            }
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["运维"])
async def health() -> HealthResponse:
    """连接池与成本额度状态：容量、空闲/占用、创建/弃置、在途成本。"""
    status = await pool.status()
    return HealthResponse(
        status="healthy",
        pool={
            "size": status.size,
            "idle": status.idle,
            "in_use": status.in_use,
            "total_created": status.total_created,
            "total_discarded": status.total_discarded,
            "closed": status.closed,
        },
        admission=executor.ledger.snapshot(),
    )


@app.post(
    "/query",
    response_model=QueryResponse,
    tags=["查询"],
    responses={
        400: {"description": "SQL 不符合只读 SELECT 约束，或已准入的 SQL 执行报错",
              "model": ErrorResponse},
        422: {"description": "请求体或截止时间不合法", "model": ErrorResponse},
        429: {"description": "估算成本超过单条或累计上限，原 SQL 未执行",
              "model": ErrorResponse},
        503: {"description": "排队超时 / 计划获取失败 / 连接被弃置",
              "model": ErrorResponse},
        504: {"description": "总截止时间耗尽（预检或执行阶段），原 SQL 未执行完",
              "model": ErrorResponse},
    },
)
async def query(body: QueryRequest) -> QueryResponse:
    """预检成本后执行一条只读 SELECT。

    ``timeout_ms`` 是包含排队、EXPLAIN 预检和执行的总预算。计划估算失败、
    预算耗尽或成本超限时都不会执行原 SQL。
    """
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    sql = validate_single_select(body.sql, settings.max_sql_chars)

    # Absolute monotonic deadline: waiting + planning + execution share one
    # budget.  A deadline expiring while queued surfaces as a 503
    # acquire_timeout; during planning as a 504; during execution as a 504.
    deadline = time.monotonic() + body.timeout_ms / 1000.0
    result = await executor.execute(sql, deadline)
    return QueryResponse(**result)
