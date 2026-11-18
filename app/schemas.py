"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，"
            "包含排队等连接、EXPLAIN 预检与执行的时间"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok"]
    executed: bool = Field(description="原 SQL 是否真正发送到了数据库后端")
    estimated_cost: float = Field(
        description="EXPLAIN (FORMAT JSON) 估算的计划总成本 (Total Cost)"
    )
    cost_limit: float = Field(description="配置的成本上限 MAX_QUERY_COST")
    in_flight_cost: float | None = Field(
        default=None,
        description="本次准入后（或拒绝时其他查询）的累计在途估算成本",
    )
    plan_ms: float | None = Field(default=None, description="预检耗时（毫秒）")
    wait_ms: float | None = Field(default=None, description="排队等连接耗时")
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None


class ErrorResponse(BaseModel):
    """错误回执。`error` 为稳定错误码；顶层字段按错误类型附带上下文。"""

    error: dict[str, Any] = Field(
        description="`{code, message}`，如 cost_limit_exceeded / plan_failed"
    )
    status: Literal["rejected", "timeout", "error"] | None = None
    executed: bool = Field(
        default=False, description="原 SQL 是否曾被发送执行"
    )
    phase: str | None = Field(
        default=None, description="失败阶段：begin / plan / execute"
    )
    estimated_cost: float | None = Field(
        default=None, description="预检得到的估算成本（如已得到）"
    )
    cost_limit: float | None = None
    in_flight_cost: float | None = Field(
        default=None, description="拒绝时其他在途查询占用的累计估算成本"
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    plan_ms: float | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    sqlstate: str | None = None
    detail: str | None = None


class PoolStatusModel(BaseModel):
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool


class AdmissionStatusModel(BaseModel):
    limit: float
    in_flight_cost: float
    in_flight_count: int
    released_total: int
    rejected_total: int


class HealthResponse(BaseModel):
    status: Literal["healthy"]
    pool: PoolStatusModel
    admission: AdmissionStatusModel
