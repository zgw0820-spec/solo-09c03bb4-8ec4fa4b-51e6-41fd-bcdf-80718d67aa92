"""Request execution: cost pre-check, deadline accounting, cancellation.

Execution protocol for one request::

    1. acquire one of the two pooled connections (waiting counts against the
       caller's total deadline);
    2. open an explicit ``BEGIN READ ONLY`` transaction;
    3. run ``EXPLAIN (FORMAT JSON)`` on the *original* SQL inside that same
       read-only transaction (``EXPLAIN`` without ``ANALYZE`` only plans,
       it never executes the statement);
    4. read the planner's ``Total Cost`` and ask the admission ledger to
       reserve it — single-query and cumulative limits are enforced there;
    5. only if admitted, run the original SQL in the same transaction,
       fetch, and ``COMMIT``;
    6. finalise the transaction (commit / verified ROLLBACK / discard) and
       only then hand the cost quota back.  Every exit path releases it.

A plan failure, an exhausted deadline, a rejected cost or a cancelled
statement all take the same road: the original SQL is *not* executed and
the connection is proved IDLE before reuse, or discarded and rebuilt.

The statement coroutine runs as a *shielded* task so a late result can never
overwrite the already-decided timeout receipt.  On deadline, libpq
``PQcancel`` asks the server to abort; the shielded task is awaited (no
coroutine may keep touching the connection); then ``ROLLBACK`` +
``transaction_status`` prove the backend is IDLE.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Any

import psycopg

from .admission import AdmissionLedger, AdmissionTicket
from .config import Settings
from .errors import (
    CostLimitExceeded,
    PlanFailed,
    QueryFailed,
    QueryTimeout,
)
from .pool import QueryConnectionPool

logger = logging.getLogger("executor")

# ``EXPLAIN`` plans of pathological queries can be slow to build, but the
# statement coroutine itself is always bounded by the shared deadline.
_EXPLAIN_PREFIX = "EXPLAIN (FORMAT JSON) "


class _DeadlineExhausted(Exception):
    """The total deadline expired before the original SQL was sent."""

    def __init__(self, phase: str) -> None:
        super().__init__(f"deadline exhausted during {phase}")
        self.phase = phase


class _ConnectionLost(Exception):
    """Wrap a connection-level failure so the outer loop can retry once."""

    def __init__(self, error: BaseException) -> None:
        super().__init__(str(error))
        self.error = error


class _Abandoned(Exception):
    """The request was settled elsewhere; the statement must not proceed."""


def _consume_task_exception(task: asyncio.Task) -> None:
    """Prevent "exception was never retrieved" noise for abandoned tasks."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug("abandoned statement task ended: %r", exc)


class QueryExecutor:
    def __init__(
        self,
        pool: QueryConnectionPool,
        settings: Settings,
        ledger: AdmissionLedger | None = None,
    ) -> None:
        self._pool = pool
        self._settings = settings
        self._ledger = ledger or AdmissionLedger(settings.max_query_cost)

    @property
    def ledger(self) -> AdmissionLedger:
        return self._ledger

    async def execute(self, sql: str, deadline: float) -> dict[str, Any]:
        """Plan-check and execute one validated read-only statement.

        A dead-on-arrival backend (e.g. terminated out-of-band while idle)
        is discarded and rebuilt; the statement is planned again and, if
        admitted, retried within the same total deadline.
        """
        last_conn_error: psycopg.Error | None = None
        # At most two physical backends exist, so at most two retries are
        # needed: each connection-level failure discards that backend and
        # builds a replacement, and the third attempt must therefore land on
        # a live one.  Every attempt shares the caller's single deadline.
        for attempt in (1, 2, 3):
            wait_started = time.monotonic()
            # Propagate PoolAcquireTimeout as a 503 receipt.  When this
            # raises, no connection was borrowed and nothing was planned or
            # executed for this request.
            conn = await self._pool.acquire(deadline)
            waited_ms = round((time.monotonic() - wait_started) * 1000, 3)
            acquired_at = time.monotonic()
            try:
                return await self._execute_on(
                    conn, sql, deadline, waited_ms, acquired_at
                )
            except _ConnectionLost as lost:
                # The backend died before/while planning or executing.  It
                # has already been discarded and its quota released; retry
                # once inside the same deadline -- re-planning is harmless.
                last_conn_error = lost.error  # type: ignore[assignment]
                if deadline - time.monotonic() <= 0:
                    raise QueryTimeout(
                        "连接故障后总截止时间已耗尽，未再重试，原 SQL 未完成",
                        extra={"status": "timeout", "executed": False,
                               "phase": "connection"},
                    ) from last_conn_error
                logger.warning(
                    "connection-level error (%d/%d), discarded; retrying "
                    "within remaining budget: %s",
                    attempt,
                    3,
                    last_conn_error,
                )
                continue

        # Defensive: the loop either returns or raises earlier.
        raise last_conn_error  # pragma: no cover

    # ------------------------------------------------------------------ #
    async def _execute_on(
        self,
        conn: psycopg.AsyncConnection,
        sql: str,
        deadline: float,
        waited_ms: float,
        acquired_at: float,
    ) -> dict[str, Any]:
        """Run plan+execute in one read-only transaction on ``conn``.

        Always finalises the connection (release or discard) and releases
        the admission quota -- including on cancellation -- so neither the
        pool slot nor the cost budget can leak.
        """
        state: dict[str, Any] = {
            "phase": "begin",
            "sql_sent": False,
            "cost": None,
            "plan_ms": None,
            "ticket": None,
            "abandoned": False,
        }
        settled = False
        task: asyncio.Task | None = None

        try:
            task = asyncio.ensure_future(
                self._plan_and_execute(conn, sql, deadline, state)
            )
            try:
                cost, rows = await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=max(0.0, deadline - time.monotonic()),
                )
            except (asyncio.TimeoutError, TimeoutError):
                timeout_receipt = await self._handle_timeout(
                    conn, task, state, waited_ms, acquired_at
                )
                settled = True
                raise timeout_receipt
            except asyncio.CancelledError:
                # The request coroutine itself is being cancelled (server
                # shutdown or client abort).  Stop the statement, prove the
                # backend idle, then let the cancellation propagate.
                logger.warning("request coroutine cancelled; settling backend")
                await self._settle_task(conn, task, state)
                settled = True
                raise
            except _DeadlineExhausted as exc:
                # The plan finished but no time is left to execute: never
                # send the original SQL.
                state["abandoned"] = True
                await self._rollback_or_discard(conn)
                settled = True
                raise QueryTimeout(
                    "总截止时间在预检阶段耗尽，原 SQL 未执行",
                    extra={
                        **self._receipt(
                            state, waited_ms, acquired_at, executed=False,
                            phase=exc.phase,
                        ),
                        "status": "timeout",
                    },
                ) from exc
            except CostLimitExceeded as exc:
                # Rejected after planning: the original SQL was not sent.
                state["abandoned"] = True
                await self._rollback_or_discard(conn)
                settled = True
                exc.extra.update(
                    self._receipt(
                        state, waited_ms, acquired_at,
                        executed=False, phase="plan",
                    )
                )
                raise
            except PlanFailed as exc:
                state["abandoned"] = True
                await self._rollback_or_discard(conn)
                settled = True
                exc.extra.update(
                    self._receipt(
                        state, waited_ms, acquired_at,
                        executed=False, phase="plan",
                    )
                )
                raise
            except psycopg.errors.QueryCanceled as exc:
                # Server-side statement_timeout backstop fired.  During the
                # plan phase this means the deadline was exhausted before
                # execution; never send the statement.
                if not state["sql_sent"]:
                    state["abandoned"] = True
                    await self._rollback_or_discard(conn)
                    settled = True
                    raise QueryTimeout(
                        "预检阶段超过总截止时间（服务器兜底超时），原 SQL 未执行",
                        extra={
                            **self._receipt(
                                state, waited_ms, acquired_at,
                                executed=False, phase="plan", cancelled=False,
                            ),
                            "status": "timeout",
                        },
                    ) from exc
                timeout_receipt = await self._handle_timeout(
                    conn, task, state, waited_ms, acquired_at, backstop=True
                )
                settled = True
                raise timeout_receipt
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Connection-level failure (backend terminated/restarted):
                # discard, rebuild via the pool, retry once if budget
                # remains.  Safe because the statement is read-only and the
                # transaction is gone with the backend.
                logger.warning("connection-level error, discarding: %s", exc)
                await self._pool.discard_and_replace(conn)
                settled = True
                raise _ConnectionLost(exc) from exc
            except psycopg.Error as exc:
                # Statement-level error while planning or executing.
                state["abandoned"] = True
                await self._rollback_or_discard(conn)
                settled = True
                receipt = self._receipt(
                    state, waited_ms, acquired_at,
                    executed=bool(state["sql_sent"]),
                    phase="execute" if state["sql_sent"] else "plan",
                    status=None if not state["sql_sent"] else "error",
                )
                receipt.update(
                    {"sqlstate": exc.sqlstate, "detail": str(exc)}
                )
                if not state["sql_sent"]:
                    raise PlanFailed(
                        "计划获取失败，原 SQL 未执行", extra=receipt
                    ) from exc
                raise QueryFailed(
                    "原 SQL 执行报错（事务已回滚，成本额度已释放）",
                    extra=receipt,
                ) from exc

            # ---- admitted and executed successfully ---------------------- #
            elapsed_ms = round((time.monotonic() - acquired_at) * 1000, 3)
            in_flight = state.get("in_flight_after_admit")
            await self._pool.release_healthy(conn)
            settled = True
            return {
                "status": "ok",
                "executed": True,
                "estimated_cost": round(cost, 3),
                "cost_limit": self._ledger.limit,
                "in_flight_cost": in_flight,
                "plan_ms": state["plan_ms"],
                "wait_ms": waited_ms,
                "execution_ms": elapsed_ms,
                "row_count": len(rows),
                "rows": [self._jsonable(row) for row in rows],
            }
        finally:
            # Whatever happened, no statement may start after the request
            # has been settled by its timeout/cancellation handler.
            state["abandoned"] = True
            if not settled:
                # Any unforeseen exit (including a second cancellation while
                # settling) must not leak the borrower slot.
                logger.error("unsettled connection on exit; discarding")
                try:
                    await self._pool.discard_and_replace(conn)
                except BaseException:
                    logger.exception("failed to discard unsettled connection")
            ticket: AdmissionTicket | None = state.get("ticket")
            if task is not None and not task.done():
                # The connection is being discarded; make sure the shielded
                # task's eventual failure is consumed, not left dangling.
                task.add_done_callback(_consume_task_exception)
            if ticket is not None:
                # Quota is held until the transaction has been finalised.
                self._ledger.release(ticket)

    # ------------------------------------------------------------------ #
    async def _plan_and_execute(
        self,
        conn: psycopg.AsyncConnection,
        sql: str,
        deadline: float,
        state: dict[str, Any],
    ) -> tuple[float, list[tuple[Any, ...]]]:
        """Plan (cost check) and execute in one explicit read-only txn."""
        async with conn.cursor() as cur:
            state["phase"] = "begin"
            await cur.execute("BEGIN READ ONLY")
            await self._set_backstop(cur, deadline)

            state["phase"] = "plan"
            plan_started = time.monotonic()
            # EXPLAIN without ANALYZE plans only; it does not execute SQL.
            await cur.execute(_EXPLAIN_PREFIX + sql)
            plan_rows = await cur.fetchall()
            cost = self._extract_cost(plan_rows)
            state["cost"] = cost
            state["plan_ms"] = round(
                (time.monotonic() - plan_started) * 1000, 3
            )

            if deadline - time.monotonic() <= 0:
                # Planning consumed the whole budget: do not execute.
                raise _DeadlineExhausted("plan")

            # Synchronous check-and-reserve: atomic on the event loop, so
            # concurrent requests cannot over-admit.  Raises (without
            # holding quota) when the single or cumulative limit is hit.
            state["ticket"] = self._ledger.reserve(cost)
            state["in_flight_after_admit"] = round(
                self._ledger.in_flight_cost, 3
            )

            # Refresh the backstop for the remaining budget, then -- and
            # only then -- send the original SQL in this same transaction.
            await self._set_backstop(cur, deadline)
            if state["abandoned"] or deadline - time.monotonic() <= 0:
                # The deadline handler settled this request (or the budget
                # ran out) while the reserve/backstop awaits ran; do not
                # start the original query now.
                state["abandoned"] = True
                raise _Abandoned()
            state["phase"] = "execute"
            state["sql_sent"] = True
            await cur.execute(sql)
            rows = await cur.fetchall()
            # Commit before the cursor block exits, or psycopg's cursor
            # teardown would implicitly ROLLBACK this txn.
            await cur.execute("COMMIT")
            state["phase"] = "done"
            return cost, rows

    async def _set_backstop(
        self, cur: psycopg.AsyncCursor, deadline: float
    ) -> None:
        """Arm the server-side statement_timeout for the remaining budget.

        It is set slightly *after* the request deadline so application
        cancellation normally wins; it only intervenes if PQcancel is late.
        """
        backstop_ms = max(
            1,
            int(
                (deadline - time.monotonic()) * 1000
                + self._settings.statement_timeout_grace_ms
            ),
        )
        await cur.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            (str(backstop_ms),),
        )

    @staticmethod
    def _extract_cost(plan_rows: list[tuple[Any, ...]]) -> float:
        """Return ``Plan.Total Cost`` from EXPLAIN (FORMAT JSON) output."""
        try:
            doc: Any = plan_rows[0][0]
            if isinstance(doc, str):
                doc = json.loads(doc)
            if isinstance(doc, dict):
                doc = [doc]
            top = doc[0]["Plan"]
            cost = float(top["Total Cost"])
            if not math.isfinite(cost) or cost < 0:
                raise ValueError(f"invalid Total Cost {cost!r}")
            return cost
        except PlanFailed:
            raise
        except Exception as exc:
            raise PlanFailed(
                "无法从 EXPLAIN (FORMAT JSON) 结果解析估算成本，原 SQL 未执行",
                extra={"status": "rejected", "executed": False,
                       "phase": "plan"},
            ) from exc

    # ------------------------------------------------------------------ #
    def _receipt(
        self,
        state: dict[str, Any],
        waited_ms: float,
        acquired_at: float,
        *,
        executed: bool,
        phase: str,
        status: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        receipt: dict[str, Any] = {
            "status": status
            or ("timeout" if phase == "execute" else "rejected"),
            "executed": executed,
            "phase": phase,
            "estimated_cost": (
                None
                if state.get("cost") is None
                else round(state["cost"], 3)
            ),
            "cost_limit": self._ledger.limit,
            "plan_ms": state.get("plan_ms"),
            "wait_ms": waited_ms,
            "execution_ms": round(
                (time.monotonic() - acquired_at) * 1000, 3
            ),
        }
        receipt.update(extra)
        return receipt

    async def _settle_task(
        self,
        conn: psycopg.AsyncConnection,
        task: asyncio.Task,
        state: dict[str, Any],
    ) -> bool:
        """Stop the shielded task and prove the backend idle.

        Returns ``True`` when the connection was safely returned to the
        pool, ``False`` when it had to be discarded.  Either way the
        transaction is over and no coroutine touches this connection.
        """
        # Flag first, then cancel: a plan-phase task that is between its
        # quota reservation and sending the original SQL must observe the
        # flag instead of starting work after the receipt was decided.
        state["abandoned"] = True
        cancel_ok = False
        try:
            await asyncio.to_thread(conn.cancel)
            cancel_ok = True
        except Exception:
            logger.exception("PQcancel failed; connection will be discarded")

        try:
            await asyncio.wait_for(task, timeout=5.0)
            logger.warning(
                "statement completed before cancel took effect; late result "
                "ignored, timeout receipt stands"
            )
        except _Abandoned:
            logger.info("abandoned plan-time statement did not start")
        except _DeadlineExhausted:
            logger.info("plan finished after the deadline; SQL never sent")
        except asyncio.TimeoutError:
            logger.error("statement task did not stop after cancel")
        except psycopg.errors.QueryCanceled:
            logger.info("server confirmed cancellation (QueryCanceled)")
        except psycopg.Error as exc:
            logger.info("statement ended after cancel: %s", exc.sqlstate)

        idle = await self._pool.confirm_idle_or_discard(conn)
        if idle:
            await self._pool.release_healthy(conn)
            return True
        await self._pool.discard_and_replace(conn)
        return False

    async def _handle_timeout(
        self,
        conn: psycopg.AsyncConnection,
        task: asyncio.Task,
        state: dict[str, Any],
        waited_ms: float,
        acquired_at: float,
        backstop: bool = False,
    ) -> QueryTimeout:
        logger.warning(
            "deadline reached (%s) in phase %s, cancelling statement",
            "server backstop" if backstop else "client deadline",
            state["phase"],
        )
        idle = await self._settle_task(conn, task, state)
        discarded = not idle
        executed = bool(state["sql_sent"])
        return QueryTimeout(
            "语句执行超过总截止时间，已请求取消"
            + ("；无法确认后端空闲，连接已弃置并补建" if discarded else ""),
            extra={
                **self._receipt(
                    state, waited_ms, acquired_at,
                    executed=executed,
                    phase="execute" if executed else "plan",
                ),
                "status": "timeout",
                "cancelled": True,
                "connection_discarded": discarded,
            },
        )

    async def _rollback_or_discard(
        self, conn: psycopg.AsyncConnection
    ) -> None:
        idle = await self._pool.confirm_idle_or_discard(conn)
        if idle:
            await self._pool.release_healthy(conn)
        else:
            await self._pool.discard_and_replace(conn)

    @staticmethod
    def _jsonable(row: tuple) -> list[Any]:
        out: list[Any] = []
        for value in row:
            # Common non-JSON types from fault logs.
            if hasattr(value, "isoformat"):
                out.append(value.isoformat())
            elif isinstance(value, (bytes, memoryview)):
                out.append(bytes(value).hex())
            else:
                out.append(value)
        return out
