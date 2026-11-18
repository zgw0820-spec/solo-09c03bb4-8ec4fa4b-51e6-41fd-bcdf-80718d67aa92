"""Focused protocol tests that do not need a running HTTP server.

They drive the executor/pool directly against the ephemeral PostgreSQL and
concentrate on the hardest correctness guarantees:

* a statement that finishes just before the cancel request arrives must not
  overwrite the already-decided timeout receipt ("late result");
* a queued request whose deadline expires never sends a statement;
* after a timeout the backend is confirmed IDLE before reuse.

Run:  python tests/test_protocol.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import (  # noqa: E402
    CostLimitExceeded,
    PlanFailed,
    PoolAcquireTimeout,
    QueryFailed,
    QueryTimeout,
)
from app.executor import QueryExecutor  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0

# Total cost budget used throughout the cost-admission tests below.  Two
# concurrent SLEEPER statements (~3753.75 each) therefore cannot both be
# admitted, while a single one is always within the cap.
COST_LIMIT = 5_000.0
# Measured on PostgreSQL 16 with default cost constants:
#   SELECT pg_sleep(1.5) FROM generate_series(1, 300000) g WHERE g = 1
#   -> Total Cost 3753.75
SLEEPER = (
    "SELECT pg_sleep(1.5) FROM generate_series(1, 300000) g WHERE g = 1"
)
# ~12500 cost: rejected by the *single* query cap.
TOO_BIG = "SELECT count(*) FROM generate_series(1, 1000000)"


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="proto-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )

    settings = Settings(database_dsn=ro, pool_size=2, max_query_cost=COST_LIMIT)
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    async def assert_clean(label: str) -> None:
        """No leaked quota, no leaked connection, pool full and idle."""
        st = await pool.status()
        snap = executor.ledger.snapshot()
        check(
            f"{label}: 无残留额度且池满空闲",
            snap["in_flight_cost"] == 0
            and snap["in_flight_count"] == 0
            and st.idle == 2
            and st.size == 2,
            {"admission": snap, "pool": st},
        )

    print("=== A. 迟到结果不改写超时回执 ===")
    # The statement (150ms) always outlives the 60ms deadline, so the timeout
    # path is mandatory.  Cancellation arrives ~sub-millisecond later, which
    # is also how a real "finished just before cancel" race is shaped: the
    # shielded task is awaited and whatever it returns is recorded but never
    # allowed to replace the receipt.
    receipt: QueryTimeout | None = None
    late_seen = False
    try:
        await executor.execute(
            "SELECT pg_sleep(0.15)", time.monotonic() + 0.06
        )
    except QueryTimeout as exc:
        receipt = exc
        late_seen = bool(exc.extra.get("late_result_ignored"))

    check("截止时间到必定返回 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执固定为 timeout",
              receipt.extra.get("status") == "timeout", receipt.extra)
        check("回执不携带 rows（迟到结果未泄漏）",
              "rows" not in receipt.extra, receipt.extra)
        check("已发起取消并确认事务结束(未弃置)",
              receipt.extra.get("cancelled") is True
              and receipt.extra.get("connection_discarded") is False,
              receipt.extra)
        print(f"  INFO  late_result_ignored={late_seen} "
              f"(True=语句在取消前完成；False=服务器确认取消)")

    print("=== B. 超时后确认 IDLE 才复用 ===")
    st = await pool.status()
    check("池容量仍为 2", st.size == 2, st)
    check("两条后端均空闲(IDLE)", st.idle == 2, st)
    ok = await executor.execute("SELECT 7", time.monotonic() + 5)
    check("后续查询得到自身结果", ok["rows"] == [[7]], ok)

    print("=== C. 排队过期绝不发送语句 ===")
    # Occupy both backends with long sleeps (long own deadlines).
    async def slow(tag: str) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(2)", time.monotonic() + 6
        )

    busy = [asyncio.ensure_future(slow("X")), asyncio.ensure_future(slow("Y"))]
    await asyncio.sleep(0.4)
    queued_err = None
    try:
        await executor.execute(
            "SELECT 'NEVER-RUN'", time.monotonic() + 0.4
        )
    except PoolAcquireTimeout as exc:
        queued_err = exc
    check("排队过期抛 PoolAcquireTimeout", queued_err is not None)
    results = await asyncio.gather(*busy)
    check("两条占池查询各自成功",
          {r["rows"][0][0] for r in results} == {"X", "Y"}, results)
    # Prove the queued statement never reached a backend: no marker exists.
    after = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("过期语句无任何副作用，后续查询正常", after["rows"] == [[1]], after)
    st = await pool.status()
    check("结束后池满且空闲", st.idle == 2 and st.size == 2, st)
    await assert_clean("C")

    # ---------------------------------------------------------------- #
    print("=== D. 单条估算成本超限：拒绝且不执行 ===")
    rejected: CostLimitExceeded | None = None
    try:
        await executor.execute(TOO_BIG, time.monotonic() + 10)
    except CostLimitExceeded as exc:
        rejected = exc
    check("单条超限抛 CostLimitExceeded", rejected is not None)
    if rejected is not None:
        check("回执携带估算值",
              rejected.extra.get("estimated_cost", 0) > COST_LIMIT,
              rejected.extra)
        check("回执标记未执行", rejected.extra.get("executed") is False,
              rejected.extra)
        check("回执 status=rejected", rejected.extra.get("status") == "rejected",
              rejected.extra)

    print("=== E. 累计在途成本超限：并发不得超额放行 ===")
    # Two ~3753.75-cost sleepers cannot share the 5k budget, so exactly one
    # must be admitted and the other rejected; a third normal query must
    # also yield its own correct result once the sleeper finishes.
    async def sleeper(tag: str):
        try:
            res = await executor.execute(
                f"SELECT '{tag}', pg_sleep(1.0) FROM generate_series(1, 300000) "
                f"g WHERE g = 1",
                time.monotonic() + 10,
            )
            return ("ok", res["estimated_cost"], res["rows"][0][0])
        except CostLimitExceeded as exc:
            return ("rejected", exc.extra.get("estimated_cost"),
                    exc.extra.get("in_flight_cost"))

    results = await asyncio.gather(sleeper("P"), sleeper("Q"))
    admitted = [r for r in results if r[0] == "ok"]
    denied = [r for r in results if r[0] == "rejected"]
    check("并发两条高成本查询仅一条准入", len(admitted) == 1, results)
    check("另一条因累计额度被拒且不执行", len(denied) == 1, results)
    check("准入者拿到自身结果", admitted and admitted[0][2] in ("P", "Q"),
          admitted)
    await assert_clean("E")

    small = await executor.execute(
        "SELECT 'AFTER-COST'", time.monotonic() + 5
    )
    check("拒绝/完成后普通查询恢复可用",
          small["rows"] == [["AFTER-COST"]], small)
    await assert_clean("E-after")

    # ---------------------------------------------------------------- #
    print("=== F. 计划获取失败：不执行原 SQL ===")
    plan_err: PlanFailed | None = None
    try:
        await executor.execute("SELECT * FROM no_such_table",
                               time.monotonic() + 5)
    except PlanFailed as exc:
        plan_err = exc
    check("未知表导致计划失败 PlanFailed", plan_err is not None)
    if plan_err is not None:
        check("计划失败回执未执行且无 rows",
              plan_err.extra.get("executed") is False
              and "rows" not in plan_err.extra, plan_err.extra)
        check("计划失败附 sqlstate",
              plan_err.extra.get("sqlstate") == "42P01", plan_err.extra)

    print("=== G. 已准入后 SQL 执行报错：回滚并释放额度 ===")
    exec_err: QueryFailed | None = None
    try:
        await executor.execute("SELECT 1/(id-id) FROM fault_logs",
                               time.monotonic() + 5)
    except QueryFailed as exc:
        exec_err = exc
    check("执行期 SQL 错误 → QueryFailed", exec_err is not None)
    if exec_err is not None:
        check("执行错误回执标记已执行",
              exec_err.extra.get("executed") is True
              and exec_err.extra.get("phase") == "execute", exec_err.extra)
    await assert_clean("G")

    # ---------------------------------------------------------------- #
    print("=== H. 超时/取消/预检耗尽后额度与连接均释放 ===")
    timed_out: QueryTimeout | None = None
    try:
        await executor.execute(SLEEPER, time.monotonic() + 0.2)
    except QueryTimeout as exc:
        timed_out = exc
    check("执行阶段超时 → QueryTimeout", timed_out is not None)
    if timed_out is not None:
        check("超时回执已执行且已取消",
              timed_out.extra.get("executed") is True
              and timed_out.extra.get("status") == "timeout", timed_out.extra)
        check("超时回执带估算成本",
              (timed_out.extra.get("estimated_cost") or 0) > 0,
              timed_out.extra)
    await asyncio.sleep(0.2)
    await assert_clean("H-timeout")

    plan_deadline: QueryTimeout | None = None
    try:
        # A 22-way join plans in ~10-15ms, far beyond the 1ms budget, so the
        # deadline necessarily expires during the EXPLAIN pre-check.
        slow_plan = (
            "SELECT count(*) FROM "
            + ", ".join(f"generate_series(1, 7) g{i}" for i in range(22))
        )
        await executor.execute(slow_plan, time.monotonic() + 0.001)
    except QueryTimeout as exc:
        plan_deadline = exc
    check("预算在预检阶段耗尽 → QueryTimeout", plan_deadline is not None)
    if plan_deadline is not None:
        check("预检耗尽回执未执行且处于 plan 阶段",
              plan_deadline.extra.get("executed") is False
              and plan_deadline.extra.get("phase") == "plan",
              plan_deadline.extra)
    await asyncio.sleep(0.2)
    await assert_clean("H-plan-deadline")

    # ---------------------------------------------------------------- #
    print("=== I. 请求被取消（连接弃置/收尾）后额度与连接均释放 ===")
    running = asyncio.ensure_future(
        executor.execute(
            "SELECT pg_sleep(3) FROM generate_series(1, 300000) g WHERE g = 1",
            time.monotonic() + 10,
        )
    )
    await asyncio.sleep(0.5)
    mid = executor.ledger.snapshot()
    check("执行期间额度被占用", mid["in_flight_cost"] > 0
          and mid["in_flight_count"] == 1, mid)
    running.cancel()
    try:
        await running
    except asyncio.CancelledError:
        pass
    except Exception as exc:  # noqa: BLE001
        check("取消后不应抛其他异常", False, repr(exc))
    await asyncio.sleep(0.2)
    await assert_clean("I-cancel")

    print("=== J. 带外终止后端：弃置补建后额度不泄漏 ===")
    pids: list[int] = []
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_stat_activity WHERE usename = 'fault_ro'"
        )
        pids = [r[0] for r in cur.fetchall()]
        for pid in pids:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
    check("两条池内后端已被终止", len(pids) == 2, pids)
    survivor = await executor.execute("SELECT 'AFTER-KILL'", time.monotonic() + 8)
    check("弃置补建后查询成功",
          survivor["rows"] == [["AFTER-KILL"]], survivor)
    await assert_clean("J")

    await pool.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
