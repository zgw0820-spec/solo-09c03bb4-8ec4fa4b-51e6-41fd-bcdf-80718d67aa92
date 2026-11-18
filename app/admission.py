"""In-process cost admission ledger.

Every request that passes the planner check *holds* its estimated cost until
its transaction has been finalised (commit, rollback, cancellation recovery
or connection discard).  A request is admitted only when both:

* its own estimate is within ``MAX_QUERY_COST``; and
* the sum of estimates of all in-flight queries stays within the same cap.

The ledger is deliberately synchronous: check-and-record runs in one
event-loop step with no ``await`` in between, so two concurrent coroutines
can never both observe "budget available" and over-admit.  The executor
measures the plan first and reserves immediately after, still before the
first statement of the admitted query is sent.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .errors import CostLimitExceeded

logger = logging.getLogger("admission")

# Guards against float noise from ``Total Cost`` (e.g. 49999.99999999999).
_EPSILON = 1e-6


@dataclass(frozen=True)
class AdmissionTicket:
    """Proof that one request currently holds ``cost`` units of quota."""

    ticket_id: int
    cost: float


class AdmissionLedger:
    """Tracks the sum of estimated costs of in-flight queries."""

    def __init__(self, limit: float) -> None:
        self._limit = float(limit)
        self._in_flight: dict[int, float] = {}
        self._next_id = 0
        self._released_total = 0
        self._rejected_total = 0

    @property
    def limit(self) -> float:
        return self._limit

    @property
    def in_flight_cost(self) -> float:
        return sum(self._in_flight.values())

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight)

    @property
    def rejected_total(self) -> int:
        return self._rejected_total

    def check(self, cost: float) -> None:
        """Raise :class:`CostLimitExceeded` if ``cost`` cannot be admitted.

        Performs no mutation of the in-flight ledger, so it is safe to call
        while describing a rejection.  Rejections are counted.
        """
        if cost > self._limit + _EPSILON:
            self._reject(
                cost,
                f"单条查询估算成本 {cost:.2f} 超过上限 {self._limit:.2f}",
            )
        total = self.in_flight_cost + cost
        if total > self._limit + _EPSILON:
            self._reject(
                cost,
                "当前在途查询累计估算成本 {:.2f} 加上本查询 {:.2f} "
                "将超过上限 {:.2f}".format(
                    self.in_flight_cost, cost, self._limit
                ),
            )

    def _reject(self, cost: float, message: str) -> None:
        self._rejected_total += 1
        raise CostLimitExceeded(
            message,
            extra={
                "status": "rejected",
                "executed": False,
                "estimated_cost": round(cost, 3),
                "cost_limit": self._limit,
                "in_flight_cost": round(self.in_flight_cost, 3),
            },
        )

    def reserve(self, cost: float) -> AdmissionTicket:
        """Atomically check-and-record ``cost``; never awaits.

        Raises :class:`CostLimitExceeded` without recording anything when
        the query does not fit, so a rejected request holds no quota.
        """
        self.check(cost)
        ticket_id = self._next_id
        self._next_id += 1
        self._in_flight[ticket_id] = cost
        logger.info(
            "admitted cost=%.2f in_flight=%.2f count=%d",
            cost,
            self.in_flight_cost,
            self.in_flight_count,
        )
        return AdmissionTicket(ticket_id=ticket_id, cost=cost)

    def release(self, ticket: AdmissionTicket) -> None:
        """Drop one holding.  Idempotent: an unknown ticket is a no-op."""
        cost = self._in_flight.pop(ticket.ticket_id, None)
        if cost is None:
            return
        self._released_total += 1
        logger.info(
            "released cost=%.2f in_flight=%.2f count=%d",
            cost,
            self.in_flight_cost,
            self.in_flight_count,
        )

    def snapshot(self) -> dict[str, float | int]:
        return {
            "limit": self._limit,
            "in_flight_cost": round(self.in_flight_cost, 3),
            "in_flight_count": self.in_flight_count,
            "released_total": self._released_total,
            "rejected_total": self._rejected_total,
        }
