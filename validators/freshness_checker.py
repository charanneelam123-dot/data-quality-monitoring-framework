"""
Freshness checker — verifies that the newest record in a DataFrame is within the SLA window.

Config example (YAML, under validators.freshness):
    timestamp_column: "_loaded_at"
    sla_hours: 26           # FAIL if the newest record is older than 26 hours
    warn_hours: 20          # WARN (still passes) if age exceeds this
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ── Result object ──────────────────────────────────────────────────────────────


@dataclass
class FreshnessResult:
    table_name: str
    timestamp_column: str
    max_timestamp: datetime | None
    age_hours: float | None
    sla_hours: float
    warn_hours: float
    passed: bool
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "validator": "FreshnessChecker",
            "table_name": self.table_name,
            "timestamp_column": self.timestamp_column,
            "max_timestamp": (
                self.max_timestamp.isoformat() if self.max_timestamp else None
            ),
            "age_hours": (
                round(self.age_hours, 2) if self.age_hours is not None else None
            ),
            "sla_hours": self.sla_hours,
            "warn_hours": self.warn_hours,
            "passed": self.passed,
            "message": self.message,
        }


# ── Validator ──────────────────────────────────────────────────────────────────


class FreshnessChecker:
    """
    Checks that the newest record is within the configured SLA window.

    Usage:
        config = {"timestamp_column": "_loaded_at", "sla_hours": 26, "warn_hours": 20}
        result = FreshnessChecker(config, table_name="fact_orders").validate(df)

    Passing reference_time overrides "now" — useful for deterministic tests.
    """

    def __init__(self, config: dict[str, Any], table_name: str = "unknown") -> None:
        self.table_name = table_name
        self.timestamp_column: str = config.get("timestamp_column", "_loaded_at")
        self.sla_hours: float = float(config.get("sla_hours", 26.0))
        self.warn_hours: float = float(config.get("warn_hours", self.sla_hours * 0.75))

    def validate(
        self,
        df: DataFrame,
        reference_time: datetime | None = None,
    ) -> FreshnessResult:
        """
        Args:
            df: DataFrame to check.
            reference_time: Override "now" for deterministic unit tests.
                            Must be timezone-aware (UTC).
        """
        now = reference_time or datetime.now(tz=timezone.utc)

        if self.timestamp_column not in df.columns:
            msg = (
                f"FAIL: Timestamp column '{self.timestamp_column}' not in DataFrame. "
                f"Available columns: {df.columns}"
            )
            logger.error("[%s] %s", self.table_name, msg)
            return self._result(passed=False, age_hours=None, max_ts=None, message=msg)

        max_ts_raw = df.select(
            F.max(F.col(self.timestamp_column)).alias("max_ts")
        ).collect()[0]["max_ts"]

        if max_ts_raw is None:
            msg = f"FAIL: All values in '{self.timestamp_column}' are null — no freshness data."
            logger.error("[%s] %s", self.table_name, msg)
            return self._result(passed=False, age_hours=None, max_ts=None, message=msg)

        # Normalise to UTC-aware datetime (PySpark returns datetime.datetime)
        if isinstance(max_ts_raw, datetime):
            max_ts = (
                max_ts_raw
                if max_ts_raw.tzinfo
                else max_ts_raw.replace(tzinfo=timezone.utc)
            )
        else:
            max_ts = datetime.fromisoformat(str(max_ts_raw)).replace(
                tzinfo=timezone.utc
            )

        age_hours = (now - max_ts).total_seconds() / 3600

        if age_hours > self.sla_hours:
            passed = False
            message = (
                f"FAIL: Data is {age_hours:.1f}h old — exceeds SLA of {self.sla_hours}h. "
                f"Last record: {max_ts.isoformat()}"
            )
        elif age_hours > self.warn_hours:
            passed = True
            message = (
                f"WARN: Data is {age_hours:.1f}h old — approaching SLA of {self.sla_hours}h "
                f"(warn_hours={self.warn_hours}h). Last record: {max_ts.isoformat()}"
            )
        else:
            passed = True
            message = (
                f"PASS: Data is {age_hours:.1f}h old — within SLA of {self.sla_hours}h. "
                f"Last record: {max_ts.isoformat()}"
            )

        (logger.warning if not passed else logger.info)(
            "[%s] %s", self.table_name, message
        )
        return self._result(
            passed=passed, age_hours=age_hours, max_ts=max_ts, message=message
        )

    def _result(
        self,
        passed: bool,
        age_hours: float | None,
        max_ts: datetime | None,
        message: str,
    ) -> FreshnessResult:
        return FreshnessResult(
            table_name=self.table_name,
            timestamp_column=self.timestamp_column,
            max_timestamp=max_ts,
            age_hours=age_hours,
            sla_hours=self.sla_hours,
            warn_hours=self.warn_hours,
            passed=passed,
            message=message,
        )
