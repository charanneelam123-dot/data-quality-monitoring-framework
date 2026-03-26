"""
Null-rate validator — checks the null percentage per column against configurable thresholds.

Config example (YAML, under validators.null_rate):
    global_threshold: 0.05          # 5% max null rate (default for every column)
    column_thresholds:              # per-column overrides
      order_id: 0.0                 # must never be null
      discount: 0.30                # allowed up to 30%
    columns_to_skip: []             # skip these columns entirely
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ── Result objects ─────────────────────────────────────────────────────────────


@dataclass
class NullRateResult:
    column: str
    null_count: int
    total_count: int
    null_rate: float
    threshold: float
    passed: bool
    message: str


@dataclass
class NullRateValidatorResult:
    table_name: str
    passed: bool
    results: list[NullRateResult] = field(default_factory=list)

    @property
    def failed_columns(self) -> list[NullRateResult]:
        return [r for r in self.results if not r.passed]

    def to_dict(self) -> dict[str, Any]:
        return {
            "validator": "NullRateValidator",
            "table_name": self.table_name,
            "passed": self.passed,
            "total_columns_checked": len(self.results),
            "failed_count": len(self.failed_columns),
            "message": (
                "All columns within null-rate thresholds."
                if self.passed
                else f"{len(self.failed_columns)} column(s) exceeded null-rate threshold."
            ),
            "failures": [
                {
                    "column": r.column,
                    "null_rate": round(r.null_rate, 4),
                    "threshold": r.threshold,
                    "null_count": r.null_count,
                    "total_count": r.total_count,
                    "message": r.message,
                }
                for r in self.failed_columns
            ],
        }


# ── Validator ──────────────────────────────────────────────────────────────────


class NullRateValidator:
    """
    Validates null rates per column against configurable thresholds.

    Usage:
        config = {"global_threshold": 0.05, "column_thresholds": {"order_id": 0.0}}
        result = NullRateValidator(config, table_name="fact_orders").validate(df)
    """

    def __init__(self, config: dict[str, Any], table_name: str = "unknown") -> None:
        self.table_name = table_name
        self.global_threshold: float = float(config.get("global_threshold", 0.05))
        self.column_thresholds: dict[str, float] = {
            k: float(v) for k, v in config.get("column_thresholds", {}).items()
        }
        self.columns_to_skip: set[str] = set(config.get("columns_to_skip", []))

    def validate(self, df: DataFrame) -> NullRateValidatorResult:
        total = df.count()
        if total == 0:
            logger.warning(
                "[%s] DataFrame is empty — skipping null-rate validation.",
                self.table_name,
            )
            return NullRateValidatorResult(table_name=self.table_name, passed=True)

        columns = [c for c in df.columns if c not in self.columns_to_skip]

        # Count nulls for all columns in a single Spark pass
        null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
        null_counts = df.select(null_exprs).collect()[0].asDict()

        results: list[NullRateResult] = []
        for col_name in columns:
            null_count = null_counts[col_name]
            null_rate = null_count / total
            threshold = self.column_thresholds.get(col_name, self.global_threshold)
            passed = null_rate <= threshold
            message = (
                f"PASS: null_rate={null_rate:.4f} <= threshold={threshold}"
                if passed
                else (
                    f"FAIL: null_rate={null_rate:.4f} > threshold={threshold} "
                    f"({null_count:,}/{total:,} nulls)"
                )
            )
            results.append(
                NullRateResult(
                    column=col_name,
                    null_count=null_count,
                    total_count=total,
                    null_rate=null_rate,
                    threshold=threshold,
                    passed=passed,
                    message=message,
                )
            )
            (logger.debug if passed else logger.warning)(
                "[%s] %s — %s", self.table_name, col_name, message
            )

        overall_passed = all(r.passed for r in results)
        return NullRateValidatorResult(
            table_name=self.table_name,
            passed=overall_passed,
            results=results,
        )
