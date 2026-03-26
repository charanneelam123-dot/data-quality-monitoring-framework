"""
Volume anomaly detector — compares current row count against a rolling N-day average.

Config example (YAML, under validators.volume_anomaly):
    threshold_pct: 0.30         # allow ±30% variance from rolling average
    lookback_days: 7
    min_baseline_days: 3        # skip check if fewer days of history exist
    count_column: "_loaded_at"  # date/timestamp column used for daily grouping
    metrics_table: "dq.volume_history"   # optional: Spark/Delta table with history
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


# ── Result object ──────────────────────────────────────────────────────────────


@dataclass
class VolumeAnomalyResult:
    table_name: str
    current_count: int
    rolling_avg: float
    variance_pct: float
    threshold_pct: float
    passed: bool
    message: str
    lookback_days: int
    reference_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "validator": "VolumeAnomalyDetector",
            "table_name": self.table_name,
            "passed": self.passed,
            "current_count": self.current_count,
            "rolling_avg": round(self.rolling_avg, 2),
            "variance_pct": round(self.variance_pct, 4),
            "threshold_pct": self.threshold_pct,
            "lookback_days": self.lookback_days,
            "message": self.message,
            "reference_counts": self.reference_counts,
        }


# ── Validator ──────────────────────────────────────────────────────────────────


class VolumeAnomalyDetector:
    """
    Detects row-count anomalies by comparing the current batch count against a
    rolling N-day average loaded from a metrics table or supplied directly.

    Usage:
        # Supply reference counts directly (most common in tests / CI):
        ref = {"2024-01-14": 50000, "2024-01-13": 48000, ...}
        result = VolumeAnomalyDetector(config, "fact_orders").validate(df, reference_counts=ref)

        # Load from a Delta metrics table automatically:
        result = VolumeAnomalyDetector(config, "fact_orders").validate(df, spark=spark)
    """

    def __init__(self, config: dict[str, Any], table_name: str = "unknown") -> None:
        self.table_name = table_name
        self.threshold_pct: float = float(config.get("threshold_pct", 0.30))
        self.lookback_days: int = int(config.get("lookback_days", 7))
        self.min_baseline_days: int = int(config.get("min_baseline_days", 3))
        self.count_column: str = config.get("count_column", "_loaded_at")
        self.metrics_table: str | None = config.get("metrics_table")

    # ── Public ─────────────────────────────────────────────────────────────────

    def validate(
        self,
        df: DataFrame,
        reference_counts: dict[str, int] | None = None,
        spark: SparkSession | None = None,
    ) -> VolumeAnomalyResult:
        """
        Args:
            df: Current batch DataFrame.
            reference_counts: Dict of {"YYYY-MM-DD": row_count} for the lookback window.
                              If None, loads from self.metrics_table (requires spark).
            spark: SparkSession — required only when reference_counts is None and
                   metrics_table is configured.
        """
        current_count = df.count()

        if reference_counts is None:
            if spark is not None and self.metrics_table:
                reference_counts = self._load_from_metrics_table(spark)
            else:
                logger.warning(
                    "[%s] No reference_counts or metrics_table provided — skipping baseline check.",
                    self.table_name,
                )
                return self._skip_result(current_count)

        if len(reference_counts) < self.min_baseline_days:
            logger.warning(
                "[%s] Only %d day(s) of history (min=%d) — skipping anomaly check.",
                self.table_name,
                len(reference_counts),
                self.min_baseline_days,
            )
            return self._skip_result(current_count, reference_counts)

        rolling_avg = sum(reference_counts.values()) / len(reference_counts)
        variance_pct = abs(current_count - rolling_avg) / max(rolling_avg, 1)
        passed = variance_pct <= self.threshold_pct
        direction = "above" if current_count > rolling_avg else "below"

        message = (
            f"PASS: count={current_count:,} within ±{self.threshold_pct:.0%} of "
            f"rolling avg {rolling_avg:,.1f} over {len(reference_counts)} day(s)"
            if passed
            else (
                f"FAIL: count={current_count:,} is {variance_pct:.1%} {direction} "
                f"rolling avg {rolling_avg:,.1f} (threshold=±{self.threshold_pct:.0%}, "
                f"lookback={len(reference_counts)} day(s))"
            )
        )
        (logger.info if passed else logger.warning)("[%s] %s", self.table_name, message)

        return VolumeAnomalyResult(
            table_name=self.table_name,
            current_count=current_count,
            rolling_avg=rolling_avg,
            variance_pct=variance_pct,
            threshold_pct=self.threshold_pct,
            passed=passed,
            message=message,
            lookback_days=self.lookback_days,
            reference_counts=reference_counts,
        )

    # ── Private ────────────────────────────────────────────────────────────────

    def _load_from_metrics_table(self, spark: SparkSession) -> dict[str, int]:
        cutoff = (
            datetime.now(tz=timezone.utc).date() - timedelta(days=self.lookback_days)
        ).isoformat()
        query = f"""  # nosec B608
            SELECT DATE(run_date) AS day, SUM(row_count) AS cnt
            FROM {self.metrics_table}
            WHERE table_name = '{self.table_name}'
              AND run_date >= '{cutoff}'
            GROUP BY 1
            ORDER BY 1
        """
        rows = spark.sql(query).collect()
        return {str(row["day"]): int(row["cnt"]) for row in rows}

    def _skip_result(
        self,
        current_count: int,
        reference_counts: dict[str, int] | None = None,
    ) -> VolumeAnomalyResult:
        return VolumeAnomalyResult(
            table_name=self.table_name,
            current_count=current_count,
            rolling_avg=0.0,
            variance_pct=0.0,
            threshold_pct=self.threshold_pct,
            passed=True,  # insufficient baseline → warn but do not fail
            message=f"SKIP: Insufficient baseline. current_count={current_count:,}",
            lookback_days=self.lookback_days,
            reference_counts=reference_counts or {},
        )
