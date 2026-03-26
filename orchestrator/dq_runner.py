"""
DQ Runner — single entry point that loads config, runs all validators, and fires alerts.

Usage:
    runner = DQRunner.from_config(
        "config/thresholds.yml",
        table_name="orders.fact_orders",
        pipeline_name="orders_etl",
    )
    result = runner.run(df, spark=spark)
    if not result.overall_passed:
        raise RuntimeError(f"DQ failed — aborting pipeline. run_id={result.run_id}")

    # Plug into any Databricks notebook or Glue job:
    print(result.to_dict())
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession

from alerts.email_notifier import EmailNotifier
from alerts.slack_notifier import SlackNotifier
from validators.freshness_checker import FreshnessChecker
from validators.null_rate_validator import NullRateValidator
from validators.schema_drift_detector import SchemaDriftDetector
from validators.volume_anomaly_detector import VolumeAnomalyDetector

logger = logging.getLogger(__name__)

# ── Result ─────────────────────────────────────────────────────────────────────


@dataclass
class DQRunResult:
    run_id: str
    table_name: str
    pipeline_name: str
    environment: str
    started_at: datetime
    completed_at: datetime | None = None
    overall_passed: bool = True
    validator_results: list[dict[str, Any]] = field(default_factory=list)
    alerts_sent: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def failed_validators(self) -> list[dict[str, Any]]:
        return [r for r in self.validator_results if not r.get("passed", True)]

    def to_dict(self) -> dict[str, Any]:
        duration = (
            (self.completed_at - self.started_at).total_seconds()
            if self.completed_at
            else None
        )
        return {
            "run_id": self.run_id,
            "table_name": self.table_name,
            "pipeline_name": self.pipeline_name,
            "environment": self.environment,
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration_seconds": round(duration, 2) if duration else None,
            "overall_passed": self.overall_passed,
            "total_validators": len(self.validator_results),
            "failed_validators": len(self.failed_validators),
            "alerts_sent": self.alerts_sent,
            "error": self.error,
            "validator_results": self.validator_results,
        }


# ── Runner ─────────────────────────────────────────────────────────────────────


class DQRunner:
    """
    Orchestrates all configured data quality validators for a given DataFrame.

    Validators are enabled/disabled entirely by their presence in the YAML config.
    All thresholds are configurable — no hardcoded values.

    Config resolution:
        1. tables["*"]  →  global defaults applied to every table
        2. tables[table_name]  →  table-specific overrides (deep-merged on top of defaults)
    """

    def __init__(
        self,
        config: dict[str, Any],
        table_name: str,
        pipeline_name: str = "unknown",
        environment: str | None = None,
    ) -> None:
        self.config = config
        self.table_name = table_name
        self.pipeline_name = pipeline_name
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")

        table_cfg = self._resolve_table_config(config, table_name)
        validators_cfg: dict[str, Any] = table_cfg.get("validators", {})
        alerts_cfg: dict[str, Any] = config.get("alerts", {})
        self._notify_on_pass: bool = bool(alerts_cfg.get("notify_on_pass", False))

        # Instantiate only the validators present in config
        self._validators: list[tuple[str, Any]] = []
        _map = {
            "null_rate": NullRateValidator,
            "volume_anomaly": VolumeAnomalyDetector,
            "schema_drift": SchemaDriftDetector,
            "freshness": FreshnessChecker,
        }
        for key, cls in _map.items():
            if key in validators_cfg:
                self._validators.append((key, cls(validators_cfg[key], table_name)))

        # Instantiate notifiers
        self._slack: SlackNotifier | None = (
            SlackNotifier(alerts_cfg["slack"]) if "slack" in alerts_cfg else None
        )
        self._email: EmailNotifier | None = (
            EmailNotifier(alerts_cfg["email"]) if "email" in alerts_cfg else None
        )

    # ── Factory ────────────────────────────────────────────────────────────────

    @classmethod
    def from_config(
        cls,
        config_path: str | Path,
        table_name: str,
        pipeline_name: str = "unknown",
        environment: str | None = None,
    ) -> "DQRunner":
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"DQ config not found: {path}")
        with path.open() as fh:
            config = yaml.safe_load(fh)
        return cls(
            config,
            table_name=table_name,
            pipeline_name=pipeline_name,
            environment=environment,
        )

    # ── Main run ───────────────────────────────────────────────────────────────

    def run(
        self,
        df: DataFrame,
        spark: SparkSession | None = None,
        reference_counts: dict[str, int] | None = None,
        reference_schema: dict[str, str] | None = None,
        reference_time: datetime | None = None,
        run_id: str | None = None,
    ) -> DQRunResult:
        """
        Execute all configured validators against the DataFrame.

        Args:
            df: DataFrame to validate (any PySpark DataFrame — table, batch, stream micro-batch).
            spark: Required only when VolumeAnomalyDetector loads history from a metrics table.
            reference_counts: Pre-loaded volume history {"YYYY-MM-DD": count}.
            reference_schema: Pre-loaded schema reference {"column": "SparkType"}.
            reference_time: Override "now" for freshness checks (deterministic tests).
            run_id: Override UUID; auto-generated if omitted.

        Returns:
            DQRunResult — check result.overall_passed before proceeding.
        """
        run_id = run_id or str(uuid.uuid4())
        started_at = datetime.now(tz=timezone.utc)

        logger.info(
            "DQ run started | run_id=%s | table=%s | pipeline=%s | env=%s",
            run_id,
            self.table_name,
            self.pipeline_name,
            self.environment,
        )

        result = DQRunResult(
            run_id=run_id,
            table_name=self.table_name,
            pipeline_name=self.pipeline_name,
            environment=self.environment,
            started_at=started_at,
        )

        try:
            for name, validator in self._validators:
                logger.info("Running validator: %s", name)
                try:
                    v_result = self._dispatch(
                        name,
                        validator,
                        df,
                        spark,
                        reference_counts,
                        reference_schema,
                        reference_time,
                    )
                    result.validator_results.append(v_result)
                    if not v_result.get("passed", True):
                        result.overall_passed = False
                        logger.warning(
                            "Validator FAILED: %s — %s",
                            name,
                            v_result.get("message", ""),
                        )
                    else:
                        logger.info("Validator PASSED: %s", name)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Validator %s raised: %s", name, exc)
                    result.validator_results.append(
                        {
                            "validator": name,
                            "table_name": self.table_name,
                            "passed": False,
                            "message": f"ERROR: {exc}",
                        }
                    )
                    result.overall_passed = False

        except Exception as exc:  # noqa: BLE001
            logger.exception("DQ run error: %s", exc)
            result.overall_passed = False
            result.error = str(exc)

        result.completed_at = datetime.now(tz=timezone.utc)
        duration = (result.completed_at - started_at).total_seconds()
        logger.info(
            "DQ run complete | run_id=%s | passed=%s | validators=%d/%d | duration=%.1fs",
            run_id,
            result.overall_passed,
            len(result.validator_results) - len(result.failed_validators),
            len(result.validator_results),
            duration,
        )

        self._send_alerts(result)
        return result

    # ── Private ────────────────────────────────────────────────────────────────

    @staticmethod
    def _dispatch(
        name: str,
        validator: Any,
        df: DataFrame,
        spark: SparkSession | None,
        reference_counts: dict | None,
        reference_schema: dict | None,
        reference_time: datetime | None,
    ) -> dict[str, Any]:
        if name == "null_rate":
            return validator.validate(df).to_dict()
        if name == "volume_anomaly":
            return validator.validate(
                df, reference_counts=reference_counts, spark=spark
            ).to_dict()
        if name == "schema_drift":
            return validator.validate(df, reference_schema=reference_schema).to_dict()
        if name == "freshness":
            return validator.validate(df, reference_time=reference_time).to_dict()
        raise ValueError(f"Unknown validator key: {name}")

    def _send_alerts(self, result: DQRunResult) -> None:
        if result.overall_passed and not self._notify_on_pass:
            return
        kwargs: dict[str, Any] = dict(
            table_name=result.table_name,
            pipeline_name=result.pipeline_name,
            results=result.validator_results,
            overall_passed=result.overall_passed,
            run_id=result.run_id,
            environment=result.environment,
        )
        if self._slack and self._slack.send_dq_report(**kwargs):
            result.alerts_sent.append("slack")
        if self._email and self._email.send_dq_report(**kwargs):
            result.alerts_sent.append("email")

    @staticmethod
    def _resolve_table_config(
        config: dict[str, Any], table_name: str
    ) -> dict[str, Any]:
        """Deep-merge global defaults with table-specific overrides."""
        tables: dict[str, Any] = config.get("tables", {})
        global_defaults: dict[str, Any] = tables.get("*", {})

        # Try exact name, then short name (after last ".")
        table_cfg = tables.get(
            table_name, tables.get(table_name.rsplit(".", 1)[-1], {})
        )

        merged: dict[str, Any] = {}
        for section in set(list(global_defaults.keys()) + list(table_cfg.keys())):
            g_val = global_defaults.get(section, {})
            t_val = table_cfg.get(section, {})
            if isinstance(g_val, dict) and isinstance(t_val, dict):
                # Deep merge: table-specific keys override global
                merged[section] = {**g_val, **t_val}
                # Special case: merge nested validator sub-dicts
                for sub_key in set(list(g_val.keys()) + list(t_val.keys())):
                    gv = g_val.get(sub_key, {})
                    tv = t_val.get(sub_key, {})
                    if isinstance(gv, dict) and isinstance(tv, dict):
                        merged[section][sub_key] = {**gv, **tv}
            else:
                merged[section] = t_val if section in table_cfg else g_val

        return merged
