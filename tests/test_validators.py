"""
Comprehensive test suite for all DQ validators.
35+ tests covering NullRateValidator, VolumeAnomalyDetector, SchemaDriftDetector,
FreshnessChecker, and DQRunner orchestrator.

Run:
    PYSPARK_PYTHON=$(which python) SPARK_LOCAL_IP=127.0.0.1 \
    pytest tests/test_validators.py -v --tb=short
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from validators.freshness_checker import FreshnessChecker
from validators.null_rate_validator import NullRateValidator
from validators.schema_drift_detector import SchemaDriftDetector
from validators.volume_anomaly_detector import VolumeAnomalyDetector

# ── Spark session fixture ──────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("dq-framework-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


# ── Shared helpers ─────────────────────────────────────────────────────────────

_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("gross_revenue", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("_loaded_at", TimestampType(), True),
    ]
)

_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
_FRESH_TS = _NOW - timedelta(hours=1)
_STALE_TS = _NOW - timedelta(hours=30)


def _make_df(spark, rows, schema=None):
    return spark.createDataFrame(rows, schema=schema or _ORDER_SCHEMA)


def _good_rows():
    ts = _FRESH_TS.replace(tzinfo=None)
    return [
        ("ORD-001", "CUST-1", datetime(2024, 6, 14).date(), 2, 100.0, 0.1, ts),
        ("ORD-002", "CUST-2", datetime(2024, 6, 14).date(), 1, 50.0, 0.0, ts),
        ("ORD-003", "CUST-3", datetime(2024, 6, 14).date(), 3, 200.0, 0.15, ts),
        ("ORD-004", "CUST-4", datetime(2024, 6, 14).date(), 5, 500.0, 0.05, ts),
        ("ORD-005", "CUST-5", datetime(2024, 6, 14).date(), 1, 25.0, 0.0, ts),
    ]


# =============================================================================
# NullRateValidator
# =============================================================================


class TestNullRateValidator:
    def test_all_columns_pass(self, spark):
        df = _make_df(spark, _good_rows())
        result = NullRateValidator({"global_threshold": 0.05}, "test").validate(df)
        assert result.passed
        assert result.failed_columns == []

    def test_single_column_null_exceeds_threshold(self, spark):
        rows = [
            ("ORD-001", None, None, 2, 100.0, 0.1, None),
            ("ORD-002", None, None, 1, 50.0, 0.0, None),
            ("ORD-003", "C3", None, 3, 200.0, 0.0, None),
        ]
        df = _make_df(spark, rows)
        result = NullRateValidator(
            {"global_threshold": 0.05, "column_thresholds": {"customer_id": 0.0}},
            "test",
        ).validate(df)
        assert not result.passed
        failed_cols = [r.column for r in result.failed_columns]
        assert "customer_id" in failed_cols

    def test_column_threshold_override_allows_high_null_rate(self, spark):
        rows = [
            ("ORD-001", "C1", None, 1, 10.0, None, None),
            ("ORD-002", "C2", None, 1, 10.0, None, None),
            ("ORD-003", "C3", None, 1, 10.0, None, None),
        ]
        df = _make_df(spark, rows)
        config = {
            "global_threshold": 0.05,
            "column_thresholds": {
                "order_date": 1.0,  # allow 100% null
                "discount": 1.0,
                "_loaded_at": 1.0,
            },
        }
        result = NullRateValidator(config, "test").validate(df)
        assert result.passed

    def test_columns_to_skip_are_ignored(self, spark):
        rows = [
            ("ORD-001", "C1", None, 1, 10.0, None, None),
        ]
        df = _make_df(spark, rows)
        config = {
            "global_threshold": 0.0,
            "columns_to_skip": ["order_date", "discount", "_loaded_at"],
        }
        result = NullRateValidator(config, "test").validate(df)
        assert result.passed

    def test_empty_dataframe_passes(self, spark):
        df = spark.createDataFrame([], schema=_ORDER_SCHEMA)
        result = NullRateValidator({}, "test").validate(df)
        assert result.passed
        assert result.results == []

    def test_zero_threshold_on_key_column_fails_on_any_null(self, spark):
        rows = [
            (None, "C1", None, 1, 10.0, 0.0, None),
            ("ORD-002", "C2", None, 1, 10.0, 0.0, None),
        ]
        df = _make_df(spark, rows)
        result = NullRateValidator(
            {"column_thresholds": {"order_id": 0.0}}, "test"
        ).validate(df)
        assert not result.passed
        assert any(r.column == "order_id" for r in result.failed_columns)

    def test_to_dict_structure(self, spark):
        df = _make_df(spark, _good_rows())
        d = NullRateValidator({}, "fact_orders").validate(df).to_dict()
        assert d["validator"] == "NullRateValidator"
        assert d["table_name"] == "fact_orders"
        assert "passed" in d
        assert "failures" in d
        assert "total_columns_checked" in d

    def test_null_rate_calculation_accuracy(self, spark):
        ts = _FRESH_TS.replace(tzinfo=None)
        rows = [
            ("ORD-001", "C1", None, 1, 10.0, 0.0, ts),
            ("ORD-002", None, None, 1, 10.0, 0.0, ts),
            ("ORD-003", None, None, 1, 10.0, 0.0, ts),
            ("ORD-004", "C4", None, 1, 10.0, 0.0, ts),
        ]
        df = _make_df(spark, rows)
        result = NullRateValidator({"global_threshold": 1.0}, "test").validate(df)
        cust_result = next(r for r in result.results if r.column == "customer_id")
        assert cust_result.null_count == 2
        assert abs(cust_result.null_rate - 0.5) < 1e-9


# =============================================================================
# VolumeAnomalyDetector
# =============================================================================


class TestVolumeAnomalyDetector:
    _REF_COUNTS = {
        "2024-06-08": 50000,
        "2024-06-09": 52000,
        "2024-06-10": 49000,
        "2024-06-11": 51000,
        "2024-06-12": 48000,
        "2024-06-13": 53000,
        "2024-06-14": 50000,
    }  # avg = 50,428

    def test_count_within_threshold_passes(self, spark):
        df = _make_df(spark, _good_rows() * 10000)  # 50,000 rows
        result = VolumeAnomalyDetector({"threshold_pct": 0.30}, "test").validate(
            df, reference_counts=self._REF_COUNTS
        )
        assert result.passed
        assert result.current_count == 50000

    def test_count_far_below_average_fails(self, spark):
        df = _make_df(spark, _good_rows())  # 5 rows vs avg ~50k
        result = VolumeAnomalyDetector({"threshold_pct": 0.30}, "test").validate(
            df, reference_counts=self._REF_COUNTS
        )
        assert not result.passed
        assert result.variance_pct > 0.30

    def test_count_far_above_average_fails(self, spark):
        df = _make_df(spark, _good_rows() * 20000)  # 100,000 rows vs avg ~50k
        result = VolumeAnomalyDetector({"threshold_pct": 0.30}, "test").validate(
            df, reference_counts=self._REF_COUNTS
        )
        assert not result.passed

    def test_insufficient_baseline_skips_check(self, spark):
        df = _make_df(spark, _good_rows())
        ref = {"2024-06-14": 50000, "2024-06-13": 52000}  # only 2 days
        result = VolumeAnomalyDetector({"min_baseline_days": 5}, "test").validate(
            df, reference_counts=ref
        )
        assert result.passed  # skip → pass (warning only)
        assert "SKIP" in result.message

    def test_no_reference_counts_no_metrics_table_skips(self, spark):
        df = _make_df(spark, _good_rows())
        result = VolumeAnomalyDetector({}, "test").validate(df)
        assert result.passed
        assert "SKIP" in result.message

    def test_to_dict_structure(self, spark):
        df = _make_df(spark, _good_rows() * 10000)
        d = (
            VolumeAnomalyDetector({"threshold_pct": 0.30}, "test")
            .validate(df, reference_counts=self._REF_COUNTS)
            .to_dict()
        )
        assert d["validator"] == "VolumeAnomalyDetector"
        assert "current_count" in d
        assert "rolling_avg" in d
        assert "variance_pct" in d

    def test_exact_threshold_boundary_passes(self, spark):
        # avg=50,000; ±30% → [35000, 65000]; use exactly 35001 → just inside
        ref = {f"2024-06-{i:02d}": 50000 for i in range(8, 15)}
        df = _make_df(spark, _good_rows() * 7001)  # 35,005 rows
        result = VolumeAnomalyDetector(
            {"threshold_pct": 0.30, "min_baseline_days": 3}, "test"
        ).validate(df, reference_counts=ref)
        assert result.passed


# =============================================================================
# SchemaDriftDetector
# =============================================================================


class TestSchemaDriftDetector:
    _REFERENCE = {
        "order_id": "StringType()",
        "customer_id": "StringType()",
        "order_date": "DateType()",
        "quantity": "IntegerType()",
        "gross_revenue": "DoubleType()",
        "discount": "DoubleType()",
        "_loaded_at": "TimestampType()",
    }

    def test_exact_schema_match_passes(self, spark):
        df = _make_df(spark, _good_rows())
        result = SchemaDriftDetector(
            {"reference_schema": self._REFERENCE}, "test"
        ).validate(df)
        assert result.passed
        assert not result.has_drift

    def test_new_column_warns_not_fails_by_default(self, spark):
        df = _make_df(spark, _good_rows()).withColumn("extra_col", F.lit("x"))
        result = SchemaDriftDetector(
            {"reference_schema": self._REFERENCE, "fail_on_new_columns": False}, "test"
        ).validate(df)
        assert result.passed
        assert len(result.new_columns) == 1
        assert result.new_columns[0].column == "extra_col"

    def test_new_column_fails_when_configured(self, spark):
        df = _make_df(spark, _good_rows()).withColumn("extra_col", F.lit("x"))
        result = SchemaDriftDetector(
            {"reference_schema": self._REFERENCE, "fail_on_new_columns": True}, "test"
        ).validate(df)
        assert not result.passed

    def test_missing_column_fails_by_default(self, spark):
        df = _make_df(spark, _good_rows()).drop("discount")
        result = SchemaDriftDetector(
            {"reference_schema": self._REFERENCE}, "test"
        ).validate(df)
        assert not result.passed
        assert any(c.column == "discount" for c in result.missing_columns)

    def test_type_change_detected(self, spark):
        # Cast quantity to string (type change: IntegerType → StringType)
        df = _make_df(spark, _good_rows()).withColumn(
            "quantity", F.col("quantity").cast("string")
        )
        result = SchemaDriftDetector(
            {"reference_schema": self._REFERENCE, "fail_on_type_changes": True}, "test"
        ).validate(df)
        assert not result.passed
        assert any(c.column == "quantity" for c in result.type_changes)

    def test_ignore_columns_excluded_from_comparison(self, spark):
        df = (
            _make_df(spark, _good_rows())
            .drop("_loaded_at")
            .withColumn("new_audit_col", F.lit(1))
        )
        result = SchemaDriftDetector(
            {
                "reference_schema": self._REFERENCE,
                "ignore_columns": ["_loaded_at", "new_audit_col"],
            },
            "test",
        ).validate(df)
        assert result.passed

    def test_no_reference_saves_baseline(self, spark, tmp_path):
        schema_path = str(tmp_path / "schema.json")
        df = _make_df(spark, _good_rows())
        result = SchemaDriftDetector(
            {"reference_schema_path": schema_path}, "test"
        ).validate(df)
        assert result.passed
        assert Path(schema_path).exists()

    def test_loads_baseline_from_file(self, spark, tmp_path):
        schema_path = tmp_path / "schema.json"
        schema_path.write_text(json.dumps(self._REFERENCE))
        df = _make_df(spark, _good_rows())
        result = SchemaDriftDetector(
            {"reference_schema_path": str(schema_path)}, "test"
        ).validate(df)
        assert result.passed

    def test_to_dict_structure(self, spark):
        df = _make_df(spark, _good_rows())
        d = (
            SchemaDriftDetector({"reference_schema": self._REFERENCE}, "test")
            .validate(df)
            .to_dict()
        )
        assert d["validator"] == "SchemaDriftDetector"
        assert "new_columns" in d
        assert "missing_columns" in d
        assert "type_changes" in d


# =============================================================================
# FreshnessChecker
# =============================================================================


class TestFreshnessChecker:
    def test_fresh_data_passes(self, spark):
        ts = (_NOW - timedelta(hours=5)).replace(tzinfo=None)
        df = _make_df(
            spark,
            [
                ("ORD-001", "C1", None, 1, 10.0, 0.0, ts),
            ],
        )
        result = FreshnessChecker({"sla_hours": 26}, "test").validate(
            df, reference_time=_NOW
        )
        assert result.passed
        assert result.age_hours is not None
        assert result.age_hours < 26

    def test_stale_data_fails(self, spark):
        ts = (_NOW - timedelta(hours=30)).replace(tzinfo=None)
        df = _make_df(
            spark,
            [
                ("ORD-001", "C1", None, 1, 10.0, 0.0, ts),
            ],
        )
        result = FreshnessChecker({"sla_hours": 26, "warn_hours": 20}, "test").validate(
            df, reference_time=_NOW
        )
        assert not result.passed
        assert result.age_hours > 26
        assert "FAIL" in result.message

    def test_warn_zone_passes_with_warning(self, spark):
        ts = (_NOW - timedelta(hours=22)).replace(tzinfo=None)
        df = _make_df(
            spark,
            [
                ("ORD-001", "C1", None, 1, 10.0, 0.0, ts),
            ],
        )
        result = FreshnessChecker({"sla_hours": 26, "warn_hours": 20}, "test").validate(
            df, reference_time=_NOW
        )
        assert result.passed  # within SLA
        assert "WARN" in result.message

    def test_missing_timestamp_column_fails(self, spark):
        df = spark.createDataFrame(
            [("ORD-001", "C1")],
            schema=StructType(
                [
                    StructField("order_id", StringType()),
                    StructField("customer_id", StringType()),
                ]
            ),
        )
        result = FreshnessChecker(
            {"timestamp_column": "updated_at", "sla_hours": 24}, "test"
        ).validate(df, reference_time=_NOW)
        assert not result.passed
        assert "not in DataFrame" in result.message

    def test_all_null_timestamps_fails(self, spark):
        df = spark.createDataFrame(
            [(None,)],
            schema=StructType([StructField("_loaded_at", TimestampType(), True)]),
        )
        result = FreshnessChecker(
            {"timestamp_column": "_loaded_at", "sla_hours": 24}, "test"
        ).validate(df, reference_time=_NOW)
        assert not result.passed
        assert "null" in result.message.lower()

    def test_uses_max_timestamp_not_first(self, spark):
        ts_old = (_NOW - timedelta(hours=50)).replace(tzinfo=None)
        ts_new = (_NOW - timedelta(hours=2)).replace(tzinfo=None)
        df = _make_df(
            spark,
            [
                ("ORD-001", "C1", None, 1, 10.0, 0.0, ts_old),
                ("ORD-002", "C2", None, 1, 10.0, 0.0, ts_new),
            ],
        )
        result = FreshnessChecker({"sla_hours": 26}, "test").validate(
            df, reference_time=_NOW
        )
        assert result.passed  # newest record is 2h old — within SLA

    def test_to_dict_structure(self, spark):
        ts = (_NOW - timedelta(hours=5)).replace(tzinfo=None)
        df = _make_df(spark, [("ORD-001", "C1", None, 1, 10.0, 0.0, ts)])
        d = (
            FreshnessChecker({"sla_hours": 26}, "test")
            .validate(df, reference_time=_NOW)
            .to_dict()
        )
        assert d["validator"] == "FreshnessChecker"
        assert "age_hours" in d
        assert "sla_hours" in d
        assert "max_timestamp" in d

    def test_sla_exactly_at_boundary(self, spark):
        ts = (_NOW - timedelta(hours=26, seconds=1)).replace(tzinfo=None)
        df = _make_df(spark, [("ORD-001", "C1", None, 1, 10.0, 0.0, ts)])
        result = FreshnessChecker({"sla_hours": 26, "warn_hours": 20}, "test").validate(
            df, reference_time=_NOW
        )
        assert not result.passed  # 26h1s > 26h SLA

    def test_custom_timestamp_column(self, spark):
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("updated_at", TimestampType()),
            ]
        )
        ts = (_NOW - timedelta(hours=10)).replace(tzinfo=None)
        df = spark.createDataFrame([("1", ts)], schema=schema)
        result = FreshnessChecker(
            {"timestamp_column": "updated_at", "sla_hours": 24}, "test"
        ).validate(df, reference_time=_NOW)
        assert result.passed


# =============================================================================
# DQRunner orchestrator (unit-level — no real Spark write operations)
# =============================================================================


class TestDQRunner:
    """Tests for DQRunner config resolution and dispatcher logic."""

    def _make_config(self, **overrides) -> dict:
        base = {
            "alerts": {},
            "tables": {
                "*": {
                    "validators": {
                        "null_rate": {"global_threshold": 0.05},
                        "freshness": {
                            "timestamp_column": "_loaded_at",
                            "sla_hours": 26,
                        },
                    }
                }
            },
        }
        base["tables"]["*"]["validators"].update(overrides)
        return base

    def test_runner_loads_and_runs_without_error(self, spark):
        from orchestrator.dq_runner import DQRunner

        config = self._make_config()
        runner = DQRunner(config, table_name="test_table", pipeline_name="unit_test")
        df = _make_df(spark, _good_rows())
        result = runner.run(df, reference_time=_NOW)
        assert result.table_name == "test_table"
        assert result.pipeline_name == "unit_test"
        assert result.completed_at is not None

    def test_runner_reports_failure_when_validator_fails(self, spark):
        from orchestrator.dq_runner import DQRunner

        config = {
            "alerts": {},
            "tables": {
                "*": {
                    "validators": {
                        "freshness": {
                            "timestamp_column": "_loaded_at",
                            "sla_hours": 1,  # 1h SLA — will fail (data is 5h old in _good_rows)
                            "warn_hours": 0.5,
                        }
                    }
                }
            },
        }
        runner = DQRunner(config, table_name="test_table")
        stale_ts = _STALE_TS.replace(tzinfo=None)  # 30h old — clearly exceeds 1h SLA
        df = _make_df(
            spark,
            [("ORD-001", "C1", None, 1, 10.0, 0.0, stale_ts)],
        )
        result = runner.run(df, reference_time=_NOW)
        assert not result.overall_passed

    def test_runner_from_config_file(self, spark, tmp_path):
        from orchestrator.dq_runner import DQRunner

        cfg = {
            "alerts": {},
            "tables": {
                "*": {
                    "validators": {
                        "null_rate": {"global_threshold": 0.10},
                    }
                }
            },
        }
        import yaml

        config_path = tmp_path / "thresholds.yml"
        config_path.write_text(yaml.dump(cfg))

        runner = DQRunner.from_config(str(config_path), table_name="test_table")
        df = _make_df(spark, _good_rows())
        result = runner.run(df)
        assert result.table_name == "test_table"

    def test_runner_table_config_override_merges_correctly(self, spark):
        from orchestrator.dq_runner import DQRunner

        config = {
            "alerts": {},
            "tables": {
                "*": {
                    "validators": {
                        "null_rate": {"global_threshold": 0.05},
                    }
                },
                "fact_orders": {
                    "validators": {
                        "null_rate": {
                            "global_threshold": 0.01,
                            "column_thresholds": {"order_id": 0.0},
                        }
                    }
                },
            },
        }
        runner = DQRunner(config, table_name="fact_orders")
        # Verify the merged config was applied (threshold is 0.01, not 0.05)
        null_validator = next(
            v for name, v in runner._validators if name == "null_rate"
        )
        assert null_validator.global_threshold == 0.01
        assert null_validator.column_thresholds.get("order_id") == 0.0

    def test_runner_result_to_dict(self, spark):
        from orchestrator.dq_runner import DQRunner

        config = {"alerts": {}, "tables": {"*": {"validators": {"null_rate": {}}}}}
        runner = DQRunner(config, table_name="test")
        df = _make_df(spark, _good_rows())
        d = runner.run(df).to_dict()
        assert "run_id" in d
        assert "overall_passed" in d
        assert "validator_results" in d
        assert "duration_seconds" in d

    def test_runner_handles_no_validators_configured(self, spark):
        from orchestrator.dq_runner import DQRunner

        config = {"alerts": {}, "tables": {"*": {}}}
        runner = DQRunner(config, table_name="test")
        df = _make_df(spark, _good_rows())
        result = runner.run(df)
        assert result.overall_passed
        assert result.validator_results == []

    def test_runner_alerts_not_sent_on_pass_by_default(self, spark):
        from orchestrator.dq_runner import DQRunner

        mock_slack = MagicMock()
        config = {"alerts": {}, "tables": {"*": {"validators": {"null_rate": {}}}}}
        runner = DQRunner(config, table_name="test")
        runner._slack = mock_slack

        df = _make_df(spark, _good_rows())
        result = runner.run(df)
        assert result.overall_passed
        mock_slack.send_dq_report.assert_not_called()

    def test_runner_alerts_sent_on_failure(self, spark):
        from orchestrator.dq_runner import DQRunner

        config = {
            "alerts": {},
            "tables": {
                "*": {
                    "validators": {
                        "freshness": {
                            "timestamp_column": "_loaded_at",
                            "sla_hours": 0.001,  # microsecond SLA → always fails
                            "warn_hours": 0.0001,
                        }
                    }
                }
            },
        }
        runner = DQRunner(config, table_name="test")
        mock_slack = MagicMock(return_value=True)
        mock_slack.send_dq_report = MagicMock(return_value=True)
        runner._slack = mock_slack

        df = _make_df(spark, _good_rows())
        result = runner.run(df, reference_time=_NOW)
        assert not result.overall_passed
        mock_slack.send_dq_report.assert_called_once()
