"""
Schema drift detector — detects new, missing, and type-changed columns vs a reference schema.

Config example (YAML, under validators.schema_drift):
    reference_schema_path: "config/schemas/fact_orders.json"
    fail_on_new_columns: false      # new columns are treated as warnings only
    fail_on_missing_columns: true
    fail_on_type_changes: true
    ignore_columns: ["_loaded_at", "_glue_run_id"]
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


# ── Result objects ─────────────────────────────────────────────────────────────


@dataclass
class ColumnDrift:
    column: str
    drift_type: str  # "new" | "missing" | "type_changed"
    expected_type: str | None = None
    actual_type: str | None = None


@dataclass
class SchemaDriftResult:
    table_name: str
    passed: bool
    new_columns: list[ColumnDrift] = field(default_factory=list)
    missing_columns: list[ColumnDrift] = field(default_factory=list)
    type_changes: list[ColumnDrift] = field(default_factory=list)
    message: str = ""

    @property
    def has_drift(self) -> bool:
        return bool(self.new_columns or self.missing_columns or self.type_changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "validator": "SchemaDriftDetector",
            "table_name": self.table_name,
            "passed": self.passed,
            "new_columns": [c.column for c in self.new_columns],
            "missing_columns": [c.column for c in self.missing_columns],
            "type_changes": [
                {
                    "column": c.column,
                    "expected": c.expected_type,
                    "actual": c.actual_type,
                }
                for c in self.type_changes
            ],
            "message": self.message,
        }


# ── Validator ──────────────────────────────────────────────────────────────────


class SchemaDriftDetector:
    """
    Compares the current DataFrame schema against a stored reference (JSON file or dict).

    First run behaviour: if no reference schema exists, the current schema is saved
    as the baseline and the check passes. Subsequent runs compare against it.

    Usage:
        detector = SchemaDriftDetector(config, table_name="fact_orders")
        result = detector.validate(df)

        # Manually update the baseline after a planned schema migration:
        detector.save_reference_schema(df)
    """

    def __init__(self, config: dict[str, Any], table_name: str = "unknown") -> None:
        self.table_name = table_name
        self.reference_schema_path: str | None = config.get("reference_schema_path")
        self.fail_on_new_columns: bool = bool(config.get("fail_on_new_columns", False))
        self.fail_on_missing_columns: bool = bool(
            config.get("fail_on_missing_columns", True)
        )
        self.fail_on_type_changes: bool = bool(config.get("fail_on_type_changes", True))
        self.ignore_columns: set[str] = set(config.get("ignore_columns", []))
        # Allow passing schema inline (useful in tests)
        self._inline_reference: dict[str, str] | None = config.get("reference_schema")

    # ── Public ─────────────────────────────────────────────────────────────────

    def validate(
        self,
        df: DataFrame,
        reference_schema: dict[str, str] | None = None,
    ) -> SchemaDriftResult:
        """
        Args:
            df: Current DataFrame.
            reference_schema: Override {column_name: spark_type_string}. Falls back to
                              inline config, then to the JSON file on disk.
        """
        ref = reference_schema or self._inline_reference or self._load_from_file()

        if ref is None:
            logger.warning(
                "[%s] No reference schema found — saving current schema as baseline.",
                self.table_name,
            )
            self._save_to_file(df.schema)
            return SchemaDriftResult(
                table_name=self.table_name,
                passed=True,
                message="SKIP: No reference schema — baseline saved for future runs.",
            )

        actual = self._schema_to_dict(df.schema)
        return self._compare(ref, actual)

    def save_reference_schema(self, df: DataFrame) -> None:
        """Persist the current DataFrame schema as the new reference baseline."""
        self._save_to_file(df.schema)

    # ── Private ────────────────────────────────────────────────────────────────

    def _compare(
        self, expected: dict[str, str], actual: dict[str, str]
    ) -> SchemaDriftResult:
        exp_cols = {k for k in expected if k not in self.ignore_columns}
        act_cols = {k for k in actual if k not in self.ignore_columns}

        new_columns = [
            ColumnDrift(column=c, drift_type="new", actual_type=actual[c])
            for c in sorted(act_cols - exp_cols)
        ]
        missing_columns = [
            ColumnDrift(column=c, drift_type="missing", expected_type=expected[c])
            for c in sorted(exp_cols - act_cols)
        ]
        type_changes = [
            ColumnDrift(
                column=c,
                drift_type="type_changed",
                expected_type=expected[c],
                actual_type=actual[c],
            )
            for c in sorted(exp_cols & act_cols)
            if expected[c] != actual[c]
        ]

        # Determine pass/fail based on configuration
        failing_parts: list[str] = []
        if self.fail_on_new_columns and new_columns:
            failing_parts.append(f"{len(new_columns)} new column(s)")
        if self.fail_on_missing_columns and missing_columns:
            failing_parts.append(f"{len(missing_columns)} missing column(s)")
        if self.fail_on_type_changes and type_changes:
            failing_parts.append(f"{len(type_changes)} type change(s)")

        passed = not bool(failing_parts)

        drift_parts: list[str] = []
        if new_columns:
            drift_parts.append(f"new={[c.column for c in new_columns]}")
        if missing_columns:
            drift_parts.append(f"missing={[c.column for c in missing_columns]}")
        if type_changes:
            drift_parts.append(
                f"type_changed={[f'{c.column}:{c.expected_type}->{c.actual_type}' for c in type_changes]}"
            )

        if not drift_parts:
            message = "PASS: Schema matches reference exactly."
        elif passed:
            message = f"PASS (warnings only): {'; '.join(drift_parts)}"
        else:
            message = f"FAIL: {', '.join(failing_parts)} — {'; '.join(drift_parts)}"

        log_fn = logger.warning if not passed else logger.info
        log_fn("[%s] %s", self.table_name, message)

        return SchemaDriftResult(
            table_name=self.table_name,
            passed=passed,
            new_columns=new_columns,
            missing_columns=missing_columns,
            type_changes=type_changes,
            message=message,
        )

    @staticmethod
    def _schema_to_dict(schema: StructType) -> dict[str, str]:
        return {f.name: str(f.dataType) for f in schema.fields}

    def _load_from_file(self) -> dict[str, str] | None:
        if not self.reference_schema_path:
            return None
        path = Path(self.reference_schema_path)
        if not path.exists():
            return None
        with path.open() as fh:
            return json.load(fh)

    def _save_to_file(self, schema: StructType) -> None:
        if not self.reference_schema_path:
            logger.warning(
                "[%s] No reference_schema_path — cannot persist baseline.",
                self.table_name,
            )
            return
        path = Path(self.reference_schema_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as fh:
            json.dump(self._schema_to_dict(schema), fh, indent=2)
        logger.info("[%s] Reference schema saved → %s", self.table_name, path)
