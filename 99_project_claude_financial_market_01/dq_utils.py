# =============================================================================
# dq_utils.py  –  Data-Quality Checks (Great Expectations lite)
# =============================================================================
# Lightweight DQ framework that works without GE installation.
# In prod the pipeline halts on any CRITICAL rule failure.
# Results are written to the audit table fin_platform_<env>.audit.dq_results.
# =============================================================================

from __future__ import annotations
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, List, Optional

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger("dq_utils")


# ---------------------------------------------------------------------------
# Enums & dataclasses
# ---------------------------------------------------------------------------

class Severity(str, Enum):
    CRITICAL = "CRITICAL"   # failure halts pipeline in prod
    WARNING  = "WARNING"    # logged but pipeline continues


@dataclass
class DQRule:
    name:        str
    description: str
    severity:    Severity
    check_fn:    Callable[[DataFrame], bool]   # True  → pass


@dataclass
class DQResult:
    rule_name:   str
    severity:    str
    passed:      bool
    details:     str


@dataclass
class DQReport:
    pipeline:    str
    layer:       str
    table:       str
    run_date:    str
    results:     List[DQResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def critical_failures(self) -> List[DQResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.CRITICAL]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class DataQualityRunner:

    def __init__(
        self,
        spark: SparkSession,
        pipeline: str,
        layer: str,
        table: str,
        halt_on_critical: bool = True,
    ):
        self.spark             = spark
        self.pipeline          = pipeline
        self.layer             = layer
        self.table             = table
        self.halt_on_critical  = halt_on_critical
        self.rules: List[DQRule] = []

    # ------------------------------------------------------------------
    # Fluent rule builders
    # ------------------------------------------------------------------

    def expect_no_nulls(self, col: str, severity: Severity = Severity.CRITICAL) -> "DataQualityRunner":
        def _check(df: DataFrame) -> bool:
            return df.filter(F.col(col).isNull()).count() == 0
        self.rules.append(DQRule(
            name=f"no_nulls_{col}", severity=severity,
            description=f"Column '{col}' must contain no NULLs.",
            check_fn=_check,
        ))
        return self

    def expect_row_count_gt(self, min_rows: int, severity: Severity = Severity.CRITICAL) -> "DataQualityRunner":
        def _check(df: DataFrame) -> bool:
            return df.count() > min_rows
        self.rules.append(DQRule(
            name=f"row_count_gt_{min_rows}", severity=severity,
            description=f"DataFrame must have more than {min_rows} rows.",
            check_fn=_check,
        ))
        return self

    def expect_column_values_in_set(
        self, col: str, valid_values: list, severity: Severity = Severity.CRITICAL
    ) -> "DataQualityRunner":
        def _check(df: DataFrame) -> bool:
            return df.filter(~F.col(col).isin(valid_values)).count() == 0
        self.rules.append(DQRule(
            name=f"values_in_set_{col}", severity=severity,
            description=f"Column '{col}' values must be in {valid_values}.",
            check_fn=_check,
        ))
        return self

    def expect_no_duplicates(self, key_cols: list, severity: Severity = Severity.CRITICAL) -> "DataQualityRunner":
        def _check(df: DataFrame) -> bool:
            total = df.count()
            distinct = df.select(*key_cols).distinct().count()
            return total == distinct
        self.rules.append(DQRule(
            name=f"no_duplicates_{'_'.join(key_cols)}", severity=severity,
            description=f"Composite key {key_cols} must be unique.",
            check_fn=_check,
        ))
        return self

    def expect_column_values_positive(self, col: str, severity: Severity = Severity.WARNING) -> "DataQualityRunner":
        def _check(df: DataFrame) -> bool:
            return df.filter(F.col(col) <= 0).count() == 0
        self.rules.append(DQRule(
            name=f"positive_{col}", severity=severity,
            description=f"Column '{col}' must be positive.",
            check_fn=_check,
        ))
        return self

    def add_custom_rule(self, rule: DQRule) -> "DataQualityRunner":
        self.rules.append(rule)
        return self

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def run(self, df: DataFrame) -> DQReport:
        df.cache()
        run_date = datetime.now(timezone.utc).isoformat()
        report   = DQReport(
            pipeline=self.pipeline, layer=self.layer,
            table=self.table,       run_date=run_date,
        )

        for rule in self.rules:
            try:
                passed = rule.check_fn(df)
                details = "PASS" if passed else f"FAIL – {rule.description}"
            except Exception as exc:
                passed  = False
                details = f"ERROR during check: {exc}"

            result = DQResult(
                rule_name=rule.name,
                severity=rule.severity.value,
                passed=passed,
                details=details,
            )
            report.results.append(result)

            level = logging.INFO if passed else (
                logging.ERROR if rule.severity == Severity.CRITICAL else logging.WARNING
            )
            logger.log(level, "DQ rule '%s': %s", rule.name, details,
                       extra={"dq_table": self.table, "dq_layer": self.layer})

        df.unpersist()
        self._persist_report(report)

        if self.halt_on_critical and report.critical_failures:
            names = [r.rule_name for r in report.critical_failures]
            raise RuntimeError(
                f"[DataQuality] Pipeline halted – critical DQ failures on "
                f"'{self.table}': {names}"
            )
        return report

    # ------------------------------------------------------------------
    # Persist to audit table
    # ------------------------------------------------------------------

    def _persist_report(self, report: DQReport):
        try:
            rows = [
                (
                    report.pipeline, report.layer, report.table, report.run_date,
                    r.rule_name, r.severity, r.passed, r.details,
                )
                for r in report.results
            ]
            schema = (
                "pipeline STRING, layer STRING, table_name STRING, run_date STRING, "
                "rule_name STRING, severity STRING, passed BOOLEAN, details STRING"
            )
            audit_df = self.spark.createDataFrame(rows, schema=schema)
            # Determine audit catalog from table name prefix
            catalog = report.table.split(".")[0] if "." in report.table else "fin_platform_dev"
            audit_df.write.format("delta").mode("append").saveAsTable(
                f"{catalog}.audit.dq_results"
            )
        except Exception as exc:
            logger.warning("Could not persist DQ report to audit table: %s", exc)
