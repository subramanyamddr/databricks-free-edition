# Databricks notebook source
# FILE: notebooks/utils/dq_engine.py
# PURPOSE: Data Quality validation engine — runs rules from dq_rules table
#          Supports: not_null, unique, range, regex, custom_sql, referential
# VERSION: 1.0 — Production Grade

# COMMAND ----------

from typing import List, Dict, Any, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import logging

logger = logging.getLogger("dq_engine")


# COMMAND ----------
# ================================================================
# DATA QUALITY ENGINE
# ================================================================

class DataQualityEngine:
    """
    Metadata-driven DQ engine.
    Rules are fetched from framework.dq_rules table.
    Results written to framework.dq_results table.
    Bad records routed to quarantine via QuarantineManager.
    """

    def __init__(
        self,
        spark: SparkSession,
        quarantine_manager,  # QuarantineManager instance
        dq_results_table: str = "framework_catalog.framework.dq_results",
        dq_rules_table: str   = "framework_catalog.framework.dq_rules"
    ):
        self.spark              = spark
        self.qm                 = quarantine_manager
        self.dq_results_table   = dq_results_table
        self.dq_rules_table     = dq_rules_table

    def run_all_rules(
        self,
        df: DataFrame,
        config: Dict[str, Any],
        run_id: str,
        audit_id: int
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Run all active DQ rules for a pipeline.
        Returns (clean_df, dq_summary).
        clean_df excludes records that failed 'error' severity rules.
        """
        rules = self._get_rules(config["pipeline_id"])

        if not rules:
            logger.info(f"No DQ rules configured for pipeline: {config['pipeline_name']}")
            return df, {"total_rules": 0, "passed": 0, "failed": 0, "warnings": 0}

        clean_df            = df
        total_rows          = df.count()
        summary             = {"total_rules": len(rules), "passed": 0, "failed": 0, "warnings": 0}
        pipeline_halted     = False

        for rule in rules:
            if pipeline_halted:
                break

            result = self._evaluate_rule(clean_df, rule, total_rows)
            self._write_result(result, config, run_id, audit_id)

            if result["status"] == "pass":
                summary["passed"] += 1
                logger.info(f"DQ PASS — {rule['rule_name']}: {result['passed_records']}/{total_rows} records passed")

            elif result["status"] == "warning":
                summary["warnings"] += 1
                logger.warning(f"DQ WARNING — {rule['rule_name']}: {result['failed_records']} records failed ({result['failure_pct']:.2f}%)")

            elif result["status"] == "fail":
                summary["failed"] += 1
                logger.error(f"DQ FAIL — {rule['rule_name']}: {result['failed_records']} records failed ({result['failure_pct']:.2f}%)")

                # Quarantine bad records
                bad_df = clean_df.filter(f"NOT ({rule['rule_expression']})")
                self.qm.write_quarantine(
                    bad_df, config, run_id,
                    error_type="dq_failure",
                    error_message=f"Failed rule: {rule['rule_name']} — {rule['rule_expression']}",
                    failed_rule_id=rule["rule_id"]
                )

                if rule["severity"] == "error" and result["failure_pct"] > rule["threshold_pct"]:
                    # Remove bad records from clean set
                    clean_df = clean_df.filter(rule["rule_expression"])
                    logger.info(f"Removed {result['failed_records']} bad records. Continuing with clean set.")

        return clean_df, summary

    def _evaluate_rule(self, df: DataFrame, rule: Dict[str, Any], total_rows: int) -> Dict[str, Any]:
        """Evaluate a single DQ rule against the DataFrame."""
        rule_expr = rule["rule_expression"]

        try:
            passed_df       = df.filter(rule_expr)
            passed_count    = passed_df.count()
            failed_count    = total_rows - passed_count
            failure_pct     = (failed_count / total_rows * 100) if total_rows > 0 else 0

            # Determine status
            threshold = float(rule.get("threshold_pct") or 0)
            if failed_count == 0:
                status = "pass"
            elif failure_pct <= threshold:
                status = "warning" if rule["severity"] == "warning" else "pass"
            else:
                status = "fail" if rule["severity"] == "error" else "warning"

            return {
                "rule_id":          rule["rule_id"],
                "rule_name":        rule["rule_name"],
                "rule_type":        rule["rule_type"],
                "column_name":      rule.get("column_name", ""),
                "total_records":    total_rows,
                "passed_records":   passed_count,
                "failed_records":   failed_count,
                "failure_pct":      round(failure_pct, 4),
                "status":           status
            }
        except Exception as e:
            logger.error(f"DQ rule evaluation error for '{rule['rule_name']}': {e}")
            return {
                "rule_id":          rule["rule_id"],
                "rule_name":        rule["rule_name"],
                "rule_type":        rule["rule_type"],
                "column_name":      rule.get("column_name", ""),
                "total_records":    total_rows,
                "passed_records":   0,
                "failed_records":   total_rows,
                "failure_pct":      100.0,
                "status":           "fail"
            }

    def _get_rules(self, pipeline_id: int) -> List[Dict[str, Any]]:
        """Load active DQ rules for pipeline from control table."""
        df = self.spark.sql(f"""
            SELECT * FROM {self.dq_rules_table}
            WHERE pipeline_id = {pipeline_id}
              AND active_flag = TRUE
            ORDER BY severity DESC, rule_id ASC
        """)
        return [row.asDict() for row in df.collect()]

    def _write_result(
        self,
        result: Dict[str, Any],
        config: Dict[str, Any],
        run_id: str,
        audit_id: int
    ) -> None:
        """Persist DQ result to dq_results table."""
        try:
            self.spark.sql(f"""
                INSERT INTO {self.dq_results_table} (
                    run_id, audit_id, rule_id, pipeline_id,
                    rule_name, rule_type, column_name,
                    total_records, passed_records, failed_records,
                    failure_pct, status, run_date
                ) VALUES (
                    '{run_id}',
                    {audit_id},
                    {result['rule_id']},
                    {config['pipeline_id']},
                    '{result['rule_name']}',
                    '{result['rule_type']}',
                    '{result.get('column_name', '')}',
                    {result['total_records']},
                    {result['passed_records']},
                    {result['failed_records']},
                    {result['failure_pct']},
                    '{result['status']}',
                    CURRENT_DATE()
                )
            """)
        except Exception as e:
            logger.warning(f"DQ result write failed (non-blocking): {e}")


logger.info("dq_engine loaded successfully")

