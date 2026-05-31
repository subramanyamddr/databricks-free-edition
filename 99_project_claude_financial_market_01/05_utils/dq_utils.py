# =============================================================================
# dq_utils.py
# Data Quality checking framework for pipeline validation
# =============================================================================

from enum import Enum
from datetime import datetime, timezone
from typing import List, Optional
import pyspark.sql.functions as F


class Severity(Enum):
    """Severity levels for data quality checks."""
    CRITICAL = "CRITICAL"  # Will halt pipeline if check fails
    WARNING = "WARNING"    # Will log but continue
    INFO = "INFO"          # Informational only


class DQCheck:
    """Represents a single data quality check."""
    
    def __init__(self, check_type, description, severity, params=None):
        self.check_type = check_type
        self.description = description
        self.severity = severity
        self.params = params or {}
        self.passed = None
        self.result_value = None
        self.error_message = None


class DataQualityRunner:
    """Runner for executing data quality checks on DataFrames.
    
    Usage:
        dq = (
            DataQualityRunner(spark, "my_pipeline", "bronze", "my_table")
            .expect_row_count_gt(0, Severity.CRITICAL)
            .expect_no_nulls("id", Severity.CRITICAL)
            .expect_no_duplicates(["id"], Severity.CRITICAL)
        )
        report = dq.run(df)
    """
    
    def __init__(self, spark, pipeline_name, layer, table_name, halt_on_critical=True):
        """Initialize the data quality runner.
        
        Args:
            spark: SparkSession instance
            pipeline_name: Name of the pipeline
            layer: Data layer (bronze, silver, gold)
            table_name: Table being validated
            halt_on_critical: If True, raise exception on critical failures
        """
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.layer = layer
        self.table_name = table_name
        self.halt_on_critical = halt_on_critical
        self.checks = []
    
    def expect_row_count_gt(self, threshold, severity):
        """Expect row count to be greater than threshold."""
        self.checks.append(DQCheck(
            check_type="row_count_gt",
            description=f"Row count > {threshold}",
            severity=severity,
            params={"threshold": threshold}
        ))
        return self
    
    def expect_no_nulls(self, column, severity):
        """Expect no null values in specified column."""
        self.checks.append(DQCheck(
            check_type="no_nulls",
            description=f"No nulls in {column}",
            severity=severity,
            params={"column": column}
        ))
        return self
    
    def expect_no_duplicates(self, columns, severity):
        """Expect no duplicate rows based on specified columns."""
        col_list = columns if isinstance(columns, list) else [columns]
        self.checks.append(DQCheck(
            check_type="no_duplicates",
            description=f"No duplicates on {', '.join(col_list)}",
            severity=severity,
            params={"columns": col_list}
        ))
        return self
    
    def expect_column_values_positive(self, column, severity):
        """Expect all values in column to be positive (> 0)."""
        self.checks.append(DQCheck(
            check_type="positive_values",
            description=f"{column} values are positive",
            severity=severity,
            params={"column": column}
        ))
        return self
    
    def run(self, df):
        """Execute all registered checks against the DataFrame.
        
        Args:
            df: PySpark DataFrame to validate
        
        Returns:
            Dictionary containing check results
        
        Raises:
            Exception: If halt_on_critical=True and a critical check fails
        """
        results = {
            "pipeline": self.pipeline_name,
            "layer": self.layer,
            "table": self.table_name,
            "timestamp": datetime.now(timezone.utc),
            "checks": [],
            "total_checks": len(self.checks),
            "passed": 0,
            "failed": 0,
            "critical_failures": 0
        }
        
        for check in self.checks:
            try:
                self._execute_check(check, df)
                results["checks"].append({
                    "type": check.check_type,
                    "description": check.description,
                    "severity": check.severity.value,
                    "passed": check.passed,
                    "result_value": check.result_value,
                    "error": check.error_message
                })
                
                if check.passed:
                    results["passed"] += 1
                else:
                    results["failed"] += 1
                    if check.severity == Severity.CRITICAL:
                        results["critical_failures"] += 1
            
            except Exception as e:
                check.passed = False
                check.error_message = str(e)
                results["checks"].append({
                    "type": check.check_type,
                    "description": check.description,
                    "severity": check.severity.value,
                    "passed": False,
                    "result_value": None,
                    "error": str(e)
                })
                results["failed"] += 1
                if check.severity == Severity.CRITICAL:
                    results["critical_failures"] += 1
        
        # Halt if critical failures and halt_on_critical is True
        if self.halt_on_critical and results["critical_failures"] > 0:
            failed_checks = [c for c in results["checks"] if not c["passed"] and c["severity"] == "CRITICAL"]
            error_msg = f"Data quality validation failed: {results['critical_failures']} critical check(s) failed\n"
            for fc in failed_checks:
                error_msg += f"  - {fc['description']}: {fc['error']}\n"
            raise Exception(error_msg)
        
        return results
    
    def _execute_check(self, check, df):
        """Execute a single check against the DataFrame."""
        if check.check_type == "row_count_gt":
            count = df.count()
            check.result_value = count
            check.passed = count > check.params["threshold"]
            if not check.passed:
                check.error_message = f"Expected > {check.params['threshold']}, got {count}"
        
        elif check.check_type == "no_nulls":
            column = check.params["column"]
            null_count = df.filter(F.col(column).isNull()).count()
            check.result_value = null_count
            check.passed = null_count == 0
            if not check.passed:
                check.error_message = f"Found {null_count} null values in {column}"
        
        elif check.check_type == "no_duplicates":
            columns = check.params["columns"]
            total_count = df.count()
            distinct_count = df.select(columns).distinct().count()
            duplicate_count = total_count - distinct_count
            check.result_value = duplicate_count
            check.passed = duplicate_count == 0
            if not check.passed:
                check.error_message = f"Found {duplicate_count} duplicate rows on {', '.join(columns)}"
        
        elif check.check_type == "positive_values":
            column = check.params["column"]
            # Cast to double for comparison, filter non-positive values
            non_positive_count = df.filter(
                (F.col(column).isNotNull()) & 
                (F.col(column).cast("double") <= 0)
            ).count()
            check.result_value = non_positive_count
            check.passed = non_positive_count == 0
            if not check.passed:
                check.error_message = f"Found {non_positive_count} non-positive values in {column}"
        
        else:
            raise ValueError(f"Unknown check type: {check.check_type}")
