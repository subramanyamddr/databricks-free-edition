# =============================================================================
# tests/test_dq_checks.py
# Unit tests for DQX check definitions and pipeline transformations
# Run with: pytest tests/ -v
# On Databricks: use a test notebook or nutter framework
# =============================================================================

import pytest
from unittest.mock import MagicMock, patch
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("sales_pipeline_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def silver_schema():
    return StructType([
        StructField("order_id",    IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product",     StringType(), True),
        StructField("quantity",    IntegerType(), True),
        StructField("unit_price",  DecimalType(10, 2), True),
        StructField("order_date",  DateType(), True),
        StructField("region",      StringType(), True),
        StructField("revenue",     DecimalType(10, 2), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_updated_at", TimestampType(), True),
        StructField("_pipeline_run", StringType(), True),
    ])


@pytest.fixture
def good_silver_rows(spark, silver_schema):
    """All rows pass every Silver DQ check."""
    from datetime import datetime
    rows = [
        (1001, "C001", "Widget A", 2, 25.00, date(2024, 1, 15), "North", 50.00,
         datetime(2024, 1, 15), datetime(2024, 1, 15), "run001"),
        (1002, "C002", "Widget B", 5, 10.00, date(2024, 1, 15), "South", 50.00,
         datetime(2024, 1, 15), datetime(2024, 1, 15), "run001"),
    ]
    return spark.createDataFrame(rows, schema=silver_schema)


@pytest.fixture
def bad_silver_rows(spark, silver_schema):
    """Contains rows that should fail various DQ checks."""
    from datetime import datetime
    rows = [
        # null order_id
        (None, "C003", "Widget A", 2, 25.00, date(2024, 1, 15), "North", 50.00,
         datetime(2024, 1, 15), datetime(2024, 1, 15), "run001"),
        # null customer_id
        (1003, None,   "Widget A", 2, 25.00, date(2024, 1, 15), "East", 50.00,
         datetime(2024, 1, 15), datetime(2024, 1, 15), "run001"),
        # quantity = 0 (below min)
        (1004, "C004", "Widget B", 0, 10.00, date(2024, 1, 17), "South", 0.00,
         datetime(2024, 1, 17), datetime(2024, 1, 17), "run001"),
        # negative unit_price
        (1005, "C005", "Widget C", 1, -5.00, date(2024, 1, 18), "East", -5.00,
         datetime(2024, 1, 18), datetime(2024, 1, 18), "run001"),
        # invalid region (warn only — should NOT be quarantined)
        (1006, "C006", "Widget A", 2, 25.00, date(2024, 1, 18), "INVALID", 50.00,
         datetime(2024, 1, 18), datetime(2024, 1, 18), "run001"),
    ]
    return spark.createDataFrame(rows, schema=silver_schema)


# ---------------------------------------------------------------------------
# Bronze transformation tests
# ---------------------------------------------------------------------------

class TestBronzeAuditColumns:
    def test_ingested_at_added(self, spark):
        df = spark.createDataFrame(
            [("1001", "C001", "Widget A", "2", "25.00", "2024-01-15", "North")],
            ["order_id", "customer_id", "product", "quantity", "unit_price", "order_date", "region"]
        )
        df_out = df.withColumn("_ingested_at", F.current_timestamp())
        assert "_ingested_at" in df_out.columns

    def test_pipeline_run_column(self, spark):
        df = spark.createDataFrame([("1001",)], ["order_id"])
        df_out = df.withColumn("_pipeline_run", F.lit("test_run_123"))
        assert df_out.first()["_pipeline_run"] == "test_run_123"


# ---------------------------------------------------------------------------
# Silver transformation tests
# ---------------------------------------------------------------------------

class TestSilverTransformations:
    def test_order_id_cast_to_int(self, spark):
        df = spark.createDataFrame([("1001",)], ["order_id"])
        result = df.withColumn("order_id", F.col("order_id").cast(IntegerType()))
        assert result.first()["order_id"] == 1001

    def test_customer_id_uppercased_and_trimmed(self, spark):
        df = spark.createDataFrame([("  c001  ",)], ["customer_id"])
        result = df.withColumn("customer_id", F.trim(F.upper(F.col("customer_id"))))
        assert result.first()["customer_id"] == "C001"

    def test_revenue_computed_correctly(self, spark):
        from decimal import Decimal
        df = spark.createDataFrame(
            [(2, Decimal("25.00"))],
            ["quantity", "unit_price"]
        )
        result = df.withColumn(
            "revenue",
            F.round(
                F.col("quantity").cast(DecimalType(10, 2)) * F.col("unit_price"),
                2
            )
        )
        assert float(result.first()["revenue"]) == 50.00

    def test_dedup_keeps_one_per_order_id(self, spark):
        df = spark.createDataFrame(
            [(1001, "C001"), (1001, "C001"), (1002, "C002")],
            ["order_id", "customer_id"]
        )
        result = df.dropDuplicates(["order_id"])
        assert result.count() == 2

    def test_invalid_order_id_casts_to_null(self, spark):
        df = spark.createDataFrame([("NOT_AN_INT",)], ["order_id"])
        result = df.withColumn("order_id", F.col("order_id").cast(IntegerType()))
        assert result.first()["order_id"] is None


# ---------------------------------------------------------------------------
# DQX check definition tests (mocked — no real DQX needed in CI)
# ---------------------------------------------------------------------------

class TestSilverDQCheckDefinitions:
    def test_silver_checks_loaded(self):
        import sys
        sys.path.insert(0, ".")
        from utils.dq_checks import SILVER_CHECKS
        assert len(SILVER_CHECKS) > 0

    def test_all_checks_have_required_keys(self):
        from utils.dq_checks import SILVER_CHECKS
        for check in SILVER_CHECKS:
            assert "name" in check, f"Missing 'name' in check: {check}"
            assert "criticality" in check, f"Missing 'criticality' in: {check}"
            assert "check" in check, f"Missing 'check' in: {check}"
            assert "function" in check["check"], f"Missing 'function' in: {check}"
            assert "arguments" in check["check"], f"Missing 'arguments' in: {check}"

    def test_criticality_values_are_valid(self):
        from utils.dq_checks import SILVER_CHECKS, GOLD_CHECKS
        valid = {"error", "warn"}
        for check in SILVER_CHECKS + GOLD_CHECKS:
            assert check["criticality"] in valid, (
                f"Invalid criticality '{check['criticality']}' in check '{check['name']}'"
            )

    def test_region_check_is_warn_only(self):
        from utils.dq_checks import SILVER_CHECKS
        region_check = next(c for c in SILVER_CHECKS if c["name"] == "region_valid")
        assert region_check["criticality"] == "warn"

    def test_order_id_check_is_error(self):
        from utils.dq_checks import SILVER_CHECKS
        oid_check = next(c for c in SILVER_CHECKS if c["name"] == "order_id_not_null")
        assert oid_check["criticality"] == "error"


class TestGoldDQCheckDefinitions:
    def test_gold_checks_loaded(self):
        from utils.dq_checks import GOLD_CHECKS
        assert len(GOLD_CHECKS) > 0

    def test_total_orders_check_present(self):
        from utils.dq_checks import GOLD_CHECKS
        names = [c["name"] for c in GOLD_CHECKS]
        assert "total_orders_positive" in names

    def test_total_revenue_check_present(self):
        from utils.dq_checks import GOLD_CHECKS
        names = [c["name"] for c in GOLD_CHECKS]
        assert "total_revenue_non_negative" in names


# ---------------------------------------------------------------------------
# Gold aggregation tests
# ---------------------------------------------------------------------------

class TestGoldAggregation:
    def test_gold_aggregation_row_count(self, spark, silver_schema):
        from datetime import datetime
        rows = [
            (1001, "C001", "Widget A", 2, 25.00, date(2024, 1, 15), "North", 50.00,
             datetime(2024, 1, 15), datetime(2024, 1, 15), "r1"),
            (1002, "C002", "Widget A", 3, 25.00, date(2024, 1, 15), "North", 75.00,
             datetime(2024, 1, 15), datetime(2024, 1, 15), "r1"),
            (1003, "C003", "Widget B", 1, 10.00, date(2024, 1, 15), "South", 10.00,
             datetime(2024, 1, 15), datetime(2024, 1, 15), "r1"),
        ]
        df = spark.createDataFrame(rows, schema=silver_schema)
        df_gold = (
            df.groupBy("order_date", "region", "product")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("quantity").alias("total_quantity"),
                F.round(F.sum("revenue"), 2).alias("total_revenue"),
                F.round(F.avg("revenue"), 2).alias("avg_order_value"),
            )
        )
        # 2 groups: (North, Widget A) and (South, Widget B)
        assert df_gold.count() == 2

    def test_gold_revenue_sum_correct(self, spark, silver_schema):
        from datetime import datetime
        from decimal import Decimal
        rows = [
            (1001, "C001", "Widget A", 2, Decimal("25.00"), date(2024, 1, 15),
             "North", Decimal("50.00"), datetime(2024, 1, 15), datetime(2024, 1, 15), "r1"),
            (1002, "C002", "Widget A", 3, Decimal("25.00"), date(2024, 1, 15),
             "North", Decimal("75.00"), datetime(2024, 1, 15), datetime(2024, 1, 15), "r1"),
        ]
        df = spark.createDataFrame(rows, schema=silver_schema)
        df_gold = (
            df.groupBy("order_date", "region", "product")
            .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
        )
        assert float(df_gold.first()["total_revenue"]) == 125.00


# ---------------------------------------------------------------------------
# Config loader tests
# ---------------------------------------------------------------------------

class TestConfigLoader:
    def test_missing_required_key_raises(self, spark):
        import sys
        sys.path.insert(0, ".")
        from utils.config_loader import _validate
        with pytest.raises(ValueError, match="Missing required config keys"):
            _validate({"env": "dev"}, "dev")

    def test_valid_config_passes(self, spark):
        from utils.config_loader import _validate
        cfg = {
            "adls_account": "devaccount",
            "catalog_name": "dev_catalog",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
            "quarantine_schema": "quarantine",
            "source_container": "raw-data",
            "source_path": "sales/",
        }
        # Should not raise
        _validate(cfg, "dev")
