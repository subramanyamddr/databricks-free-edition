# =============================================================================
# tests/test_pipeline.py
# Unit tests for transformations, config loading, and DQX check definitions.
# Run with: pytest tests/ -v
# =============================================================================

import pytest
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DecimalType, DateType
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("retail_pipeline_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Silver transformations
# ---------------------------------------------------------------------------

class TestSilverTransformations:
    def test_order_id_cast_to_long(self, spark):
        df = spark.createDataFrame([("100001",)], ["order_id"])
        result = df.withColumn("order_id", F.col("order_id").cast(LongType()))
        assert result.first()["order_id"] == 100001

    def test_invalid_order_id_casts_to_null(self, spark):
        df = spark.createDataFrame([("NOT_AN_ID",)], ["order_id"])
        result = df.withColumn("order_id", F.col("order_id").cast(LongType()))
        assert result.first()["order_id"] is None

    def test_customer_id_normalised(self, spark):
        df = spark.createDataFrame([("  c001  ",)], ["customer_id"])
        result = df.withColumn("customer_id", F.trim(F.upper(F.col("customer_id"))))
        assert result.first()["customer_id"] == "C001"

    def test_gross_amount_calculation(self, spark):
        df = spark.createDataFrame([(2, Decimal("25.00"))], ["quantity", "unit_price"])
        result = df.withColumn(
            "gross_amount",
            F.round(F.col("quantity").cast(DecimalType(12, 2)) * F.col("unit_price"), 2)
        )
        assert float(result.first()["gross_amount"]) == 50.00

    def test_net_amount_applies_discount(self, spark):
        df = spark.createDataFrame(
            [(2, Decimal("25.00"), Decimal("10.00"))],
            ["quantity", "unit_price", "discount_pct"]
        )
        result = df.withColumn(
            "net_amount",
            F.round(
                F.col("quantity").cast(DecimalType(12, 2)) * F.col("unit_price")
                * (F.lit(1) - F.col("discount_pct") / F.lit(100)),
                2
            )
        )
        # 2 * 25 = 50; 10% off = 45.00
        assert float(result.first()["net_amount"]) == 45.00

    def test_dedup_keeps_one_per_order_id(self, spark):
        df = spark.createDataFrame([(100001,), (100001,), (100002,)], ["order_id"])
        result = df.dropDuplicates(["order_id"])
        assert result.count() == 2

    def test_negative_quantity_preserved_for_dq_check(self, spark):
        # Negative quantity should cast fine but fail the DQX range check downstream
        df = spark.createDataFrame([("-2",)], ["quantity"])
        result = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
        assert result.first()["quantity"] == -2


# ---------------------------------------------------------------------------
# Gold dimension staging
# ---------------------------------------------------------------------------

class TestDimensionStaging:
    def test_dim_customer_dedup(self, spark):
        df = spark.createDataFrame(
            [("C001", "Alice", "Consumer", "NY", "NY"),
             ("C001", "Alice", "Consumer", "NY", "NY"),
             ("C002", "Bob", "Corporate", "LA", "CA")],
            ["customer_id", "customer_name", "customer_segment", "customer_city", "customer_state"]
        )
        result = df.dropDuplicates(["customer_id"])
        assert result.count() == 2

    def test_date_key_format(self, spark):
        df = spark.createDataFrame([(date(2024, 1, 15),)], ["order_date"])
        result = df.withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        assert result.first()["date_key"] == 20240115


# ---------------------------------------------------------------------------
# dim_date generation logic
# ---------------------------------------------------------------------------

class TestDimDateGeneration:
    def test_dim_date_row_count(self, spark):
        df = spark.sql("""
            SELECT explode(sequence(to_date('2024-01-15'), to_date('2024-01-17'), interval 1 day)) AS full_date
        """)
        assert df.count() == 3

    def test_dim_date_columns(self, spark):
        df = (
            spark.sql("SELECT to_date('2024-01-15') AS full_date")
            .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("full_date"))
            .withColumn("is_weekend",
                        (((F.dayofweek("full_date") + 5) % 7) + 1).isin(6, 7))
        )
        row = df.first()
        assert row["date_key"] == 20240115
        assert row["year"] == 2024
        assert row["is_weekend"] is False  # 2024-01-15 is a Monday


# ---------------------------------------------------------------------------
# DQX check definitions
# ---------------------------------------------------------------------------

class TestDQCheckDefinitions:
    def test_silver_checks_loaded(self):
        from utils.dq_checks import SILVER_CHECKS
        assert len(SILVER_CHECKS) > 0

    def test_all_silver_checks_have_required_keys(self):
        from utils.dq_checks import SILVER_CHECKS
        for check in SILVER_CHECKS:
            assert "name" in check
            assert "criticality" in check
            assert check["criticality"] in {"error", "warn"}
            assert "function" in check["check"]
            assert "arguments" in check["check"]

    def test_gold_fact_checks_loaded(self):
        from utils.dq_checks import GOLD_FACT_CHECKS
        names = [c["name"] for c in GOLD_FACT_CHECKS]
        for fk in ["date_key_is_not_null", "customer_key_is_not_null",
                   "product_key_is_not_null", "store_key_is_not_null"]:
            assert fk in names

    def test_gold_dim_checks_have_uniqueness(self):
        from utils.dq_checks import (
            GOLD_DIM_CUSTOMER_CHECKS, GOLD_DIM_PRODUCT_CHECKS, GOLD_DIM_STORE_CHECKS
        )
        for checks in (GOLD_DIM_CUSTOMER_CHECKS, GOLD_DIM_PRODUCT_CHECKS, GOLD_DIM_STORE_CHECKS):
            functions = [c["check"]["function"] for c in checks]
            assert "is_unique" in functions

    def test_quantity_range_check_bounds(self):
        from utils.dq_checks import SILVER_CHECKS
        qty_check = next(c for c in SILVER_CHECKS if c["name"] == "quantity_in_valid_range")
        args = qty_check["check"]["arguments"]
        assert args["min_limit"] == 1
        assert args["max_limit"] == 10000

    def test_discount_pct_range_check(self):
        from utils.dq_checks import SILVER_CHECKS
        check = next(c for c in SILVER_CHECKS if c["name"] == "discount_pct_in_valid_range")
        assert check["check"]["arguments"]["min_limit"] == 0
        assert check["check"]["arguments"]["max_limit"] == 100


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

class TestConfigLoader:
    def test_missing_required_key_raises(self):
        from utils.config_loader import _validate
        with pytest.raises(ValueError, match="Missing required config keys"):
            _validate({"env": "dev"}, "dev")

    def test_valid_config_passes(self):
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
        _validate(cfg, "dev")  # should not raise


# ---------------------------------------------------------------------------
# Sample data sanity checks (validates the bundled test fixtures)
# ---------------------------------------------------------------------------

class TestSampleData:
    @pytest.mark.parametrize("filename,expected_min_rows", [
        ("data/2024-01-15/sales_20240115.csv", 10),
        ("data/2024-01-16/sales_20240116.csv", 10),
        ("data/2024-01-17/sales_20240117.csv", 10),
    ])
    def test_sample_file_row_counts(self, spark, filename, expected_min_rows):
        df = spark.read.option("header", "true").csv(filename)
        assert df.count() >= expected_min_rows

    def test_sample_data_has_known_bad_rows(self, spark):
        # Day 1 contains one null customer_id and one negative quantity
        df = spark.read.option("header", "true").csv("data/2024-01-15/sales_20240115.csv")
        null_customer = df.filter(F.col("customer_id").isNull()).count()
        negative_qty = df.filter(F.col("quantity") == "-2").count()
        assert null_customer == 1
        assert negative_qty == 1


# ---------------------------------------------------------------------------
# DQX engine integration — runs the REAL DQEngine (WorkspaceClient mocked)
# against SILVER_CHECKS, verifying the error/warn split semantics that
# run_dq_checks() in utils/dq_checks.py relies on.
# ---------------------------------------------------------------------------

class TestDQXEngineIntegration:
    SILVER_SCHEMA = StructType([
        StructField("order_id", LongType(), True),
        StructField("order_date", DateType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("discount_pct", DecimalType(5, 2), True),
    ])

    @pytest.fixture
    def mixed_df(self, spark):
        rows = [
            # good row
            (100001, date(2024, 1, 15), "C001", "Consumer", "P001", "S01", 2, Decimal("1200.00"), Decimal("5.00")),
            # null customer_id -> error
            (100011, date(2024, 1, 15), None, "Consumer", "P003", "S03", 1, Decimal("150.00"), Decimal("10.00")),
            # negative quantity -> error
            (100012, date(2024, 1, 15), "C004", "Home Office", "P005", "S04", -2, Decimal("4.50"), Decimal("0.00")),
            # discount_pct > 100 -> error
            (100023, date(2024, 1, 16), "C001", "Consumer", "P005", "S01", 5, Decimal("4.50"), Decimal("150.00")),
            # unit_price below minimum (0.00 < 0.01) -> error
            (100024, date(2024, 1, 16), "C003", "Consumer", "P001", "S03", 1, Decimal("0.00"), Decimal("0.00")),
            # invalid customer_segment -> warn only (passes through)
            (100099, date(2024, 1, 16), "C009", "Freelancer", "P002", "S02", 1, Decimal("25.00"), Decimal("0.00")),
            # future order_date -> warn only (passes through)
            (100100, date(2099, 12, 31), "C010", "Consumer", "P002", "S02", 1, Decimal("25.00"), Decimal("0.00")),
        ]
        return spark.createDataFrame(rows, schema=self.SILVER_SCHEMA)

    def test_split_separates_errors_from_warn_only_rows(self, spark, mixed_df, monkeypatch):
        from utils import dq_checks
        from utils.dq_checks import SILVER_CHECKS, run_dq_checks

        # Avoid writing to a real Delta table for quarantined rows
        monkeypatch.setattr(dq_checks, "_write_quarantine", lambda *a, **k: None)

        class FakeLogger:
            def log_dq_result(self, r):
                self.summary = r

        logger = FakeLogger()
        config = {"dq_min_pass_rate_pct": 0.0}  # disable threshold for this test

        valid_df, quarantine_df, summary = run_dq_checks(
            spark=spark, df=mixed_df, checks=SILVER_CHECKS, layer="silver_test",
            quarantine_table="dummy.dummy.quarantine_test", config=config,
            logger=logger, pipeline_run="test_run",
        )

        valid_ids = sorted(r.order_id for r in valid_df.select("order_id").collect())
        quarantine_ids = sorted(r.order_id for r in quarantine_df.select("order_id").collect())

        # The 4 rows with error-criticality failures are quarantined ...
        assert quarantine_ids == [100011, 100012, 100023, 100024]
        # ... while the good row AND the 2 warn-only rows pass through to Silver
        assert valid_ids == [100001, 100099, 100100]

        # valid_df must not carry DQX bookkeeping columns
        assert "_errors" not in valid_df.columns
        assert "_warnings" not in valid_df.columns

        # Summary stats reflect the corrected (non-overlapping) split
        assert summary["total_rows"] == 7
        assert summary["passed_rows"] == 3
        assert summary["quarantined_rows"] == 4
        assert summary["pass_rate_pct"] == round(3 / 7 * 100, 2)

    def test_pass_rate_threshold_halts_pipeline(self, spark, mixed_df, monkeypatch):
        from utils import dq_checks
        from utils.dq_checks import SILVER_CHECKS, run_dq_checks

        monkeypatch.setattr(dq_checks, "_write_quarantine", lambda *a, **k: None)

        class FakeLogger:
            def log_dq_result(self, r):
                pass

        # pass rate is 3/7 = 42.86% -> below a 90% threshold should raise
        config = {"dq_min_pass_rate_pct": 90.0}

        with pytest.raises(ValueError, match="pass rate"):
            run_dq_checks(
                spark=spark, df=mixed_df, checks=SILVER_CHECKS, layer="silver_test",
                quarantine_table="dummy.dummy.quarantine_test", config=config,
                logger=FakeLogger(), pipeline_run="test_run",
            )
