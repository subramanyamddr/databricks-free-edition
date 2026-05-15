# Databricks notebook source
# FILE: notebooks/transformation/gold_aggregation_engine.py
# PURPOSE: Gold layer — consumption-ready aggregated tables for BI, DS, Finance
#          Star schema, wide flat tables, domain-specific aggregations
# VERSION: 1.0 — Production Grade

# COMMAND ----------
# %run ../utils/framework_utils

# COMMAND ----------

import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("gold_aggregation")

# COMMAND ----------
dbutils.widgets.text("gold_table_name", "", "Gold Table Name")
GOLD_TABLE_NAME = dbutils.widgets.get("gold_table_name")


# COMMAND ----------
# ================================================================
# GOLD BUILDERS — one class per gold table / domain
# ================================================================

class GoldTableBuilder:
    """Base class for Gold table builders."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build(self, run_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _overwrite_table(self, df: DataFrame, target_table: str) -> int:
        """Full overwrite — used for aggregated gold tables (always recomputed)."""
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        count = df.count()
        logger.info(f"Gold table written: {target_table} | rows: {count}")
        return count

    def _merge_table(self, df: DataFrame, target_table: str, pk_cols: List[str]) -> Dict[str, int]:
        """MERGE into gold table — used for slowly changing gold tables."""
        if not safe_table_exists(self.spark, target_table):
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
            cnt = df.count()
            return {"rows_inserted": cnt, "rows_updated": 0}

        merge_cond = " AND ".join([f"t.{pk} = s.{pk}" for pk in pk_cols])
        update_set = {c: f"s.{c}" for c in df.columns if c not in pk_cols}

        DeltaTable.forName(self.spark, target_table).alias("t") \
            .merge(df.alias("s"), merge_cond) \
            .whenMatchedUpdate(set=update_set) \
            .whenNotMatchedInsert(values={c: f"s.{c}" for c in df.columns}) \
            .execute()

        metrics = self.spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()[0]["operationMetrics"]
        return {
            "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
            "rows_updated":  int(metrics.get("numTargetRowsUpdated", 0))
        }


# COMMAND ----------

class CustomerSalesFact(GoldTableBuilder):
    """
    Gold: Daily customer sales fact table for BI/Power BI.
    Star schema: fact_customer_sales with dim references.
    """
    TARGET = f"{GOLD_CATALOG}.reporting.fact_customer_sales"

    def build(self, run_id: str) -> Dict[str, Any]:
        df = self.spark.sql(f"""
            SELECT
                c.customer_id,
                c.country_code,
                c.status                            AS customer_status,
                t.transaction_date,
                t.fiscal_year,
                t.fiscal_quarter,
                t.currency_code,
                COUNT(t.transaction_id)             AS transaction_count,
                SUM(t.amount)                       AS total_revenue,
                AVG(t.amount)                       AS avg_transaction_value,
                MIN(t.amount)                       AS min_transaction_value,
                MAX(t.amount)                       AS max_transaction_value,
                CURRENT_TIMESTAMP()                 AS _gold_updated_ts,
                '{run_id}'                          AS _run_id
            FROM silver_catalog.conformed.crm_customers c
            INNER JOIN silver_catalog.conformed.finance_gl_transactions t
                ON c.customer_id = t.customer_id
            WHERE c._is_current = TRUE
              AND t._is_current = TRUE
            GROUP BY
                c.customer_id, c.country_code, c.status,
                t.transaction_date, t.fiscal_year, t.fiscal_quarter,
                t.currency_code
        """)
        count = self._overwrite_table(df, self.TARGET)
        return {"rows_written": count}


class CustomerLifetimeValue(GoldTableBuilder):
    """
    Gold: Customer Lifetime Value — for data science / marketing.
    Wide flat table optimised for ML feature extraction.
    """
    TARGET = f"{GOLD_CATALOG}.data_science.customer_ltv"

    def build(self, run_id: str) -> Dict[str, Any]:
        df = self.spark.sql(f"""
            WITH customer_metrics AS (
                SELECT
                    c.customer_id,
                    c.full_name,
                    c.email,
                    c.country_code,
                    c.status,
                    c.hire_date                                         AS customer_since,
                    DATEDIFF(CURRENT_DATE(), c.hire_date)               AS days_as_customer,
                    COUNT(DISTINCT t.transaction_id)                    AS total_transactions,
                    SUM(t.amount)                                       AS lifetime_revenue,
                    AVG(t.amount)                                       AS avg_order_value,
                    MAX(t.transaction_date)                             AS last_transaction_date,
                    DATEDIFF(CURRENT_DATE(), MAX(t.transaction_date))   AS days_since_last_purchase,
                    COUNT(DISTINCT t.fiscal_year)                       AS active_years,
                    SUM(t.amount) / NULLIF(DATEDIFF(CURRENT_DATE(), c.hire_date), 0) * 365
                                                                        AS annual_revenue_rate
                FROM silver_catalog.conformed.crm_customers c
                LEFT JOIN silver_catalog.conformed.finance_gl_transactions t
                    ON c.customer_id = t.customer_id
                    AND t._is_current = TRUE
                WHERE c._is_current = TRUE
                GROUP BY c.customer_id, c.full_name, c.email, c.country_code, c.status, c.hire_date
            ),
            rfm_scores AS (
                SELECT
                    customer_id,
                    NTILE(5) OVER (ORDER BY days_since_last_purchase ASC)  AS recency_score,
                    NTILE(5) OVER (ORDER BY total_transactions DESC)        AS frequency_score,
                    NTILE(5) OVER (ORDER BY lifetime_revenue DESC)          AS monetary_score
                FROM customer_metrics
            )
            SELECT
                m.*,
                r.recency_score,
                r.frequency_score,
                r.monetary_score,
                r.recency_score + r.frequency_score + r.monetary_score  AS rfm_total_score,
                CASE
                    WHEN r.recency_score + r.frequency_score + r.monetary_score >= 13 THEN 'Champions'
                    WHEN r.recency_score + r.frequency_score + r.monetary_score >= 10 THEN 'Loyal Customers'
                    WHEN r.recency_score >= 4 AND r.frequency_score <= 2               THEN 'New Customers'
                    WHEN r.recency_score <= 2 AND r.monetary_score >= 4                THEN 'At Risk'
                    WHEN r.recency_score <= 2                                          THEN 'Lost'
                    ELSE 'Potential Loyalists'
                END                                                     AS customer_segment,
                CURRENT_TIMESTAMP()                                     AS _gold_updated_ts,
                '{run_id}'                                              AS _run_id
            FROM customer_metrics m
            JOIN rfm_scores r ON m.customer_id = r.customer_id
        """)
        count = self._overwrite_table(df, self.TARGET)
        return {"rows_written": count}


class FinanceRevenueRollup(GoldTableBuilder):
    """
    Gold: Monthly revenue rollup for Finance team.
    Pre-aggregated for fast reporting in Power BI / Synapse.
    """
    TARGET = f"{GOLD_CATALOG}.finance.monthly_revenue_summary"

    def build(self, run_id: str) -> Dict[str, Any]:
        df = self.spark.sql(f"""
            SELECT
                t.fiscal_year,
                t.fiscal_quarter,
                DATE_TRUNC('month', t.transaction_date)     AS report_month,
                t.currency_code,
                c.country_code,
                COUNT(DISTINCT c.customer_id)               AS unique_customers,
                COUNT(t.transaction_id)                     AS transaction_count,
                SUM(t.amount)                               AS gross_revenue,
                AVG(t.amount)                               AS avg_transaction_value,
                -- Month-over-month growth
                SUM(t.amount) - LAG(SUM(t.amount)) OVER (
                    PARTITION BY t.currency_code, c.country_code
                    ORDER BY DATE_TRUNC('month', t.transaction_date)
                )                                           AS mom_revenue_delta,
                -- Running total
                SUM(SUM(t.amount)) OVER (
                    PARTITION BY t.fiscal_year, t.currency_code
                    ORDER BY DATE_TRUNC('month', t.transaction_date)
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )                                           AS ytd_revenue,
                CURRENT_TIMESTAMP()                         AS _gold_updated_ts,
                '{run_id}'                                  AS _run_id
            FROM silver_catalog.conformed.finance_gl_transactions t
            JOIN silver_catalog.conformed.crm_customers c
                ON t.customer_id = c.customer_id
                AND c._is_current = TRUE
            WHERE t._is_current = TRUE
            GROUP BY
                t.fiscal_year, t.fiscal_quarter,
                DATE_TRUNC('month', t.transaction_date),
                t.currency_code, c.country_code
        """)
        count = self._overwrite_table(df, self.TARGET)
        return {"rows_written": count}


class DigitalClickstreamSummary(GoldTableBuilder):
    """
    Gold: Hourly clickstream aggregation for product analytics.
    Feeds real-time dashboards.
    """
    TARGET = f"{GOLD_CATALOG}.reporting.hourly_clickstream_summary"

    def build(self, run_id: str) -> Dict[str, Any]:
        df = self.spark.sql(f"""
            SELECT
                event_date,
                event_hour,
                event_type,
                device_type,
                country_code,
                COUNT(*)                        AS event_count,
                COUNT(DISTINCT session_id)      AS unique_sessions,
                COUNT(DISTINCT user_id)         AS unique_users,
                SUM(revenue)                    AS revenue,
                AVG(revenue)                    AS avg_revenue_per_event,
                CURRENT_TIMESTAMP()             AS _gold_updated_ts,
                '{run_id}'                      AS _run_id
            FROM silver_catalog.conformed.digital_clickstream
            WHERE _is_current = TRUE
              AND event_date >= CURRENT_DATE() - INTERVAL 90 DAYS
            GROUP BY event_date, event_hour, event_type, device_type, country_code
        """)
        metrics = self._merge_table(
            df, self.TARGET,
            pk_cols=["event_date", "event_hour", "event_type", "device_type", "country_code"]
        )
        return {"rows_written": metrics["rows_inserted"] + metrics["rows_updated"], **metrics}


# COMMAND ----------
# ================================================================
# GOLD TABLE REGISTRY — add new gold tables here
# ================================================================

GOLD_TABLE_REGISTRY: Dict[str, type] = {
    "fact_customer_sales":          CustomerSalesFact,
    "customer_ltv":                 CustomerLifetimeValue,
    "monthly_revenue_summary":      FinanceRevenueRollup,
    "hourly_clickstream_summary":   DigitalClickstreamSummary
}


# COMMAND ----------
# ================================================================
# MAIN
# ================================================================

def run_gold_pipeline(gold_table_name: str) -> None:
    spark       = SparkSession.getActiveSession()
    sm          = SecretManager()
    notif_mgr   = NotificationManager(sm)

    run_id = str(uuid.uuid4())
    logger.info(f"Gold pipeline starting: {gold_table_name} | run_id: {run_id}")

    builder_class = GOLD_TABLE_REGISTRY.get(gold_table_name)
    if not builder_class:
        raise ValueError(
            f"Unknown gold table: '{gold_table_name}'. "
            f"Available: {list(GOLD_TABLE_REGISTRY.keys())}"
        )

    try:
        builder = builder_class(spark)
        metrics = builder.build(run_id)
        logger.info(f"Gold pipeline SUCCESS: {gold_table_name} | {metrics}")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Gold pipeline FAILED: {gold_table_name} | {error_msg}")
        raise


# COMMAND ----------
if GOLD_TABLE_NAME:
    run_gold_pipeline(GOLD_TABLE_NAME)

