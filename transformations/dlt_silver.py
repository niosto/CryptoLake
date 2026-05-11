# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Bronze → DLT
# MAGIC
# MAGIC **Pipeline**: `cryptolake_dlt`
# MAGIC **Upstream table**: `bronze_crypto_prices`
# MAGIC **Table produced**: `silver_crypto_prices`
# MAGIC
# MAGIC Reads the Bronze streaming table and applies:
# MAGIC - Type casting (`produced_at` string → timestamp)
# MAGIC - Data quality expectations (drop rows that fail)
# MAGIC - Deduplication by `(symbol, produced_at)`
# MAGIC - Derived metric: `liquidity_ratio = volume24 / market_cap_usd`

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Silver DLT table
# ---------------------------------------------------------------------------
@dlt.table(
    name="crypto_prices_clean",
    schema="silver",
    comment=(
        "Cleaned and enriched crypto-price stream. "
        "Invalid rows are dropped; duplicates are eliminated; "
        "produced_at is cast to a proper timestamp; "
        "liquidity_ratio is calculated."
    ),
    table_properties={
        "quality":                         "silver",
        "pipelines.autoOptimize.managed":  "true",
        "delta.enableChangeDataFeed":      "true",
    },
)
# ── Data quality rules ────────────────────────────────────────────────────────
# Rows that violate ANY expect_or_drop constraint are silently discarded.
# Rows that violate an expect constraint are kept but flagged in the pipeline UI.
@dlt.expect_or_drop("valid_price",      "price_usd IS NOT NULL AND price_usd > 0")
@dlt.expect_or_drop("valid_symbol",     "symbol_upper IS NOT NULL AND symbol_upper != ''")
@dlt.expect_or_drop("valid_market_cap", "market_cap_usd IS NOT NULL AND market_cap_usd > 0")
@dlt.expect("non_negative_volume",      "volume24 IS NULL OR volume24 >= 0")
def crypto_prices_clean():
    """
    Streaming transformation: bronze_crypto_prices → silver_crypto_prices.

    New / derived columns
    ---------------------
    - produced_at_ts  : produced_at parsed as TIMESTAMP (UTC)
    - symbol_upper    : symbol normalised to uppercase
    - liquidity_ratio : volume24 / market_cap_usd  (null when denominator is 0)
    - price_change_category : 'up' | 'down' | 'flat' based on percent_change_1h
    """
    return (
        # dlt.read_stream keeps the Silver table continuous / streaming
        dlt.read_stream("crypto_prices")

            # ── Deduplication ────────────────────────────────────────────────
            # DLT does not natively deduplicate streams; use dropDuplicates
            # within the micro-batch on the natural key.
            .dropDuplicates(["symbol", "produced_at"])

            # ── Type casting ─────────────────────────────────────────────────
            .withColumn(
                "produced_at_ts",
                F.to_timestamp(F.col("produced_at")),   # ISO-8601 → TimestampType
            )

            # ── Normalisation ────────────────────────────────────────────────
            .withColumn("symbol_upper", F.upper(F.col("symbol")))
            .withColumn("rank",         F.col("rank").cast("integer"))

            # ── Derived metrics ──────────────────────────────────────────────
            .withColumn(
                "liquidity_ratio",
                F.when(
                    F.col("market_cap_usd").isNotNull() & (F.col("market_cap_usd") != 0),
                    F.round(F.col("volume24") / F.col("market_cap_usd"), 6),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "price_change_category",
                F.when(F.col("percent_change_1h") > 0.5,  F.lit("up"))
                 .when(F.col("percent_change_1h") < -0.5, F.lit("down"))
                 .otherwise(F.lit("flat")),
            )

            # ── Final column selection (explicit ordering) ───────────────────
            .select(
                # Identity
                "symbol_upper",
                "name",
                "rank",

                # Prices & changes
                "price_usd",
                "percent_change_1h",
                "percent_change_24h",
                "price_change_category",

                # Market data
                "market_cap_usd",
                "volume24",
                "csupply",
                "liquidity_ratio",

                # Timestamps
                "produced_at_ts",
                "kafka_timestamp",
                "ingestion_ts",

                # Lineage
                "kafka_partition",
                "kafka_offset",
            )
    )
