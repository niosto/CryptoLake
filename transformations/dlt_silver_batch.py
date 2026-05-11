# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Batch APIs → DLT
# MAGIC
# MAGIC **Pipeline**: `cryptolake_batch_dlt`
# MAGIC **Upstream tables**: `bronze_coingecko`, `bronze_coincap`
# MAGIC **Table produced**: `silver_crypto_enriched`
# MAGIC
# MAGIC Joins both Bronze snapshots on `symbol`, applies data quality rules,
# MAGIC and computes the `liquidity_ratio` enrichment metric.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Silver – joined and enriched
# ---------------------------------------------------------------------------
@dlt.table(
    name="silver.crypto_enriched",
    comment=(
        "CoinGecko × CoinCap joined snapshot. "
        "Rows with invalid price or symbol are dropped. "
        "liquidity_ratio = volume_24h (CoinCap) / market_cap_usd (CoinCap)."
    ),
    table_properties={
        "quality":                         "silver",
        "pipelines.autoOptimize.managed":  "true",
        "delta.enableChangeDataFeed":      "true",
    },
)
# ── Data quality expectations ─────────────────────────────────────────────────
@dlt.expect_or_drop("valid_price",      "price_usd IS NOT NULL AND price_usd > 0")
@dlt.expect_or_drop("valid_symbol",     "symbol IS NOT NULL AND symbol != ''")
@dlt.expect_or_drop("valid_market_cap", "market_cap IS NOT NULL AND market_cap > 0")
@dlt.expect("non_negative_volume",      "volume_24h IS NULL OR volume_24h >= 0")
def silver_crypto_enriched():
    """
    Batch transformation: bronze_coingecko ⟕ bronze_coincap → silver_crypto_enriched.

    Join strategy
    -------------
    LEFT JOIN on upper(symbol) so that coins only present in CoinGecko are
    preserved. CoinCap data (market_cap_usd, cc_volume_24h) will be NULL
    for unmatched rows.

    New / derived columns
    ---------------------
    - symbol_upper         : symbol normalised to uppercase (join key)
    - liquidity_ratio      : cc_volume_24h / market_cap_usd
    - price_change_category: 'up' | 'down' | 'flat' based on percent_change_24h
    - ingestion_ts         : timestamp of this Silver run
    """
    # ── Read both Bronze tables (batch, not streaming) ────────────────────────
    df_cg = (
        dlt.read("bronze.coingecko")
           .withColumn("symbol_upper", F.upper(F.col("symbol")))
           # Drop rows missing the JOIN key or critical metrics
           .dropna(subset=["symbol", "price_usd", "market_cap", "volume_24h"])
    )

    df_cc = (
        dlt.read("bronze.coincap")
           .withColumn("symbol_upper", F.upper(F.col("symbol")))
           .dropna(subset=["symbol", "price_usd", "market_cap_usd", "volume_24h"])
           # Only bring the columns we need from CoinCap to avoid ambiguity
           .select(
               "symbol_upper",
               F.col("volume_24h").alias("cc_volume_24h"),
               F.col("market_cap_usd").alias("cc_market_cap_usd"),
               F.col("percent_change_24h").alias("cc_percent_change_24h"),
           )
    )

    # ── LEFT JOIN: keep all CoinGecko rows ───────────────────────────────────
    df_joined = df_cg.join(df_cc, on="symbol_upper", how="left")

    # ── Derived metrics ───────────────────────────────────────────────────────
    df_silver = (
        df_joined
        .withColumn(
            "liquidity_ratio",
            F.when(
                F.col("cc_market_cap_usd").isNotNull()
                & (F.col("cc_market_cap_usd") != 0),
                F.round(F.col("cc_volume_24h") / F.col("cc_market_cap_usd"), 6),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "price_change_category",
            F.when(F.col("percent_change_24h") > 2.0,  F.lit("up"))
             .when(F.col("percent_change_24h") < -2.0, F.lit("down"))
             .otherwise(F.lit("flat")),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    # ── Final column selection ────────────────────────────────────────────────
    return df_silver.select(
        # Identity
        F.col("symbol_upper").alias("symbol"),
        "name",

        # CoinGecko primary metrics
        "price_usd",
        "market_cap",
        "volume_24h",
        "percent_change_24h",
        "high_24h",
        "low_24h",
        "last_updated",

        # CoinCap cross-reference
        "cc_volume_24h",
        "cc_market_cap_usd",
        "cc_percent_change_24h",

        # Derived
        "liquidity_ratio",
        "price_change_category",

        # Metadata
        "ingestion_ts",
    )
