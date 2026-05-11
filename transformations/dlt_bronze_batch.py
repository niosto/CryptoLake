# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer – Batch APIs → DLT
# MAGIC
# MAGIC **Pipeline**: `cryptolake_batch_dlt`
# MAGIC **Tables produced**: `bronze_coingecko`, `bronze_coincap`
# MAGIC
# MAGIC Fetches the top-50 coins from CoinGecko and CoinCap on every scheduled
# MAGIC pipeline run and lands each snapshot as a managed Delta Live Table.
# MAGIC Each run appends a new version — Delta handles time travel automatically.
# MAGIC
# MAGIC > **Before running**: set the following keys in the DLT pipeline configuration:
# MAGIC > - `spark.cryptolake.coingecko.api_key`
# MAGIC > - `spark.cryptolake.coincap.api_key`

# COMMAND ----------

import dlt
import config
import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration  (inject via DLT pipeline → Configuration tab or config.py)
# ---------------------------------------------------------------------------
COINGECKO_API_KEY = spark.conf.get("spark.cryptolake.coingecko.api_key", config.COINGECKO_API_KEY)
COINCAP_API_KEY   = spark.conf.get("spark.cryptolake.coincap.api_key", config.COINCAP_API_KEY)

COINGECKO_LIMIT = 50
COINCAP_LIMIT   = 50

# ---------------------------------------------------------------------------
# Fetch helpers  (plain Python — called inside each @dlt.table function)
# ---------------------------------------------------------------------------

def _fetch_coingecko(api_key: str, limit: int) -> pd.DataFrame:
    """GET /coins/markets from CoinGecko and return a normalised DataFrame."""
    url     = "https://api.coingecko.com/api/v3/coins/markets"
    headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}
    params  = {
        "vs_currency": "usd",
        "order":       "market_cap_desc",
        "per_page":    limit,
        "page":        1,
        "sparkline":   "false",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    cols = [
        "id", "symbol", "name",
        "current_price", "market_cap", "total_volume",
        "price_change_percentage_24h", "high_24h", "low_24h",
        "last_updated",
    ]
    pdf = pd.DataFrame(resp.json())[cols]
    return pdf.rename(columns={
        "current_price":               "price_usd",
        "total_volume":                "volume_24h",
        "price_change_percentage_24h": "percent_change_24h",
    })

def _fetch_coincap(api_key: str, limit: int) -> pd.DataFrame:
    """GET /assets from CoinCap v3 and return a normalised DataFrame."""
    url     = "https://rest.coincap.io/v3/assets"
    headers = {"Authorization": f"Bearer {api_key}"}
    resp = requests.get(url, headers=headers, params={"limit": limit}, timeout=15)
    resp.raise_for_status()

    cols = [
        "id", "symbol", "name", "rank",
        "priceUsd", "marketCapUsd", "changePercent24Hr",
        "volumeUsd24Hr", "supply",
    ]
    pdf = pd.DataFrame(resp.json()["data"])[cols]
    pdf = pdf.rename(columns={
        "priceUsd":         "price_usd",
        "marketCapUsd":     "market_cap_usd",
        "changePercent24Hr":"percent_change_24h",
        "volumeUsd24Hr":    "volume_24h",
    })
    # CoinCap returns all numerics as strings
    numeric_cols = ["price_usd", "market_cap_usd", "percent_change_24h",
                    "volume_24h", "supply", "rank"]
    for col in numeric_cols:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce")
    return pdf


# ---------------------------------------------------------------------------
# Bronze – CoinGecko
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze.coingecko",
    comment=(
        "Top-50 coins snapshot from CoinGecko /coins/markets. "
        "One full snapshot per pipeline run; Delta time travel provides history."
    ),
    table_properties={
        "quality":                         "bronze",
        "pipelines.autoOptimize.managed":  "true",
        "delta.enableChangeDataFeed":      "true",
    },
)
@dlt.expect("non_null_price",  "price_usd IS NOT NULL")
@dlt.expect("non_null_symbol", "symbol IS NOT NULL")
def coingecko():
    """
    Fetch CoinGecko /coins/markets and return a Spark DataFrame.
    DLT manages the Delta table; each run adds a new Delta version.

    Columns
    -------
    id, symbol, name, price_usd, market_cap, volume_24h,
    percent_change_24h, high_24h, low_24h, last_updated,
    ingestion_ts, source
    """
    pdf = _fetch_coingecko(COINGECKO_API_KEY, COINGECKO_LIMIT)
    return (
        spark.createDataFrame(pdf)
             .withColumn("ingestion_ts", F.current_timestamp())
             .withColumn("source",       F.lit("coingecko"))
    )


# ---------------------------------------------------------------------------
# Bronze – CoinCap
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze.coincap",
    comment=(
        "Top-50 coins snapshot from CoinCap /assets. "
        "One full snapshot per pipeline run; Delta time travel provides history."
    ),
    table_properties={
        "quality":                         "bronze",
        "pipelines.autoOptimize.managed":  "true",
        "delta.enableChangeDataFeed":      "true",
    },
)
@dlt.expect("non_null_price",  "price_usd IS NOT NULL")
@dlt.expect("non_null_symbol", "symbol IS NOT NULL")
def coincap():
    """
    Fetch CoinCap /assets and return a Spark DataFrame.
    DLT manages the Delta table; each run adds a new Delta version.

    Columns
    -------
    id, symbol, name, rank, price_usd, market_cap_usd, volume_24h,
    percent_change_24h, supply, ingestion_ts, source
    """
    pdf = _fetch_coincap(COINCAP_API_KEY, COINCAP_LIMIT)
    return (
        spark.createDataFrame(pdf)
             .withColumn("ingestion_ts", F.current_timestamp())
             .withColumn("source",       F.lit("coincap"))
    )
