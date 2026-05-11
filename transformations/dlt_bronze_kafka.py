# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer – Kafka → DLT
# MAGIC
# MAGIC **Pipeline**: `cryptolake_dlt`
# MAGIC **Table produced**: `bronze_crypto_prices`
# MAGIC
# MAGIC Reads the `crypto-prices` Kafka topic (produced every 30 s by the
# MAGIC CoinLore producer running locally) via Spark Structured Streaming and
# MAGIC lands every raw event, with Kafka metadata preserved, into a managed
# MAGIC Delta Live Table.
# MAGIC
# MAGIC > **Before running**: set `KAFKA_BOOTSTRAP_SERVERS` in the DLT pipeline
# MAGIC > configuration (e.g. `<your-public-ip>:9092`).

# COMMAND ----------

import os
import dlt
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Try to load from .env, fallback to DLT spark configurations
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    spark.conf.get("spark.cryptolake.kafka.bootstrap_servers", "localhost:9092")
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")

# ---------------------------------------------------------------------------
# Schema emitted by the CoinLore producer (producer.py)
# ---------------------------------------------------------------------------
COINLORE_SCHEMA = StructType(
    [
        StructField("id",                 StringType(),  True),
        StructField("symbol",             StringType(),  True),
        StructField("name",               StringType(),  True),
        StructField("rank",               IntegerType(), True),
        StructField("price_usd",          DoubleType(),  True),
        StructField("percent_change_24h", DoubleType(),  True),
        StructField("percent_change_1h",  DoubleType(),  True),
        StructField("market_cap_usd",     DoubleType(),  True),
        StructField("volume24",           DoubleType(),  True),
        StructField("csupply",            DoubleType(),  True),
        StructField("produced_at",        StringType(),  True),   # ISO-8601 UTC string
    ]
)

# ---------------------------------------------------------------------------
# Bronze DLT table
# ---------------------------------------------------------------------------
@dlt.table(
    name="crypto_prices",
    schema="bronze",
    comment=(
        "Raw crypto-price events streamed from the crypto-prices Kafka topic. "
        "One row per coin per poll cycle (~30 s). Kafka metadata is preserved "
        "alongside the parsed JSON payload."
    ),
    table_properties={
        "quality":                         "bronze",
        "pipelines.autoOptimize.managed":  "true",
        "delta.enableChangeDataFeed":      "true",
    },
)
def crypto_prices():
    """
    Structured Streaming source: Kafka → Delta Live Table (append-only).

    Columns produced
    ----------------
    All fields from the CoinLore JSON payload, plus:
      - kafka_offset       : Kafka offset within the partition
      - kafka_partition    : Kafka partition number
      - kafka_timestamp    : broker-side timestamp of the message
      - ingestion_ts       : wall-clock time when Databricks processed the row
    """
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            # Start from the latest offset the first time; DLT checkpoints
            # handle subsequent incremental reads automatically.
            .option("startingOffsets", "latest")
            # Tolerate temporary broker unavailability (3 minutes)
            .option("kafka.request.timeout.ms",        "180000")
            .option("kafka.session.timeout.ms",        "180000")
            # Safety cap: never fetch more than 500 records per micro-batch
            # (increase once you are confident in the pipeline)
            .option("maxOffsetsPerTrigger", "500")
            .load()

            # Decode: Kafka `value` is bytes → cast to string → parse JSON
            .withColumn(
                "payload",
                F.from_json(F.col("value").cast(StringType()), COINLORE_SCHEMA),
            )

            # Flatten the struct into top-level columns
            .select(
                # ── Payload columns ──────────────────────────────────────────
                F.col("payload.id"),
                F.col("payload.symbol"),
                F.col("payload.name"),
                F.col("payload.rank"),
                F.col("payload.price_usd"),
                F.col("payload.percent_change_24h"),
                F.col("payload.percent_change_1h"),
                F.col("payload.market_cap_usd"),
                F.col("payload.volume24"),
                F.col("payload.csupply"),
                F.col("payload.produced_at"),

                # ── Kafka metadata ───────────────────────────────────────────
                F.col("offset").alias("kafka_offset"),
                F.col("partition").alias("kafka_partition"),
                F.col("timestamp").alias("kafka_timestamp"),

                # ── Processing metadata ──────────────────────────────────────
                F.current_timestamp().alias("ingestion_ts"),
            )
    )
