# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — CryptoLake
# MAGIC **Responsable**: Maria Alejandra Ocampo — Módulo 4
# MAGIC
# MAGIC Genera 3 tablas Gold a partir de `silver.crypto_enriched`:
# MAGIC - `gold.volatility_monthly` — volatilidad mensual por activo + picos
# MAGIC - `gold.price_daily`        — evolución del precio promedio diario
# MAGIC - `gold.market_summary`     — resumen actual por activo para dashboards

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Verificar que Silver existe

# COMMAND ----------

spark.sql("USE CATALOG cryptolake")
df_silver = spark.table("silver.crypto_enriched")
print(f"Registros en silver.crypto_enriched: {df_silver.count()}")
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear schema Gold (si no existe)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
print("Schema 'gold' listo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. gold.volatility_monthly
# MAGIC Volatilidad promedio mensual por activo + detección de picos

# COMMAND ----------

df_volatility = (
    df_silver
    # Parsear last_updated a timestamp
    .withColumn("updated_ts", F.to_timestamp(F.col("last_updated")))
    .withColumn("year",  F.year(F.col("updated_ts")))
    .withColumn("month", F.month(F.col("updated_ts")))

    # Filtrar filas válidas
    .filter(
        F.col("year").isNotNull() &
        F.col("month").isNotNull() &
        F.col("percent_change_24h").isNotNull()
    )

    # Agregar por símbolo + año + mes
    .groupBy("symbol", "name", "year", "month")
    .agg(
        F.round(F.avg("percent_change_24h"),    4).alias("avg_change_24h"),
        F.round(F.stddev("percent_change_24h"), 4).alias("stddev_change_24h"),
        F.round(F.max("percent_change_24h"),    4).alias("max_change_24h"),
        F.round(F.min("percent_change_24h"),    4).alias("min_change_24h"),
        F.count("*").alias("sample_count"),
    )

    # Marcar alta volatilidad si stddev > 2%
    .withColumn(
        "is_high_volatility",
        F.when(F.col("stddev_change_24h") > 2.0, F.lit(True))
         .otherwise(F.lit(False))
    )
    .withColumn("ingestion_ts", F.current_timestamp())
)

# Guardar como tabla Delta
df_volatility.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold.volatility_monthly")

print(" gold.volatility_monthly creada")
df_volatility.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. gold.price_daily
# MAGIC Evolución del precio promedio diario por activo

# COMMAND ----------

df_price_daily = (
    df_silver
    .withColumn("updated_ts", F.to_timestamp(F.col("last_updated")))
    .withColumn("date", F.to_date(F.col("updated_ts")))

    .filter(
        F.col("date").isNotNull() &
        F.col("price_usd").isNotNull() &
        (F.col("price_usd") > 0)
    )

    .groupBy("symbol", "name", "date")
    .agg(
        F.round(F.avg("price_usd"),   6).alias("avg_price_usd"),
        F.round(F.min("price_usd"),   6).alias("low_price_usd"),
        F.round(F.max("price_usd"),   6).alias("high_price_usd"),
        F.round(F.first("price_usd"), 6).alias("open_price_usd"),
        F.round(F.last("price_usd"),  6).alias("close_price_usd"),
        F.round(F.avg("volume_24h"),  2).alias("avg_volume_24h"),
        F.round(F.avg("market_cap"),  2).alias("avg_market_cap"),
    )
    .withColumn("ingestion_ts", F.current_timestamp())
    .orderBy("symbol", "date")
)

df_price_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold.price_daily")

print(" gold.price_daily creada")
df_price_daily.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. gold.market_summary
# MAGIC Snapshot más reciente por activo para dashboards

# COMMAND ----------

window_latest = Window.partitionBy("symbol").orderBy(F.col("last_updated").desc())

df_market_summary = (
    df_silver
    .withColumn("row_num", F.row_number().over(window_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select(
        "symbol", "name",
        "price_usd",
        "percent_change_24h",
        "high_24h", "low_24h",
        "market_cap",
        "volume_24h",
        "liquidity_ratio",
        "price_change_category",
        "last_updated",
    )
    .withColumn("ingestion_ts", F.current_timestamp())
)

df_market_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold.market_summary")

print(" gold.market_summary creada")
df_market_summary.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar las 3 tablas Gold

# COMMAND ----------

spark.sql("SHOW TABLES IN gold").show()