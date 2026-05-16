# Databricks notebook source
# Celda 1 – Importaciones y configuración inicial
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, round, lit

spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG cryptolake")
spark.sql("USE bronze")

# Leer tablas Bronze
df_cg = spark.table("cryptolake.bronze.coingecko")
df_cc = spark.table("cryptolake.bronze.coincap")

print(f"Registros CoinGecko: {df_cg.count()}")
print(f"Registros CoinCap: {df_cc.count()}")

# Validación de nulos
def check_nulls(df, name):
    print(f"\n--- Nulos en {name} ---")
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        print(f"{c}: {null_count}")

check_nulls(df_cg, "CoinGecko")
check_nulls(df_cc, "CoinCap")

# Limpiar nulos en columnas críticas
df_cg_clean = df_cg.dropna(subset=["symbol", "price_usd", "market_cap", "volume_24h"])
df_cc_clean = df_cc.dropna(subset=["symbol", "price_usd", "market_cap_usd", "volume_24h"])

# Estandarizar símbolos a mayúsculas
df_cg_clean = df_cg_clean.withColumn("symbol_upper", upper(col("symbol")))
df_cc_clean = df_cc_clean.withColumn("symbol_upper", upper(col("symbol")))

# COMMAND ----------

# Celda 2 – JOIN y cálculo de liquidity_ratio
df_joined = df_cg_clean.join(
    df_cc_clean.select(
        "symbol_upper",
        col("volume_24h").alias("cc_volume"),
        "market_cap_usd"
    ),
    on="symbol_upper",
    how="left"
)

df_silver = df_joined.withColumn(
    "liquidity_ratio",
    when(col("cc_volume").isNotNull(), round(col("cc_volume") / col("market_cap_usd"), 6))
    .otherwise(None)
)

# CORREGIDO: eliminado 'source_file' que ya no existe en tablas Bronze DLT
final_cols = [
    "symbol_upper", "name", "price_usd", "market_cap", "volume_24h",
    "percent_change_24h", "ingestion_ts", "liquidity_ratio"
]
df_silver = df_silver.select(final_cols).withColumnRenamed("symbol_upper", "symbol")
df_silver.show(5, truncate=False)

# COMMAND ----------

# Celda 3 – Escritura de Silver (con mergeSchema habilitado)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("cryptolake.silver.crypto_assets")

print("✅ Tabla cryptolake.silver.crypto_assets creada")

# COMMAND ----------

# Celda 4 – Time Travel (deshabilitado temporalmente)
# bronze_table = "qubika.bronze.coingecko"
# print("Historial de versiones:")
# display(spark.sql(f"DESCRIBE HISTORY {bronze_table}"))

# df_v0 = spark.read.format("delta").option("versionAsOf", 0).table(bronze_table)
# df_v1 = spark.read.format("delta").option("versionAsOf", 1).table(bronze_table)

# print(f"Registros versión 0 (primer lote): {df_v0.count()}")
# print(f"Registros versión 1 (segundo lote): {df_v1.count()}")

# btc_v0 = df_v0.filter(col("symbol") == "btc").select("price_usd", "ingestion_ts").collect()
# btc_v1 = df_v1.filter(col("symbol") == "btc").select("price_usd", "ingestion_ts").collect()
# print("BTC lote 1:", btc_v0)
# print("BTC lote 2:", btc_v1)

# COMMAND ----------

# Celda 5 – Schema Evolution (deshabilitado)
# df_new = df_cg_clean.limit(10).withColumn("ath", lit(69000.0))
# df_new.write.format("delta") \
#     .mode("append") \
#     .option("mergeSchema", "true") \
#     .saveAsTable("qubika.bronze.coingecko")

# df_updated = spark.table("qubika.bronze.coingecko")
# print("Columnas después de schema evolution:", df_updated.columns)