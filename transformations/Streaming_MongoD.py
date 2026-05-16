# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming & MongoDB 
# MAGIC #  ---
# MAGIC # 
# MAGIC # 
# MAGIC ##  1. Lee eventos de precios de criptomonedas del tópico Kafka `crypto-prices`
# MAGIC ##  2. Aplica una **ventana tumbling de 5 minutos** por símbolo y calcula: open, close, min, max y pct_change
# MAGIC ##  3. Detecta activos con **variación de precio > 3%** en la ventana
# MAGIC ##  4. Escribe los resultados en la colección `price_windows` de **MongoDB**
# MAGIC ##  5. Configura **checkpointing en DBFS** para garantizar semántica at-least-once
# MAGIC # 
# MAGIC #  ```
# MAGIC ###  Kafka (crypto-prices) → Spark Structured Streaming → MongoDB (price_windows)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 — Instalar dependencias
# MAGIC # 
# MAGIC ##  Necesitamos el conector de MongoDB para Spark.

# COMMAND ----------

# MAGIC %pip install "pymongo[srv]==4.7.2"

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 — Configuración
# MAGIC

# COMMAND ----------

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "98.91.118.194:9092"
KAFKA_TOPIC             = "crypto-prices"

# ── MongoDB Atlas ─────────────────────────────────────────────────────────────
MONGO_URI        = "mongodb+srv://dzapataa3_db_user:6Jpfu3sgJ1Lbet9A@cluster0.zxty80c.mongodb.net/?appName=Cluster0"
MONGO_DATABASE   = "cryptolake"
MONGO_COLLECTION = "price_windows"

# ── Checkpointing ─────────────────────────────────────────────────────────────
CHECKPOINT_PATH = "dbfs:/cryptolake/checkpoints/streaming_modulo2"
WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "10 minutes"

print("✅ Configuración cargada correctamente")
print(f"   Kafka  : {KAFKA_BOOTSTRAP_SERVERS} → tópico '{KAFKA_TOPIC}'")
print(f"   MongoDB: Atlas → {MONGO_DATABASE}.{MONGO_COLLECTION}")
print(f"   Checkpoint: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 — Definir el esquema de los eventos
# MAGIC # 
# MAGIC ##  Kafka recibe mensajes en formato JSON. Le decimos a Spark cómo leerlos
# MAGIC ##  (qué campos tiene cada mensaje y de qué tipo son).
# MAGIC

# COMMAND ----------

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Esquema exacto que publica el productor de Nicolás (producer.py)
COINLORE_SCHEMA = StructType([
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
    StructField("produced_at",        StringType(),  True),  # timestamp ISO-8601
])

print("✅ Esquema definido — coincide con el productor Kafka del Módulo 1")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 — Leer el stream de Kafka
# MAGIC # 
# MAGIC ##  Conectamos Spark a Kafka y empezamos a leer los mensajes del tópico `crypto-prices`.
# MAGIC ##  Cada mensaje es un precio de una criptomoneda publicado cada 30 segundos.

# COMMAND ----------

from pyspark.sql import functions as F

# Leer el stream desde Kafka
raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")           # empieza desde ahora
        .option("kafka.request.timeout.ms", "180000")
        .option("kafka.session.timeout.ms", "180000")
        .option("failOnDataLoss", "false")             # tolerante a offsets perdidos
        .load()
)

# Decodificar el JSON y aplanar en columnas
parsed_stream = (
    raw_stream
        .withColumn(
            "payload",
            F.from_json(F.col("value").cast(StringType()), COINLORE_SCHEMA)
        )
        .select(
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
            # Convertir produced_at de string a timestamp real
            F.to_timestamp(F.col("payload.produced_at")).alias("produced_at"),
        )
        # Filtrar filas sin precio o símbolo (eventos corruptos)
        .filter(
            F.col("price_usd").isNotNull() &
            F.col("symbol").isNotNull() &
            (F.col("price_usd") > 0)
        )
)

print("✅ Stream de Kafka conectado y parseado correctamente")


# COMMAND ----------

# MAGIC %md
# MAGIC # 5 — Aplicar ventana tumbling de 5 minutos
# MAGIC # 
# MAGIC ##  Agrupamos los eventos por **símbolo** y por **ventanas de 5 minutos**.
# MAGIC ##  Dentro de cada ventana calculamos:
# MAGIC ##  - **open**: primer precio de la ventana
# MAGIC ##  - **close**: último precio de la ventana
# MAGIC ##  - **min**: precio mínimo
# MAGIC ##  - **max**: precio máximo
# MAGIC ##  - **pct_change**: variación porcentual entre open y close

# COMMAND ----------

# Aplicar watermark para manejar eventos tardíos
with_watermark = parsed_stream.withWatermark("produced_at", WATERMARK_DELAY)

# Ventana tumbling de 5 minutos agrupada por símbolo
windowed = (
    with_watermark
        .groupBy(
            F.col("symbol"),
            F.col("name"),
            F.window(F.col("produced_at"), WINDOW_DURATION)
        )
        .agg(
            F.first("price_usd").alias("open"),
            F.last("price_usd").alias("close"),
            F.min("price_usd").alias("min"),
            F.max("price_usd").alias("max"),
            F.count("*").alias("event_count"),
        )
)

# Calcular pct_change y detectar variación > 3%
price_windows = (
    windowed
        .select(
            F.col("symbol"),
            F.col("name"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("open"),
            F.col("close"),
            F.col("min"),
            F.col("max"),
            F.col("event_count"),
            # pct_change = ((close - open) / open) * 100
            F.round(
                ((F.col("close") - F.col("open")) / F.col("open")) * 100, 4
            ).alias("pct_change"),
        )
        # Marcar activos con variación absoluta > 3%
        .withColumn(
            "high_volatility",
            F.abs(F.col("pct_change")) > 3.0
        )
        # Timestamp de cuando se procesó esta ventana
        .withColumn("processed_at", F.current_timestamp())
)

print("✅ Ventana tumbling de 5 minutos configurada")
print(f"   Campos: symbol, window_start, window_end, open, close, min, max, pct_change, high_volatility")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6 — Función para escribir en MongoDB
# MAGIC # 
# MAGIC ##  Esta función toma cada micro-batch (grupo de filas procesadas) y las guarda
# MAGIC ##  en MongoDB en la colección `price_windows`.

# COMMAND ----------

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json

def write_to_mongodb(batch_df, batch_id):
    """
    Escribe cada micro-batch de ventanas cerradas en MongoDB.
    Se llama automáticamente por Spark Structured Streaming.
    """
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Sin datos — saltando escritura")
        return

    # Convertir el batch a lista de documentos Python
    rows = batch_df.collect()
    documents = []
    for row in rows:
        doc = {
            "symbol":         row["symbol"],
            "name":           row["name"],
            "window_start":   row["window_start"].isoformat() if row["window_start"] else None,
            "window_end":     row["window_end"].isoformat()   if row["window_end"]   else None,
            "open":           row["open"],
            "close":          row["close"],
            "min":            row["min"],
            "max":            row["max"],
            "pct_change":     row["pct_change"],
            "high_volatility": row["high_volatility"],
            "event_count":    row["event_count"],
            "processed_at":   row["processed_at"].isoformat() if row["processed_at"] else None,
        }
        documents.append(doc)

    # Escribir en MongoDB
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db     = client[MONGO_DATABASE]
        col    = db[MONGO_COLLECTION]
        result = col.insert_many(documents)

        # Mostrar cuántos activos con variación > 3% se detectaron
        high_vol = [d["symbol"] for d in documents if d.get("high_volatility")]

        print(f"[Batch {batch_id}]  {len(result.inserted_ids)} ventanas escritas en MongoDB")
        if high_vol:
            print(f"[Batch {batch_id}]  Activos con variación >3%: {', '.join(high_vol)}")
        else:
            print(f"[Batch {batch_id}]    Sin activos con variación >3% en este batch")

        client.close()

    except ConnectionFailure as e:
        print(f"[Batch {batch_id}]  Error conectando a MongoDB: {e}")
    except Exception as e:
        print(f"[Batch {batch_id}]  Error inesperado: {e}")
        raise

print("Función de escritura en MongoDB definida")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7 — Crear índice compuesto en MongoDB
# MAGIC # 
# MAGIC ##  El enunciado pide crear un **índice compuesto sobre `symbol + window_start`**
# MAGIC ##  para que las consultas sean rápidas. Esta celda lo crea una sola vez.

# COMMAND ----------

from pymongo import MongoClient, ASCENDING

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db  = client[MONGO_DATABASE]
    col = db[MONGO_COLLECTION]

    # Índice compuesto symbol + window_start
    index_name = col.create_index(
        [("symbol", ASCENDING), ("window_start", ASCENDING)],
        name="idx_symbol_window_start"
    )
    print(f" Índice creado en MongoDB: {index_name}")
    print("   Permite consultas rápidas como: '¿precio más reciente de BTC?'")
    client.close()

except Exception as e:
    print(f" Error creando índice: {e}")
    print("   Verifica que MongoDB esté corriendo con 'docker compose up'")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8 — Iniciar el stream
# MAGIC # 
# MAGIC ##  Esta es la celda principal. Arranca el job de streaming que:
# MAGIC ##  - Lee continuamente de Kafka
# MAGIC ##  - Procesa las ventanas de 5 minutos
# MAGIC ##  - Escribe en MongoDB
# MAGIC ##  - Guarda el checkpoint en DBFS (para no perder datos si se reinicia)
# MAGIC # 
# MAGIC #   **Esta celda corre indefinidamente.** Para detenerla haz clic en **"Cancel"** (el cuadrado arriba).
# MAGIC

# COMMAND ----------

query = (
    price_windows
        .writeStream
        .outputMode("update")               # escribe solo ventanas actualizadas
        .foreachBatch(write_to_mongodb)      # llama nuestra función por cada micro-batch
        .option("checkpointLocation", CHECKPOINT_PATH)  # semántica at-least-once
        .trigger(processingTime="30 seconds")  # procesa cada 30 segundos
        .start()
)

print("Stream iniciado correctamente")
print(f"   Checkpoint guardado en: {CHECKPOINT_PATH}")
print(f"   Tópico Kafka: {KAFKA_TOPIC}")
print(f"   MongoDB: {MONGO_DATABASE}.{MONGO_COLLECTION}")
print("")
print("   Esperando datos... (los primeros resultados aparecen en ~5 minutos)")
print("   Para detener: haz clic en 'Cancel' arriba ↑")

# Esperar a que el stream termine (o sea cancelado manualmente)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # 9 — Verificar resultados en MongoDB
# MAGIC # 
# MAGIC ##  Ejecuta esta celda **en un notebook separado** (o después de cancelar el stream)
# MAGIC ##  para ver qué se guardó en MongoDB.

# COMMAND ----------

from pymongo import MongoClient

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db  = client[MONGO_DATABASE]
    col = db[MONGO_COLLECTION]

    total = col.count_documents({})
    print(f"✅ Total de ventanas en MongoDB: {total}")

    print("\n📊 Últimas 5 ventanas guardadas:")
    for doc in col.find().sort("processed_at", -1).limit(5):
        doc.pop("_id", None)   # ocultar el ID interno de MongoDB
        print(f"  {doc['symbol']:6} | {doc['window_start']} → pct_change: {doc['pct_change']:+.2f}% | high_volatility: {doc['high_volatility']}")

    print("\n🚨 Activos con variación >3% detectados:")
    alertas = list(col.find({"high_volatility": True}, {"_id": 0, "symbol": 1, "pct_change": 1, "window_start": 1}))
    if alertas:
        for a in alertas[:10]:
            print(f"  {a['symbol']:6} | {a['window_start']} | {a['pct_change']:+.2f}%")
    else:
        print("  (ninguno aún — espera a que lleguen más datos)")

    client.close()

except Exception as e:
    print(f"❌ Error: {e}")
    print("   Verifica que MongoDB esté corriendo y el stream haya procesado datos")