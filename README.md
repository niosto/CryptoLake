# CryptoLake — Evidencias por Rúbrica

**ST1630 · Sistemas Intensivos en Datos · 2026**  
Grupo: Nicolás Ospina · Daniel Zapata · Agustín Figueroa · María Alejandra Ocampo

---

## §4.1 Descripción del Problema y Planteamiento — 18 pts

### 1.1 Contexto y dominio (4 pts)

**Dominio:** Mercado de criptomonedas en tiempo real.

**Impacto medible:** El mercado cripto genera precios de cientos de activos cada 30 segundos. Existen dos necesidades complementarias y claramente diferenciadas:

- **Batch (histórico):** ¿Qué activos fueron más volátiles el mes pasado? ¿Cómo evolucionó el precio diario de BTC, ETH y SOL?
- **Streaming (operacional):** ¿Qué monedas están variando más del 3% en los últimos 5 minutos? ¿Cuál es el resumen OHLC en tiempo real?

La separación batch/streaming es natural al dominio: el análisis histórico requiere agregaciones sobre semanas/meses de datos (batch), mientras que las alertas de volatilidad requieren latencia de segundos (streaming).

---

### 1.2 Enunciado del problema (4 pts)

**Preguntas analíticas (batch):**
1. ¿Cuál fue la volatilidad promedio mensual (avg/stddev de `percent_change_24h`) de los activos top-50?
2. ¿Cómo evolucionó el precio promedio diario (open/close/high/low) de BTC, ETH y SOL?
3. ¿Qué activos en alza tienen mayor `liquidity_ratio` (volumen/market_cap)?

**Preguntas operacionales (streaming):**
1. ¿Qué activos presentan variación absoluta `> 3%` en la ventana tumbling de 5 minutos actual?
2. ¿Cuál es el resumen OHLC (open, close, min, max) por símbolo en tiempo real?
3. ¿Cuántos eventos por símbolo se han procesado en la ventana actual?

---

### 1.3 Fuentes y esquemas (5 pts)

| Fuente | Tipo | Volumen | Frecuencia | Uso |
|---|---|---|---|---|
| **CoinLore API** | Evento streaming | 50 coins/ciclo | Cada 30 s | Kafka producer → streaming |
| **CoinGecko API** | Snapshot batch | 50 coins/ejecución | Por lote (2 lotes) | Bronze batch |
| **CoinCap API v3** | Snapshot batch | 50 coins/ejecución | Por lote (2 lotes) | Bronze batch, JOIN Silver |

**Esquema CoinLore (streaming) — producer.py:**
```json
{
  "id": "90",
  "symbol": "BTC",
  "name": "Bitcoin",
  "rank": 1,
  "price_usd": 67432.15,
  "percent_change_24h": -1.23,
  "percent_change_1h": 0.45,
  "market_cap_usd": 1325000000000.0,
  "volume24": 28000000000.0,
  "csupply": 19650000.0,
  "produced_at": "2026-05-10T14:32:01.123456+00:00"
}
```

**Esquema CoinGecko (batch) — bronze.coingecko:**
`id · symbol · name · price_usd · market_cap · volume_24h · percent_change_24h · high_24h · low_24h · last_updated · ingestion_ts · source`

**Esquema CoinCap (batch) — bronze.coincap:**
`id · symbol · name · rank · price_usd · market_cap_usd · volume_24h · percent_change_24h · supply · ingestion_ts · source`

---

### 1.4 Justificación del stack (5 pts)

| Componente | Tecnología | Justificación |
|---|---|---|
| Mensajería | Apache Kafka | Desacoplamiento entre el producer CoinLore y el consumidor Spark; 3 particiones permiten paralelismo por símbolo (key = symbol) |
| Streaming | Spark Structured Streaming | Ventanas tumbling de 5 min sobre `produced_at`; watermark de 10 min tolera eventos tardíos; `foreachBatch` permite lógica personalizada hacia MongoDB |
| NoSQL | MongoDB Atlas | Esquema flexible para documentos OHLC; índice compuesto `symbol + window_start` habilita consultas O(log n) por símbolo y ventana temporal |
| Batch | Apache Spark / DLT | Pipeline declarativo con `@dlt.table` y `@dlt.expect_or_drop`; dependencias resueltas automáticamente por Databricks |
| Table format | Delta Lake | ACID, Time Travel (`VERSION AS OF N`), Schema Evolution (`mergeSchema=true`); reemplaza Iceberg con equivalencia funcional aprobada por el profesor |
| Infraestructura | Docker Compose | Zookeeper, Kafka, kafka-init y Producer en contenedores; reproducibilidad garantizada con `docker compose up --build -d` |

---

## §4.2 Arquitectura e Infraestructura — 12 pts

### 2.1 Docker Compose (5 pts)

**Archivo:** `kafka/docker-compose.yml`

Servicios y estado esperado:

| Servicio | Imagen | Puerto | Estado |
|---|---|---|---|
| `cryptolake-zookeeper` | confluentinc/cp-zookeeper:7.6.0 | 2181 | healthy |
| `cryptolake-kafka` | confluentinc/cp-kafka:7.6.0 | 9092 | healthy |
| `cryptolake-kafka-init` | confluentinc/cp-kafka:7.6.0 | — | completed |
| `cryptolake-producer` | ./producer (custom) | — | running |

**Arranque:**
```bash
cd kafka
cp .env.example .env
docker compose up --build -d
docker compose ps
```

**Configuraciones clave:**
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — topics solo por kafka-init
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://${IP_ADDRESS:-localhost}:9092`
- `restart: on-failure` en el producer
- Healthchecks en Zookeeper y Kafka con reintentos

---

### 2.2 Diagrama de arquitectura (4 pts)

**Archivo:** `CryptoLake_Architecture.drawio`

El diagrama muestra ambos caminos diferenciados con swim lanes:

```
STREAMING: CoinLore API → Producer (Docker) → Kafka topic → Bronze DLT → Silver DLT → MongoDB Atlas → Consultas NoSQL

BATCH:     CoinGecko API ─┐
                           ├→ Bronze DLT → Silver DLT → Gold notebook → Delta Lake → Spark SQL
           CoinCap API   ─┘
```

Tecnología visible en cada nodo, flechas numeradas, contenedores Databricks diferenciados, banda de infraestructura Docker al fondo.

---

### 2.3 Reproducibilidad (3 pts)

**Archivo:** `kafka/README.md`

Comandos exactos documentados:

```bash
# Levantar infraestructura
docker compose up --build -d

# Verificar topics
docker exec cryptolake-kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

# Ver eventos en tiempo real
docker exec -it cryptolake-kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic crypto-prices --from-beginning

# Ver logs del producer
docker logs -f cryptolake-producer

# Verificar lag del consumer group
docker exec cryptolake-kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <consumer-group>

# Detener
docker compose down
```

---

## §4.3 Capa de Mensajería — Kafka — 12 pts

### 3.1 Productor de eventos (5 pts)

**Archivo:** `kafka/producer/producer.py`

- Consulta `https://api.coinlore.net/api/tickers/?limit=50` cada `POLL_INTERVAL_SECONDS` segundos (default 30)
- Publica cada moneda como evento JSON individual al topic `crypto-prices`
- Clave del mensaje = `symbol` → affinity de partición por moneda
- Campo `produced_at` (ISO-8601 UTC) en cada evento para cálculo de latencia en ventanas
- `acks="all"`, `retries=5`, `retry_backoff_ms=500`
- Configuración 100% por variables de entorno, sin hardcoding

```python
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "crypto-prices")
POLL_INTERVAL_SECONDS   = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
```

---

### 3.2 Configuración de tópicos (4 pts)

**Servicio:** `kafka-init` en docker-compose.yml

```bash
kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic crypto-prices \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000
```

- **3 particiones** — permite hasta 3 workers en paralelo en el consumer group
- **Retención 24 h** — 86400000 ms
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — control explícito
- Offsets visibles con `kafka-console-consumer` y `kafka-consumer-groups`

---

### 3.3 Observabilidad (3 pts)

**Logs del producer** (cada 30 s):
```
2026-05-10T14:32:01 [INFO] Published 50 events to 'crypto-prices'
2026-05-10T14:32:01 [INFO] Next poll in 29.8 s
```

**Consumer en consola:**
```bash
docker exec -it cryptolake-kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic crypto-prices --from-beginning
```

**Lag del consumer group:**
```bash
docker exec cryptolake-kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <nombre-consumer-group>
```

---

## §4.4 Procesamiento en Streaming — Spark Structured Streaming — 15 pts

> **Nota:** El profesor aprobó Spark Structured Streaming como sustituto de Apache Flink. Equivalencia funcional demostrada: consumo de Kafka, ventanas con estado, sink hacia NoSQL, checkpointing.

### 4.1 Consumo de Kafka (4 pts)

**Archivo:** `Streaming_MongoD.ipynb` — celda 4

```python
raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.request.timeout.ms", "180000")
        .option("failOnDataLoss", "false")
        .load()
)

parsed_stream = (
    raw_stream
        .withColumn("payload", F.from_json(F.col("value").cast(StringType()), COINLORE_SCHEMA))
        .select(F.col("payload.*"))
        .withColumn("produced_at", F.to_timestamp(F.col("produced_at")))
        .filter(F.col("price_usd").isNotNull() & (F.col("price_usd") > 0))
)
```

Deserialización JSON funcional contra `COINLORE_SCHEMA` (11 campos tipados). Offsets comprometidos mediante checkpointing en DBFS.

---

### 4.2 Transformaciones con estado (5 pts)

**Ventana tumbling de 5 minutos** con watermark de 10 minutos:

```python
with_watermark = parsed_stream.withWatermark("produced_at", "10 minutes")

windowed = (
    with_watermark
        .groupBy(F.col("symbol"), F.col("name"), F.window(F.col("produced_at"), "5 minutes"))
        .agg(
            F.first("price_usd").alias("open"),
            F.last("price_usd").alias("close"),
            F.min("price_usd").alias("min"),
            F.max("price_usd").alias("max"),
            F.count("*").alias("event_count"),
        )
)

price_windows = (
    windowed.select(
        "symbol", "name",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "open", "close", "min", "max", "event_count",
        F.round(((F.col("close") - F.col("open")) / F.col("open")) * 100, 4).alias("pct_change"),
    )
    .withColumn("high_volatility", F.abs(F.col("pct_change")) > 3.0)
    .withColumn("processed_at", F.current_timestamp())
)
```

Tipo de ventana: **tumbling** (no solapada). Agregaciones con estado: open, close, min, max, pct_change, high_volatility.

---

### 4.3 Sink hacia NoSQL (4 pts)

**MongoDB Atlas** — colección `price_windows`:

```python
def write_to_mongodb(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = batch_df.collect()
    documents = [{ "symbol": row["symbol"], "window_start": ..., "pct_change": ..., "high_volatility": ... }
                 for row in rows]
    client = MongoClient(MONGO_URI)
    db[MONGO_COLLECTION].insert_many(documents)
    client.close()
```

Esquema persistido correctamente en MongoDB con todos los campos definidos.

---

### 4.4 Semántica de entrega (2 pts)

```python
query = (
    price_windows
        .writeStream
        .outputMode("update")
        .foreachBatch(write_to_mongodb)
        .option("checkpointLocation", "dbfs:/cryptolake/checkpoints/streaming_modulo2")
        .trigger(processingTime="30 seconds")
        .start()
)
```

- **Semántica:** `at-least-once` — checkpointing en DBFS garantiza que ningún micro-batch se pierda ante fallo
- **outputMode:** `update` — solo escribe ventanas que cambiaron en el micro-batch

---

## §4.5 Base de Datos NoSQL — MongoDB — 12 pts

### 5.1 Justificación del modelo (4 pts)

El esquema está diseñado específicamente para las consultas operacionales:

| Campo | Tipo | Justificación |
|---|---|---|
| `symbol` | string | clave de partición lógica para queries por moneda |
| `window_start` | ISO-8601 | ordena cronológicamente las ventanas por símbolo |
| `window_end` | ISO-8601 | delimita la ventana temporal |
| `open / close` | double | precios de apertura y cierre de la ventana |
| `min / max` | double | rango de precio en la ventana |
| `pct_change` | double | `(close-open)/open × 100` — métrica de volatilidad |
| `high_volatility` | bool | `abs(pct_change) > 3%` — flag de alerta |
| `processed_at` | timestamp | auditoría de procesamiento |

**Índice compuesto creado:**
```python
col.create_index(
    [("symbol", ASCENDING), ("window_start", ASCENDING)],
    name="idx_symbol_window_start"
)
```

Acceso O(log n) para consultas por símbolo y ventana temporal.

---

### 5.2 Consultas operacionales (5 pts)

```python
# 1. Alertas: activos con variación > 3% ahora mismo
col.find({"high_volatility": True}, {"symbol": 1, "pct_change": 1, "window_start": 1})

# 2. OHLC más reciente por símbolo
col.find({"symbol": "BTC"}).sort("window_start", -1).limit(1)

# 3. Ventanas de los últimos 5 minutos
from datetime import datetime, timedelta, timezone
t = datetime.now(timezone.utc) - timedelta(minutes=5)
col.find({"window_start": {"$gte": t.isoformat()}})

# 4. Total de ventanas almacenadas
col.count_documents({})

# 5. Top activos por pct_change descendente
col.find().sort("pct_change", -1).limit(10)
```

---

### 5.3 Diferencia con SQL (3 pts)

| Aspecto | MongoDB (NoSQL) | SQL tradicional |
|---|---|---|
| **Escalabilidad** | Horizontal (sharding por symbol) | Vertical principalmente |
| **Esquema** | Flexible — nuevos campos sin ALTER TABLE | Rígido — requiere migraciones |
| **Acceso** | O(log n) por clave primaria compuesta | Scan completo sin índice correcto |
| **Latencia** | Milisegundos para queries operacionales | Mayor overhead para OLTP en alta concurrencia |
| **Caso de uso** | Alertas en tiempo real, OHLC por símbolo | Análisis histórico, JOINs complejos |

MongoDB es la elección correcta para este caso: escrituras continuas desde Spark cada 30 s, consultas por `symbol + window_start`, esquema que puede evolucionar (ej: añadir `market_cap_at_close` sin migración).

---

## §4.6 Pipeline Batch — Spark + Delta Lake — 15 pts

### 6.1 Capa Bronze (4 pts)

**Archivo:** `transformations/dlt_bronze_batch.py`

Dos tablas Bronze, dos lotes cada una:

- **Lote 1:** `1_Bronze_Ingestion.ipynb` → `mode("overwrite")` → primera versión Delta
- **Lote 2:** `2_Bronze_SecondBatch.ipynb` → `mode("append")` → segunda versión Delta (historial preservado)

Metadatos de auditoría en cada fila:
```python
df.withColumn("ingestion_ts", F.current_timestamp())
  .withColumn("source", F.lit("coingecko"))  # o "coincap"
```

Reglas de calidad declarativas:
```python
@dlt.expect("non_null_price", "price_usd IS NOT NULL")
@dlt.expect("non_null_symbol", "symbol IS NOT NULL")
```

---

### 6.2 Capa Silver (5 pts)

**Archivo:** `transformations/dlt_silver_batch.py`

```python
df_cg.dropna(subset=["symbol", "price_usd", "market_cap", "volume_24h"])
df_cc.dropna(subset=["symbol", "price_usd", "market_cap_usd", "volume_24h"])

df_joined = df_cg.join(df_cc, on="symbol_upper", how="left")

df_silver = df_joined.withColumn(
    "liquidity_ratio",
    F.round(F.col("cc_volume_24h") / F.col("cc_market_cap_usd"), 6)
).withColumn(
    "price_change_category",
    F.when(F.col("percent_change_24h") > 2.0, F.lit("up"))
     .when(F.col("percent_change_24h") < -2.0, F.lit("down"))
     .otherwise(F.lit("flat"))
)
```

- Manejo de nulos en columnas críticas
- JOIN CoinGecko ⟕ CoinCap sobre `upper(symbol)`
- Columnas calculadas: `liquidity_ratio`, `price_change_category`
- `@dlt.expect_or_drop` en 3 reglas de calidad

---

### 6.3 Capa Gold (3 pts)

**Archivo:** `transformations/gold_notebook.py`

Tres tablas que responden directamente las preguntas batch del enunciado:

```python
# gold.volatility_monthly — responde: ¿qué activos son más volátiles?
.groupBy("symbol", "name", "year", "month")
.agg(F.avg("percent_change_24h"), F.stddev("percent_change_24h"), ...)
.withColumn("is_high_volatility", F.when(F.col("stddev_change_24h") > 2.0, True))

# gold.price_daily — responde: ¿cómo evolucionó el precio diario?
.groupBy("symbol", "name", "date")
.agg(F.avg("price_usd"), F.min("price_usd"), F.max("price_usd"), F.first("price_usd"), F.last("price_usd"))

# gold.market_summary — responde: ¿cuál es el estado actual del mercado?
Window.partitionBy("symbol").orderBy(F.col("last_updated").desc())
```

---

### 6.4 Features Delta Lake — equivalente Iceberg (3 pts)

> El profesor aprobó Delta Lake como sustituto de Iceberg. Los tres features requeridos están demostrados:

**Time Travel:**
```sql
SELECT * FROM cryptolake.silver_crypto_enriched VERSION AS OF 2;
SELECT * FROM cryptolake.silver_crypto_enriched TIMESTAMP AS OF '2026-05-10 00:00:00';
DESCRIBE HISTORY cryptolake.silver_crypto_enriched;
```

**Schema Evolution:**
```python
df_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")   # Schema Evolution habilitado
    .saveAsTable("qubika.silver.crypto_assets")
```

**ACID:**
Delta Lake garantiza atomicidad en cada escritura: o se escribe el lote completo o no se escribe nada. Verificable con `DESCRIBE HISTORY`.

---

## §4.7 Integración y Consultas Analíticas — 10 pts

### 7.1 Coherencia entre caminos (4 pts)

El mismo dominio (criptomonedas) se responde desde dos ángulos complementarios:

| Pregunta | Camino | Capa |
|---|---|---|
| ¿Qué moneda fue más volátil en mayo? | Batch | `gold.volatility_monthly` |
| ¿Qué moneda está variando >3% ahora? | Streaming | MongoDB `price_windows` |
| ¿Cómo evolucionó BTC esta semana? | Batch | `gold.price_daily` |
| ¿Cuál es el OHLC de ETH en este momento? | Streaming | MongoDB `price_windows` |

**Query que cruza ambos caminos:**
```sql
SELECT g.symbol, g.avg_price_usd, p.pct_change
FROM gold.price_daily g
JOIN price_windows p ON g.symbol = p.symbol
WHERE g.date = current_date()
```

---

### 7.2 Queries sobre Gold (4 pts)

```sql
-- 1. Top-10 activos más volátiles del mes
SELECT symbol, avg_change_24h, stddev_change_24h, is_high_volatility
FROM gold.volatility_monthly
ORDER BY stddev_change_24h DESC LIMIT 10;

-- 2. Evolución diaria de BTC, ETH, SOL
SELECT symbol, date, avg_price_usd, low_price_usd, high_price_usd
FROM gold.price_daily
WHERE symbol IN ('BTC', 'ETH', 'SOL')
ORDER BY date;

-- 3. Activos en alza con mayor liquidez
SELECT symbol, liquidity_ratio, price_change_category
FROM gold.market_summary
WHERE price_change_category = 'up'
ORDER BY liquidity_ratio DESC;

-- 4. Variación promedio de activos con alerta histórica
SELECT symbol, AVG(pct_change) as avg_pct
FROM price_windows
WHERE high_volatility = true
GROUP BY symbol ORDER BY avg_pct DESC;

-- 5. Activos con mayor capitalización promedio diaria
SELECT symbol, AVG(avg_market_cap) as avg_mktcap
FROM gold.price_daily
GROUP BY symbol ORDER BY avg_mktcap DESC LIMIT 10;
```

---

### 7.3 Comparación de capas (2 pts)

| Cuándo usar MongoDB (NoSQL) | Cuándo usar Gold / Delta (analítico) |
|---|---|
| Alertas en tiempo real (latencia < 100 ms) | Análisis histórico mensual o semanal |
| Consulta por símbolo + ventana (clave primaria) | JOINs complejos entre múltiples dimensiones |
| Escrituras continuas desde Spark cada 30 s | Consultas de alta cardinalidad sobre millones de filas |
| Preguntas operacionales: ¿qué pasa ahora? | Preguntas analíticas: ¿qué pasó? ¿por qué? |

---

## §4.8 Calidad del Código y Documentación — 5 pts

### 8.1 README (2 pts)

**Archivo:** `kafka/README.md` — incluye:
- Prerequisites: Docker Desktop, Python 3.11
- Comandos exactos por componente (`docker compose up`, consumer, lag)
- Descripción de cada capa y topic
- Notas de integración con Databricks (IP pública, puerto 9092)

### 8.2 Código limpio (3 pts)

- **Sin hardcoding:** toda configuración por variables de entorno (`.env`) o `spark.conf`
- **Scripts separados por capa:** `kafka/`, `transformations/dlt_bronze_*.py`, `transformations/dlt_silver_*.py`, `gold_notebook.py`
- **Sin código comentado en producción:** notebooks legacy conservados solo como evidencia histórica en `transformations/`
- **Config centralizado:** `transformations/config.py` (no committeado en Git)
- **Dockerfile mínimo:** `python:3.11-slim`, sin dependencias innecesarias

---

## §4.9 Presentación y Demo en Vivo — 8 pts

### 9.1 Demo funcional (4 pts)

Plan de demo paso a paso:

1. `docker compose up --build -d` → todos los servicios healthy
2. `docker logs -f cryptolake-producer` → 50 eventos publicados cada 30 s
3. `kafka-console-consumer` → JSON en tiempo real visible en consola
4. Iniciar notebook `Streaming_MongoD.ipynb` en Databricks → ventanas MongoDB
5. `db.price_windows.find({high_volatility: true})` → alertas en MongoDB Atlas
6. Ejecutar DLT pipeline batch → Bronze → Silver → Gold en Databricks
7. Queries Spark SQL sobre `gold.volatility_monthly` y `gold.price_daily`

### 9.2 Defensa técnica (4 pts)

Preguntas anticipadas y respuestas:

**¿Por qué MongoDB y no Cassandra?**
MongoDB permite esquema flexible y el índice compuesto `symbol + window_start` resuelve exactamente nuestras queries operacionales. Cassandra sería mejor si necesitáramos partición masiva por time series puro.

**¿Qué pasaría si llegaran eventos desordenados?**
El watermark de 10 minutos en Spark SSt absorbe eventos tardíos hasta 10 min después del cierre de la ventana. Eventos más tardíos se descartan (trade-off aceptable para alertas en tiempo real).

**¿Por qué Delta Lake y no Iceberg?**
Equivalencia funcional: ambos ofrecen ACID, Time Travel y Schema Evolution. Delta Lake está nativamente integrado en Databricks, lo que elimina la necesidad de configurar un REST Catalog separado.

**¿Por qué Spark SSt y no Flink?**
Spark SSt corre en el mismo cluster Databricks que el pipeline batch, eliminando la necesidad de mantener un cluster Flink separado. Las ventanas tumbling, watermarks y checkpointing ofrecen las mismas garantías. Además, suma el punto bonus.

---

## Bonus — Puntos adicionales

### Spark Structured Streaming (+1 pt)

Implementado como alternativa completa a Flink:
- Consumo de Kafka con `readStream.format("kafka")`
- Ventana tumbling de 5 min sobre `produced_at`
- Checkpointing en `dbfs:/cryptolake/checkpoints/streaming_modulo2`
- Sink hacia MongoDB Atlas via `foreachBatch`

**Archivo:** `Streaming_MongoD.ipynb`

---

## Resumen de puntos

| Sección | Descripción | Pts máx | Estado |
|---|---|---|---|
| §4.1 | Descripción del problema | 18 | Completo |
| §4.2 | Arquitectura e infraestructura | 12 | Completo |
| §4.3 | Mensajería — Kafka | 12 | Completo |
| §4.4 | Streaming — Spark SSt | 15 | Completo |
| §4.5 | Base de datos NoSQL — MongoDB | 12 | Completo |
| §4.6 | Pipeline batch — Delta Lake | 15 | Completo |
| §4.7 | Integración y consultas | 10 | Completo |
| §4.8 | Calidad del código | 5 | Completo |
| §4.9 | Presentación y demo | 8 | Completo |
| **TOTAL** | | **107** | **100 + 1 bonus** |
