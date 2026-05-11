# CryptoLake — Transformaciones

Esta carpeta contiene los pipelines de **Databricks Delta Live Tables (DLT)** que
conforman la capa de transformación de datos del proyecto CryptoLake.

---

## ¿Qué es Delta Live Tables (DLT)?

**Delta Live Tables** es un framework de Databricks para construir pipelines de
datos confiables y declarativos. En lugar de escribir código imperativo que lee,
transforma y escribe datos manualmente, se *declara* qué debe contener cada tabla
usando un decorador de Python — Databricks se encarga automáticamente del orden de
ejecución, los checkpoints, los reintentos y el seguimiento de calidad de datos.

### Conceptos clave

| Concepto | Significado |
|---|---|
| `@dlt.table` | Declara una función cuyo valor de retorno se convierte en una tabla Delta administrada |
| `@dlt.expect` | Define una regla de calidad de datos; las violaciones se registran pero las filas se conservan |
| `@dlt.expect_or_drop` | Igual que el anterior, pero las filas que no cumplen la regla se eliminan silenciosamente |
| `dlt.read("tabla")` | Lee otra tabla DLT del mismo pipeline (modo batch) |
| `dlt.read_stream("tabla")` | Lee otra tabla DLT del mismo pipeline como stream continuo |
| Pipeline | Conjunto de notebooks/archivos que DLT orquesta como una unidad |
Ñ
### ¿Por qué usar DLT en lugar de `df.write.format("delta").save(...)`?

| Escritura manual | DLT |
|---|---|
| Tú administras la ruta, el particionado y el esquema | DLT administra la tabla Delta por completo |
| Sin calidad de datos integrada — los fallos son silenciosos | Las reglas de calidad son de primer nivel; las violaciones aparecen en la UI del pipeline |
| El checkpointing de streams debe codificarse manualmente | DLT gestiona el estado del stream automáticamente |
| Las dependencias entre notebooks son difíciles de gestionar | DLT resuelve el grafo de dependencias y ejecuta en el orden correcto |
| El time travel de Delta sigue funcionando | El time travel de Delta sigue funcionando |

---

## Arquitectura Medallion

Este proyecto sigue el patrón medallón **Bronze → Silver**:

```
Fuentes externas
       │
       ├── Kafka topic (crypto-prices) ─────────────── CoinLore API (cada 30 s)
       │
       └── REST APIs ───────────────────────────────── CoinGecko + CoinCap (batch)
       │
       ▼
┌─────────────┐
│   BRONZE    │  Datos crudos, transformación mínima, historial completo preservado
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Limpio, tipado, deduplicado, enriquecido con métricas derivadas
└─────────────┘
```

---

## Archivos en esta carpeta

### Pipelines DLT (activos)

| Archivo | Pipeline | Tablas producidas | Modo |
|---|---|---|---|
| `dlt_bronze_kafka.py` | `cryptolake_kafka_dlt` | `bronze_crypto_prices` | **Continuo** (streaming) |
| `dlt_silver.py` | `cryptolake_kafka_dlt` | `silver_crypto_prices` | **Continuo** (streaming) |
| `dlt_bronze_batch.py` | `cryptolake_batch_dlt` | `bronze_coingecko`, `bronze_coincap` | **Disparado** (programado) |
| `dlt_silver_batch.py` | `cryptolake_batch_dlt` | `silver_crypto_enriched` | **Disparado** (programado) |

### Notebooks heredados (referencia histórica)

| Archivo | Propósito |
|---|---|
| `1_Bronze_Ingestion.ipynb` | Primera ingesta batch de CoinGecko y CoinCap (manual, sin DLT) |
| `2_Bronze_SecondBatch.ipynb` | Segunda ingesta batch — demuestra el append en Delta |
| `3_Silver_Transformation.ipynb` | JOIN Silver manual + schema evolution + demo de time travel |

> Estos notebooks fueron producidos para un módulo de entrega anterior y se conservan
> como evidencia. Los archivos `.py` con DLT son la implementación canónica a futuro.

---

## Pipeline 1 — Kafka Streaming (`cryptolake_kafka_dlt`)

**Fuente de datos**: topic Kafka `crypto-prices`, alimentado por el productor de
CoinLore que corre localmente cada 30 segundos (ver carpeta `kafka/`).

```
CoinLore API → Kafka (crypto-prices) → bronze_crypto_prices → silver_crypto_prices
```

### `dlt_bronze_kafka.py` — Bronze

Lee el topic de Kafka usando Spark Structured Streaming con
`spark.readStream.format("kafka")`. Parsea el payload JSON contra un esquema fijo
y preserva los metadatos de Kafka (offset, partición, timestamp del broker).

**Opciones clave:**

| Opción | Valor | Por qué |
|---|---|---|
| `startingOffsets` | `latest` | Omite los offsets históricos en la primera ejecución |
| `maxOffsetsPerTrigger` | `500` | Limita el tamaño del micro-batch para latencia predecible |
| `kafka.request.timeout.ms` | `180000` | Tolera indisponibilidad breve del broker |

**Esquema** (coincide con `kafka/producer/producer.py`):

| Columna | Tipo | Origen |
|---|---|---|
| `id`, `symbol`, `name` | `string` | CoinLore |
| `rank` | `integer` | CoinLore |
| `price_usd`, `percent_change_24h`, `percent_change_1h` | `double` | CoinLore |
| `market_cap_usd`, `volume24`, `csupply` | `double` | CoinLore |
| `produced_at` | `string` (ISO-8601 UTC) | Productor |
| `kafka_offset`, `kafka_partition` | `long` / `integer` | Kafka |
| `kafka_timestamp` | `timestamp` | Broker Kafka |
| `ingestion_ts` | `timestamp` | Databricks |

### `dlt_silver.py` — Silver

Lee `bronze_crypto_prices` como stream mediante `dlt.read_stream(...)` y aplica:

1. **Deduplicación** — `dropDuplicates(["symbol", "produced_at"])`
2. **Conversión de tipos** — `produced_at` string → `TimestampType`
3. **Normalización** — `symbol_upper = upper(symbol)`
4. **Métricas derivadas:**
   - `liquidity_ratio = volume24 / market_cap_usd`
   - `price_change_category` → `"up"` / `"down"` / `"flat"` (umbral ±0.5% en cambio 1h)
5. **Reglas de calidad** (las filas que no las cumplen se eliminan):
   - `price_usd IS NOT NULL AND price_usd > 0`
   - `symbol IS NOT NULL AND symbol != ''`
   - `market_cap_usd IS NOT NULL AND market_cap_usd > 0`

### Configuración en Databricks — Pipeline 1

1. Subir `dlt_bronze_kafka.py` y `dlt_silver.py` al workspace.
2. Crear un pipeline DLT (**Workflows → Delta Live Tables → Create**):
   - **Modo del pipeline**: Continuous
   - **Código fuente**: agregar ambos archivos
   - **Configuración**: agregar clave/valor:
     ```
     spark.cryptolake.kafka.bootstrap_servers = <tu-ip-publica>:9092
     ```
3. Asegurarse de que el puerto TCP `9092` sea accesible desde Databricks (IP pública de tu máquina, firewall abierto).

---

## Pipeline 2 — APIs Batch (`cryptolake_batch_dlt`)

**Fuentes de datos**: REST APIs de CoinGecko `/coins/markets` y CoinCap `/assets`.

```
CoinGecko API ─┐
               ├→ bronze_coingecko ─┐
CoinCap API  ──┘   bronze_coincap ──┴→ silver_crypto_enriched
```

### `dlt_bronze_batch.py` — Bronze

Cada ejecución del pipeline llama a ambas APIs y retorna un Spark DataFrame.
DLT lo escribe como una nueva versión Delta — el time travel permite comparar
snapshots entre ejecuciones sin código adicional.

**Columnas de `bronze_coingecko`:**

| Columna | Tipo | Notas |
|---|---|---|
| `id`, `symbol`, `name` | `string` | |
| `price_usd` | `double` | Renombrado desde `current_price` |
| `market_cap` | `double` | |
| `volume_24h` | `double` | Renombrado desde `total_volume` |
| `percent_change_24h` | `double` | |
| `high_24h`, `low_24h` | `double` | |
| `last_updated` | `string` | ISO-8601 de CoinGecko |
| `ingestion_ts` | `timestamp` | Databricks |
| `source` | `string` | `"coingecko"` |

**Columnas de `bronze_coincap`:**

| Columna | Tipo | Notas |
|---|---|---|
| `id`, `symbol`, `name` | `string` | |
| `rank` | `double` | Convertido desde string |
| `price_usd`, `market_cap_usd` | `double` | Convertidos desde string |
| `volume_24h`, `percent_change_24h` | `double` | Convertidos desde string |
| `supply` | `double` | Convertido desde string |
| `ingestion_ts` | `timestamp` | Databricks |
| `source` | `string` | `"coincap"` |

### `dlt_silver_batch.py` — Silver

Lee ambas tablas Bronze mediante `dlt.read(...)` (batch, no streaming) y aplica:

1. **Eliminación de nulos** en `symbol`, `price_usd`, `market_cap`, `volume_24h`
2. **LEFT JOIN** sobre `upper(symbol)` — CoinGecko es la tabla principal/izquierda
3. **Reglas de calidad** (las filas que no las cumplen se eliminan):
   - `price_usd IS NOT NULL AND price_usd > 0`
   - `symbol IS NOT NULL AND symbol != ''`
   - `market_cap IS NOT NULL AND market_cap > 0`
4. **Métricas derivadas:**
   - `liquidity_ratio = cc_volume_24h / cc_market_cap_usd`
   - `price_change_category` → `"up"` / `"down"` / `"flat"` (umbral ±2% en cambio 24h)

### Configuración en Databricks — Pipeline 2

1. Subir `dlt_bronze_batch.py` y `dlt_silver_batch.py` al workspace.
2. Crear un pipeline DLT:
   - **Modo del pipeline**: Triggered
   - **Código fuente**: agregar ambos archivos
   - **Programación**: cada hora (o según necesidad)
   - **Configuración**: agregar:
     ```
     spark.cryptolake.coingecko.api_key = <tu-clave-coingecko>
     spark.cryptolake.coincap.api_key   = <tu-clave-coincap>
     ```

---

## Calidad de datos en DLT

Cada regla `@dlt.expect` o `@dlt.expect_or_drop` aparece en la **UI del grafo del
pipeline** en Databricks, mostrando cuántas filas pasaron o fallaron por ejecución.
Esto reemplaza la función manual `check_nulls()` usada en los notebooks heredados.

```python
@dlt.expect("volumen_no_negativo", "volume_24h IS NULL OR volume_24h >= 0")
# → conserva la fila pero registra la violación en la UI

@dlt.expect_or_drop("precio_valido", "price_usd IS NOT NULL AND price_usd > 0")
# → elimina silenciosamente las filas que no cumplen esta condición
```

---

## Time Travel con tablas DLT

Dado que las tablas DLT son tablas Delta, el time travel está disponible sin
configuración adicional:

```sql
-- Ver todas las ejecuciones del pipeline que escribieron en la tabla
DESCRIBE HISTORY cryptolake.silver_crypto_enriched;

-- Leer el snapshot de hace 2 ejecuciones
SELECT * FROM cryptolake.silver_crypto_enriched VERSION AS OF 2;

-- Leer el snapshot tal como estaba ayer
SELECT * FROM cryptolake.silver_crypto_enriched
TIMESTAMP AS OF '2026-05-10 00:00:00';
```
