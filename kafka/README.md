## Servicios

| Servicio | Imagen | Puerto |
|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.6.0 | 2181 |
| Kafka | confluentinc/cp-kafka:7.6.0 | 9092 |
| MongoDB | mongo:7.0 | 27017 |
| Producer | ./producer (custom) | — |

## Estructura de archivos

```
kafka/
├── docker-compose.yml
├── .env.example          ← copiar a .env y ajustar valores
├── .env                  ← git-ignored, contiene credenciales
└── producer/
    ├── producer.py
    ├── Dockerfile
    └── requirements.txt
```

## Arranque rápido

```bash
# 1. Clonar / ubicarse en la carpeta del módulo
cd kafka

# 2. Crear el archivo de entorno
cp .env.example .env

# 3. Levantar todos los servicios
docker compose up --build -d

# 4. Verificar que todos los contenedores están corriendo
docker compose ps
```

Todos los servicios deben aparecer en estado `running` (excepto `kafka-init`, que termina tras crear el topic).

## Verificación del topic

```bash
# Listar topics
docker exec cryptolake-kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

# Describir topic crypto-prices (3 particiones, retención 24h)
docker exec cryptolake-kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic crypto-prices
```

## Verificar lag del consumer group

```bash
# Ver offsets y lag del consumer group ()
docker exec cryptolake-kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <nombre-del-consumer-group-de-daniel>
```

## Ver eventos en tiempo real (consola consumer)

```bash
docker exec -it cryptolake-kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning
```

Deberías ver eventos JSON como:
```json
{
  "id": "90",
  "symbol": "BTC",
  "name": "Bitcoin",
  "rank": "1",
  "price_usd": 67432.15,
  "percent_change_24h": -1.23,
  "percent_change_1h": 0.45,
  "market_cap_usd": 1325000000000.0,
  "volume24": 28000000000.0,
  "csupply": 19650000.0,
  "produced_at": "2026-05-10T14:32:01.123456+00:00"
}
```

## Ver logs del productor

```bash
docker logs -f cryptolake-producer
```

Salida esperada cada 30 segundos:
```
2026-05-10T14:32:01 [INFO] Published 50 events to 'crypto-prices'
2026-05-10T14:32:01 [INFO] Next poll in 29.8 s
```

## Detener servicios

```bash
docker compose down          # detiene y elimina contenedores
docker compose down -v       # además elimina el volumen de MongoDB
```

## Notas para Daniel (Módulo 2)

- El broker Kafka es accesible en `localhost:9092` desde fuera de Docker.
- Desde Databricks, usar la IP pública de tu máquina en lugar de `localhost`.
- El topic `crypto-prices` tiene **3 particiones** — el consumer group puede tener hasta 3 workers en paralelo.
- Cada evento tiene el campo `produced_at` (ISO 8601 UTC) para calcular latencia en las ventanas de 5 minutos.
- Las credenciales de MongoDB están en el archivo `.env` — compartirlas de forma segura (no por chat).