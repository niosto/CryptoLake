"""
CryptoLake — Módulo 1
Kafka Producer: CoinLore → crypto-prices topic

Polls CoinLore API every POLL_INTERVAL_SECONDS seconds and publishes
each coin as an individual JSON event to the Kafka topic.

Environment variables (no hardcoding):
  KAFKA_BOOTSTRAP_SERVERS  — broker address (default: kafka:29092)
  KAFKA_TOPIC              — target topic   (default: crypto-prices)
  POLL_INTERVAL_SECONDS    — poll cadence   (default: 30)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("cryptolake.producer")

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto-prices")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))

COINLORE_URL = "https://api.coinlore.net/api/tickers/?limit=50"


# ---------------------------------------------------------------------------
# Kafka producer factory
# ---------------------------------------------------------------------------
def build_producer() -> KafkaProducer:
    """Create a KafkaProducer with JSON serialization and retry logic."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Use coin symbol as key for partition affinity
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",            # wait for all in-sync replicas
        retries=5,
        retry_backoff_ms=500,
    )

# ---------------------------------------------------------------------------
# CoinLore fetch
# ---------------------------------------------------------------------------
def fetch_coins() -> list[dict]:
    """Fetch current coin data from CoinLore. Returns list of coin dicts."""
    try:
        response = requests.get(COINLORE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
    except requests.RequestException as exc:
        log.error("Failed to fetch CoinLore data: %s", exc)
        return []

# ---------------------------------------------------------------------------
# Publish events
# ---------------------------------------------------------------------------
def publish_coins(producer: KafkaProducer, coins: list[dict]) -> int:
    """
    Publish each coin as an individual event to Kafka.
    Adds produced_at timestamp to every event.
    Returns the number of events published.
    """
    produced_at = datetime.now(timezone.utc).isoformat()
    published = 0

    for coin in coins:
        event = {
            "id":                 coin.get("id"),
            "symbol":             coin.get("symbol"),
            "name":               coin.get("name"),
            "rank":               coin.get("rank"),
            "price_usd":          _to_float(coin.get("price_usd")),
            "percent_change_24h": _to_float(coin.get("percent_change_24h")),
            "percent_change_1h":  _to_float(coin.get("percent_change_1h")),
            "market_cap_usd":     _to_float(coin.get("market_cap_usd")),
            "volume24":           _to_float(coin.get("volume24")),
            "csupply":            _to_float(coin.get("csupply")),
            "produced_at":        produced_at,   # added by producer
        }

        try:
            future = producer.send(
                KAFKA_TOPIC,
                key=event["symbol"],
                value=event,
            )
            future.get(timeout=10)   # block to catch per-message errors
            published += 1
        except KafkaError as exc:
            log.error("Failed to publish event for %s: %s", event["symbol"], exc)

    return published

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_float(value) -> float | None:
    """Safely cast CoinLore string numbers to float."""
    try:
        return float(value) if value not in (None, "", "?") else None
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("CryptoLake Producer starting up")
    log.info("  Broker  : %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("  Topic   : %s", KAFKA_TOPIC)
    log.info("  Interval: %s s", POLL_INTERVAL_SECONDS)

    producer = build_producer()
    log.info("Kafka producer connected")

    try:
        while True:
            loop_start = time.monotonic()

            coins = fetch_coins()
            if coins:
                count = publish_coins(producer, coins)
                log.info("Published %d events to '%s'", count, KAFKA_TOPIC)
            else:
                log.warning("No coins received from CoinLore — skipping this cycle")

            # Sleep for the remainder of the interval
            elapsed = time.monotonic() - loop_start
            sleep_for = max(0, POLL_INTERVAL_SECONDS - elapsed)
            log.info("Next poll in %.1f s", sleep_for)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log.info("Shutting down producer...")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed cleanly")

if __name__ == "__main__":
    main()
