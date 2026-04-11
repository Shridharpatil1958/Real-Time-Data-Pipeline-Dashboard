#!/usr/bin/env python3
# producer/transaction_producer.py
# Simulates a high-throughput stream of financial transactions into Kafka

from __future__ import annotations

import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer, KafkaException
from faker import Faker

# Allow running from project root
sys.path.insert(0, ".")
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRANSACTIONS,
    PRODUCER_INTERVAL_MS,
)
from models.transaction import Transaction, TransactionType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("producer")
fake = Faker()

# ─────────────────────────────────────────────
# Static reference data
# ─────────────────────────────────────────────

MERCHANTS = {
    "Food & Dining":    ["McDonald's", "Starbucks", "Uber Eats", "DoorDash", "Chipotle", "Subway"],
    "Shopping":         ["Amazon", "Walmart", "Target", "Best Buy", "eBay", "Etsy"],
    "Travel":           ["Delta Airlines", "Uber", "Airbnb", "Booking.com", "Expedia"],
    "Entertainment":    ["Netflix", "Spotify", "Steam", "PlayStation Store", "Disney+"],
    "Healthcare":       ["CVS Pharmacy", "Walgreens", "LabCorp", "UnitedHealth"],
    "Utilities":        ["AT&T", "Comcast", "National Grid", "PG&E"],
    "Finance":          ["PayPal", "Venmo", "Robinhood", "Coinbase", "Chase"],
}

CITIES = [
    ("New York", "US"), ("Los Angeles", "US"), ("Chicago", "US"), ("Houston", "US"),
    ("London", "UK"), ("Paris", "FR"), ("Berlin", "DE"), ("Tokyo", "JP"),
    ("Sydney", "AU"), ("Toronto", "CA"), ("Mumbai", "IN"), ("Singapore", "SG"),
    ("Dubai", "AE"), ("São Paulo", "BR"),
]

DEVICE_TYPES = ["mobile_ios", "mobile_android", "web_browser", "pos_terminal", "atm"]

# Weighted transaction type distribution (realistic)
TXN_TYPES = [
    (TransactionType.PURCHASE,   0.65),
    (TransactionType.TRANSFER,   0.15),
    (TransactionType.WITHDRAWAL, 0.10),
    (TransactionType.DEPOSIT,    0.07),
    (TransactionType.REFUND,     0.03),
]


# ─────────────────────────────────────────────
# Amount generation (log-normal — realistic)
# ─────────────────────────────────────────────

def generate_amount(txn_type: TransactionType) -> float:
    """Generates realistic transaction amounts with occasional spikes."""
    # 2 % chance of a very large "whale" transaction for anomaly seeding
    if random.random() < 0.02:
        return round(random.uniform(5001, 50000), 2)

    base_ranges = {
        TransactionType.PURCHASE:   (1,   800),
        TransactionType.TRANSFER:   (10,  5000),
        TransactionType.WITHDRAWAL: (20,  1000),
        TransactionType.DEPOSIT:    (100, 10000),
        TransactionType.REFUND:     (1,   500),
    }
    lo, hi = base_ranges[txn_type]
    # Use log-normal so small amounts dominate
    raw = random.lognormvariate(3.5, 1.2)
    return round(min(max(raw, lo), hi), 2)


# ─────────────────────────────────────────────
# Transaction factory
# ─────────────────────────────────────────────

USER_POOL = [fake.uuid4() for _ in range(200)]   # 200 simulated users

def create_transaction() -> Transaction:
    txn_type = random.choices(
        [t for t, _ in TXN_TYPES],
        weights=[w for _, w in TXN_TYPES],
    )[0]

    category = random.choice(list(MERCHANTS.keys()))
    merchant = random.choice(MERCHANTS[category])
    city, country = random.choice(CITIES)

    return Transaction(
        user_id=random.choice(USER_POOL),
        amount=generate_amount(txn_type),
        transaction_type=txn_type,
        merchant=merchant,
        merchant_category=category,
        location_city=city,
        location_country=country,
        card_last4=str(random.randint(1000, 9999)),
        ip_address=fake.ipv4(),
        device_type=random.choice(DEVICE_TYPES),
    )


# ─────────────────────────────────────────────
# Delivery callback
# ─────────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        log.debug(
            "Delivered → topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ─────────────────────────────────────────────
# Producer entrypoint
# ─────────────────────────────────────────────

class TransactionProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
            "linger.ms": 5,
            "batch.size": 16384,
            "compression.type": "snappy",
        })
        self.running = True
        self.sent = 0
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        log.info("Shutdown signal received — flushing and exiting …")
        self.running = False

    def run(self, interval_ms: int = PRODUCER_INTERVAL_MS):
        log.info(
            "▶  Producer started  |  topic=%s  |  interval=%d ms",
            KAFKA_TOPIC_TRANSACTIONS, interval_ms,
        )
        while self.running:
            txn = create_transaction()
            payload = json.dumps(txn.to_dict()).encode("utf-8")
            try:
                self.producer.produce(
                    topic=KAFKA_TOPIC_TRANSACTIONS,
                    key=txn.user_id.encode("utf-8"),
                    value=payload,
                    callback=delivery_report,
                )
                self.sent += 1
                if self.sent % 100 == 0:
                    log.info("Sent %d transactions so far …", self.sent)
                self.producer.poll(0)
            except KafkaException as exc:
                log.error("Kafka error: %s", exc)

            time.sleep(interval_ms / 1000.0)

        self.producer.flush()
        log.info("Producer stopped. Total sent: %d", self.sent)


if __name__ == "__main__":
    TransactionProducer().run()
