#!/usr/bin/env python3
# consumer/transaction_consumer.py
# Consumes raw transactions from Kafka, enriches them, detects anomalies,
# writes processed results back to Kafka and to MongoDB/MySQL.

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

sys.path.insert(0, ".")
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRANSACTIONS,
    KAFKA_TOPIC_PROCESSED,
    KAFKA_TOPIC_ALERTS,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
)
from models.transaction import ProcessedTransaction, RiskLevel, TransactionAlert
from models.anomaly_detector import AnomalyDetector
from database.mysql_writer import MySQLWriter
from database.mongo_writer import MongoWriter
from database.redis_cache import RedisCache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("consumer")


# ─────────────────────────────────────────────
# Enrichment helpers
# ─────────────────────────────────────────────

def amount_bucket(amount: float) -> str:
    if amount < 10:    return "micro"
    if amount < 100:   return "small"
    if amount < 1000:  return "normal"
    if amount < 5000:  return "large"
    return "whale"


def enrich(raw: dict) -> ProcessedTransaction:
    ts = datetime.fromisoformat(raw["timestamp"])
    start = time.perf_counter()

    txn = ProcessedTransaction(
        transaction_id=raw["transaction_id"],
        user_id=raw["user_id"],
        amount=raw["amount"],
        transaction_type=raw["transaction_type"],
        merchant=raw["merchant"],
        merchant_category=raw["merchant_category"],
        location_city=raw["location_city"],
        location_country=raw["location_country"],
        timestamp=ts,
        status=raw.get("status", "pending"),
        hour_of_day=ts.hour,
        day_of_week=ts.weekday(),
        is_weekend=ts.weekday() >= 5,
        amount_bucket=amount_bucket(raw["amount"]),
        processing_time_ms=round((time.perf_counter() - start) * 1000, 3),
    )
    return txn


# ─────────────────────────────────────────────
# Consumer class
# ─────────────────────────────────────────────

class TransactionConsumer:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
        })
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": 1,
            "linger.ms": 10,
        })
        self.detector   = AnomalyDetector()
        self.mysql      = MySQLWriter()
        self.mongo      = MongoWriter()
        self.redis      = RedisCache()
        self.running    = True
        self.processed  = 0
        self.anomalies  = 0

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        log.info("Shutdown signal → draining …")
        self.running = False

    # ─────────────────────────────────────────
    # Main loop
    # ─────────────────────────────────────────

    def run(self):
        self.consumer.subscribe([KAFKA_TOPIC_TRANSACTIONS])
        log.info("▶  Consumer started  |  subscribed to '%s'", KAFKA_TOPIC_TRANSACTIONS)

        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Consumer error: %s", msg.error())
                continue

            try:
                raw = json.loads(msg.value().decode("utf-8"))
                self._process(raw)
                self.consumer.commit(asynchronous=False)
            except Exception as exc:
                log.exception("Failed to process message: %s", exc)

        self.consumer.close()
        log.info("Consumer stopped. processed=%d  anomalies=%d", self.processed, self.anomalies)

    # ─────────────────────────────────────────
    # Per-message processing
    # ─────────────────────────────────────────

    def _process(self, raw: dict):
        # 1. Enrich
        txn = enrich(raw)

        # 2. Anomaly detection
        is_anomaly, score, reasons, risk_level = self.detector.analyze(txn)
        txn.is_anomaly    = is_anomaly
        txn.anomaly_score = score
        txn.risk_level    = risk_level
        txn.anomaly_reasons = reasons
        txn.status = "flagged" if is_anomaly else "completed"

        # 3. Write to databases
        self.mysql.insert_transaction(txn)
        self.mongo.insert_transaction(txn)

        # 4. Update live Redis metrics
        self.redis.update_metrics(txn)

        # 5. Publish processed event
        self.producer.produce(
            KAFKA_TOPIC_PROCESSED,
            key=txn.user_id.encode(),
            value=json.dumps(txn.to_dict()).encode(),
        )

        # 6. Raise alert if anomaly
        if is_anomaly:
            alert = self.detector.create_alert(txn, reasons, risk_level)
            self.producer.produce(
                KAFKA_TOPIC_ALERTS,
                key=txn.user_id.encode(),
                value=json.dumps(alert.to_dict()).encode(),
            )
            self.mongo.insert_alert(alert)
            self.anomalies += 1
            log.warning(
                "🚨 ANOMALY | id=%s user=%s amount=$%.2f risk=%s reasons=%s",
                txn.transaction_id[:8], txn.user_id[:8],
                txn.amount, risk_level.value, reasons,
            )

        self.producer.poll(0)
        self.processed += 1
        if self.processed % 50 == 0:
            log.info("Processed %d | Anomalies %d", self.processed, self.anomalies)


if __name__ == "__main__":
    TransactionConsumer().run()
