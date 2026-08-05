#!/usr/bin/env python3
# database/mongo_writer.py
# Writes enriched transactions and alerts to MongoDB

from __future__ import annotations

import logging
import time

from pymongo import MongoClient, DESCENDING, errors as mongo_errors

import sys
sys.path.insert(0, ".")
from config.settings import MONGO_URI, MONGO_DB
from models.transaction import ProcessedTransaction, TransactionAlert

log = logging.getLogger("mongo_writer")


class MongoWriter:
    def __init__(self):
        self._client = None
        self._db = None
        self._connect()
        self._ensure_indexes()

    def _connect(self, retries: int = 10):
        for attempt in range(1, retries + 1):
            try:
                self._client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
                self._client.admin.command("ping")
                self._db = self._client[MONGO_DB]
                log.info("MongoDB connected ✓")
                return
            except Exception as exc:
                log.warning("MongoDB connect attempt %d/%d failed: %s", attempt, retries, exc)
                time.sleep(3)
        raise RuntimeError("Could not connect to MongoDB")

    def _ensure_indexes(self):
        txns = self._db["transactions"]
        txns.create_index([("timestamp", DESCENDING)])
        txns.create_index([("user_id", DESCENDING)])
        txns.create_index([("is_anomaly", DESCENDING)])
        txns.create_index("transaction_id", unique=True)

        alerts = self._db["alerts"]
        alerts.create_index([("timestamp", DESCENDING)])
        alerts.create_index("alert_id", unique=True)

    # ─────────────────────────────────────────
    # Write
    # ─────────────────────────────────────────

    def insert_transaction(self, txn: ProcessedTransaction):
        try:
            self._db["transactions"].update_one(
                {"transaction_id": txn.transaction_id},
                {"$set": txn.to_dict()},
                upsert=True,
            )
        except mongo_errors.DuplicateKeyError:
            pass
        except Exception as exc:
            log.error("Mongo insert_transaction failed: %s", exc)

    def insert_alert(self, alert: TransactionAlert):
        try:
            self._db["alerts"].update_one(
                {"alert_id": alert.alert_id},
                {"$set": alert.to_dict()},
                upsert=True,
            )
        except Exception as exc:
            log.error("Mongo insert_alert failed: %s", exc)

    # ─────────────────────────────────────────
    # Read (for dashboard / analytics)
    # ─────────────────────────────────────────

    def fetch_recent_transactions(self, limit: int = 50) -> list[dict]:
        cursor = (
            self._db["transactions"]
            .find({}, {"_id": 0})
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )
        return list(cursor)

    def fetch_recent_alerts(self, limit: int = 20) -> list[dict]:
        cursor = (
            self._db["alerts"]
            .find({}, {"_id": 0})
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )
        return list(cursor)

    def fetch_anomaly_stats(self) -> dict:
        pipeline = [
            {"$group": {
                "_id": "$risk_level",
                "count": {"$sum": 1},
                "total_amount": {"$sum": "$amount"},
            }},
            {"$sort": {"count": -1}},
        ]
        return {doc["_id"]: doc for doc in self._db["transactions"].aggregate(pipeline)}

    def fetch_location_stats(self, limit: int = 15) -> list[dict]:
        pipeline = [
            {"$group": {
                "_id": {"city": "$location_city", "country": "$location_country"},
                "count": {"$sum": 1},
                "total": {"$sum": "$amount"},
            }},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
        return [
            {
                "city": d["_id"]["city"],
                "country": d["_id"]["country"],
                "count": d["count"],
                "total": d["total"],
            }
            for d in self._db["transactions"].aggregate(pipeline)
        ]
