#!/usr/bin/env python3
# database/redis_cache.py
# Fast in-memory metrics cache using Redis

from __future__ import annotations

import json
import logging
import time

import redis

import sys
sys.path.insert(0, ".")
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_DB
from models.transaction import ProcessedTransaction

log = logging.getLogger("redis_cache")

METRICS_KEY  = "pipeline:metrics"
TXN_LIST_KEY = "pipeline:recent_txns"
MAX_LIST_LEN = 500


class RedisCache:
    def __init__(self):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            decode_responses=True, socket_connect_timeout=5,
        )
        try:
            self._r.ping()
            log.info("Redis connected ✓")
        except Exception as exc:
            log.warning("Redis unavailable (%s) — metrics cache disabled", exc)
            self._r = None

    # ─────────────────────────────────────────
    # Update live metrics
    # ─────────────────────────────────────────

    def update_metrics(self, txn: ProcessedTransaction):
        if not self._r:
            return
        pipe = self._r.pipeline()
        pipe.incrbyfloat(f"{METRICS_KEY}:total_amount", txn.amount)
        pipe.incr(f"{METRICS_KEY}:total_txns")
        if txn.is_anomaly:
            pipe.incr(f"{METRICS_KEY}:anomaly_count")
        pipe.incr(f"{METRICS_KEY}:type:{txn.transaction_type}")
        pipe.incr(f"{METRICS_KEY}:category:{txn.merchant_category}")
        pipe.incr(f"{METRICS_KEY}:risk:{txn.risk_level.value}")

        # Push to recent list (capped)
        summary = json.dumps({
            "id":       txn.transaction_id[:8],
            "user":     txn.user_id[:8],
            "amount":   txn.amount,
            "type":     txn.transaction_type,
            "merchant": txn.merchant,
            "anomaly":  txn.is_anomaly,
            "risk":     txn.risk_level.value,
            "ts":       txn.timestamp.isoformat(),
        })
        pipe.lpush(TXN_LIST_KEY, summary)
        pipe.ltrim(TXN_LIST_KEY, 0, MAX_LIST_LEN - 1)
        pipe.execute()

    # ─────────────────────────────────────────
    # Read live snapshot
    # ─────────────────────────────────────────

    def get_snapshot(self) -> dict:
        if not self._r:
            return {}
        keys = self._r.keys(f"{METRICS_KEY}:*")
        snapshot = {}
        for key in keys:
            short = key.replace(f"{METRICS_KEY}:", "")
            try:
                snapshot[short] = float(self._r.get(key) or 0)
            except Exception:
                pass
        return snapshot

    def get_recent_transactions(self, limit: int = 50) -> list[dict]:
        if not self._r:
            return []
        raw = self._r.lrange(TXN_LIST_KEY, 0, limit - 1)
        result = []
        for item in raw:
            try:
                result.append(json.loads(item))
            except Exception:
                pass
        return result

    def reset(self):
        """Wipe all pipeline metrics (for testing)."""
        if self._r:
            for key in self._r.keys(f"{METRICS_KEY}:*"):
                self._r.delete(key)
            self._r.delete(TXN_LIST_KEY)
