#!/usr/bin/env python3
# models/anomaly_detector.py
# Rule-based + statistical anomaly detection for transaction streams

from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from typing import Dict, List, Tuple

from models.transaction import ProcessedTransaction, RiskLevel, TransactionAlert
from config.settings import (
    ANOMALY_AMOUNT_THRESHOLD,
    ANOMALY_VELOCITY_WINDOW_SEC,
    ANOMALY_VELOCITY_MAX_TXNS,
    ANOMALY_ZSCORE_THRESHOLD,
)


class AnomalyDetector:
    """
    Stateful anomaly detector.  Maintains rolling windows per user
    to detect high-amount, high-velocity, and statistical outliers.
    """

    def __init__(self):
        # Per-user sliding window of (timestamp, amount) tuples
        self._user_windows:    Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        # Global rolling stats for z-score computation
        self._global_amounts:  deque = deque(maxlen=2000)
        self._global_mean:     float = 0.0
        self._global_std:      float = 1.0
        self._update_count:    int   = 0

    # ─────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────

    def analyze(self, txn: ProcessedTransaction) -> Tuple[bool, float, List[str], RiskLevel]:
        """
        Returns (is_anomaly, score 0-1, reasons, risk_level).
        Side-effect: updates internal state.
        """
        reasons: List[str] = []
        score = 0.0
        now = time.time()

        # 1. High-amount rule
        if txn.amount >= ANOMALY_AMOUNT_THRESHOLD:
            reasons.append(f"Amount ${txn.amount:,.2f} exceeds threshold ${ANOMALY_AMOUNT_THRESHOLD:,.0f}")
            score += 0.4

        # 2. Velocity check
        window = self._user_windows[txn.user_id]
        recent = [(ts, amt) for ts, amt in window if now - ts <= ANOMALY_VELOCITY_WINDOW_SEC]
        if len(recent) >= ANOMALY_VELOCITY_MAX_TXNS:
            reasons.append(
                f"Velocity: {len(recent)} transactions in last {ANOMALY_VELOCITY_WINDOW_SEC}s "
                f"(max {ANOMALY_VELOCITY_MAX_TXNS})"
            )
            score += 0.35

        # 3. Statistical outlier (z-score)
        z = self._zscore(txn.amount)
        if abs(z) >= ANOMALY_ZSCORE_THRESHOLD:
            reasons.append(f"Statistical outlier: z-score={z:.2f} (|z|≥{ANOMALY_ZSCORE_THRESHOLD})")
            score += 0.25

        # 4. Off-hours flag (00:00–05:00 local, crude approximation)
        if txn.hour_of_day < 5:
            reasons.append(f"Transaction at unusual hour: {txn.hour_of_day:02d}:xx")
            score += 0.10

        # 5. Large round-number amounts (common in fraud)
        if txn.amount >= 1000 and txn.amount % 500 == 0:
            reasons.append(f"Suspiciously round amount: ${txn.amount:,.0f}")
            score += 0.10

        # Clamp score
        score = min(score, 1.0)
        is_anomaly = score >= 0.4

        risk_level = self._risk_level(score)

        # Update state
        window.append((now, txn.amount))
        self._update_global_stats(txn.amount)

        return is_anomaly, round(score, 4), reasons, risk_level

    def create_alert(self, txn: ProcessedTransaction, reasons: List[str], risk_level: RiskLevel) -> TransactionAlert:
        return TransactionAlert(
            transaction_id=txn.transaction_id,
            user_id=txn.user_id,
            amount=txn.amount,
            risk_level=risk_level,
            reasons=reasons,
        )

    # ─────────────────────────────────────────
    # Internals
    # ─────────────────────────────────────────

    def _zscore(self, amount: float) -> float:
        if self._global_std == 0:
            return 0.0
        return (amount - self._global_mean) / self._global_std

    def _update_global_stats(self, amount: float):
        self._global_amounts.append(amount)
        self._update_count += 1
        # Recompute every 50 transactions (Welford-like approximation)
        if self._update_count % 50 == 0 and len(self._global_amounts) > 1:
            n = len(self._global_amounts)
            self._global_mean = sum(self._global_amounts) / n
            variance = sum((x - self._global_mean) ** 2 for x in self._global_amounts) / (n - 1)
            self._global_std = math.sqrt(variance) or 1.0

    @staticmethod
    def _risk_level(score: float) -> RiskLevel:
        if score < 0.4:
            return RiskLevel.LOW
        if score < 0.6:
            return RiskLevel.MEDIUM
        if score < 0.8:
            return RiskLevel.HIGH
        return RiskLevel.CRITICAL
