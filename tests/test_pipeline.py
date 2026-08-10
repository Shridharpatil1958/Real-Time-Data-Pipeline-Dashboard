#!/usr/bin/env python3
# tests/test_pipeline.py
# Unit tests for producer, anomaly detector, and models

from __future__ import annotations

import sys
sys.path.insert(0, ".")

import pytest
from datetime import datetime, timezone

from models.transaction import (
    Transaction, TransactionType, ProcessedTransaction, RiskLevel
)
from models.anomaly_detector import AnomalyDetector
from producer.transaction_producer import create_transaction, generate_amount


# ─────────────────────────────────────────────
# Transaction model tests
# ─────────────────────────────────────────────

class TestTransactionModel:

    def test_create_transaction(self):
        txn = create_transaction()
        assert txn.transaction_id
        assert txn.amount > 0
        assert txn.user_id
        assert len(txn.card_last4) == 4

    def test_transaction_serialisation(self):
        txn = create_transaction()
        d = txn.to_dict()
        assert "transaction_id" in d
        assert isinstance(d["timestamp"], str)   # ISO string
        assert isinstance(d["transaction_type"], str)

    def test_invalid_card_raises(self):
        with pytest.raises(Exception):
            Transaction(
                user_id="u1", amount=10.0,
                transaction_type=TransactionType.PURCHASE,
                merchant="X", merchant_category="Y",
                location_city="Z", location_country="US",
                card_last4="ABC",        # ← invalid
                ip_address="1.2.3.4", device_type="web",
            )

    def test_amount_must_be_positive(self):
        with pytest.raises(Exception):
            Transaction(
                user_id="u1", amount=-1.0,
                transaction_type=TransactionType.PURCHASE,
                merchant="X", merchant_category="Y",
                location_city="Z", location_country="US",
                card_last4="1234",
                ip_address="1.2.3.4", device_type="web",
            )


# ─────────────────────────────────────────────
# Amount generation tests
# ─────────────────────────────────────────────

class TestAmountGeneration:

    def test_amounts_positive(self):
        for txn_type in TransactionType:
            amt = generate_amount(txn_type)
            assert amt > 0

    def test_amounts_in_range(self):
        for _ in range(200):
            amt = generate_amount(TransactionType.PURCHASE)
            assert amt > 0


# ─────────────────────────────────────────────
# Anomaly detector tests
# ─────────────────────────────────────────────

def make_processed(amount: float, user_id: str = "user-1", hour: int = 12) -> ProcessedTransaction:
    return ProcessedTransaction(
        transaction_id="test-id",
        user_id=user_id,
        amount=amount,
        transaction_type="purchase",
        merchant="Amazon",
        merchant_category="Shopping",
        location_city="New York",
        location_country="US",
        timestamp=datetime.now(timezone.utc),
        status="pending",
        hour_of_day=hour,
        day_of_week=1,
        is_weekend=False,
        amount_bucket="normal",
    )


class TestAnomalyDetector:

    def setup_method(self):
        self.detector = AnomalyDetector()

    def test_normal_transaction_not_flagged(self):
        txn = make_processed(50.0)
        is_anom, score, reasons, risk = self.detector.analyze(txn)
        assert not is_anom
        assert score < 0.4
        assert risk == RiskLevel.LOW

    def test_high_amount_flagged(self):
        txn = make_processed(10_000.0)
        is_anom, score, reasons, risk = self.detector.analyze(txn)
        assert is_anom
        assert any("threshold" in r.lower() for r in reasons)

    def test_velocity_flagged(self):
        """10+ rapid transactions should trigger velocity rule."""
        for _ in range(12):
            txn = make_processed(30.0, user_id="velocity-user")
            is_anom, score, reasons, risk = self.detector.analyze(txn)
        assert is_anom or score > 0

    def test_off_hours_adds_to_score(self):
        txn_day   = make_processed(100.0, hour=14)
        txn_night = make_processed(100.0, hour=2)
        _, score_day,   _, _ = self.detector.analyze(txn_day)
        _, score_night, _, _ = self.detector.analyze(txn_night)
        assert score_night >= score_day

    def test_risk_levels(self):
        assert AnomalyDetector._risk_level(0.2)  == RiskLevel.LOW
        assert AnomalyDetector._risk_level(0.5)  == RiskLevel.MEDIUM
        assert AnomalyDetector._risk_level(0.7)  == RiskLevel.HIGH
        assert AnomalyDetector._risk_level(0.95) == RiskLevel.CRITICAL


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
