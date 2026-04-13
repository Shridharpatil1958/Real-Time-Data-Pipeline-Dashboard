# models/transaction.py
# Pydantic models and schema definitions for the pipeline

from __future__ import annotations

import uuid
import random
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────

class TransactionType(str, Enum):
    PURCHASE   = "purchase"
    REFUND     = "refund"
    TRANSFER   = "transfer"
    WITHDRAWAL = "withdrawal"
    DEPOSIT    = "deposit"


class TransactionStatus(str, Enum):
    PENDING   = "pending"
    COMPLETED = "completed"
    FAILED    = "failed"
    FLAGGED   = "flagged"


class RiskLevel(str, Enum):
    LOW    = "low"
    MEDIUM = "medium"
    HIGH   = "high"
    CRITICAL = "critical"


# ─────────────────────────────────────────────
# Core Transaction Model
# ─────────────────────────────────────────────

class Transaction(BaseModel):
    """Represents a single financial transaction flowing through the pipeline."""

    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id:        str = Field(..., description="Unique customer identifier")
    amount:         float = Field(..., gt=0, description="Transaction amount in USD")
    currency:       str = Field(default="USD")
    transaction_type: TransactionType
    merchant:       str
    merchant_category: str
    location_city:  str
    location_country: str
    card_last4:     str
    timestamp:      datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status:         TransactionStatus = TransactionStatus.PENDING
    ip_address:     str
    device_type:    str

    @field_validator("card_last4")
    @classmethod
    def validate_card(cls, v: str) -> str:
        if not v.isdigit() or len(v) != 4:
            raise ValueError("card_last4 must be exactly 4 digits")
        return v

    def to_dict(self) -> dict:
        return {
            **self.model_dump(),
            "timestamp": self.timestamp.isoformat(),
            "transaction_type": self.transaction_type.value,
            "status": self.status.value,
        }


# ─────────────────────────────────────────────
# Processed / Enriched Transaction
# ─────────────────────────────────────────────

class ProcessedTransaction(BaseModel):
    """Transaction enriched after Spark processing."""

    transaction_id:   str
    user_id:          str
    amount:           float
    transaction_type: str
    merchant:         str
    merchant_category: str
    location_city:    str
    location_country: str
    timestamp:        datetime
    status:           str

    # Enriched fields
    is_anomaly:       bool = False
    anomaly_score:    float = 0.0
    risk_level:       RiskLevel = RiskLevel.LOW
    anomaly_reasons:  list[str] = Field(default_factory=list)
    processing_time_ms: float = 0.0
    hour_of_day:      int = 0
    day_of_week:      int = 0
    is_weekend:       bool = False
    amount_bucket:    str = "normal"   # micro / small / normal / large / whale

    def to_dict(self) -> dict:
        return {
            **self.model_dump(),
            "timestamp": self.timestamp.isoformat(),
            "risk_level": self.risk_level.value,
        }


# ─────────────────────────────────────────────
# Alert Model
# ─────────────────────────────────────────────

class TransactionAlert(BaseModel):
    """Alert raised when an anomaly is detected."""

    alert_id:       str = Field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str
    user_id:        str
    amount:         float
    risk_level:     RiskLevel
    reasons:        list[str]
    timestamp:      datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolved:       bool = False

    def to_dict(self) -> dict:
        return {
            **self.model_dump(),
            "timestamp": self.timestamp.isoformat(),
            "risk_level": self.risk_level.value,
        }


# ─────────────────────────────────────────────
# Dashboard Metrics Snapshot
# ─────────────────────────────────────────────

class PipelineMetrics(BaseModel):
    """Aggregated metrics for the dashboard."""

    snapshot_at:            datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_transactions:     int = 0
    total_amount_usd:       float = 0.0
    anomaly_count:          int = 0
    anomaly_rate_pct:       float = 0.0
    avg_amount:             float = 0.0
    max_amount:             float = 0.0
    transactions_per_min:   float = 0.0
    top_merchant_categories: list[dict] = Field(default_factory=list)
    transactions_by_type:   dict[str, int] = Field(default_factory=dict)
    risk_distribution:      dict[str, int] = Field(default_factory=dict)
