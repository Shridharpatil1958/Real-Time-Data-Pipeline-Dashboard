#!/usr/bin/env python3
# database/mysql_writer.py
# Writes processed transactions and metrics to MySQL

from __future__ import annotations

import logging
import time

import pymysql
from pymysql.cursors import DictCursor

import sys
sys.path.insert(0, ".")
from config.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD
from models.transaction import ProcessedTransaction

log = logging.getLogger("mysql_writer")

DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id   VARCHAR(36)    NOT NULL UNIQUE,
    user_id          VARCHAR(36)    NOT NULL,
    amount           DECIMAL(15,2)  NOT NULL,
    transaction_type VARCHAR(20)    NOT NULL,
    merchant         VARCHAR(100)   NOT NULL,
    merchant_category VARCHAR(50)   NOT NULL,
    location_city    VARCHAR(100)   NOT NULL,
    location_country CHAR(2)        NOT NULL,
    status           VARCHAR(20)    NOT NULL,
    is_anomaly       TINYINT(1)     NOT NULL DEFAULT 0,
    anomaly_score    FLOAT          NOT NULL DEFAULT 0,
    risk_level       VARCHAR(10)    NOT NULL DEFAULT 'low',
    amount_bucket    VARCHAR(10)    NOT NULL,
    hour_of_day      TINYINT        NOT NULL,
    day_of_week      TINYINT        NOT NULL,
    is_weekend       TINYINT(1)     NOT NULL,
    processing_ms    FLOAT          NOT NULL DEFAULT 0,
    created_at       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_user      (user_id),
    INDEX idx_created   (created_at),
    INDEX idx_anomaly   (is_anomaly),
    INDEX idx_risk      (risk_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hourly_metrics (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    bucket_hour      DATETIME       NOT NULL,
    total_txns       INT            NOT NULL DEFAULT 0,
    total_amount     DECIMAL(20,2)  NOT NULL DEFAULT 0,
    anomaly_count    INT            NOT NULL DEFAULT 0,
    avg_amount       DECIMAL(15,2)  NOT NULL DEFAULT 0,
    UNIQUE KEY uq_bucket (bucket_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

INSERT_TXN = """
INSERT IGNORE INTO transactions
  (transaction_id, user_id, amount, transaction_type, merchant, merchant_category,
   location_city, location_country, status, is_anomaly, anomaly_score, risk_level,
   amount_bucket, hour_of_day, day_of_week, is_weekend, processing_ms)
VALUES
  (%(transaction_id)s, %(user_id)s, %(amount)s, %(transaction_type)s, %(merchant)s,
   %(merchant_category)s, %(location_city)s, %(location_country)s, %(status)s,
   %(is_anomaly)s, %(anomaly_score)s, %(risk_level)s, %(amount_bucket)s,
   %(hour_of_day)s, %(day_of_week)s, %(is_weekend)s, %(processing_ms)s)
"""


class MySQLWriter:
    def __init__(self):
        self._conn = None
        self._connect()
        self._ensure_schema()

    def _connect(self, retries: int = 10):
        for attempt in range(1, retries + 1):
            try:
                self._conn = pymysql.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    db=MYSQL_DB,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    charset="utf8mb4",
                    autocommit=True,
                    cursorclass=DictCursor,
                    connect_timeout=10,
                )
                log.info("MySQL connected ✓")
                return
            except Exception as exc:
                log.warning("MySQL connect attempt %d/%d failed: %s", attempt, retries, exc)
                time.sleep(3)
        raise RuntimeError("Could not connect to MySQL")

    def _ensure_schema(self):
        with self._conn.cursor() as cur:
            for stmt in DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)

    def _reconnect_if_needed(self):
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            self._connect()

    def insert_transaction(self, txn: ProcessedTransaction):
        self._reconnect_if_needed()
        row = {
            "transaction_id":   txn.transaction_id,
            "user_id":          txn.user_id,
            "amount":           txn.amount,
            "transaction_type": txn.transaction_type,
            "merchant":         txn.merchant,
            "merchant_category": txn.merchant_category,
            "location_city":    txn.location_city,
            "location_country": txn.location_country,
            "status":           txn.status,
            "is_anomaly":       int(txn.is_anomaly),
            "anomaly_score":    txn.anomaly_score,
            "risk_level":       txn.risk_level.value,
            "amount_bucket":    txn.amount_bucket,
            "hour_of_day":      txn.hour_of_day,
            "day_of_week":      txn.day_of_week,
            "is_weekend":       int(txn.is_weekend),
            "processing_ms":    txn.processing_time_ms,
        }
        with self._conn.cursor() as cur:
            cur.execute(INSERT_TXN, row)

    def fetch_recent(self, limit: int = 200) -> list[dict]:
        self._reconnect_if_needed()
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM transactions ORDER BY created_at DESC LIMIT %s", (limit,)
            )
            return cur.fetchall()

    def fetch_metrics(self) -> dict:
        self._reconnect_if_needed()
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)              AS total_txns,
                    SUM(amount)           AS total_amount,
                    SUM(is_anomaly)       AS anomaly_count,
                    AVG(amount)           AS avg_amount,
                    MAX(amount)           AS max_amount
                FROM transactions
                WHERE created_at >= NOW() - INTERVAL 1 HOUR
            """)
            return cur.fetchone() or {}

    def fetch_category_breakdown(self) -> list[dict]:
        self._reconnect_if_needed()
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT merchant_category, COUNT(*) AS cnt, SUM(amount) AS total
                FROM transactions
                WHERE created_at >= NOW() - INTERVAL 1 HOUR
                GROUP BY merchant_category
                ORDER BY cnt DESC
                LIMIT 10
            """)
            return cur.fetchall()

    def fetch_timeseries(self, minutes: int = 30) -> list[dict]:
        self._reconnect_if_needed()
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT
                    DATE_FORMAT(created_at, '%%H:%%i') AS minute_bucket,
                    COUNT(*)                            AS txn_count,
                    SUM(is_anomaly)                     AS anomaly_count,
                    AVG(amount)                         AS avg_amount
                FROM transactions
                WHERE created_at >= NOW() - INTERVAL %s MINUTE
                GROUP BY minute_bucket
                ORDER BY minute_bucket
            """, (minutes,))
            return cur.fetchall()
