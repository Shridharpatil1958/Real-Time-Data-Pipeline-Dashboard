# config/settings.py
# Central configuration for the Real-Time Data Pipeline

import os
from dataclasses import dataclass, field
from typing import List

# ─────────────────────────────────────────────
# Kafka Configuration
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC_TRANSACTIONS: str = "transactions"
KAFKA_TOPIC_ALERTS: str = "transaction_alerts"
KAFKA_TOPIC_PROCESSED: str = "processed_transactions"
KAFKA_GROUP_ID: str = "pipeline_consumer_group"
KAFKA_AUTO_OFFSET_RESET: str = "latest"

# ─────────────────────────────────────────────
# MySQL Configuration
# ─────────────────────────────────────────────
MYSQL_HOST: str = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB: str = os.getenv("MYSQL_DB", "pipeline_db")
MYSQL_USER: str = os.getenv("MYSQL_USER", "pipeline_user")
MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "pipeline_pass")
MYSQL_URI: str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# ─────────────────────────────────────────────
# MongoDB Configuration
# ─────────────────────────────────────────────
MONGO_HOST: str = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT: int = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB: str = os.getenv("MONGO_DB", "pipeline_db")
MONGO_USER: str = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD: str = os.getenv("MONGO_PASSWORD", "adminpass")
MONGO_URI: str = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

# ─────────────────────────────────────────────
# Redis Configuration
# ─────────────────────────────────────────────
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = 0

# ─────────────────────────────────────────────
# Spark Configuration
# ─────────────────────────────────────────────
SPARK_APP_NAME: str = "RealTimePipeline"
SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
SPARK_KAFKA_PACKAGE: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
SPARK_MYSQL_PACKAGE: str = "com.mysql:mysql-connector-j:8.0.33"

# ─────────────────────────────────────────────
# Producer Configuration
# ─────────────────────────────────────────────
PRODUCER_INTERVAL_MS: int = int(os.getenv("PRODUCER_INTERVAL_MS", "500"))   # ms between events
PRODUCER_BATCH_SIZE: int = 100

# ─────────────────────────────────────────────
# Anomaly Detection Thresholds
# ─────────────────────────────────────────────
ANOMALY_AMOUNT_THRESHOLD: float = 5000.0       # Flag transactions above this
ANOMALY_VELOCITY_WINDOW_SEC: int = 60           # Window for velocity check
ANOMALY_VELOCITY_MAX_TXNS: int = 10             # Max txns per user in window
ANOMALY_ZSCORE_THRESHOLD: float = 3.0           # Z-score for statistical outlier

# ─────────────────────────────────────────────
# Dashboard Configuration
# ─────────────────────────────────────────────
DASHBOARD_REFRESH_INTERVAL: int = 2             # seconds
DASHBOARD_MAX_CHART_POINTS: int = 100
DASHBOARD_PORT: int = 8501
