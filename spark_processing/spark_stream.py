#!/usr/bin/env python3
# spark_processing/spark_stream.py
# PySpark Structured Streaming job that reads from Kafka,
# performs windowed aggregations, and writes results to MySQL.

from __future__ import annotations

import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, BooleanType,
)

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_PROCESSED,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_KAFKA_PACKAGE,
    SPARK_MYSQL_PACKAGE,
    MYSQL_URI,
    MYSQL_USER,
    MYSQL_PASSWORD,
)

# ─────────────────────────────────────────────
# Processed transaction schema
# ─────────────────────────────────────────────

SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    False),
    StructField("user_id",           StringType(),    False),
    StructField("amount",            DoubleType(),    False),
    StructField("transaction_type",  StringType(),    False),
    StructField("merchant",          StringType(),    True),
    StructField("merchant_category", StringType(),    True),
    StructField("location_city",     StringType(),    True),
    StructField("location_country",  StringType(),    True),
    StructField("status",            StringType(),    True),
    StructField("is_anomaly",        BooleanType(),   True),
    StructField("anomaly_score",     DoubleType(),    True),
    StructField("risk_level",        StringType(),    True),
    StructField("amount_bucket",     StringType(),    True),
    StructField("hour_of_day",       StringType(),    True),
    StructField("timestamp",         TimestampType(), True),
])


def write_batch_to_mysql(df, epoch_id: int):
    """Foreachbatch sink — writes windowed stats to MySQL."""
    (
        df.write.format("jdbc")
        .option("url", MYSQL_URI.replace("pymysql", "mysql"))
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "spark_window_metrics")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .mode("append")
        .save()
    )


def main():
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            f"{SPARK_KAFKA_PACKAGE},{SPARK_MYSQL_PACKAGE}",
        )
        # Reduce log noise
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read from Kafka ──────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_PROCESSED)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload ───────────────────────────────────────────
    parsed = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "30 seconds")
    )

    # ── 3. Windowed aggregation (1-minute tumbling windows) ─────────────
    windowed = (
        parsed
        .groupBy(
            F.window("timestamp", "1 minute"),
            "merchant_category",
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "merchant_category",
            "txn_count",
            "total_amount",
            "avg_amount",
            "max_amount",
            "anomaly_count",
        )
    )

    # ── 4. Risk-level breakdown ─────────────────────────────────────────
    risk_stream = (
        parsed
        .groupBy(F.window("timestamp", "1 minute"), "risk_level")
        .agg(F.count("*").alias("count"), F.sum("amount").alias("volume"))
    )

    # ── 5. Write to console (dev) ───────────────────────────────────────
    console_query = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── 6. Write to MySQL via foreachBatch ──────────────────────────────
    # (Uncomment when MySQL is running)
    #
    # mysql_query = (
    #     windowed.writeStream
    #     .outputMode("update")
    #     .foreachBatch(write_batch_to_mysql)
    #     .trigger(processingTime="30 seconds")
    #     .start()
    # )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
