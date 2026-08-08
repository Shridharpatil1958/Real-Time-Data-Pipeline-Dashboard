-- mysql-init/init.sql
-- Runs automatically when MySQL container starts

CREATE DATABASE IF NOT EXISTS pipeline_db;
USE pipeline_db;

GRANT ALL PRIVILEGES ON pipeline_db.* TO 'pipeline_user'@'%';
FLUSH PRIVILEGES;

-- Pre-create tables so consumer can start immediately
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

CREATE TABLE IF NOT EXISTS spark_window_metrics (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_start     DATETIME       NOT NULL,
    window_end       DATETIME       NOT NULL,
    merchant_category VARCHAR(50)   NOT NULL,
    txn_count        INT            NOT NULL DEFAULT 0,
    total_amount     DECIMAL(20,2)  NOT NULL DEFAULT 0,
    avg_amount       DECIMAL(15,2)  NOT NULL DEFAULT 0,
    max_amount       DECIMAL(15,2)  NOT NULL DEFAULT 0,
    anomaly_count    INT            NOT NULL DEFAULT 0,
    created_at       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_window (window_start),
    INDEX idx_cat    (merchant_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
