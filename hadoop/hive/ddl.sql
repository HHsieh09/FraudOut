-- Hive DDL for Fraud Detection Data Warehouse
CREATE DATABASE IF NOT EXISTS fraudout;
USE fraudout;

-- 1) Raw JSON data, partitioned by dt=YYYY-MM-DD
--    Note: event_json retains the full original text for traceability and reprocessing
CREATE EXTERNAL TABLE IF NOT EXISTS raw_data (
    event_key string,
    event_json string,
    kafka_ts string,
    ingestion_ts string
)
PARTITIONED BY (dt string)
-- ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' -- Using JSON SerDe
-- Because bitnami/spark:3 image does not include Hive's built-in JsonSerDe
-- ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' -- Using Hive's built-in JSON SerDe
-- Default is TEXTFILE, so the next line is optional
-- STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/raw';

-- 2) Curated transactions (curated Parquet), partitioned by dt=YYYY-MM-DD
--    Corresponding to the columns produced in Spark curated_df
CREATE EXTERNAL TABLE IF NOT EXISTS tx_curated (
    -- From raw_df
    event_key string,
    event_json string,
    kafka_ts timestamp,
    ingestion_ts timestamp,
    -- From txn schema
    type string,
    token string,
    amount double,
    network string,
    channel string,
    created_time string,
    user_transaction_time string,
    currency_code string,
    state string,
    user_token string,
    card_token string,
    -- New fields for fraud detection
    mid string,
    mcc string,
    -- Extracted event_metadata fields
    country string,
    device_id string,
    synthetic_card_id string,
    lat double,
    lon double
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/curated/tx_curated';

-- 3) Features table (features), partitioned by dt=YYYY-MM-DD
--    Note: Spark uses window(struct<start,end>) field; Hive can directly declare
CREATE EXTERNAL TABLE IF NOT EXISTS tx_features (
--    event_key string, -- Remove event_key; window field is more accurate as timestamp type
    entity_id string,
    `window` struct<start:string,end:string>,
    txn_count_5min bigint,
    txn_amount_5min double
    )
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/curated/tx_features';

-- 4) Rule-based scores, partitioned by dt=YYYY-MM-DD
CREATE EXTERNAL TABLE IF NOT EXISTS tx_scores (
    event_key string,
    token string,
    amount double,
    truth_fraud int,
    truth_score_prob double,
    predicted_fraud int,
    decision string
    )
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/curated/tx_scores';

-- 5) Data Quality (dq), partitioned by dt=YYYY-MM-DD
CREATE EXTERNAL TABLE IF NOT EXISTS dq_results (
    token_not_null boolean,
    amount_positive boolean,
    dq_run_ts timestamp
    )
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/curated/dq_results';

-- 6) Spark Processing Metrics, partitioned by dt=YYYY-MM-DD
CREATE EXTERNAL TABLE IF NOT EXISTS stream_metrics (
  window_start timestamp,
  window_end   timestamp,
  rows         bigint,
  avg_latency_sec double
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/metrics';

-- Scan for partitions
-- MSCK = Metastore Check
-- REPAIR TABLE adds any partitions found on HDFS that are not in the Hive metastore
MSCK REPAIR TABLE raw_data;
MSCK REPAIR TABLE tx_curated;
MSCK REPAIR TABLE tx_features;
MSCK REPAIR TABLE tx_scores;
MSCK REPAIR TABLE dq_results;
MSCK REPAIR TABLE stream_metrics;