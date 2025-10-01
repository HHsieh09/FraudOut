# FraudOut

Credit card fraud detection sandbox you can run on a laptop. Events are simulated, streamed through Kafka, processed by Spark Structured Streaming, stored on HDFS, and served to SQL tools via Spark and Hive.

![Status](https://img.shields.io/badge/status-active-brightgreen) ![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20Spark%20%7C%20HDFS%20%7C%20Hive-blue) ![Made with](https://img.shields.io/badge/made%20with-Python%20%7C%20Docker-informational)

---

## Table of Contents
- [Project Goals](#project-goals)
- [Architecture](#architecture)
- [Core Features](#core-features)
- [Tech Stack](#tech-stack)
- [Repository Layout](#repository-layout)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Run the Simulators](#run-the-simulators)
- [Run Streaming Jobs](#run-streaming-jobs)
- [Create Hive Tables](#create-hive-tables)
- [Verify End to End](#verify-end-to-end)
- [Data Model](#data-model)
- [How Fraud Detection Works](#how-fraud-detection-works)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Project Goals
- Learn and demonstrate a modern streaming big data stack end to end
- Start from zero and document the journey. Treat this repo as a learning lab
- Provide a small but realistic pipeline for teaching, demos, and experiments

Outputs
- A reproducible data platform via Docker Compose
- Kafka to Spark Structured Streaming to HDFS, then Hive for SQL and BI
- External Hive tables with partitioning and Parquet storage

---

## Architecture
```
[Simulators] → [Kafka] → [Spark Structured Streaming] → [HDFS]
                                              ↘
                                               [Hive Metastore + HiveServer2] → [BI/SQL]
```
Services
- ZooKeeper: Kafka coordination
- Kafka: high throughput event bus
- HDFS NameNode/DataNode: storage for raw, curated, checkpoints
- Spark: streaming ETL and batch SQL
- Hive Metastore + Postgres: schema catalog
- HiveServer2: JDBC/ODBC endpoint

---

## Core Features
- Transaction simulation with realistic fields from Marqeta API and ground truth label
- Exactly-once style sinks for raw and curated layers (via checkpoints)
- Windowed feature aggregation per entity
- Rule based scoring with tunable threshold
- Lightweight data quality checks and streaming metrics

---

## Tech Stack
- **Runtime**: Docker, Docker Compose
- **Messaging**: Apache Kafka
- **Processing**: Apache Spark Structured Streaming
- **Storage**: HDFS, Parquet
- **SQL**: Spark SQL, Apache Hive (Metastore + HiveServer2)
- **Language**: Python

---

## Repository Layout
```
FraudOut/
├── README.md
├── dashboards
├── docker
│   ├── Dockerfile
│   └── docker-compose.yml # All infra services
├── docs
│   ├── architecture.md
│   └── hdfs_plan.md
├── dq
├── hadoop
│   └── hive
│       └── ddl.sql # External tables (Parquet), partitioned by dt
├── hadoop-conf
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── hive-site.xml
├── ingest
│   └── marqeta_simulator.py # Event simulator and producer
├── metastore_db
├── poetry.lock
├── pyproject.toml
├── r_analysis
├── serving
├── spark
│   ├── fraudout_spark_sql.py # SQL metrics and analysis
│   └── spark_to_hdfs.py # Streaming ETL job (Kafka → HDFS/Hive)
├── streaming
└── tnx_raw
    └── simulator_output.json # Sample output from Marqeta API
```

---

## Quickstart
1) Create docker network
```bash
docker network create fraudout_network
```

2) Bring up core services
```bash
docker compose up -d namenode datanode zookeeper kafka spark metastore-db hive-metastore hive-server2
```
Restart the Kafka container if shutdown

3) Prepare HDFS folders
```bash
docker exec -it namenode bash -lc '\
  hdfs dfs -mkdir -p /user/spark/fraudout/{raw,curated,checkpoints} && \
  hdfs dfs -chmod -R 775 /user/spark'
```

4) Copy Hadoop configs into Spark
```bash
mkdir -p ./hadoop-conf
# Pull from namenode
docker cp namenode:/opt/hadoop-3.2.1/etc/hadoop/core-site.xml ./hadoop-conf/
docker cp namenode:/opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml ./hadoop-conf/
# Push into spark
docker cp ./hadoop-conf/core-site.xml spark:/opt/bitnami/spark/conf/
docker cp ./hadoop-conf/hdfs-site.xml spark:/opt/bitnami/spark/conf/
```

5) Create Kafka topic
```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --replication-factor 1 --partitions 3 \
  --topic fraudout.txn.raw
```

6) Spark Streaming to HDFS
```bash
/opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  /opt/bitnami/spark/jobs/spark_to_hdfs.py
```

7) Create Hive external tables using hadoop/hive/ddl.sql
Run in spark-sql or beeline
```bash
CREATE DATABASE IF NOT EXISTS fraudout;
USE fraudout;

# Example: For the raw (JSON) data
CREATE EXTERNAL TABLE IF NOT EXISTS raw_tx (
  event_key string,
  token string,
  amount double,
  kafka_ts timestamp
)
PARTITIONED BY (yyyy string, mm string, dd string, hour string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'hdfs://namenode:8020/user/spark/fraudout/raw';

# Example: Scan partitions (MSCK REPAIR must be run for every table after new data is cleansed and written by Spark)
MSCK REPAIR TABLE raw_tx;
```

8) Align Spark and Hive Metastore configuration
```bash
# Copy hive-site.xml into the Spark container (Spark reads these settings for Hive connectivity)
docker cp ./hadoop-conf/hive-site.xml spark:/opt/bitnami/spark/conf/
```

9) Start Metastore service
```bash
# Enter the hive-metastore container
docker exec -it hive-metastore bash

# Start the Metastore service
/opt/hive/bin/hive --service metastore -p 9083
```

10) Send simulation data via simulator docker containers
```bash
# Start Simulators (Adjust the quantity based on your load requirement)
docker compose up simulator-1 simulator-2 simulator-3 simulator-4 simulator-5 simulator-6 simulator-7 simulator-8 simulator-9 simulator-10
```

---

## Configuration
Connectivity rule of thumb
- Containers talk to Kafka using `kafka:9092`
- Host tools talk to Kafka using `localhost:9094` (advertised listener)

Common env for the simulator
- `KAFKA_BOOTSTRAP_SERVERS`: `localhost:9094` when running on host
- `KAFKA_TOPIC`: `fraudout.txn.raw`
- `SIM_INTERVAL_SEC`: average send interval
- `PRODUCER_ID`: identifies the simulator instance

---

## Run the Simulators
From project root
```bash
poetry run python ingest/marqeta_simulator.py \
  --bootstrap-server localhost:9094 \
  --topic fraudout.txn.raw
```
Scale by launching multiple shells with different `--producer-id` if desired.

---

## Run Streaming Jobs
Submit the Spark job inside the Spark container
```bash
docker exec -it spark bash -lc '\
  /opt/bitnami/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /opt/bitnami/spark/jobs/spark_to_hdfs.py'
```
This writes to HDFS under `/user/spark/fraudout/{raw,curated,checkpoints}`.

---

## Create Hive Tables
Connect via Beeline or Spark SQL and run the DDL
```sql
CREATE DATABASE IF NOT EXISTS fraudout;
USE fraudout;
-- See hadoop/hive/ddl.sql for full definitions
-- After new data arrives
MSCK REPAIR TABLE tx_curated;
MSCK REPAIR TABLE tx_scores;
```

---

## Verify End to End
```sql
USE fraudout;
SELECT * FROM tx_curated WHERE dt = current_date LIMIT 10;
SELECT decision, count(*) FROM tx_scores WHERE dt = current_date GROUP BY decision;
```
If you see rows and a mix of OK and REVIEW decisions, the pipeline is flowing.

---

## Data Model
All tables are external and partitioned by `dt` (YYYY-MM-DD)

| Table            | Purpose                                        | Key columns (subset) |
|------------------|------------------------------------------------|----------------------|
| raw_data         | Original Kafka JSON for replay and audit       | event_json, event_key, kafka_ts, ingestion_ts, dt |
| tx_curated       | Flattened transaction facts in Parquet         | type, token, amount, network, user_transaction_time, user_token, card_token, mid, mcc, country, device_id, synthetic_card_id, lat, lon, dt |
| tx_features      | 5-min window features per entity               | entity_id, window_start, window_end, txn_count_5min, txn_amount_5min, dt |
| tx_scores        | Rule based scores and decisions                | event_key, token, amount, truth_fraud, truth_score_prob, predicted_fraud, decision, dt |
| dq_results       | Batch level data quality checks                | token_not_null, amount_positive, dq_run_ts, dt |
| stream_metrics   | Streaming throughput and latency snapshots     | window_start, window_end, rows, avg_latency_sec, dt |

Entity id for features is `coalesce(synthetic_card_id, card_token, device_id)`.

---

## How Fraud Detection Works
Simulation
- `marqeta_simulator.py` generates realistic transactions and a ground truth label
- Base fraud probability is boosted by risk factors such as high risk MCC, cross border, night hours in UTC, rapid bursts within 5 minutes, and large amount, then clipped at 0.95

Streaming
- `spark_to_hdfs.py` consumes Kafka, writes raw JSON to `raw_data` and curated Parquet to `tx_curated`
- Features are aggregated over 5 minute windows with a 10 minute watermark per entity
- Scores are produced by comparing `truth_score_prob` with `THRESHOLD` (default 0.5). If above threshold → `predicted_fraud=1` and `decision='REVIEW'`, else `OK`

Analysis
- `fraudout_spark_sql.py` joins scores with curated facts to compute confusion matrix, daily fraud rate, and risk by MCC, amount band, and country

---

## Troubleshooting
- **NoBrokersAvailable**: host tools must use `localhost:9094`. Containers use `kafka:9092`. Check advertised listeners.
- **No new data in Spark**: clear checkpoints
```bash
docker exec -it namenode bash -lc 'hdfs dfs -rm -r -skipTrash /user/spark/fraudout/checkpoints'
```
- **Hive cannot see new partitions**: run `MSCK REPAIR TABLE ...`
- **SerDe errors**: everything writes Parquet to avoid JSON SerDe issues

---

## Roadmap
- Add reason_codes to tx_scores for explainability
- Tune threshold from offline ROC and record model_version
- More features and anomaly rules using geolocation and device patterns
- Metrics to Grafana via Prometheus scraping

---

## Contributing
PRs are welcome. Please include a short description, steps to reproduce, and sample data if relevant.

---

## License
