# Airflow Workflows Reference

> **Architecture context:** See the [Data Flow & Architecture Deep Dive](./data_flow.md) for how Airflow orchestrates the overall pipeline.

This guide provides an overview of the Airflow Directed Acyclic Graphs (DAGs) in this project. Each DAG serves a specific role in the data platform lifecycle, from infrastructure initialization to daily reporting exports.

## 1. Operations & Infrastructure (Ops)

These DAGs are typically triggered manually during system setup or maintenance.

### `ops_check_connectivity`
*   **Type:** Diagnostics
*   **Schedule:** Manual (`None`)
*   **Purpose:**
    *   Verifies connections to critical services: **PostgreSQL**, **Kafka**, **MinIO** and **Spark**.
    *   Run this first if you suspect network or container issues.
*   **Key Tasks:** `ping_postgres`, `ping_kafka`, `ping_minio`, `ping_spark`.
    *   PostgreSQL (via `PostgresHook`)
    *   MinIO/S3 (via `S3Hook`)
    *   Spark (via `SparkSubmitOperator`)
    *   Kafka (via `KafkaAdminClient`)

### `ops_init_infrastructure`
*   **Type:** Setup / Initialization
*   **Schedule:** Manual (`None`)
*   **Purpose:**
    *   Prepares all necessary resources before the streaming pipeline starts.
    *   This DAG is **idempotent** (can be run multiple times safely).
*   **Key Tasks:**
    *   **Create PostgreSQL raw schema** — Executes [`producers/sql/ddl_raw.sql`](../../producers/sql/ddl_raw.sql) via `SQLExecuteQueryOperator`
    *   **Create Kafka topic** — `crypto_trades` (2 partitions, replication factor 1)
    *   **Create MinIO bucket** — `crypto-data`

---

## 2. Real-Time Pipeline (Streaming)

This is the core "always-on" engine of the project.

### `crypto_trades_realtime`
*   **Type:** Streaming / Ingestion
*   **Schedule:** Manual / Continuous
*   **Purpose:**
    *   Orchestrates the real-time data flow.
    *   Starts the Python [Producer] to fetch data from Binance.
    *   Submits the [Spark Structured Streaming] job to process Kafka data and write to Postgres.
*   **Key Tasks:**
    *   **Pre-checks:** Validates Kafka topic and MinIO bucket exist before starting
    *   **`produce_to_kafka`:** Runs the Python producer as a `PythonOperator` (long-running)
    *   **`ingest_postgres`:** Submits the Spark Structured Streaming job via `SparkSubmitOperator`

*   **Note:** If this DAG fails, real-time data ingestion stops.

---

## 3. Data Transformation (Batched)

### `dbt_crypto_transform`
*   **Type:** ELT / Transformation
*   **Schedule:** Scheduled (`1 minute`)
*   **Purpose:**
    *   Triggers **dbt** to transform raw data into analytical models (Silver/Gold layers).
    *   Runs `dbt run` and `dbt test` to ensure data quality.
*   Runs all dbt models via astronomer-cosmos **DbtTaskGroup**:
    *   Auto-generates Airflow tasks from dbt's DAG
    *   `install_deps: True` — automatically installs dbt packages (e.g., `dbt_utils`)
    *   `full_refresh: False` — uses incremental strategy
    *   Tests run after each model (`TestBehavior.AFTER_EACH`)

---

## 4. Data Lake Export & Dev Tools

### `etl_export_gold_to_datalake`
*   **Type:** Archival / Backup
*   **Schedule:** `@daily`
*   **Purpose:**
    *   Extracts "Gold" layer data from Postgres.
    *   **Snapshot Strategy:** Uses Airflow macros (`{{ ds }}`) for daily partitioning → `s3://crypto-data/dbt/dt=YYYY-MM-DD/gold/{table}.csv`
    *   **Incremental Filter:** If a `trade_date` column exists, only exports rows matching the execution date. Otherwise falls back to full snapshot.

### `dev_fetch_datalake_sample`
*   **Type:** Developer Tool
*   **Schedule:** Manual (`None`)
*   **Purpose:**
    *   Helper tool for developers to download a specific dataset from MinIO to the local machine for inspection (e.g., check `data/datalake_sample` folder).
