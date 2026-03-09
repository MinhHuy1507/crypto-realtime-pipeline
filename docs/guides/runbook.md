# Operations Runbook

This document outlines standard operating procedures (SOPs) for maintaining, monitoring, and troubleshooting this project. 

It includes instructions for starting/stopping the data pipeline, performing routine maintenance tasks, and resolving common issues.

> **Note:** Must finish setting up the system as the [Installation Guide](./installation.md) and ensure all configuration files are correctly applied before following these procedures.

## 1. Routine Operations

### 1.1. Starting the Pipeline (Cold Start)
The pipeline is orchestrated via Apache Airflow.

1.  **Infrastructure Up:** Ensure all containers are running (`docker-compose up -d`).
2.  **Access Airflow:** Go to [http://localhost:8080](http://localhost:8080).
3.  **Trigger DAGs in Order:**
    * **Step 1 (Check):** Trigger `ops_check_connectivity` to ensure Postgres, Kafka, and MinIO are reachable.
    * **Step 2 (Init):** Trigger `ops_init_infrastructure`. This creates the bucket, topic, and database schemas (IDEMPOTENT).
    * **Step 3 (Ingestion):** Unpause and trigger `crypto_trades_realtime`. This starts the Binance Producer and Spark Streaming job.
    * **Step 4 (Transform):** Unpause `dbt_crypto_transform` to start incremental transformations.
    * **Step 5 (Backup):** Ensure `etl_export_gold_to_datalake` is turned ON (Unpaused) for daily backups.


### 1.2. Stopping the Pipeline (Graceful Shutdown)
To prevent data corruption (especially in Spark Checkpoints):
1.  **Pause** all DAGs in the Airflow UI or via CLI:
    ```bash
    airflow dags pause crypto_trades_realtime
    airflow dags pause dbt_crypto_transform
    ```
2.  **Stop Containers:**
    ```bash
    docker-compose stop
    ```

## 2. Maintenance Tasks

### 2.1. Database Maintenance (Performance Tuning)
As the data tables grow, query performance in Grafana may degrade. Run these SQL commands periodically (e.g., weekly) inside the Postgres container:

```sql
-- Rebuild indexes to optimize scan speed
REINDEX TABLE gold.mart_crypto__technical_analysis;
REINDEX TABLE gold.mart_market__market_breadth;
REINDEX TABLE gold.mart_crypto__periodic_performance;

-- Update query planner statistics
VACUUM ANALYZE gold.mart_crypto__technical_analysis;
VACUUM ANALYZE raw.candles_log;

```

### 2.2. Resetting dbt Incremental State

If you modify the logic of a dbt model (e.g., changing how RSI is calculated), the existing incremental table might become inconsistent. You must force a full rebuild:

```bash
# Execute inside the airflow-worker or dbt container
dbt run --full-refresh

```

* **Warning:** This will drop the existing tables and recalculate everything from the raw data. It may take time depending on data volume.

## 3. Monitoring & Observability

If the system behaves unexpectedly, check these logs:

| Component | Access Method | What to check |
| --- | --- | --- |
| **Airflow** | UI -> Grid -> Click Task -> Logs | Scheduling delays, Python script errors. |
| **Spark Driver** | [http://localhost:8090](http://localhost:8090) -> Click Application or [http://localhost:4040](http://localhost:4040)| "Stuck" batches, high processing latency, Executor memory errors. |
| **Kafka Broker** | `docker logs kafka` or Kafka UI http://localhost:8088 | Topic creation errors, client disconnection warnings. |
| **Postgres** | `docker logs postgres` | "Connection pool exhausted", slow query warnings. |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) -> Targets | Check if all exporters are UP. |
