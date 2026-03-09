# Configuration Reference

This document provides a reference for all configuration files and environment variables used in the Crypto Real-time Pipeline.

## 1. Configuration Files Overview

| Category | File | Location | Purpose |
| :--- | :--- | :--- | :--- |
| **Orchestration** | `docker-compose.yml` | Root | Infrastructure orchestration and service definitions |
| **App Config** | `config/*.py` | `config/` | Python-based configs for Kafka, Postgres, S3, Spark, dbt |
| **Pipeline** | `airflow.cfg` | `config/airflow.cfg` | Airflow core settings and overrides |
| **Transformation**| `dbt_project.yml` | `dbt/` | dbt project definition and model configurations |
| | `profiles.yml` or `profiles.yml.example` | `dbt/` | Database connection profiles for dbt |
| **Monitoring** | `prometheus.yml` | `monitoring/prometheus/` | Prometheus scraping jobs and targets |
| | `spark-jmx-config.yml` | `monitoring/spark_jmx/` | JMX Exporter agent configuration for Spark |
| | `alerting.yml` | `monitoring/grafana/provisioning/alerting/` | Grafana Alert rules (Infrastructure as Code) |
| | `datasources.yml` | `monitoring/grafana/provisioning/datasources/` | Grafana Datasource configurations (Infrastructure as Code) |
| | `dashboards.yml` | `monitoring/grafana/provisioning/dashboards/` | Grafana Dashboard configurations (Infrastructure as Code) |

### Environment & Orchestration Strategy

- **`docker-compose.yml`**: **local development**, utilizing the `.env` file for configuration injection (e.g., credentials, ports).
- **`.env.example`**: Template for environment variables.

> **Important Note on Configuration:**
The system relies on environment variables defined in `.env`. **MUST** configure this file (copied from `.env.example`) before starting the services to ensure all credentials and settings are correctly applied. The **Alerting System** (Grafana) also requires valid SMTP settings in this file to send email notifications.

```ini
# --- Grafana Configuration (MANDATORY for Alerting) ---
GF_SMTP_USER=<your-email@example.com>
GF_SMTP_PASSWORD=<your-app-password>
GF_SMTP_FROM_ADDRESS=<your-email@example.com>
```

> For more details, refer to the **Environment Configuration** section in the [Installation Guide](./installation.md).

## 2. Kafka Configuration

**File:** [`config/kafka.py`](../../config/kafka.py)

| Variable | Default Value | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address for internal communication |
| `KAFKA_TOPIC` | `crypto_trades` | Topic name for trade events |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Schema Registry endpoint |
| `SCHEMA_PATH` | `/opt/airflow/config/schemas/crypto_trades.avsc` | Path to Avro schema file |

**Schema Definition:**
The pipeline uses **Avro** for data serialization. The schema is defined in [`config/schemas/crypto_trades.avsc`](../../config/schemas/crypto_trades.avsc) and describes the structure of trade events (symbol, price, quantity, timestamp) ensuring type safety and evolution compatibility.

**External Ports:**
- Kafka: `9092` (internal), `29092` (external/localhost)
- Schema Registry: `8085` (mapped from internal 8081)
- Kafka UI: `8088`

## 3. PostgreSQL Configuration

**File:** [`config/postgres.py`](../../config/postgres.py)

| Variable | Default Value | Description |
| --- | --- | --- |
| `POSTGRES_URL` | `jdbc:postgresql://postgres:5432/airflow` | JDBC connection string for Spark |
| `POSTGRES_USER` | `airflow` | Database username |
| `POSTGRES_PASSWORD` | `airflow` | Database password |
| `POSTGRES_DRIVER` | `org.postgresql.Driver` | JDBC driver class |
| `POSTGRES_CONN_ID` | `POSTGRES_DW` | Airflow connection ID |

**External Port:** `5433` (mapped from internal 5432) (5433:5432). Avoid conflicts with local Postgres port (5432:5432).

**Database Schemas:**
- `raw`: Raw ingested data from Spark
- `bronze`: Staging layer (dbt)
- `silver`: Intermediate layer (dbt)
- `gold`: Mart layer (dbt)

## 4. MinIO/S3 Configuration

**File:** [`config/s3.py`](../../config/s3.py)

| Variable | Default Value | Description |
| --- | --- | --- |
| `S3_ENDPOINT` | `http://minio:9000` | MinIO S3 API endpoint |
| `S3_ACCESS_KEY` | `minioadmin` | Access key (matches MINIO_ROOT_USER) |
| `S3_SECRET_KEY` | `minioadmin123` | Secret key (matches MINIO_ROOT_PASSWORD) |
| `S3_BUCKET_NAME` | `crypto-data` | Default bucket for data storage |
| `S3_CHECKPOINT_DIR` | `s3a://crypto-data/checkpoints` | Spark checkpoint location |
| `AWS_CONN_ID` | `MINIO_S3` | Airflow connection ID |

**External Ports:**
- S3 API: `9000`
- Web Console: `9001`

## 5. Spark Configuration

**File:** `config/spark.py`

| Variable | Default Value | Description |
| --- | --- | --- |
| `SPARK_HOME` | `/opt/spark` | Spark installation directory |
| `SPARK_SUBMIT` | `/opt/spark/bin/spark-submit` | Path to spark-submit binary |
| `SPARK_MASTER` | `spark://spark-master:7077` | Spark master URL |

**Required JARs:**
- `spark-sql-kafka-0-10_2.12-3.5.0.jar`
- `kafka-clients-3.4.1.jar`
- `spark-avro_2.12-3.5.0.jar`
- `postgresql-42.6.0.jar`
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

**Spark UI Port:** `8090`

## 6. Airflow Configuration

**Environment Variables in docker-compose.yml:**

| Variable | Value | Description |
| --- | --- | --- |
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Execution mode |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}/${POSTGRES_DB}` | Metadata database |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Disable example DAGs |

**Airflow Connections (Auto-configured):**
- `SPARK_DEFAULT`: connection for SparkSubmitOperator
- `POSTGRES_DW`: PostgreSQL data warehouse connection
- `MINIO_S3`: MinIO S3 connection

**Web UI Port:** `8080`

## 7. dbt Configuration

**Project Config:** [`dbt/dbt_project.yml`](../../dbt/dbt_project.yml)

```yaml
name: 'crypto_pipeline'
version: '1.0.0'
profile: 'crypto_pipeline'

models:
  crypto_pipeline:
    staging:
      +materialized: view
      +schema: bronze
    intermediate:
      +materialized: table
      +schema: silver
    marts:
      +materialized: table
      +schema: gold
```

**Profile Config:** `dbt/profiles.yml` or [`dbt/profiles.yml.example`](../../dbt/profiles.yml.example)


The profile uses environment variables passed via docker-compose:
`DBT_HOST`, `DBT_USER`, `DBT_PASS`, `DBT_DB`, `DBT_PORT`.

## 8. Monitoring Configuration

The monitoring stack is fully containerized and pre-provisioned.

**Location:** `monitoring/`

### 8.1. Prometheus
**Config File:** `monitoring/prometheus/prometheus.yml`  
**Port:** `9090` (Web UI)

**Scrape Targets:**
| Job Name | Address | Description |
| :--- | :--- | :--- |
| `prometheus` | `localhost:9090` | Self-monitoring |
| `kafka-exporter` | `kafka-exporter:9308` | Kafka consumer/topic metrics |
| `spark-driver-streaming` | `airflow-scheduler:9101` | Spark streaming driver metrics (JMX) |
| `spark-executor-streaming`| `spark-worker-1:9102` | Spark worker/executor metrics (JMX) |

### 8.2. Grafana
**Port:** `3000`  
**Provisioning location:** `monitoring/grafana/provisioning/`

**Datasources Config:** [`monitoring/grafana/provisioning/datasources/datasource.yml`](../../monitoring/grafana/provisioning/datasources/datasource.yml)
- **Prometheus:** `http://prometheus:9090`
- **PostgreSQL:** `postgres:5432`

**Dashboards Config:** [`monitoring/grafana/provisioning/dashboards/dashboard.yml`](../../monitoring/grafana/provisioning/dashboards/dashboard.yml)
- Configures a Dashboard Provider to load files from `/var/lib/grafana/dashboards`.
- The Prometheus JMX Java Agent is attached to Spark Driver and Executor processes.
- It translates generic JMX MBeans (Heap, GC, Threads) into Prometheus metrics exposed at ports 9101/9102.

## 9. Resource Limits

Recommended Docker resource allocation:

| Service | Memory | CPU |
| --- | --- | --- |
| Spark Master | 256MB | - |
| Spark Worker | 800MB | 2 cores |
| Kafka | 400MB | - |
| Zookeeper | 256MB | - |
| PostgreSQL | Default | - |
| Airflow (each) | Default | - |

**Minimum Total:** 8GB RAM recommended for smooth operation.
