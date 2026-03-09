# Installation and Setup Guide

This guide provides step-by-step instructions for deploying the infrastructure locally using Docker and Docker Compose.

## 1. Prerequisites


* **Operating System:** Linux, Windows (via WSL2).
* **Docker Engine:** Version 20.10 or higher.
* **Docker Compose:** Version 2.0 or higher.
* **Git:** For cloning the repository.
* **Hardware Resources:**
    * **RAM:** Minimum 8GB (16GB recommended for smooth Spark & Airflow performance).
    * **CPU:** 4 vCPUs recommended.

## 2. Cloning the Repository

Open terminal and clone the project:

```bash
git clone https://github.com/MinhHuy1507/crypto-realtime-pipeline
cd crypto-realtime-pipeline
```

## 3. Environment Configuration

```bash
cp .env.example .env
```

**Steps for Gmail Users:**
1.  Go to **[Google Account Security](https://myaccount.google.com/security)**.
2.  Enable **2-Step Verification** if it isn't already.
3.  Search for **"App passwords"** in the settings (or find it under the 2-Step Verification section).
4.  Create a new App Password:
    *   **Select app:** Mail
    *   **Select device:** Other (Custom name) -> Enter "CryptoGrafana"
5.  Copy the generated 16-character code.

**Update your `.env` file:**
Fill in the Grafana section with your details:
```ini
# --- Grafana Configuration ---
GF_SMTP_USER=your-email@gmail.com
GF_SMTP_PASSWORD=xxxx xxxx xxxx xxxx  <-- Paste your 16-char App Password here
GF_SMTP_FROM_ADDRESS=your-email@gmail.com
```

## 4. Deployment Steps

### Step 1: Configure dbt
```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

### Step 2: Build and Start Services
```bash
docker-compose up -d --build
```

* **Note:** The first run may take **5-15 minutes**.

### Step 3: Verify Installation

```bash
docker ps
```

You should see the following containers in the `Up` or `Healthy` state:

* `airflow-apiserver` & `airflow-scheduler` & `airflow-dag-processor` & `airflow-triggerer`
* `spark-master` & `spark-worker-1`
* `kafka` & `zookeeper`
* `schema-registry`
* `kafka-ui`
* `postgres`
* `dbt_dev`
* `minio`
* `grafana`
* `prometheus`
* `kafka-exporter`

## 5. Post-Installation Setup

### Accessing the Interfaces

> **Note:** The credentials listed below are the defaults configured in [`.env.example`](../../.env.example).

| Service | URL | Credentials |
| --- | --- | --- |
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Spark Master | http://localhost:8090 | None |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Kafka UI | http://localhost:8088 | None |
| Schema Registry | http://localhost:8085 | None |
| Prometheus | http://localhost:9090 | None |


## 6. Stopping and Resetting

### To Stop Services

Stops containers but preserves data in volumes:

```bash
docker-compose stop
```

### To Reset Environment (Danger Zone)

Stops containers and **deletes all data** (Database, MinIO buckets, Kafka topics):

```bash
docker-compose down -v
```

