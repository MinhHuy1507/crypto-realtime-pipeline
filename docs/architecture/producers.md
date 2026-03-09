# Producers Documentation

> **Architecture context:** See the [Data Flow & Architecture Deep Dive](./data_flow.md) for how the producer fits into the overall pipeline.

## Overview

The **Producers** component ingests real-time cryptocurrency trade data from Binance and publishes it to Kafka. It follows a **Source → Transform → Sink** pattern and is designed for resilience, handling connection drops with automatic reconnection.

---

## Architecture

```
Binance WebSocket ──→ BinanceClient ──→ TradeTransformer ──→ AvroSerializer ──→ KafkaClient ──→ Kafka Topic
```

![Producer Architecture](../assets/flow_chart/producers.png)

### Project Structure

```
producers/
├── main.py                         # Entry point: orchestrates Source → Transform → Sink
├── connectors/
│   ├── binance_client.py           # WebSocket connection to Binance
│   ├── kafka_client.py             # Confluent Kafka producer wrapper
│   ├── schema_registry.py          # Avro serialization via Schema Registry
│   └── logging_config.py           # Centralized logging setup
├── domain/
│   └── transformer.py              # JSON → structured dict transformation
└── sql/
    ├── ddl_raw.sql                 # PostgreSQL raw schema DDL
    └── ddl_gold_index.sql          # Gold table indexes
```

---

## Component Details

### 1. Main Entry Point ([`main.py`](../../producers/main.py))

Orchestrates the full data flow:

1. Initializes logging and the `KafkaClient`.
2. Defines a `handle_message` callback that transforms, serializes, and produces each message.
3. Creates a `BinanceClient` with the callback and starts the WebSocket connection.
4. Handles graceful shutdown and error logging.

The **key** is the `symbol` string (used for Kafka partitioning), and the **value** is the Avro-serialized trade record.

---

### 2. Binance Client ([`connectors/binance_client.py`](../../producers/connectors/binance_client.py))

- **Library:** `websocket-client` for persistent WebSocket connections.
- **Streams:** Subscribes to the `@trade` stream for 10 symbols simultaneously via a combined URL:

```
wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade/bnbusdt@trade/...
```

**Tracked Symbols:**

```
btcusdt, ethusdt, bnbusdt, solusdt, adausdt,
xrpusdt, dogeusdt, dotusdt, avaxusdt, polusdt
```

- **Resilience:** The `start()` method runs in an infinite loop. If the WebSocket drops or encounters an error, it automatically reconnects after a **5-second delay**. Only a `KeyboardInterrupt` stops the client.
- **Callbacks:** `on_open`, `on_message`, `on_error`, `on_close` — all properly logged.

---

### 3. Data Transformer ([`domain/transformer.py`](../../producers/domain/transformer.py))

Converts raw Binance JSON into a clean dictionary matching the Avro schema.

**Input** (raw Binance trade event):
```json
{ "e": "trade", "s": "BTCUSDT", "p": "50000.00", "q": "0.001", "T": 1678900000123, ... }
```

**Output** (structured record):
```json
{
  "symbol": "BTCUSDT",
  "price": 50000.0,
  "quantity": 0.001,
  "event_time": 1678900000123,
  "processing_time": 1678900000500
}
```

**Responsibilities:**
- Validates the event type (`data['e'] == 'trade'`), ignoring non-trade messages.
- Extracts and casts fields: `symbol` (str), `price` (float), `quantity` (float), `event_time` (int, ms).
- Appends `processing_time` — the current server time in milliseconds — to measure ingestion latency.
- Returns `None` on invalid/unparseable messages (logged, not raised) so the pipeline continues.

---

### 4. Schema Validation & Serialization ([`connectors/schema_registry.py`](../../producers/connectors/schema_registry.py))

Uses **Apache Avro** for compact binary serialization, validated against the **Confluent Schema Registry**.

**Why Avro + Schema Registry?**
- Avro messages don't contain field names (unlike JSON), reducing bandwidth and storage.
- The Schema Registry stores schema definitions, handles versioning, and ensures producers and consumers always agree on the data structure.
- Decouples the producer from consumers — schema changes don't require coordinated deployments.

**Schema File:** [`config/schemas/crypto_trades.avsc`](../../config/schemas/crypto_trades.avsc)

| Field | Type | Logical Type | Description |
|---|---|---|---|
| `symbol` | `string` | — | Currency pair symbol (e.g., `BTCUSDT`) |
| `price` | `double` | — | Trade execution price |
| `quantity` | `double` | — | Amount of asset traded |
| `event_time` | `long` | `timestamp-millis` | Time the trade occurred on the exchange |
| `processing_time` | `long` | `timestamp-millis` | Time when the producer ingested the event |

> The `timestamp-millis` logical type enables Spark to automatically convert these fields to `TimestampType` during Avro deserialization.

#### Confluent Wire Format

Each serialized message follows the Confluent wire format:

| Bytes | Content | Description |
|---|---|---|
| Byte 0 | `0x00` | Magic Byte |
| Bytes 1-4 | Schema ID | 4-byte Big Endian integer |
| Bytes 5+ | Avro payload | Binary-encoded record |

This header allows consumers to dynamically fetch the correct schema from the Registry without prior configuration. See the [Spark section](./spark_jobs.md#31-handling-confluent-avro-in-spark) for how the consumer handles this.

---

### 5. Kafka Client ([`connectors/kafka_client.py`](../../producers/connectors/kafka_client.py))

Built on `confluent_kafka.Producer`, configured for **high reliability and throughput**.

#### Configuration Parameters

| Parameter | Value | Description | Rationale |
|---|---|---|---|
| `bootstrap.servers` | from config | Broker connection | Loaded from `config/kafka.py` (env-configurable) |
| `client.id` | `"crypto_producer"` | Logical client identifier | Tracking and debugging on broker side |
| `enable.idempotence` | `True` | Exactly-once semantics per partition | **Critical for financial data** — prevents duplicates during retries |
| `acks` | `'all'` | Full ISR acknowledgment | **Maximum Durability** — data is not lost if leader fails |
| `retries` | `1,000,000` | Near-infinite retries | Ensures delivery during transient outages (idempotence handles duplicates) |
| `delivery.timeout.ms` | `120,000` (2 min) | Total delivery time budget | 2-minute buffer for network blips and leader elections |
| `linger.ms` | `20` | Batch wait time | Trades off 20ms latency for better batching efficiency |
| `compression.type` | `'lz4'` | Batch compression | LZ4: fast compression/decompression, low CPU, saves bandwidth |
| `batch.size` | `16,384` (16 KB) | Max batch size | Balances memory with throughput, works with `linger.ms` |
| `request.timeout.ms` | `30,000` (30 sec) | Broker response timeout | Triggers retry faster than waiting for full delivery timeout |

#### Kafka Topic: `crypto_trades`

* **Partitions:** 2 (created via [`ops_init_infrastructure`](../../dags/ops_init_infrastructure.py) DAG)
* **Replication Factor:** 1 (single-broker development setup)
* **Partitioning Key:** `symbol` string — ensures all trades for one coin go to the same partition, preserving per-symbol ordering.

---

## Data Flow Summary

```
1. WebSocket connects → wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade/...
2. Raw JSON arrives   → { "e": "trade", "s": "BTCUSDT", "p": "50000.00", "q": "0.001", "T": ... }
3. Transform          → { symbol, price, quantity, event_time, processing_time }
4. Avro serialize     → Schema Registry validates → binary payload with 5-byte header
5. Kafka produce      → Topic: crypto_trades (key=symbol, value=avro_bytes)
```

---

## Local Testing

Start the producer inside the `airflow-triggerer` container:

```bash
# Step 1: Enter the container
docker exec -it airflow-triggerer bash

# Step 2: Run the producer
python producers/main.py
```

Expected output: log lines showing successful WebSocket connection and messages being produced to Kafka.