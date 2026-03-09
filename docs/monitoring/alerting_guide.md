# Monitoring Alert Documentation

> **Architecture context:** See [Data Flow & Architecture Deep Dive](../architecture/data_flow.md#8-monitoring-stack) for how alerting fits into the overall pipeline.

> **Notification Channel:** Email (SMTP)

## 1. Overview

The alerting system's aim is to provide notifications regarding the health of the Crypto Real-time Pipeline.

The system covers three critical failure domains:
1.  **Availability:** Is the Spark Job Ok (Running)?
2.  **Performance:** Is the Job processing data within acceptable latency limits?
3.  **Stability:** Is the hardware/JVM approaching limits?

## 2. Alert Lifecycle & Operational Workflow


### Lifecycle Phases

| Phase | System State | Metric Behavior | Alerting Logic & Action |
| :--- | :--- | :--- | :--- |
| **Phase 1: Startup** | Docker containers are up, but the Spark Job has not been submitted yet. | Prometheus receives no metrics for the specific job. | **Logic:** The query `vector(0)` fills the null gaps. <br> **Action:** Input rate is perceived as 0. If the job is not started within 5 minutes, the **"Spark Job Stopped"** alert triggers. |
| **Phase 2: Running (Healthy)** | Job is processing data normally. | Input Rate > 0, Latency is low, Memory usage is moderate. | **Action:** All rules evaluate to **NORMAL** (Green). No notifications are sent. |
| **Phase 3: Running (Degraded)** | Job encounters data spikes or memory pressure. | Latency exceeds thresholds or Heap usage spikes. | **Action:** <br> 1. Rule switches to **PENDING** state (waiting for the defined duration). <br> 2. If the issue persists, status becomes **FIRING**. <br> 3. Email notification is sent. |
| **Phase 4: Crash / Termination** | Job stops unexpectedly or is manually killed. | Metrics stop reporting completely. | **Logic:** The `or vector(0)` clause in Prometheus query treats the absence of data as 0 value. <br> **Action:** **"Spark Job Stopped"** alert triggers after the 5-minute threshold. |

---

## 3. Alert Rules Reference

### Rule 1: Critical - Spark Job Stopped
**Severity:** CRITICAL  
**Target:** Availability

*   **Description:** Triggers when the steaming job is no longer processing data, either due to a crash, manual stop, or failure to start.
*   **Technical Logic:**
    ```promql
    sum(spark_streaming_input_rate) or vector(0) < 1
    ```
    *   *Interpretation:* Calculate the total input rate. If the metric is missing (Job dead), default to 0. If the resulting value is less than 1 row/second, the condition is met.
*   **Duration (For):** 5 minutes.
    *   *Reasoning:* Prevents flapping alerts caused by temporary network glitches or momentary pauses in data ingestion.
*   **Trigger Scenarios:**
    1.  The container runs but `spark-submit` was never executed.
    2.  The application crashed due to an unhandled exception.
    3.  Kafka connectivity is totally lost (Input = 0).

### Rule 2: Warning - Spark Latency High
**Severity:** WARNING  
**Target:** Performance

*   **Description:** Triggers when the processing time for micro-batches becomes unacceptably slow, indicating a bottleneck.
*   **Technical Logic:**
    ```promql
    avg(spark_streaming_latency) > 10000
    ```
    *   *Interpretation:* The average end-to-end latency exceeds 10,000 milliseconds (10 seconds).
*   **Duration (For):** 2 minutes.
*   **Trigger Scenarios:**
    1.  **Data Spikes:** Sudden burst of traffic from Kafka.
    2.  **Inefficient Code:** Complex joining or aggregation logic slowing down the micro-batch.
    3.  **Resource Contention:** CPU throttling on the worker nodes.
*   **Note:** If the Job is stopped (No Data), this rule evaluates to OK (Normal) because latency metrics simply do not exist to be measured.

### Rule 3: Warning - JVM Memory Critical
**Severity:** WARNING  
**Target:** Stability

*   **Description:** Early warning system for potential Out-Of-Memory (OOM) errors.
*   **Technical Logic:**
    ```promql
    (sum(jvm_memory_bytes_used{area='heap'}) / sum(jvm_memory_bytes_max{area='heap'})) * 100 > 85
    ```
    *   *Interpretation:* The ratio of Used Heap to Max Heap exceeds 85%.
*   **Duration (For):** 5 minutes.
*   **Trigger Scenarios:**
    1.  **Undersized Resources:** `spark.executor.memory` configuration is too low for the workload.
    2.  **Memory Leak:** Objects are accumulating in the heap without being garbage collected.
    3.  **State Explosion:** The streaming state store has grown too large (check Watermark configuration).

---

## 4. Notification & Recovery

*   **Notification Channel:** Emails are sent via the configured SMTP server defined in `GF_SMTP_FROM_ADDRESS`.
*   **Resolution:**
    *   When metrics return to safe levels (e.g., Latency drops below 10s, or Input Rate rises above 0), the Alerting system automatically sends a **RESOLVED** notification.
    *   Alerts repeat notifications are sent every 4 hours if the issue remains unresolved.

For more details on configuring the SMTP settings, refer to the [Installation Guide](../guides/installation.md#3-environment-configuration).