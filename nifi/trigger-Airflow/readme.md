# NiFi → Airflow Trigger Pipeline

## Overview

This project demonstrates a **data pipeline integration between Apache NiFi and Apache Airflow**, where NiFi is responsible for triggering an Airflow DAG via REST API.

The flow simulates a simple orchestration pattern where NiFi acts as the upstream trigger system, and **Airflow is responsible for executing downstream data transformations using dbt (data build tool).**

The DAG was configured to run **every 1 minute**, enabling near real-time orchestration and continuous pipeline execution.

---

## Pipeline Flow

The pipeline works as follows:

1. **GenerateFlowFile**

   * Creates a sample JSON payload.

2. **UpdateAttribute**

   * Injects authentication details (Token / Session / CSRF).

3. **InvokeHTTP**

   * Sends a POST request to Airflow REST API to trigger a DAG run.

4. **LogAttribute**

   * Logs request/response for debugging and monitoring.

---

## Airflow Processing Layer (dbt Execution)

Once triggered by NiFi, **Airflow executes dbt models** as part of the pipeline.

* Airflow acts as the orchestration layer
* dbt handles **data transformation logic (T → T / ELT layer)**
* Each DAG run executes dbt models to process and transform data

This design separates:

* **Orchestration (Airflow)**
* **Transformation (dbt)**

---

## Schedule Configuration

The Airflow DAG is configured to run:

> ⏱ Every 1 minute

This allows:

* Continuous pipeline execution
* Near real-time processing simulation
* Frequent triggering validation from NiFi

---

## Airflow Trigger Mechanism

NiFi triggers Airflow using the following endpoint:

```
POST /api/v1/dags/{dag_id}/dagRuns
```

### Example Payload:

```json
{
  "conf": {
    "triggered_by": "nifi",
    "source": "manual"
  }
}
```

---

## Pipeline Screenshots

### 1️⃣ NiFi Triggering Airflow

![NiFi Trigger Airflow](../screenshots/trigger_Airflow.png)

---

### 2️⃣ Airflow DAG Execution (dbt Run)

![Airflow DBT Run](../screenshots/Airflow_run.png)

---

## NiFi Flow Components

| Component        | Purpose               |
| ---------------- | --------------------- |
| GenerateFlowFile | Creates input JSON    |
| UpdateAttribute  | Injects auth headers  |
| InvokeHTTP       | Calls Airflow API     |
| LogAttribute     | Logs request/response |

---

## Key Idea

This pipeline demonstrates how **NiFi can act as an external trigger system for Airflow**, while Airflow handles downstream execution of **dbt-based transformations**, enabling a modular and scalable hybrid data architecture.

 