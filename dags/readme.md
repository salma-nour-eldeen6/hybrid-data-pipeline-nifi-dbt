# Airflow Orchestration

The project uses Apache Airflow to orchestrate the dbt transformation workflow across the Medallion Architecture layers.

## DAG: `dbt_insurance_pipeline`

The DAG executes the following stages sequentially:

```text
Bronze → Silver → Snapshot → Gold → Tests
```

## Workflow Description

![The run of work flow](../imgs/dag.png)

| Task | Description |
|---|---|
| dbt_bronze | Executes bronze layer models |
| dbt_silver | Executes silver layer transformations |
| dbt_snapshot | Captures slowly changing dimensions using dbt snapshots |
| dbt_gold | Builds analytics-ready gold models |
| dbt_test | Runs dbt tests for data quality validation |

## Technologies

- Apache Airflow
- dbt Core
- PostgreSQL
- Docker

## DAG File Location

```text
airflow/dags/dbt_insurance_pipeline.py
```

## Example Task Dependency

```python
dbt_bronze >> dbt_silver >> dbt_snapshot >> dbt_gold >> dbt_test
```