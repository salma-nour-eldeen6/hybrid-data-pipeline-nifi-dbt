from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "salma",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

DBT_DIR = "cd /usr/app/dbt"
PROFILES = "--profiles-dir /usr/app/dbt"

with DAG(
    dag_id="dbt_insurance_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Full dbt pipeline: bronze → silver → gold + snapshots + tests",
) as dag:

    # ── 1. Bronze Layer ──────────────────────────────────────
    dbt_bronze = BashOperator(
        task_id="dbt_bronze",
        bash_command=f"{DBT_DIR} && dbt run --select bronze {PROFILES}"
    )

    # ── 2. Silver Layer ──────────────────────────────────────
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=f"{DBT_DIR} && dbt run --select silver {PROFILES}"
    )

    # ── 3. Snapshot  
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_DIR} && dbt snapshot {PROFILES}"
    )

    # ── 4. Gold Layer ────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=f"{DBT_DIR} && dbt run --select gold {PROFILES}"
    )

    # ── 5. Tests ─────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_DIR} && dbt test {PROFILES}"
    )
 

    # ── Pipeline Order ───────────────────────────────────────
    dbt_bronze >> dbt_silver >> dbt_snapshot >> dbt_gold >> dbt_test 