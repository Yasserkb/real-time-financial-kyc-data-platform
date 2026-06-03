from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="kyc_daily_reconciliation",
    description="Quality-gated daily reconciliation for the KYC lakehouse.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kyc", "reconciliation", "data-quality"],
) as dag:
    start = EmptyOperator(task_id="start")
    build_gold = BashOperator(task_id="build_gold_customer_risk", bash_command="python /opt/airflow/repo/spark_jobs/jobs/batch_gold_customer_risk.py")
    reconcile = BashOperator(task_id="run_quality_reconciliation", bash_command="python /opt/airflow/repo/spark_jobs/jobs/batch_reconciliation.py")
    dbt_build = BashOperator(task_id="dbt_build_kyc_marts", bash_command="cd /opt/airflow/repo/dbt_kyc && dbt build --target ${DBT_TARGET:-local}")
    end = EmptyOperator(task_id="end")
    start >> build_gold >> reconcile >> dbt_build >> end
