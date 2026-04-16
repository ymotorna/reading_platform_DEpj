from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DBT_PJ_DIR = '/usr/local/airflow/dbt/dbt_reading_platform'
DBT_VENV = '/usr/local/airflow/dbt_venv/bin/activate'


# ------------------------------------------------------------------

# failure callback func
def failure_callback(context):
    logger.error(
        "Task failed — task_id=%s | dag_id=%s | run_id=%s",
        context['task_instance'].task_id,
        context['dag'].dag_id,
        context['run_id']
    )


# ---------------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 14),
    # Retry & alert strategy
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

dag = DAG(
    "reading_platform_orchestration",
    default_args=default_args,
    schedule="@hourly",
    catchup=False
)


# ---------------------------------------------
#  trigger dag from minio_to_duckdb.py  \\  Minio (json+large csv)  \\  @hourly, dynamic data  \\  payments/sessions/reviews
trigger_minio = TriggerDagRunOperator(
    task_id="trigger_minio_dag",
    trigger_dag_id="reading_platform_minio",
    wait_for_completion=True,
    dag=dag
)


# refresh seeds  (needed?)
dbt_seed = BashOperator(
    task_id="dbt_seed",
    bash_command=(
        f"source {DBT_VENV} && "
        f"cd {DBT_PJ_DIR} && "
        f"dbt seed --full-refresh"
    ),
    dag=dag
)


# run dbt hourly models
dbt_build_hourly = BashOperator(
    task_id="dbt_build_hourly",
    bash_command=(
        f"source {DBT_VENV} && "
        f"cd {DBT_PJ_DIR} && "
        f"dbt build --select tag:hourly"
    ),
    dag=dag
)


# execution pipeline
trigger_minio >> dbt_seed >> dbt_build_hourly











