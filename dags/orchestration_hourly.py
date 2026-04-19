from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
from airflow.operators.python import PythonOperator
import etl_minio_function

logger = logging.getLogger(__name__)

dbt_dir = '/usr/local/airflow/dbt/dbt_reading_platform'
dbt_venv = '/usr/local/airflow/dbt_venv/bin/activate'


# ------------------------------------------------------------------
# MiniO (json+large csv) -> DuckDB  \\  ETL  \\  payments/reading_sessions/reviews info \\ structured+large/semi-structured data \\  @hourly
def etl_payments(task_instance):
    etl_minio_function.etl_minio('payments', 'last_change_at')

def etl_reading_sessions(task_instance):
    etl_minio_function.etl_minio('reading_sessions', 'ended_at')

def etl_reviews(task_instance):
    etl_minio_function.etl_minio('reviews', 'created_at')


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
    "hourly_pipeline",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    max_active_runs=1
)


# ---------------------------------------------
# tasks for Minio etl  \\  @hourly, dynamic data  \\  payments/sessions/reviews

load_new_payments = PythonOperator(
    task_id="new_payments",
    python_callable=etl_payments,
    dag=dag
)

load_new_reading_sessions = PythonOperator(
    task_id="new_reading_sessions",
    python_callable=etl_reading_sessions,
    dag=dag
)

load_new_reviews = PythonOperator(
    task_id="new_reviews",
    python_callable=etl_reviews,
    dag=dag
)



# ---------------------------------------------
# run dbt hourly models  \\  fct
dbt_build_hourly = BashOperator(
    task_id="dbt_build_hourly",
    bash_command=(
        f"source {dbt_venv} && "
        f"cd {dbt_dir} && "
        f"dbt build --select tag:hourly"
    ),
    dag=dag
)


# execution pipeline
load_new_payments >> load_new_reading_sessions >> load_new_reviews >> dbt_build_hourly











