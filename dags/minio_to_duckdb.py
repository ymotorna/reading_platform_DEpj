# MiniO (json+large csv) -> DuckDB  \\  ETL  \\  payments/reading_sessions/reviews info \\ structured+large / semi-structured data \\  @hourly

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import etl_minio_function

logger = logging.getLogger(__name__)

duckdb_path = "/usr/local/airflow/include/reading_platform.db"

def etl_payments(task_instance):
    etl_minio_function.etl_minio('payments', 'last_change_at')

def etl_reding_sessions(task_instance):
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback
}

dag = DAG(
    "reading_platform_minio",
    default_args=default_args,
    schedule="@hourly",
    catchup=False
)

# ---------------------------------------------

load_new_payments = PythonOperator(
    task_id="new_payments",
    python_callable=etl_payments,
    dag=dag
)

load_new_reding_sessions = PythonOperator(
    task_id="new_reding_sessions",
    python_callable=etl_reding_sessions,
    dag=dag
)

load_new_reviews = PythonOperator(
    task_id="new_reviews",
    python_callable=etl_reviews,
    dag=dag
)


# execution queue
load_new_payments >> load_new_reding_sessions >> load_new_reviews











