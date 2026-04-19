from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
from airflow.operators.python import PythonOperator
import etl_function


logger = logging.getLogger(__name__)

DBT_PJ_DIR = '/usr/local/airflow/dbt/dbt_reading_platform'
DBT_VENV = '/usr/local/airflow/dbt_venv/bin/activate'


# ------------------------------------------------------------------
# OLTP (PostgresSQL) -> DuckDB  \\  ETL  \\  authors/books/subscriptions/users info \\ structured data
def etl_authors(task_instance):
    etl_function.etl_tbl('authors', 'added_at')

def etl_books(task_instance):
    etl_function.etl_tbl('books', 'added_to_platform_at')

def etl_subscriptions(task_instance):
    etl_function.etl_tbl('subscriptions', 'last_change_at')

def etl_users(task_instance):
    etl_function.etl_tbl('users', 'last_change_at')


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
    "daily_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    max_active_runs=1
)

# ---------------------------------------------
# tasks for etl processes  \\  OLTP  \\  @daily, >static data

load_new_authors = PythonOperator(
    task_id="new_authors",
    python_callable=etl_authors,
    dag=dag
)

load_new_books = PythonOperator(
    task_id="new_books",
    python_callable=etl_books,
    dag=dag
)

load_new_subscriptions = PythonOperator(
    task_id="new_subscriptions",
    python_callable=etl_subscriptions,
    dag=dag
)

load_new_users = PythonOperator(
    task_id="new_users",
    python_callable=etl_users,
    dag=dag
)

# -----------------------------------------------
# refresh seeds  (needed?)  \\  \\  genres/languages/country_codes/subscription_types
dbt_seed = BashOperator(
    task_id="dbt_seed",
    bash_command=(
        f"source {DBT_VENV} && "
        f"cd {DBT_PJ_DIR} && "
        f"dbt seed --full-refresh"
    ),
    dag=dag
)

# -----------------------------------------------
# run dbt daily models  \\  dims + daily stat
dbt_build_daily = BashOperator(
    task_id="dbt_build_daily",
    bash_command=(
        f"source {DBT_VENV} && "
        f"cd {DBT_PJ_DIR} && "
        f"dbt build --select tag:daily"
    ),
    dag=dag
)


# execution pipeline
load_new_authors >> load_new_books >> load_new_subscriptions >> load_new_users >> dbt_seed >> dbt_build_daily











