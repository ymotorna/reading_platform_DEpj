from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DBT_PJ_DIR = '/usr/local/airflow/dbt/dbt_reading_platform'
DBT_VENV = '/usr/local/airflow/dbt_venv/bin/activate'















