from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Alana Barrett-Frew',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

schedule = '0 0 1 1,4,7,10 *'  # First day of each quarter

with DAG(
    'flowcase_etl_quarterly',
    default_args=default_args,
    description='Run Flowcase ETL at the start of every quarter',
    schedule=schedule,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'quarterly'],
) as dag:

    run_etl = BashOperator(
        task_id='run_etl_pipeline',
        bash_command="""
            set -a
            source /opt/airflow/.env
            set +a
            cd /opt/airflow
            export PYTHONPATH=/opt/airflow/src
            python -m flowcase_etl_pipeline.cli
        """,
    )
