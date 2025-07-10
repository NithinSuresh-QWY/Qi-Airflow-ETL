import sys, os, json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl_main.etl_task import run_etl_pipeline
from etl_main.post_etl import run_post_calculations
from utils.db_utils import get_so_date_before_pull
from etl_main.load_config import load_config
config = load_config()

# Hardcoded fetch date for now (can also be dynamic if needed)
fetch_until_date = "2025-06-30 23:59:59"

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'catchup': False
}

with DAG(
    dag_id='etl_to_metabase_postgres',
    default_args=default_args,
    schedule_interval=None,
    tags=['etl'],
    description='ETL pipeline for Metabase Postgres DB'
) as dag:

    def capture_so_date_before_etl(**kwargs):
        so_date_before = get_so_date_before_pull()
        print(f"[DEBUG] Captured pre-ETL so_date_before_pull: {so_date_before}")
        kwargs['ti'].xcom_push(key='so_date_before_pull', value=so_date_before)

    capture_so_date = PythonOperator(
        task_id='capture_so_date_before_etl',
        python_callable=capture_so_date_before_etl,
        provide_context=True
    )

    def run_post_with_xcom(**kwargs):
        ti = kwargs['ti']
        so_date_before_pull = ti.xcom_pull(
            task_ids='capture_so_date_before_etl',
            key='so_date_before_pull'
        )
        print(f"[DEBUG] Using so_date_before_pull from XCom: {so_date_before_pull}")
        run_post_calculations(
            so_date_before_pull=so_date_before_pull,
            fetch_until_date=fetch_until_date
        )

    run_post = PythonOperator(
        task_id="run_post_calculations",
        python_callable=run_post_with_xcom,
        provide_context=True
    )

    config = load_config()
    etl_tasks = []
    for step in config["steps"]:
        task = PythonOperator(
            task_id=f"etl_{step['target_table']}",
            python_callable=run_etl_pipeline,
            op_kwargs={
                "source_db": step["source_db"],
                "target_table": step["target_table"],
                "query_file": step["query_file"],
                "unique_key": step["unique_check_key"],
                "use_last_created_on": step["use_last_created_on"],
                "fetch_until_date": fetch_until_date
            }
        )
        etl_tasks.append(task)

    # Chain tasks together: capture date → all ETL steps → post calc
    capture_so_date >> etl_tasks >> run_post
