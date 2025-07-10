from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from io import BytesIO
import os

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}

# Function to read SQL from file
def read_sql(file_name):
    path = os.path.join(os.path.dirname(__file__), '../sql_queries', file_name)
    with open(path, 'r') as file:
        return file.read()

# Query configs (refer to SQL files instead of writing SQL inline)
query_configs = [
    {
        'table': 'sale_orders',
        'sql_file': 'sale_orders_erp.sql',
        'conn_id': 'erp_prod'
    }
]

# ETL function
def extract_and_upload(config):
    sql = read_sql(config['sql_file'])
    pg_hook = PostgresHook(postgres_conn_id=config['conn_id'])
    df = pg_hook.get_pandas_df(sql)

    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)

    today = datetime.today().strftime('%Y-%m-%d')
    s3_key = f"etl/{config['table']}/dt={today}/part-000.parquet"

    s3 = S3Hook(aws_conn_id='metabase-etl')
    s3.load_file_obj(buffer, bucket_name='metabase-etl', key=s3_key, replace=True)

with DAG('multi_pg_to_s3_sqlfiles',
         schedule_interval=None,
         default_args=default_args,
         tags=['etl']) as dag:

    for q in query_configs:
        PythonOperator(
            task_id=f"extract_{q['table']}",
            python_callable=extract_and_upload,
            op_kwargs={'config': q}
        )
