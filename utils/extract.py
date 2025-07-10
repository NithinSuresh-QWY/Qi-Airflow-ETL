import pandas as pd
from sqlalchemy import text
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine import Engine 

def get_last_created_on(conn, table_name, config):
    # Pull the column name dynamically for each table
    tracking_columns = config.get("created_date_columns", {})
    tracking = tracking_columns.get(table_name, "created_on").strip()

    if not tracking:
        raise ValueError(f"❌ No tracking (created) column defined for table '{table_name}'")

    sql = f"SELECT MAX({tracking}) FROM {table_name}"
    result = conn.execute(text(sql)).scalar()

    if result:
        return result.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return "2025-02-01 00:00:00"




def fetch_new_data(source_conn_id, query):    
    postgres_hook = PostgresHook(postgres_conn_id=source_conn_id)
    df = postgres_hook.get_pandas_df(sql=query)
    return df


def render_query(path, context):
    from jinja2 import Template
    with open(f"sql_queries/{path}", "r") as f:
        template = Template(f.read())
    return template.render(**context)
