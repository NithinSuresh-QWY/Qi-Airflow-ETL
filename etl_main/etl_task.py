import logging
import json
import pandas as pd
from etl_main.load_config import load_config
from etl_main.sql_renderer import render_query
from utils.db_utils import get_sqlalchemy_conn, check_existing_records
from utils.extract import get_last_created_on
from utils.load import insert_into_target
from sqlalchemy.engine import Engine
from utils.extract import fetch_new_data
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.data_cleaning import clean_dataframe
logger = logging.getLogger(__name__)


def run_etl_pipeline(source_db, target_table, query_file, unique_key, use_last_created_on, fetch_until_date):
    config = load_config()

    source_conn_id = config["databases"][source_db]["conn_id"]
    target_conn_id = config["databases"]["target"]["conn_id"]

    source_engine = get_sqlalchemy_conn(source_conn_id)
    target_engine = get_sqlalchemy_conn(target_conn_id)

    # Optional filter based on last_created_on
    last_created_on = get_last_created_on(target_engine, target_table, config) if use_last_created_on else None

    query = render_query(query_file, {
        "last_created_on": last_created_on,
        "fetch_until_date": fetch_until_date
    })

    source_hook = PostgresHook(postgres_conn_id=source_conn_id) 

    df = source_hook.get_pandas_df(sql=query)


    if unique_key:
        existing = check_existing_records(target_engine, target_table, df, unique_key)
        df = df[~df[unique_key].isin(existing)]


    # Validate DataFrame
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df is not a DataFrame — got type {type(df)}")
    
    # df = clean_dataframe(df)
    for col in df.columns:
        try:
            if pd.api.types.is_float_dtype(df[col]):
                if (df[col].dropna() % 1 == 0).all():
                    df[col] = df[col].astype('Int64')  # Nullable integer type
        except Exception as e:
            print(f"Error processing column '{col}': {e}")    
    
    if not df.empty:
        rows_inserted = insert_into_target(target_conn_id, target_table, df, unique_key)
        print(f"Inserted {rows_inserted} rows into {target_table}")
    else:
        print("No new data to insert")
        
