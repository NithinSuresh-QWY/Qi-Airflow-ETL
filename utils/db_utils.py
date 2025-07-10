from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl_main.load_config import load_config
import sqlalchemy
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine  
import pandas as pd
from etl_main.load_config import load_config
config = load_config() 
import logging 

logger  = logging.getLogger(__name__)
logging.basicConfig(level=logging.info)



def get_sqlalchemy_conn(conn_id: str):
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(conn_id)
    uri = conn.get_uri()

    # Fix for deprecated URI prefix
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

    return create_engine(uri)



def get_source_conn(db_config):

    conn_id = db_config["conn_id"]
    return get_sqlalchemy_conn(conn_id)


def get_target_conn(config: dict):

    conn_id = config["databases"]["target"]["conn_id"]
    return get_sqlalchemy_conn(conn_id)


def check_existing_records(target_conn, table_name, df: pd.DataFrame, unique_key: str):

    if df.empty or unique_key not in df.columns:
        return []

    if table_name == "orders":
        unique_values = df[unique_key].dropna().astype(str).unique().tolist()
    else:
         unique_values = df[unique_key].dropna().unique().tolist()
    

    if not unique_values:
        return []

    placeholders = ', '.join([f":val{i}" for i in range(len(unique_values))])
    bind_params = {f"val{i}": val for i, val in enumerate(unique_values)}

    sql = text(f"""
        SELECT {unique_key}
        FROM {table_name}
        WHERE {unique_key} IN ({placeholders})
    """)

    result = target_conn.execute(sql, bind_params)
    return [row[0] for row in result.fetchall()]



def get_so_date_before_pull():
    target_conn_id = config["databases"]["target"]["conn_id"]
    conn = BaseHook.get_connection(target_conn_id)
    conn_str = (
        f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    logger.info(f"Connecting to: {conn.host}:{conn.port}/{conn.schema} as {conn.login}")
    
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(create_date) FROM sale_orders"))
            max_create_date = result.scalar()
            # Convert to string in 'YYYY-MM-DD HH:MM:SS' format, or use fetch_until_date if NULL
            if max_create_date:
                logger.info(f"[DEBUG] MAX create_date from DB: {max_create_date}")
                logger.info(f"date from which actual and estimated cost will update: {max_create_date.strftime('%Y-%m-%d %H:%M:%S')}")
                return max_create_date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                logger.info("⚠️ No create_date found in sale_orders. Using default fetch_until_date.")
            
    except Exception as e:
        print(f"❌ Failed to fetch MAX(create_date): {e}")
        raise