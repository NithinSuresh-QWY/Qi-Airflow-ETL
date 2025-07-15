import psycopg2
from airflow.hooks.base import BaseHook
import io
import pandas as pd
from psycopg2.extras import execute_values
from airflow.hooks.base import BaseHook
import numpy as np
import json


def insert_into_target(target_conn_id, target_table, df, unique_key):
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df is not a DataFrame — got type {type(df)}")

    # # Sanitize text columns (replace tabs with spaces)
    # for col in df.select_dtypes(include=['object']).columns:
    #     try:
    #         df[col] = df[col].astype(str).str.replace('\t', ' ', regex=False)
    #     except Exception as e:
    #         print(f"⚠️ Skipping tab-replace for column '{col}' due to: {str(e)}")

    # Replace empty strings and 'None' with pd.NA
    df = df.replace(['', 'None'], pd.NA)

    # Ensure numeric columns are properly typed
    numeric_columns = [
        'customer_rating', 'till_pickup_distance', 'order_distance', 'weight',
        'order_amount', 'discount_amount', 'total_amount', 'merchant_order_amount',
        'merchant_discount_amount', 'merchant_total_amount', 'total_product_qty',
        'amount_untaxed', 'amount_tax', 'actual_order_cost', 'estimated_order_cost'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(pd.NA)

    # Convert integer-like columns to native Python int
    integer_columns = [
        'id', 'order_status_id', 'partner_id', 'customer_segment_id',
        'product_line_id', 'region_id', 'industry_id', 'total_product_qty'
    ]
    for col in integer_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer').fillna(pd.NA).astype('Int64')  # Nullable integer type

    # Build connection string
    conn = BaseHook.get_connection(target_conn_id)
    conn_str = (
        f"host={conn.host} dbname={conn.schema} "
        f"user={conn.login} password={conn.password} port={conn.port}"
    )

    try:
        with psycopg2.connect(conn_str) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT {unique_key} FROM {target_table};")
                existing_keys = set(row[0] for row in cursor.fetchall())

                initial_count = len(df)
                # df = df[~df[unique_key].isin(existing_keys)]
                existing_keys = set(str(key) for key in existing_keys)
                df = df[df[unique_key].astype(str).map(lambda x: x not in existing_keys)]
                filtered_count = len(df)
                print(f"🧹 Removed {initial_count - filtered_count} duplicate rows based on key '{unique_key}'")

                if df.empty:
                    print("⚠️ No new rows to insert after deduplication.")
                    return 0
                
                if df.columns.duplicated().any():
                    print("⚠️ Duplicate columns found and removed:", df.columns[df.columns.duplicated()].tolist())
                df = df.loc[:, ~df.columns.duplicated()]

                columns = df.columns.tolist()
                columns_sql = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                insert_query = f"INSERT INTO {target_table} ({columns_sql}) VALUES %s"

                values = [tuple(convert_value(val) for val in row) for row in df.itertuples(index=False)]

                execute_values(cursor, insert_query, values, page_size=1000)
                connection.commit()

        print(f"✅ Inserted {filtered_count} new rows (skipped {initial_count - filtered_count} duplicates).")
        return filtered_count

    except Exception as e:
        print(f"❌ psycopg2 insert failed: {e}")
        if len(df) >= 147:
            print(f"Problematic row (line 147, index 146): {df.iloc[146].to_dict()}")
        elif len(df) > 0:
            print(f"First row for reference: {df.iloc[0].to_dict()}")
        raise




def convert_value(val):
    if isinstance(val, (list, dict)):
        return json.dumps(val)
    
    if pd.isna(val):
        return None
    elif isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.floating):
        return float(val)
    
    return val    