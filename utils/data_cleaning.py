
import pandas as pd

import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        try:
            col_dtype = df[col].dtype

            if pd.api.types.is_string_dtype(col_dtype):
                df[col] = df[col].fillna("Unknown")

            elif pd.api.types.is_numeric_dtype(col_dtype):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            elif pd.api.types.is_datetime64_any_dtype(col_dtype):
                df[col] = pd.to_datetime(df[col], errors="coerce")

        except Exception as e:
            print(f"❌ Error cleaning column '{col}': {e}")

    return df

