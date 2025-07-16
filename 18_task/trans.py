#tran.py
import pandas as pd

def transform_data(df):
    try:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
        df["budget"] = pd.to_numeric(df["budget"], errors="coerce")
        return df
    except Exception as e:
        print("Transformation failed:", e)
        return df
