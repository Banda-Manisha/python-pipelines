from connect_mongo import extract_table
import pandas as pd

def transform_data():
    df = extract_table()
    # calculating durations 
    df['duration_days'] = (pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.days 
    #exploding
    df = df.explode("technologies")
    # mapping
    orders = {"Planned": 1,"In Progress": 2,"Completed":3}
    df["status_tier"] = df["status"].map(orders)
    print("transformed")
    return df
if __name__ == "__main__":
    transform_data()



 
