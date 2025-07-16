# transform.py

import json 
import os
import pandas as pd


DOWNLOAD_PATH = "downloads"

def load_json_as_dataframe(file_name):
    path = os.path.join(DOWNLOAD_PATH, file_name)
    with open(path, "r") as f:
        data = json.load(f)
        return pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
    
def transform_product_dimension():
    df = load_json_as_dataframe("product_dimension.json")
    df["is_active"] = df["is_active"].astype(int)

    return df

def transform_sales_dimension():
    df = load_json_as_dataframe("sales_dimensions.json")

    df_supplier = df[[
        "supplier_id","supplier_name","contact_email","supplier_country",
        "reliability_score","region_id"
    ]].drop_duplicates()

    df_region = df[[
        "region_id","region_name","region_country","regional_manager"
    ]].drop_duplicates()

    df_promotion = df[[
        "promotion_id","promotion_name","discount_percentage","start_date","end_date"
    ]].drop_duplicates()

    return df_supplier,df_region,df_promotion


def transform_time_dimension():
    df = load_json_as_dataframe("time_dimension.json")

    bool_cols = ["is_holiday","is_weekend","is_business_day"]
    for col in bool_cols:
        df[col] = df[col].astype(int)

    return df 

def transform_store_dimension():
    df = load_json_as_dataframe("store_dimension.json")

    df["opening_date"] = pd.to_datetime(df["opening_date"], errors = "coerce")

    return df

def transform_sales_fact():
 
 
    # Load base sales fact data
    df_fact = load_json_as_dataframe("sales_fact.json")
 
    # Load dim_product and dim_store for joins
    df_products = load_json_as_dataframe("product_dimension.json")
    df_stores = load_json_as_dataframe("store_dimension.json")
 
    # Join to get supplier_id from product
    df_fact = df_fact.merge(df_products[['product_id', 'supplier_id']], on='product_id', how='left')
 
    # Join to get region_id from store
    df_fact = df_fact.merge(df_stores[['store_id', 'region_id']], on='store_id', how='left')
 
    # Clean float columns to 2 decimal places and fill NaNs
    float_cols = ['discount_applied', 'tax_amount', 'net_amount', 'total_amount']
    for col in float_cols:
        df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce').fillna(0).round(2)
 
    # Clean int columns
    int_cols = ['promotion_id', 'region_id', 'supplier_id']
    for col in int_cols:
        df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce').fillna(0).astype(int)
 
    # Keep only relevant columns
    required_columns = [
        'sale_id', 'product_id', 'date_id', 'store_id', 'promotion_id',
        'quantity_sold', 'discount_applied', 'tax_amount',
        'net_amount', 'total_amount',
        'region_id', 'customer_id', 'supplier_id'
    ]
    df_fact = df_fact[required_columns]
 
    return df_fact