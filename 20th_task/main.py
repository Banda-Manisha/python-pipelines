# main.py

# main.py
import pandas as pd
from connect import read_config, get_sharepoint_context, get_db_connection
from extract import download_json_files
from transform import (
    transform_product_dimension,
    transform_sales_dimension,
    transform_store_dimension,
    transform_time_dimension,
    transform_sales_fact
)
from load import (
    load_dim_product,
    load_dim_supplier,
    load_dim_region,
    load_dim_promotion,
    load_dim_store,
    load_dim_time,
    load_fact_sales
)

def main():
    print("Starting ETL process...")

    # Read config and set up connections
    config = read_config()
    ctx = get_sharepoint_context(config)
    conn = get_db_connection(config)

    #Optional: Download JSON files from SharePoint
    folder_url = config['sharepoint']['folder_url']
    download_json_files(ctx, folder_url)

    # Transform each dataset
    df_product = transform_product_dimension()
    df_supplier, df_region, df_promotion = transform_sales_dimension()
    df_store = transform_store_dimension()
    df_time = transform_time_dimension()
    df_fact = transform_sales_fact()
     
    print(f"Final df_fact rows before loading: {len(df_fact)}")
    print(df_fact.head())  # Optional


    
        


    # Load all dimension tables
    #load_dim_product(conn, df_product)
    #load_dim_supplier(conn, df_supplier)
    #load_dim_region(conn, df_region)
    #load_dim_promotion(conn, df_promotion)
    #load_dim_store(conn, df_store)
    #load_dim_time(conn, df_time)
    load_fact_sales(conn,df_fact)

    # Load fact table only if there's data
   

    conn.close()
    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()
