# main.py
from con1 import get_sharepoint_context, read_config
from extract1 import download_parquet_files
from transfrom1 import transform_all_parquet_files
from load1 import load_spark_dfs_to_sqlserver

def main():
    config = read_config()
    ctx = get_sharepoint_context()
    folder_url = config['sharepoint']['folder_url']

    print("Starting extraction...")
    download_parquet_files(ctx, folder_url)

    print("Starting transformation...")
    transformed_data = transform_all_parquet_files()

    print("Starting load to SQL Server...")
    load_spark_dfs_to_sqlserver(config, transformed_data)

    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
