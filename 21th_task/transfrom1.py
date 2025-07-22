# transform1.py
import os
from con1 import create_spark_session

def transform_all_parquet_files(local_download_dir="download"):
    spark = create_spark_session()
    table_dataframes = {}

    for filename in os.listdir(local_download_dir):
        if filename.endswith(".parquet"):
            file_path = os.path.join(local_download_dir, filename)
            table_name = os.path.splitext(filename)[0].lower().replace("-", "_")

            df = spark.read.parquet(file_path)
            df_cleaned = df.dropna().dropDuplicates()

            table_dataframes[table_name] = df_cleaned
            print(f"Transformed table: {table_name}")

    return table_dataframes
