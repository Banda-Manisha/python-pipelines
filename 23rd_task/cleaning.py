import configparser
import os
from  pyspark.sql  import SparkSession
from pyspark.sql.functions import trim,lower,split,regexp_replace,col
import pandas as pd

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Manish\Desktop\sparks\venv\Scripts\python.exe"

#connections 
def read_config(filepath=r"C:\Users\Manish\Desktop\sparks\23rd_task\config.ini"):
    config = configparser.ConfigParser()
    config.read(filepath)
    return config

def spark_session():
    spark = SparkSession.builder\
            .appName("cleaning csv and loading")\
            .config("spark.driver.extraClassPath","C:/spark/spark-4.0.0-bin-hadoop3/jars/mssql-jdbc-12.10.1.jre11.jar")\
            .getOrCreate()   

    return spark

#cleaning 
def cleaning_csv():
    csv_path = r"C:\Users\Manish\Downloads\us_customer_data 1.csv"

    spark = spark_session()
    pd_df = pd.read_csv(csv_path)
    spark_df = spark.createDataFrame(pd_df)
    #dropping duplicates
    spark_df = spark_df.dropDuplicates(["email"])
    #removing - from phone numbers 
    spark_df = spark_df.withColumn("phone",regexp_replace(col("phone"),"-",""))
    #replacing null values with "unknown"
    spark_df = spark_df.fillna({'email':'unknown','phone':'not provided'})
    #triming spaces for names and email
    spark_df = spark_df.withColumn("email",lower(trim(col("email"))))
    spark_df = spark_df.withColumn("first_name", trim(split(col("name"), " ").getItem(0)))
    spark_df = spark_df.withColumn("last_name", trim(split(col("name"), " ").getItem(1)))
    
    #dropping name col
    spark_df = spark_df.drop("name")

    return spark_df 
    
#loading into sql
def load_to_sql(config, spark_df):
    sql = config['SQL_SERVER']

    
    jdbc_url = f"jdbc:sqlserver://{sql['server']};databaseName={sql['database']};encrypt=true;trustServerCertificate=true"

    props = {
        "user": sql['username'],
        "password": sql['password'],
        "driver": sql['driver']
    }

    spark_df.write.mode("overwrite").jdbc(
        url=jdbc_url,
        table='spark_cleaned',
        properties=props
    )

    print("Loaded into SQL Server")


if __name__ == "__main__":
    print("ETL process starting")
    config = read_config()
    
    spark_df = cleaning_csv()
    print("successfully cleaned csv file")
    load_to_sql(config,spark_df)
    print("ETL process done")

