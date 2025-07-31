import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Manish\Desktop\sparks\venv\Scripts\python.exe"
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower,concat,regexp_replace,lit,trim,udf,when,split,concat_ws
from pyspark.sql.types import StringType
import os
import random as re
import configparser

def read_config(file_path=r'C:\Users\Manish\Desktop\sparks\22nd_task\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

#creating spark session 
def spark_session():
    spark = SparkSession.builder\
          .appName("CSV to sql server ")\
          .config("spark.driver.extraClassPath","C:/spark/spark-4.0.0-bin-hadoop3/jars/mssql-jdbc-12.10.1.jre11.jar")\
          .getOrCreate()
    
    return spark

def generate_phone():
        return f"+1{re.randint(7000000000,9000000000)}"

generate_phone_udf = udf(generate_phone,StringType())

def extract_and_transform():
    csv_path = r"C:\Users\Manish\Downloads\us_customer_data 1.csv"

    spark = spark_session()
    customers = spark.read.csv(csv_path, header=True, inferSchema=True)
    customers = customers.withColumn("phone",regexp_replace(col("phone"),"-",""))

    customers = customers.withColumn(
         "phone",
         when(col("phone").isNull() | (col("phone") == ""),generate_phone_udf()).otherwise(col("phone"))
    )
    
    customers = customers.withColumn("first_name", trim(split(col("name"), " ").getItem(0)))
    customers = customers.withColumn("last_name", trim(split(col("name"), " ").getItem(1)))

    customers = customers.withColumn(
    "email",
    when(
        (col("email").isNull()) | (col("email") == ""),
        concat_ws("", lower(col("first_name")), lower(col("last_name")), lit("@email.com")) #concate with separator here it is nthg and we can also use concat only  , lit is leteral value(constant)
    ).otherwise(col("email"))
)

    return customers


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
        table="spark_csv",
        properties=props  
    )
    print("Loaded table into SQL Server successfully.")

    
if __name__ == "__main__":     

    config = read_config()

    customers = extract_and_transform()

    load_to_sql(config,customers)
    
    
