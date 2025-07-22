# con1.py
import configparser
from pyspark.sql import SparkSession
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

def read_config(file_path=r'C:\Users\Manish\Desktop\sparks\21th_task\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def create_spark_session():
    spark = SparkSession.builder \
        .appName("SharePointParquetToSQLServer") \
        .config("spark.driver.extraClassPath", "C:/spark/spark-4.0.0-bin-hadoop3/jars/mssql-jdbc-12.10.1.jre11.jar") \
        .getOrCreate()
    print("Spark session created.")
    return spark

def get_sharepoint_context():
    config = read_config()
    site_url = config['sharepoint']['site_url']
    username = config['sharepoint']['username']
    password = config['sharepoint']['password']

    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
    print("Connected to SharePoint.")
    return ctx

def get_sqlserver_jdbc_config():
    config = read_config()
    jdbc_url = config['SQL_SERVER']['url']
    jdbc_properties = {
        "user": config['SQL_SERVER']['username'],
        "password": config['SQL_SERVER']['password'],
        "driver": config['SQL_SERVER']['driver']
    }
    return jdbc_url, jdbc_properties
