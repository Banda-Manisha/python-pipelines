from pymongo import MongoClient
import pyodbc
import json
import pandas as pd
import configparser
import urllib
from sqlalchemy import create_engine
#building connection to mongodb and loading 

def connect_to_mongodb():
    """
    Connects to MongoDB and returns the 'projects' collection from the 'task10' database.
    """
    client = MongoClient("mongodb://localhost:27017")
    db = client["task10"]
    collection = db["project1"]
    print("python-mongodb success")
    """  #loading data into mongodb 
    with open('data_unstructured.json') as file:
       file_data = json.load(file)
    if isinstance(file_data, list):
      collection.insert_many(file_data)  
    else:
      collection.insert_one(file_data)
    print("loaded to mongodb")  """
    return collection

#sql server connect
def read_config(file_path=r'C:\Users\Manish\Desktop\mongodb1\config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config
 
def get_sqlalchemy_engine(config):
    sql_config = config['SQL_SERVER']
 
    conn_str = urllib.parse.quote_plus(
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )
 
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")
 


# extracting data from mongodb
def extract_table():
    collection = connect_to_mongodb()
    data = list(collection.find()) 
    df = pd.DataFrame(data)
    print("extracted")
    print(df.head())
    return df 

