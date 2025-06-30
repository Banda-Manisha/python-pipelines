from transform import transform_data
from connect_mongo import get_sqlalchemy_engine,read_config
import pyodbc 
import pandas as pd
from  sqlalchemy import create_engine

def load_to_sql():
    df=transform_data()
    config=read_config()
    connect=get_sqlalchemy_engine(config)
    df['_id'] = df['_id'].astype(str)  
    df.to_sql('projects',con=connect,if_exists='replace', index=False)
    print("loading is completed")

"""
if __name__ == "__main__":
    load_to_sql()
"""

   
    
       