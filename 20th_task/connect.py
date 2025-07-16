#connect.py

import configparser
import pyodbc
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

def read_config(file_path=r'C:\Users\Manish\Desktop\JSONData\config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def get_sharepoint_context(config):
    site_url = config['sharepoint']['site_url']
    username = config['sharepoint']['username']
    password = config['sharepoint']['password']

    ctx = ClientContext(site_url).with_credentials(UserCredential(username,  password))
    print("Connected to SharePoint site.")
    return ctx

def get_db_connection(config):
    sql_config = config['SQL_SERVER']

    conn_str = (
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )
    print("connected to SQL Server")
    return pyodbc.connect(conn_str) 
  