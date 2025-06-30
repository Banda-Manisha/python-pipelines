import configparser
import pyodbc
import mysql.connector
import urllib.parse
from sqlalchemy import create_engine
 
config = configparser.ConfigParser()
config.read('config.ini')
 
sql_server = config['SQLSERVER']['server']
sql_database = config['SQLSERVER']['Database']
sql_username = config['SQLSERVER']['username']
sql_password = config['SQLSERVER']['password']
 
conn = pyodbc.connect(
    f"DRIVER={{SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"
)
print("Connected to SQL Server!")
 
mysql_host = config['MYSQL']['host']
mysql_database = config['MYSQL']['Database']
mysql_username = config['MYSQL']['username']
mysql_password = config['MYSQL']['password']
mysql_port = config['MYSQL']['port']
 
conn_mysql = mysql.connector.connect(
    host=mysql_host,
    user=mysql_username,
    password=mysql_password,
    database=mysql_database,
    port=mysql_port
)
 
encoded_password = urllib.parse.quote(mysql_password)
conn_mysql_engine = create_engine(
    f"mysql+pymysql://{mysql_username}:{encoded_password}@{mysql_host}:{mysql_port}/{mysql_database}"
)
 
print("Connected to MySQL!")