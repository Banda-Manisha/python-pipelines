from connect import conn, conn_mysql_engine
import pandas as pd
from datetime import datetime
 
query1 = "SELECT * FROM orders"
df = pd.read_sql(query1, conn)
print(df.head())
print("Extraction successful")
df['last_load']=datetime.now()
df.to_sql(name='orders_view', con=conn_mysql_engine, if_exists='replace', index=False)
print("Data successfully written to MySQL!")