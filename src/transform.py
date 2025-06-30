from connect import conn, conn_mysql_engine
import pandas as pd
import datetime
cursor=conn.cursor()
"""
#scd type 1
cursor.execute('''UPDATE orders set order_status='completed' where customer_id=503''')
conn.commit()
print("updated")
"""
 #scd type 2 
cursor.execute('''ALTER table orders add start_date DATETIME, end_date DATETIME, is_current BIT DEFAULT 0''')
conn.commit()

