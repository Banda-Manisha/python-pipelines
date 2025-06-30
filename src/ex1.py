from connect import conn, conn_mysql_engine
import pandas as pd
from datetime import datetime
cursor=conn.cursor()
check_column_query = """ALTER TABLE orders ADD LastModifiedDate DATETIME"""
cursor.execute(check_column_query)
create_trigger_query = """
IF NOT EXISTS (
    SELECT * FROM sys.triggers WHERE name = 'trg_UpdateLastModified'
)
BEGIN
    EXEC('
    CREATE TRIGGER trg_UpdateLastModified
    ON orders
    AFTER INSERT, UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE orders
        SET LastModifiedDate = GETDATE()
        FROM orders o
        INNER JOIN inserted i ON o.order_id = i.order_id;
    END')
END"""
cursor.execute(create_trigger_query)
conn.commit()
 
conn.commit()
print(" added LastModifiedDate column.")
#Creating trigger to auto-update LastModifiedDate
print("Created or verified trigger to update LastModifiedDate.")
#extraction AND FULL LOAD
query1 = "SELECT * FROM orders"
df = pd.read_sql(query1, conn)
df['last_load'] = datetime.now()
print(df.head())
df.to_sql(name='orders2', con=conn_mysql_engine, if_exists='replace', index=False)
print("Data successfully written to MySQL!")
 