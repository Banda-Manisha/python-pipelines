import pandas as pd
from connect import conn, conn_mysql_engine

# Step 1: Get latest LastModifiedDate from MySQL
latest_query = "SELECT MAX(LastModifiedDate) AS max_date FROM orders2"
latest_df = pd.read_sql(latest_query, conn_mysql_engine)
max_date = latest_df.iloc[0]['max_date']

# ✅ Fix: Convert to native datetime object for SQL Server
if pd.notnull(max_date):
    max_date = pd.to_datetime(max_date).to_pydatetime()

# Step 2: Pull new/updated records from SQL Server
if pd.isnull(max_date):
    print("⚠️ No LastModifiedDate found in MySQL. Doing full load.")
    query = "SELECT * FROM orders"
    df_new = pd.read_sql(query, conn)
else:
    print(f"🔄 Doing incremental load since: {max_date}")
    query = "SELECT * FROM orders WHERE LastModifiedDate > ?"
    df_new = pd.read_sql(query, conn, params=[max_date])

# Step 3: Load new data into MySQL only if there are new rows
if not df_new.empty:
    print(df_new)  # ✅ Print preview of rows to be inserted
    df_new.to_sql('orders2', conn_mysql_engine, if_exists='append', index=False)
    print(f"✅ {len(df_new)} new rows loaded into orders2.")
else:
    print("ℹ️ No new records to sync.")
