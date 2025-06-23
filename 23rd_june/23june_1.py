import pyodbc

# Connect to SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-C0GHVGK;'
    'DATABASE=pyconnect1;'
    'UID=sa;'
    'PWD=admin@123'
)

# Create a cursor object to run SQL commands
cursor = conn.cursor()

print("Connection successful.")


create_table_query= '''
    CREATE TABLE customers1(
    customer_id INT PRIMARY KEY,
    name VARCHAR(50),
    email NVARCHAR(100),
    address NVARCHAR(100)
    )
'''

conn.execute(create_table_query)
conn.commit()

