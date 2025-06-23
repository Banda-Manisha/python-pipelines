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

# CREATE A TABLE 
"""
create_table_query= '''
    CREATE TABLE customer2(
    row_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT,
    name VARCHAR(50),
    email NVARCHAR(100),
    phone INT,
    address NVARCHAR(100)
    )
'''

conn.execute(create_table_query)
conn.commit()

insert_data = [(1,'jhane smith','jhanesmith@ex.com',2349622,'mumbai'),
               (2,'john doe','johndoe@example.com',123446,'dubai'),
               (3,'Brian Gomez','pittsjennifer@example.com',678888,'usa'),
               (4,'Mary Buck','marybuck@example.com',987654,'delhi'),
               (5,'Lisa Smith','wyoung@example.org',234667,'lucknow')]

cursor.executemany('''INSERT INTO customer2 (customer_id,name,email,phone,address)
               values(?,?,?,?,?)
               ''',insert_data)
conn.commit()

cursor.execute(''' 
Alter table customer2 add previous_email  NVARCHAR(100), previous_address NVARCHAR(100) 
               '''
               )
print('table altered')

conn.commit()
"""
cursor.execute('''update customer2 set previous_email = email, 
               previous_address = address where customer_id = ?
               ''',(1))
print('updated')
conn.commit()

cursor.execute(''' update customer2 set email=?,address=?
               where customer_id = ?
               ''',('jhanesmith12@gmailcom','india',1))
print('updated')
conn.commit()