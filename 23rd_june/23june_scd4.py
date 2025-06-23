import pyodbc 

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-C0GHVGK;'
    'DATABASE=pyconnect1;'
    'UID=sa;'
    'PWD=admin@123'
)

cursor = conn.cursor()

print('connection success')
"""
cursor.execute('''
create table customer_history(
               id INT IDENTITY(1,1) PRIMARY KEY,
               customer_id INT,
               name VARCHAR(50),
               email NVARCHAR(50),
               phone INT,
               address NVARCHAR(50)
               )'''
)
conn.commit()

cursor.execute(''' alter table customer_history add start_date DATETIME,end_date DATETIME
               ''')

conn.commit()
"""
"""
create_table_query= '''
    CREATE TABLE customer_current(
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

cursor.executemany('''INSERT INTO customer_current(customer_id,name,email,phone,address) 
values(?,?,?,?,?)''',insert_data)

print('inserted')                                      
conn.commit()


cursor.execute('''insert into customer_history(customer_id,name,email,phone,address,start_date,end_date)
               values(?,?,?,?,?,?,getdate())
               ''',(1,'jhane smith','jhanesmith@ex.com',2349622,'mumbai','2025-06-21'))
print("insert")
conn.commit()

cursor.execute('''update customer_current set email=?,address=?
               where customer_id = ?
               ''',('jhanessjaness@mail.com','uk',1))
print('updated current ')
conn.commit()

"""
cursor.execute('''
      insert into customer_history(customer_id,name,email,phone,address,start_date,end_date)  
               values(?,?,?,?,?,getdate(),null)    
    
                          ''',(1,'jhane smith','jhanessjaness@mail.com',2349622,'uk') )
print('insert succedd')
conn.commit()