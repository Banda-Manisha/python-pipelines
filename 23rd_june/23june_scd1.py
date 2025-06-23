import pyodbc

# Connect to SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-C0GHVGK;'
    'DATABASE=pyconnect1;'
    'UID=sa;'
    'PWD=admin@123'
)

cursor = conn.cursor()

print("Connection successful.")

#cursor.execute('''
#          INSERT INTO customers1(customer_id,name,email,address) VALUES(?,?,?,?)
#''',(5,'Lisa Smith','wyoung@example.org','6134 Henson Run, Sarabury, IN 41868')
#)

#cursor.execute('SELECT * FROM customers1') 
#for row in cursor.fetchall():
#      print(row)

#conn.commit()


####################SCD 1 - no previous history 

cursor.execute('''
update customers1 set customer_id = ?,name = ?, email = ?,address= ?
    where customer_id = 1''',(1,'jhane smith','jhanesmith@ex.com','22253 Carolyn Curve Apt. 653 West Rebecca, PA 56512')  )

conn.commit()

cursor.execute('select * from customers1')
for row in cursor.fetchall():
    print(row)
conn.commit

###########SCD 2-  PREVIOUS ROW AND CURRENT ROW WITH START AND END DATE 

cursor.execute(''' ALTER TABLE customers1 ADD
    '''
)