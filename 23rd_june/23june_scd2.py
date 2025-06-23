import pyodbc

conn= pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-C0GHVGK;'
    'DATABASE=pyconnect1;'
    'UID=sa;'
    'PWD=admin@123'
)

cursor= conn.cursor()
print('connection success')


# step1 : alter table by adding 3 new colms
"""
cursor.execute(''' ALTER TABLE customers1 ADD start_date DATE, end_date DATE, is_current VARCHAR(5)
            ''')
print('table alterd ')
conn.commit()

# now updating the other values so that i can see their start_date,end_date and current_status
cursor.execute('''
     UPDATE customers1
   SET start_date = COALESCE(start_date, GETDATE()),
        end_date = COALESCE(end_date, NULL),
     is_current = COALESCE(is_current, 'Yes');''')
conn.commit()
"""
#now i want to add end_date and change current status to no for customer_id = 1 -- PAST DATA
cursor.execute(''' UPDATE customers1 set start_date = ?,end_date = getdate(),is_current= ?
               where customer_id = ?
              ''' ,('2025-06-20','no',1))
conn.commit()


# now  added new row for current data -- CURRENT DATA 
cursor.execute(''' insert into customers1(customer_id,name,email,address,start_date,end_date,is_current) 
               values(?,?,?,?,?,NULL,?)
               ''',(1,'jhane lucy','jhanes@example.com','22253 Carolyn Curve Apt. 653 West Rebecca, PA 56512',
                   '2025-06-23', 'yes'))
conn.commit()

# to fetch values 
cursor.execute('select * from customers1')
for row in cursor.fetchall():
   print(row)
conn.commit()