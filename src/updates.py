# scd_update.py
from connect import conn
from datetime import datetime
import uuid
"""
cursor = conn.cursor()

# === INPUTS ===
customer_id = 351
new_status = 'completed'
now = datetime.now()
new_order_id = str(uuid.uuid4())  

# === SCD TYPE 1 === (Overwrite data, no history)
print("\n--- SCD TYPE 1 ---")
cursor.execute('''
UPDATE orders
SET order_status = ?
WHERE customer_id = ?
''', (new_status, customer_id))
conn.commit()
print("SCD1: Updated order_status directly without keeping history.")
"""
"""
# === SCD TYPE 2 === (Preserve history with new row)
print("\n--- SCD TYPE 2 ---")
cursor.execute('''
SELECT * FROM orders
WHERE customer_id = ? AND is_current = 1
''', (customer_id))
current_row = cursor.fetchone()

if current_row:
    existing_status = current_row[2]  # assuming order_status is column index 2
    if existing_status != new_status:
        cursor.execute('''
        UPDATE orders
        SET is_current = 0,start_date=?, end_date = ?
        WHERE customer_id = ? AND is_current = 1
        ''', ('2025-06-20',now, customer_id))

        cursor.execute('''
        INSERT INTO orders (customer_id, order_status, start_date, end_date, is_current)
        VALUES (?, ?, ?, ?, ?)
        ''', (customer_id, new_status, now, None, 1))
        conn.commit()
        print("SCD2: Updated old row to inactive and inserted new version.")
    else:
        print("SCD2: No change in order_status. Nothing done.")
else:
    cursor.execute('''
    INSERT INTO orders (customer_id, order_status, start_date, end_date, is_current)
    VALUES (?, ?, ?, ?, ?)
    ''', (customer_id, new_status, now, None, 1))
    conn.commit()
    print("SCD2: First-time insert done.")
"""


cursor = conn.cursor()

customer_id = 351
new_status = 'completed'
now = datetime.now()
new_order_id = str(uuid.uuid4())

cursor.execute('''
SELECT * FROM orders
WHERE customer_id = ? AND is_current = 1
''', (customer_id,))
current_row = cursor.fetchone()

if current_row:
    existing_status = current_row[3]  # adjust index to match order_status
    if existing_status != new_status:
        # Close old row
        cursor.execute('''
        UPDATE orders
        SET is_current = 0, end_date = ?
        WHERE customer_id = ? AND is_current = 1
        ''', (now, customer_id))

        # Insert new row with UUID
        cursor.execute('''
        INSERT INTO orders (order_id, customer_id, order_status, start_date, end_date, is_current)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (new_order_id, customer_id, new_status, now, None, 1))

        conn.commit()
        print("✅ SCD2: Old row closed, new version inserted.")
    else:
        print("⚠️ SCD2: No change.")
else:
    # First-time insert
    cursor.execute('''
    INSERT INTO orders (order_id, customer_id, order_status, start_date, end_date, is_current)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (new_order_id, customer_id, new_status, now, None, 1))
    conn.commit()
    print("✅ SCD2: First-time insert.")

















"""


# === SCD TYPE 3 === (Store current and previous value in same row)
print("\n--- SCD TYPE 3 ---")
cursor.execute('''
SELECT order_status FROM orders
WHERE customer_id = ? AND is_current = 1
''', (customer_id,))
row = cursor.fetchone()

if row:
    current_status = row[0]
    if current_status != new_status:
        cursor.execute('''
        UPDATE orders
        SET previous_order_status = order_status,
            order_status = ?
        WHERE customer_id = ? AND is_current = 1
        ''', (new_status, customer_id))
        conn.commit()
        print("SCD3: Updated order_status and saved previous in previous_order_status.")
    else:
        print("SCD3: No change in status.")
else:
    print("SCD3: No row found.")
    """