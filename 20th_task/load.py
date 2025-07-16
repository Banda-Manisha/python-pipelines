#load.py

import pyodbc
def load_dim_product(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        try:
            cursor.execute("""
                INSERT INTO dim_product (
                    product_id, product_name, category, product_line, brand,
                    price, sku, description, weight_kg, supplier_id, is_active, stock_level
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.product_id, row.product_name, row.category, row.product_line, row.brand,
                row.price, row.sku, row.description, row.weight_kg, row.supplier_id,
                row.is_active, row.stock_level
            ))
        except pyodbc.IntegrityError:
            print(f"Skipping duplicate product_id: {row.product_id}")
            continue
    conn.commit()
    print("dim_product loaded.")



def load_dim_supplier(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        try:
            cursor.execute("""
                INSERT INTO dim_supplier (
                    supplier_id,
                    supplier_name,
                    contact_email,
                    supplier_country,
                    reliability_score,
                    region_id
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row.supplier_id,
                row.supplier_name,
                row.contact_email,
                row.supplier_country,
                row.reliability_score,
                row.region_id
            ))
        except pyodbc.IntegrityError:
            print(f"Skipping duplicate supplier_id: {row.supplier_id}")
            continue
    conn.commit()
    print("dim_supplier loaded.")


def load_dim_region(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO dim_region (
                region_id, region_name, region_country, regional_manager
            )
            VALUES (?, ?, ?, ?)
        """, (
            row.region_id, row.region_name, row.region_country, row.regional_manager
        ))
    conn.commit()
    print("dim_region loaded.")


def load_dim_promotion(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO dim_promotion (
                promotion_id, promotion_name, discount_percentage, start_date, end_date
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            row.promotion_id, row.promotion_name, row.discount_percentage,
            row.start_date, row.end_date
        ))
    conn.commit()
    print("dim_promotion loaded.")


def load_dim_store(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO dim_store (
                store_id, store_name, city, state, country,
                store_type, manager_name, opening_date, region_id,
                square_footage, employee_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.store_id, row.store_name, row.city, row.state, row.country,
            row.store_type, row.manager_name, row.opening_date, row.region_id,
            row.square_footage, row.employee_count
        ))
    conn.commit()
    print("dim_store loaded.")


def load_dim_time(conn, df):
    cursor = conn.cursor()
    for row in df.itertuples(index=False):
        cursor.execute("""
            INSERT INTO dim_time (
                date_id, date, day_of_week, month, quarter, year,
                fiscal_year, fiscal_quarter, is_holiday, holiday_name,
                week_number, is_weekend, is_business_day
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.date_id, row.date, row.day_of_week, row.month, row.quarter, row.year,
            row.fiscal_year, row.fiscal_quarter, row.is_holiday, row.holiday_name,
            row.week_number, row.is_weekend, row.is_business_day
        ))
    conn.commit()
    print("dim_time loaded.")


def load_fact_sales(conn, df):
    cursor = conn.cursor()
    success_count = 0
    for row in df.itertuples(index=False):
        try:
            cursor.execute("""
                INSERT INTO fact_sales (
                    sale_id, product_id, date_id, store_id, promotion_id,
                    quantity_sold, discount_applied, tax_amount,
                    net_amount, total_amount, region_id, customer_id, supplier_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.sale_id, row.product_id, row.date_id, row.store_id, row.promotion_id,
                row.quantity_sold, row.discount_applied, row.tax_amount,
                row.net_amount, row.total_amount, row.region_id, row.customer_id, row.supplier_id
            ))
            success_count += 1
        except pyodbc.ProgrammingError as e:
            print(f"ProgrammingError on sale_id {row.sale_id}: {e}")
            continue
        except pyodbc.IntegrityError as e:
            print(f"IntegrityError on sale_id {row.sale_id}: {e}")
            continue
    conn.commit()
    print(f"fact_sales loaded: {success_count} rows")
