#load.py

def load_data_to_sql(df, conn):
    try:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Projects2 (
                    project_name,
                    status,
                    project_manager,
                    start_date,
                    end_date,
                    budget
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            row['project_name'],
            row['status'],
            row['project_manager'],
            row['start_date'],
            row['end_date'],
            row['budget']
            )

        conn.commit()
        cursor.close()
        print("Data loaded into SQL Server.")
    except Exception as e:
        print("Failed to load data into SQL Server:", e)
