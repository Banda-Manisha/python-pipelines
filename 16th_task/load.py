#load.py
from connect import get_db_connection, read_config

def insert_to_sql(parsed_data):
    config = read_config()
    conn = get_db_connection(config)
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO ProjectDetails (
        [Project_Name],
        [Client_Name],
        [Domain],
        [Technology],
        [Start_Date],
        [End_Date],
        [Summary]
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    cursor.execute(insert_sql, (
        parsed_data['Project_Name'],
        parsed_data['Client_Name'],
        parsed_data['Domain'],
        parsed_data['Technology'],
        parsed_data['Start_Date'],
        parsed_data['End_Date'],
        parsed_data['Summary']
    ))

    conn.commit()
    cursor.close()
    conn.close()


