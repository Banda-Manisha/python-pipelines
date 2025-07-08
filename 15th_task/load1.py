#load.py
import os

def insert_parsed_data(conn, parsed):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO Resumes (Name, Email, Summary, Experience, Skills, Education)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        parsed.get('Name'),
        parsed.get('Email'),
        parsed.get('Summary'),
        parsed.get('Experience'),
        parsed.get('Skills'),
        parsed.get('Education')
    ))

    conn.commit()
    cursor.close()
    print("Resume data inserted into SQL Server")


def archive_file(s3, bucket_name, original_key, archive_prefix='archive/'):
    archive_key = archive_prefix + os.path.basename(original_key)
    s3.copy_object(
        Bucket=bucket_name,
        CopySource=f"{bucket_name}/{original_key}",
        Key=archive_key
    )
    s3.delete_object(Bucket=bucket_name, Key=original_key)
    print(f"Archived resume to {archive_key}")
