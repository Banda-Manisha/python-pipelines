#main.py

import os
from extract import list_doc_files,download_doc_file,extract_text_from_doc
from transform import parse_text
from load import insert_to_sql
from connect import connect_to_s3,get_db_connection,read_config


def move_file_to_archive(bucket, key):
    s3, _ = connect_to_s3()
    archive_key = key.replace('resumes/','resumes/archive/')
    s3.copy_object(Bucket=bucket, CopySource={'Bucket':bucket, 'Key':key}, Key=archive_key)
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"Moved {key} to {archive_key}")


def main():
    s3, bucket = connect_to_s3()
    doc_files = list_doc_files(bucket)

    if not doc_files:
        print("No DOC files found in s3 bucket")
        return
    
    for key in doc_files:
        print(f"Processing file: {key}")
        filepath = download_doc_file(bucket, key)
        text = extract_text_from_doc(filepath)
        parsed_data = parse_text(text)
        insert_to_sql(parsed_data)
        move_file_to_archive(bucket, key)
        os.remove(filepath)

if __name__ == "__main__":
    main()