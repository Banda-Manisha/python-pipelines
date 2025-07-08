#main.py
from connect import connect_to_s3, get_db_connection, read_config
from extract1 import process_resume_from_s3
from transform1 import parse_resume_text
from load1 import insert_parsed_data, archive_file
import boto3
 
def main():
    resume_key = "Banda_Manisha_Resume.pdf"    
 
    # Step 1: Connect to AWS S3
    s3, bucket_name = connect_to_s3()
    print(f" Connected to S3 bucket: {bucket_name}")
 
    #  Step 2: Diagnostic - List objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    print(f"\n Objects in bucket '{bucket_name}':")
    if "Contents" in response:
        for obj in response["Contents"]:
            print(f"  - {obj['Key']}")
    else:
        print("Bucket is empty or inaccessible.")
 
    print(f" Key being used: '{resume_key}'\n")
 
    # Step 3: Connect to SQL Server
    config = read_config()
    conn = get_db_connection(config)
    print(f" Connected to SQL Server")
 
    try:
        # Step 4: Extract resume text from S3 PDF
        text = process_resume_from_s3(bucket_name, resume_key)
 
        # Step 5: Transform unstructured text into structured data
        parsed_data = parse_resume_text(text)
 
        # Step 6: Load parsed data into SQL Server
        insert_parsed_data(conn, parsed_data)
 
        # Step 7: Archive processed file
        archive_file(s3, bucket_name, resume_key)
 
        print(" Resume ETL pipeline completed successfully.")
 
    except Exception as e:
        print(f"Pipeline failed: {e}")
 
    finally:
        conn.close()
        print(" Database connection closed.")
 
if __name__ == "__main__":
    main()