from connect import get_gmail_service
from extract import extract_email_data
from transform import transform_email_with_s3_urls
from load import load_email_to_sql

def main():
    print("connecting to Gmail")
    service = get_gmail_service()

    print("Extracting recent emails")
    emails = extract_email_data(service, max_results=5)  

    if not emails :
        print("no new emails found.")
        return 
    
    print("uploading attachments to s3")
    transformed_emails = transform_email_with_s3_urls(service, emails)

    print("Inserting emails into sql server")
    load_email_to_sql(transformed_emails)

    print("ETL process completed successfully")


if __name__ == "__main__":
    main()  