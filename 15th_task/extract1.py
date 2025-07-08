#extract1.py
import os
import pdfplumber
import boto3
from botocore.exceptions import ClientError
from connect import connect_to_s3
import tempfile
 
def download_resume(bucket_name, key, local_path):
    s3 = boto3.client("s3")
 
    try:
        # Confirm object exists
        s3.head_object(Bucket=bucket_name, Key=key)
        print(f"Confirmed object exists: {key}")
 
        # Proceed with download
        s3.download_file(bucket_name, key, local_path)
        print(f"Downloaded {key} to {local_path}")
 
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f" Object not found: {key} in bucket {bucket_name}")
        else:
            print(f" Unexpected error: {e}")
        raise

def extract_text_from_pdf(pdf_path):
    if not os.path.exists(pdf_path):
        print(f"PDF file not found at {pdf_path}")
        return ""
 
    with pdfplumber.open(pdf_path) as pdf:
        if not pdf.pages:
            print(" PDF has no pages.")
            return ""
        text = ''.join(page.extract_text() or '' for page in pdf.pages)
 
    print("Text extraction completed.")
    return text
 
def save_text_to_file(text, txt_path):
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f" Saved extracted text to {txt_path}")
 
def process_resume_from_s3(bucket_name, key):
    print(f" Processing resume: bucket = {bucket_name}, key = {key}")
 
    filename = os.path.basename(key)
    base_filename = os.path.splitext(filename)[0]
    temp_dir = tempfile.gettempdir()
    local_pdf_path = os.path.join(temp_dir, filename)
    local_txt_path = os.path.join(temp_dir, f"{base_filename}.txt")
 
    try:
        download_resume(bucket_name, key, local_pdf_path)
        text = extract_text_from_pdf(local_pdf_path)
        save_text_to_file(text, local_txt_path)
        return text
    except ClientError as e:
        print(f"Resume processing failed: {e}")
        return ""