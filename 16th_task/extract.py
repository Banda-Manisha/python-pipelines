#extract.py
import os
import tempfile
from docx import Document 
from connect import connect_to_s3


def list_doc_files(bucket_name, prefix=''):
    s3, _ = connect_to_s3()
    response = s3.list_objects_v2(Bucket=bucket_name,Prefix=prefix)
    print("S3 Contents:", [item['Key'] for item in response.get('Contents', [])])
    doc_files = [item['Key'] for item in response.get('Contents',[]) if item['Key'].endswith('.docx')]
    return doc_files

def download_doc_file(bucket, key):
    s3, _ = connect_to_s3()
    
    # Step 1: Create a named temp file and close it
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".docx")
    tmp_path = tmp_file.name
    tmp_file.close()  

    # Step 2: Download from S3
    s3.download_file(bucket, key, tmp_path)

    return tmp_path


def extract_text_from_doc(filepath):
    doc = Document(filepath)
    full_text = "\n".join([para.text for para in doc.paragraphs])
    return full_text