#connect.py
import boto3
import configparser
import pyodbc
import os
from dotenv import load_dotenv

# Gmail API
from google.oauth2.credentials import Credentials # to get access token from token.json 
from google_auth_oauthlib.flow import InstalledAppFlow # window for login
from googleapiclient.discovery import build # is used to create a service object that is used to call any google API
from google.auth.transport.requests import Request # is used to refresh the expired credentials 

# Load AWS config from .env file
def load_aws_config(env_file=r'C:\Users\Manish\Desktop\GmailData\config\config.env'):
    load_dotenv(env_file)

    config = {
        'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'region': os.getenv('AWS_REGION'),
        'bucket': os.getenv('BUCKET_NAME')
    }

    if not all(config.values()):
        raise ValueError("Missing one or more AWS configuration variables")

    return config

# S3 connection
def connect_to_s3():
    aws = load_aws_config()

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws['access_key'],
        aws_secret_access_key=aws['secret_key'],
        region_name=aws['region']
    )

    print(f"Connected to S3 bucket: {aws['bucket']}")
    return s3, aws['bucket']

# Read SQL config from .ini file
def read_config(file_path=r'C:\Users\Manish\Desktop\GmailData\config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    print(f"Reading from: {file_path}")
    print("Sections loaded:", config.sections())
    return config

# SQL Server DB connection
def get_db_connection(config):
    sql_config = config['SQL_SERVER']

    conn_str = (
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )

    return pyodbc.connect(conn_str)

# Gmail API connection
def get_gmail_service(credentials_path=r'C:\Users\Manish\Desktop\GmailData\credentials.json', token_path='token.json'):
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly'] # this tells that emails can only be read . do not change or delete them 

    creds = None

    if os.path.exists(token_path):# checking saved token file already existed or not 
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid: # if missing it will try to refresh or generate new credentials 
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request()) 
        else: # if does not refresh it 
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES) # opens a window asking for a login 
            creds = flow.run_local_server(port=0) #after login goes to a local server 

        # Save the credentials for next run
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds) # creates a gmail API service client 
    print("Connected to Gmail API")
    return service
