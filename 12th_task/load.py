import boto3 
import pandas as pd
from extract import extract_documentt

def connect_to_dynamodb():
    dynamodb = boto3.resource('dynamodb',region_name='ap-south-1')
    
    table = dynamodb.Table('projectCatalog')
    print("connected to dynamodb")

    return table

def load_to_dynamodb(df,table):
    for index,row in df.iterrows():
        item ={col:str(row[col]) for col in df.columns}

        table.put_item(Item=item)
    print("data load completed")
"""
df = extract_documentt()
dynamo_table = connect_to_dynamodb()
load_to_dynamodb(df, dynamo_table)
"""