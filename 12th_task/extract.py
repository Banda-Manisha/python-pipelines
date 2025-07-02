import pandas as pd

from pymongo import MongoClient
 
def connect_to_mongodb():

    client = MongoClient("mongodb://localhost:27017")

    db = client["task10"]

    print("python-mongodb success")

    return db["projects"]
 
def extract_documentt():

    collection = connect_to_mongodb()

    documents = list(collection.find())

    df = pd.DataFrame(documents)

    print("extracted")
 
    # Optional: drop _id if not needed

    if '_id' in df.columns:

        df.drop(columns=['_id'], inplace=True)
 
    print(df.head())

    return df
 