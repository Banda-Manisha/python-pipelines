import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus  
import  re
import random as ra

server = 'DESKTOP-C0GHVGK'    
database = 'pyconnect1'                     
username = 'sa'                            
password = 'admin@123'                    
driver = 'ODBC Driver 17 for SQL Server'   

encoded_password = quote_plus(password)


connection_string = (
    f"mssql+pyodbc://{username}:{encoded_password}@{server}/{database}"
    f"?driver={quote_plus(driver)}"
)


engine = create_engine(connection_string)

customers_df = pd.read_sql("SELECT * FROM customers", con=engine)
orders_df = pd.read_sql("SELECT * FROM orders", con=engine)



# 1. Name Processing
def clean_and_split_name(name):
    prefixes = ['Mr.', 'Mrs.', 'Miss', 'Dr.']
    suffixes = ['Jr.', 'Sr.', 'II', 'III']
    for prefix in prefixes:
        name = name.replace(prefix, '')
    for suffix in suffixes:
       name = name.replace(suffix, '')
       name = name.strip()




customers_df["name"] = customers_df["name"].astype(str).apply(clean_and_split_name).str.strip().str.title()
customers_df[["First Name", "Last Name"]] = customers_df["name"].str.split(n=1, expand=True)
customers_df['email']= customers_df['email'].fillna((customers_df['First Name'] + customers_df['Last Name']).str.lower() +'@example.com')

def generate_phone():
    return f"{ra.randint(7000000000, 9999999999)}"
customers_df['phone'] = customers_df['phone'].fillna(value=pd.Series([generate_phone() for _ in range(len(customers_df))]))

# Load country codes CSV
countrycodes = pd.read_csv(r"C:\Users\Manish\Downloads\Country-codes.csv", encoding='ISO-8859-1')

# Create mapping from country code to dialing code
country_dialing_map = dict(zip(countrycodes['Country_code'], countrycodes['International_dialing']))


customers_df['country_code'] = customers_df['address'].str.slice(-8, -6)


customers_df['phone'] = customers_df['phone'].astype(str).str.replace(r'\D', '', regex=True)


customers_df['dialing_code'] = customers_df['country_code'].map(country_dialing_map)


customers_df['international_phone'] = customers_df['dialing_code'].fillna('')+'-'+ customers_df['phone']

 

 
 

# 3. Customer Classification
tier_map = {'Gold':2, 'Silver':1,  'Bronze':0}
customers_df['Customer_Tier'] = customers_df['loyalty_status'].map(tier_map)


unified_df = pd.merge(orders_df, customers_df, on='customer_id', how='left')


unified_df.to_sql('unified_customer_view8', con=engine, if_exists='replace', index=False)

