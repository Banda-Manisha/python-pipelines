#main.py
from connect import read_config, get_sharepoint_context, get_db_connection
from extract import extract_sharepoint_data
from trans import transform_data
from load import load_data_to_sql

def main():
    print("Starting SharePoint ETL Process...")

    # Load config
    config = read_config()
 
    # Connect to SharePoint
    ctx = get_sharepoint_context(config)
    print("Connected to SharePoint site.")

    # Get SharePoint list object
    list_obj = ctx.web.lists.get_by_title(config['sharepoint']['list_name'])

    # Extract data from SharePoint list
    df = extract_sharepoint_data(list_obj)

    # Check if DataFrame is empty
    if df.empty:
        print("No data extracted. ETL stopped.")
        return

    # Transform the data
    df_transformed = transform_data(df)

    # Connect to SQL Server and load the data
    conn = get_db_connection(config)
    load_data_to_sql(df_transformed, conn)

    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()   
