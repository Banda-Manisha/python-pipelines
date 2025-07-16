#extract.py
import pandas as pd

def extract_sharepoint_data(list_obj):
    print("Extracting data from SharePoint list...")

    items = list_obj.items.get().execute_query()

    data = []
    for item in items:
        data.append({
            'project_name': item.properties.get('Title', ''),
            'status': item.properties.get('Status', ''),
            'project_manager': item.properties.get('Project_x0020_Manager', ''),
            'start_date': item.properties.get('Start_x0020_Date', ''),
            'end_date': item.properties.get('End_x0020_Date', ''),
            'budget': item.properties.get('Budget', 0),
            'department': item.properties.get('Department', '')
        })

    df = pd.DataFrame(data)
    print("DataFrame created from SharePoint items.")
    return df