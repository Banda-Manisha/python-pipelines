from connect_to_mongodb import extract_table
import pandas as pd
from datetime import datetime

def transform_data():
    df = extract_table()

    # Explode technologies
    df = df.explode("technologies")

    # Convert _id to string
    df['_id'] = df['_id'].astype(str)

    # Today's date
    today = datetime.strptime("2025-06-30", "%Y-%m-%d")

    # --- Flatten client fields ---
    client_names = []
    industries = []
    cities = []
    countries = []

    for client in df['client']:
        if isinstance(client, dict):
            client_names.append(client.get('name'))
            industries.append(client.get('industry'))
            location = client.get('location', {})
            cities.append(location.get('city'))
            countries.append(location.get('country'))
        else:
            client_names.append(None)
            industries.append(None)
            cities.append(None)
            countries.append(None)

    df['client_name'] = client_names
    df['industry'] = industries
    df['city'] = cities
    df['country'] = countries

    # --- Extract and flatten milestone fields ---
    milestone_names = []
    due_dates = []
    statuses = []

    for milestones in df['milestones']:
        if isinstance(milestones, list) and len(milestones) > 0:
            first = milestones[0]
            milestone_names.append(first.get('name'))
            due = datetime.strptime(first['due_date'], "%Y-%m-%d")
            due_dates.append(first['due_date'])
            if due < today:
                statuses.append("Overdue")
            elif (due - today).days <= 30:
                statuses.append("Upcoming")
            else:
                statuses.append("Future")
        else:
            milestone_names.append(None)
            due_dates.append(None)
            statuses.append(None)

    df['first_milestone_name'] = milestone_names
    df['first_milestone_due'] = due_dates
    df['first_milestone_status'] = statuses

    # Drop nested objects
    df = df.drop(columns=['client', 'milestones', 'team'])

    print("\nMerged Project Data Preview:")
    print(df.head())

    return df
