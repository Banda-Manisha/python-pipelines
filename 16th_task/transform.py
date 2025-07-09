#transform.py
import re

def parse_text(text):
    project_name = re.search(r'Project Name\s*[:\-]?\s*(.+)', text, re.IGNORECASE)
    client = re.search(r'Client\s*[:\-]?\s*(.+)', text, re.IGNORECASE)
    domain = re.search(r'Domain\s*[:\-]?\s*(.+)', text, re.IGNORECASE)
    technology = re.search(r'Technologies Used\s*[:\-]?\s*(.+)', text, re.IGNORECASE)
    start_date = re.search(r'Start Date\s*[:\-]?\s*(.+)', text, re.IGNORECASE)
    end_date = re.search(r'End Date\s*[:\-]?\s*(.+)', text, re.IGNORECASE)
    
    # Get everything after "Summary:"
    summary = re.search(r'Summary\s*[:\-]?\s*(.+)', text, re.IGNORECASE | re.DOTALL)

    return {
        "Project_Name": project_name.group(1).strip() if project_name else "Not Found",
        "Client_Name": client.group(1).strip() if client else "Not Found",
        "Domain": domain.group(1).strip() if domain else "Not Found",
        "Technology": technology.group(1).strip() if technology else "Not Found",
        "Start_Date": start_date.group(1).strip() if start_date else "Not Found",
        "End_Date": end_date.group(1).strip() if end_date else "Not Found",
        "Summary": summary.group(1).strip() if summary else "Not Found"
    }
