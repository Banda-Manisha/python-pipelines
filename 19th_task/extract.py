#extract.py

from connect import get_gmail_service
import base64  # is used to decode the email body as gmail API return in the encoded form 
import email #parse raw email formats 


def extract_email_data(service, max_results=5):
    result = service.users().messages().list(userId='me', labelIds=['INBOX'], maxResults=max_results).execute()  
    messages = result.get('messages', []) #returns a list of messages if none found returns empty list 
    
    all_emails = [] # initializing empty list to store details of each email   

    for m in messages:#loops through each message from retrieved 
        msg_id = m['id']
        msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()#gets full details from message such as headers etc

        sender = '' #intialises the variables 
        receiver = ''
        cc = ''
        subject = ''
        body = ''
        attachments = []

        for h in msg['payload'].get('headers', []): #loops through all the headers of the email
            name = h['name'].lower() # coverts header to lower case for easy comparision 
            if name == 'from':
                sender = h['value']
            elif name == 'to':
                receiver = h['value']
            elif name == 'cc':
                cc = h['value']
            elif name == 'subject':
                subject = h['value']

        for part in msg['payload'].get('parts', []):# loops through parts 
            filename = part.get('filename')# gets file name for attachment
            mime_type = part.get('mimeType')
            body_data = part.get('body', {})

            if mime_type == 'text/plain' and 'data' in body_data: #if its plain text decodes and converts to a readable format
                raw_body = body_data['data']
                body = base64.urlsafe_b64decode(raw_body).decode('utf-8')

            if filename and 'attachmentId' in body_data:# stores the filename and attachment id in the list of attachments
                attachments.append({
                    'filename': filename,
                    'attachmentId': body_data['attachmentId']
                })

        email_info = { # stores all the extracted details in a dictionary 
            'id': msg_id,
            'sender': sender,
            'receiver': receiver,
            'cc': cc,
            'subject': subject,
            'body': body,
            'attachments': attachments
        }

        all_emails.append(email_info) # adds current email details to all_email list 

    return all_emails # returns that list 
