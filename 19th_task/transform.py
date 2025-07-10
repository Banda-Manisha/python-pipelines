from connect import connect_to_s3
import base64

def transform_email_with_s3_urls(service, emails):
    s3, bucket = connect_to_s3()


    for email in emails: # loops through each email and intitailises to store all the  uploaded attached urls 
        attachment_urls = []


        for attachment in email['attachments']:# loops through each attachment in the current email 
            att_id = attachment['attachmentId']# gets attachment id and filename from the attachment dict
            filename = attachment['filename']


            att = service.users().messages().attachments().get(  # makes a gmail api call to get attachments details for this specific 
                userId='me',# id
                messageId=email['id'],
                id=att_id# attachment id 
            ).execute()


            file_data=base64.urlsafe_b64decode(att['data'].encode('UTF-8'))# decodes the base 64 attachment to its original binary form 


            #Upload to s3

            s3_key = f'email_attachments/{filename}' # uploads the decoded file to s3bucket 
            s3.put_object(Bucket=bucket, Key = s3_key, Body=file_data) # s3_key is the path to upload like where to upload and # body is the actual content

            #add s3 url to list
            s3_url = f's3://{bucket}/{s3_key}'#creates the s3 url (bucket path) and add it to the list 
            attachment_urls.append(s3_url)

        #attach s3 urls to the email
        email['attachment_1_url'] = attachment_urls[0] if len(attachment_urls) > 0 else None#Adds up to two attachment URLs to the email dictionary
        email['attachment_2_url'] = attachment_urls[1] if len(attachment_urls) > 1 else None

    return emails    # returns the updated list of emails 