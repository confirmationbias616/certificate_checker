from itertools import chain
import email
import imaplib
import json
import pandas as pd

imap_ssl_host = 'imap.gmail.com'
imap_ssl_port = 993
username = 'dilfo.hb.release'
with open(".password.txt") as file: 
	password = file.read()

def get_job_input_data():
    def parse_email(data):
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_string(response_part[1].decode('UTF-8'))
                sender = msg['from']
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        content = part.get_payload(None, True).decode('UTF-8')
                        break
                    else:
                        content = ''
                return sender, content
    server = imaplib.IMAP4_SSL(imap_ssl_host, imap_ssl_port)
    server.login(username, password)
    server.select('INBOX')
    type, data = server.search(None, 'ALL') # UNSEEN or ALL
    mail_ids = data[0]
    id_list = mail_ids.split()
    results = []
    for i in id_list:
        typ, data = server.fetch(i, '(RFC822)' )
        sender, content = parse_email(data)
        results.append({"sender":sender, "content":content})
    server.logout()
    return results

file_path = './data/raw_dilfo_certs.csv'
df = pd.read_csv(file_path)

for email in get_job_input_data():
    try:
    	dict_input = {x.split('=')[0]:x.split('=')[1] for x in email["content"].strip('\r\n').split('\r\n')}
        dict_input.update({"receiver_email":email["sender"]})
        df = df.append(dict_input, ignore_index=True)
    except IndexError:
        print(f'Could not process e-mail from {email["sender"]}')

df.drop_duplicates(subset=["address", "title", "owner"])
df.to_csv(file_path, index=False)

