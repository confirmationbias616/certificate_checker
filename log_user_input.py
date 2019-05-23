from itertools import chain
import email
import imaplib
import json
import pandas as pd
import numpy as np
import re
from urllib.parse import unquote
from db_tools import create_connection

import sys
import logging
logger = logging.getLogger(__name__)


log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)


def log_user_input():    
    imap_ssl_host = 'imap.gmail.com'
    imap_ssl_port = 993
    username = 'dilfo.hb.release'
    with open(".password.txt") as file: 
        password = file.read()
    def get_job_input_data():
        logger.info('scanning inbox and fetching new email...')
        def parse_email(data):
            for response_part in data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_string(response_part[1].decode('UTF-8'))
                    sender = msg['from']
                    subject = msg['subject']
                    for part in msg.walk():
                        if part.get_content_type() == 'text/plain':
                            content = part.get_payload(None, True).decode('UTF-8')
                            break
                        else:
                            content = ''
                    return sender, subject, content
        server = imaplib.IMAP4_SSL(imap_ssl_host, imap_ssl_port)
        server.login(username, password)
        server.select('INBOX')
        _, data = server.search(None, 'UNSEEN')  # UNSEEN or ALL
        mail_ids = data[0]
        id_list = mail_ids.split()
        if len(id_list) == 0:
            logger.info(f'no new e-mails to process')
        results = []
        for i in id_list:
            _, data = server.fetch(i, '(RFC822)')
            logger.info(f'parsing new email {int(i)} of {len(id_list)}')
            sender, subject, content = parse_email(data)
            results.append({"sender": sender, "subject": subject, "content": content})
        server.logout()
        return results

    for email_obj in get_job_input_data():
        table = 'dilfo_open'  # by defualt, log new entries in the open table
        try:
            dict_input = {
                unquote(x.split('=')[0]):str(unquote(x.split('=')[1])).replace('+', ' ') for x in email_obj['content'].split('&')}            
            try:
                if dict_input['cc_email'] != '':
                    dict_input['cc_email'] += '@dilfo.com'
            except KeyError:
                pass
            dict_input.update({"receiver_email": re.compile('<?(\S+@\S+\.\w+)>?').findall(email_obj["sender"])[0].lower()})
            try:  # if entry was indicated as being a test_entry by form filler
                table = 'dilfo_matched' if 'yes' in dict_input['test_entry'] else table
            except KeyError:
                table = table
            with create_connection() as conn:
                df = pd.read_sql(f"SELECT * FROM {table}", conn)
                df = df.append(dict_input, ignore_index=True)
                df = df.dropna(thresh=7).drop_duplicates(subset=["job_number"], keep='last')
                df.to_sql(table, conn, if_exists='replace', index=False)  # we're replacing here instead of appending because of the 2 previous lines
            logger.info(f"saved job {dict_input['job_number']} in table `{table}`")
        except IndexError:
            logger.info(f'Could not process e-mail from {email_obj["sender"]}')


if __name__ == '__main__':
    log_user_input()

