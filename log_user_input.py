from itertools import chain
import email
import imaplib
import json
import pandas as pd
import numpy as np
import re
from urllib.parse import unquote
from db_tools import create_connection
import traceback
import datetime
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

imap_ssl_host = 'imap.gmail.com'
imap_ssl_port = 993
username = 'dilfo.hb.release'
try:
    with open(".password.txt") as file: 
        password = file.read()
except FileNotFoundError:  # no password if running in CI
    pass

def parse_email(data):
    for response_part in data:
        if isinstance(response_part, tuple):
            msg = email.message_from_string(response_part[1].decode('UTF-8'))
            sender = msg['from']
            subject = msg['subject']
            date = msg['date']
            for part in msg.walk():
                if part.get_content_type() == 'text/plain':
                    content = part.get_payload(None, True).decode('UTF-8')
                    break
                else:
                    content = ''
            return sender, subject, date, content

def get_job_input_data():
    logger.info('scanning inbox and fetching new email...')
    server = imaplib.IMAP4_SSL(imap_ssl_host, imap_ssl_port)
    server.login(username, password)
    server.select('INBOX')
    _, data = server.search(None, 'UNSEEN')  # UNSEEN or ALL
    mail_ids = data[0]
    id_list = mail_ids.split()
    if len(id_list) == 0:
        logger.info(f'no new e-mails to process')
    results = []
    for i, email_id in enumerate(id_list, 1):
        _, data = server.fetch(email_id, '(RFC822)')
        logger.info(f'parsing new email {i} of {len(id_list)}')
        sender, subject, date, content = parse_email(data)
        results.append({"sender": sender, "subject": subject, "date": date, "content": content})
    server.logout()
    return results

def process_as_form(email_obj):
    dict_input = {
        unquote(x.split('=')[0]):str(unquote(x.split('=')[1])).replace('+', ' ') for x in email_obj['content'].split('&')}
    job_number = dict_input['job_number']
    with create_connection() as conn:
        try:
            was_prev_closed = pd.read_sql(f"SELECT * FROM df_dilfo WHERE job_number={job_number}", conn).iloc[0].closed
        except IndexError:
            was_prev_closed = 0
    if was_prev_closed:
        logger.info(f"job was already matched successfully and logged as `closed`... skipping.")
        return
    try:
        if dict_input['cc_email'] != '':
            dict_input['cc_email'] += '@dilfo.com'
    except KeyError:
        pass
    try:
        dcn_key = re.findall('[\w-]*',dict_input.pop('link_to_cert'))[0]
    except (IndexError, KeyError):
        dcn_key = ''
    dict_input.update({"receiver_email": re.findall('<?(\S+@\S+\.\w+)>?', email_obj["sender"])[0].lower()})
    dict_input.update({"log_date": email_obj["date"]})
    if dcn_key:
        dict_input.update({"closed": 1})
        with create_connection() as conn:
            df = pd.read_sql("SELECT * FROM df_matched", conn)
            match_dict_input = {
                'job_number': dict_input['job_number'],
                'dcn_key': dcn_key,
                'ground_truth': 1,
                'verifier': dict_input['receiver_email'],
                'source': 'input',
                'log_date': str(datetime.datetime.now().date()),
                'validate': 0,
            }
            df = df.append(match_dict_input, ignore_index=True)
            df = df.drop_duplicates(subset=["job_number", "dcn_key"], keep='last')
            df.to_sql('df_matched', conn, if_exists='replace', index=False)
    else:
        dict_input.update({"closed": 0})
    with create_connection() as conn:
        df = pd.read_sql("SELECT * FROM df_dilfo", conn)
        df = df.append(dict_input, ignore_index=True)
        #loop through duplicates to drop the first records but retain their contacts
        for dup_i in df[df.duplicated(subset=["job_number"], keep='last')].index:
            dup_job_number = df.iloc[dup_i].job_number
            dup_receiver = df.iloc[dup_i].receiver_email
            dup_cc = df.iloc[dup_i].cc_email
            df = df.drop(dup_i)
            try:
                dup_addrs = '; '.join([x for x in dup_cc + dup_receiver if x]) # filter out empty addresses and join them into one string  
                update_i = df[df.job_number==dup_job_number].index
                df.loc[update_i,'cc_email'] = df.loc[update_i,'cc_email'] + '; ' + dup_addrs
            except TypeError:
                pass
        df.to_sql('df_dilfo', conn, if_exists='replace', index=False)  # we're replacing here instead of appending because of the 2 previous lines

def process_as_reply(email_obj):
    job_number = re.findall("(?<=#)[\d]+(?= - Upc)", email_obj['subject'])[0]
    feedback = re.findall("^[\W]*([Oo\d]){1}(?=[\W]*)", email_obj['content'].replace('#','').replace('link', ''))[0]
    feedback = int(0 if feedback == ('O' or 'o') else feedback)
    dcn_keys = dict(enumerate(re.findall('\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', email_obj['content']),1))
    logger.info(f"got feedback `{feedback}` for job #`{job_number}`")
    with create_connection() as conn:
        was_prev_closed = pd.read_sql(f"SELECT * FROM df_dilfo WHERE job_number={job_number}", conn).iloc[0].closed
    if was_prev_closed:
        logger.info(f"job was already matched successfully and logged as `closed`... skipping.")
        return
    if feedback > 0:
        logger.info(f"got feeback that the following DCN key {dcn_keys[feedback]} was correct")
        update_status_query = "UPDATE df_dilfo SET closed = 1 WHERE job_number = {}"
        with create_connection() as conn:
            conn.cursor().execute(update_status_query.format(job_number))
        logger.info(f"updated df_dilfo to show `closed` status for job #{job_number}")
    for i, dcn_key in dcn_keys.items():
        with create_connection() as conn:
            df = pd.read_sql("SELECT * FROM df_matched", conn)
            match_dict_input = {
                'job_number': job_number,
                'dcn_key': dcn_key,
                'ground_truth': 1 if i == feedback else 0,
                'verifier': email_obj["sender"],
                'source': 'feedback',
                'log_date': str(datetime.datetime.now().date()),
                'validate': 0,
            }
            df = df.append(match_dict_input, ignore_index=True)
            df = df.drop_duplicates(subset=["job_number", "dcn_key"], keep='last')
            df.to_sql('df_matched', conn, if_exists='replace', index=False)
        logger.info(f"DCN key `{dcn_key}` was a {'successful match' if i == feedback else 'mis-match'} for job #{job_number}")
    logger.info(f"updated df_matched with user feedback for job #{job_number}")

def log_user_input():    
    for email_obj in get_job_input_data():
        try:    
            if email_obj['subject'].startswith("DO NOT MODIFY"):  # e-mail generated by html form
                logger.info(f'processing e-mail from {email_obj["sender"]} as user input via html form')
                process_as_form(email_obj)
            elif len(re.findall('\d', email_obj['content'])) >= 1:  # True if it's a response to a match notification email
                logger.info(f'processing e-mail from {email_obj["sender"]} as user feedback via email response')
                process_as_reply(email_obj)
        except IndexError as e:
            logger.info(e)
            logger.info(traceback.format_exc())
            logger.warning(f'Could not process e-mail from {email_obj["sender"]}')

if __name__ == '__main__':
    log_user_input()

