from itertools import chain
import email
import imaplib
import json
import pandas as pd
import numpy as np
import re
from urllib.parse import unquote
from db_tools import create_connection
from matcher import match
from communicator import send_email
import traceback
import datetime
import os
import sys
import logging
import smtplib, ssl


logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s "
        "- line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

lookup_url = "https://canada.constructconnect.com/dcn/certificates-and-notices/"
imap_ssl_host = 'imap.gmail.com'
imap_ssl_port = 993
username = 'dilfo.hb.release'
try:
    with open(".password.txt") as file: 
        password = file.read()
except FileNotFoundError:  # no password if running in CI
    pass

def process_as_form(email_obj, test=False):
    dict_input = {
        unquote(x.split('=')[0]):str(unquote(x.split('=')[1])).replace('+', ' ') for x in email_obj['content'].split('&')}
    job_number = dict_input['job_number']
    with create_connection() as conn:
        try:
            was_prev_closed = pd.read_sql("SELECT * FROM company_projects WHERE job_number=?", conn, params=[job_number]).iloc[0].closed
            was_prev_logged = 1
        except IndexError:
            was_prev_closed = 0
            was_prev_logged = 0
    receiver_email = re.findall('<?(\S+@\S+\.\w+)>?', email_obj["sender"])[0].lower()
    dict_input.update({"receiver_email": receiver_email})
    try:
        if dict_input['cc_email'] != '':
            dict_input['cc_email'] += '@dilfo.com'
    except KeyError:
        pass
    try:
        dcn_key = dict_input.pop('link_to_cert')
    except (IndexError, KeyError):
        dcn_key = ''
    if dcn_key:
        try:
            dcn_key = dcn_key.split('-notices/')[1]
        except IndexError:
            pass
        dcn_key = re.findall('[\w-]*',dcn_key)[0]
    try:
        dict_input.pop('instant_scan')
        instant_scan = True
    except (IndexError, KeyError):
        instant_scan = False
    if was_prev_closed:
        logger.info("job was already matched successfully and logged as `closed`. Sending e-mail!")
        # Send email to inform of previous match
        with create_connection() as conn:
            prev_match = pd.read_sql(
                "SELECT * FROM attempted_matches WHERE job_number=? AND ground_truth=1",
                conn, params=[job_number]).iloc[0]
        verifier = prev_match.verifier
        log_date = prev_match.log_date
        dcn_key = prev_match.dcn_key
        message = (
        f"From: HBR Bot"
        f"\n"
        f"To: {receiver_email}"
        f"\n"
        f"Subject: Previously Matched: #{job_number}"
        f"\n\n"
        f"Hi {receiver_email.split('.')[0].title()},"
        f"\n\n"
        f"It looks like "
        f"job #{job_number} corresponds to the following certificate:\n"
        f"{lookup_url}{dcn_key}"
        f"\n\n"
        f"This confirmation was provided by {verifier.split('.')[0].title()}"
        f"{' on ' + log_date if log_date is not None else ''}."
        f"\n\n"
        f"If any of the information above seems to be inaccurate, please reply "
        f"to this e-mail for corrective action."
        f"\n\n"
        f"Thanks,\n"
		f"HBR Bot\n"
        )
        send_email(receiver_email, message, test)
        return
    elif dcn_key:
        dict_input.update({"closed": 1})
        with create_connection() as conn:
            df = pd.read_sql("SELECT * FROM attempted_matches", conn)
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
            df.to_sql('attempted_matches', conn, if_exists='replace', index=False)
    else:
        dict_input.update({"closed": 0})
    with create_connection() as conn:
        df = pd.read_sql("SELECT * FROM company_projects", conn)
        df = df.append(dict_input, ignore_index=True)
        #loop through duplicates to drop the first records but retain their contacts
        for dup_i in df[df.duplicated(subset=["job_number"], keep='last')].index:
            dup_job_number = df.iloc[dup_i].job_number
            dup_receiver = df.iloc[dup_i].receiver_email
            dup_cc = df.iloc[dup_i].cc_email
            
            # next few lines below will need to be refctored big time for clarity!
            a = df.iloc[dup_i].to_dict()
            b = df[df.job_number==job_number].iloc[1].to_dict()
            c = {k: [a[k], b[k]] for k in a if k in b and a[k] != b[k]}
            d = {k: c.get(k,None) for k in ['title', 'city', 'address', 'contractor', 'engineer', 'owner']}
            change_msg = 'Here are the changes you made compared to the prior version:\n'
            no_change = True
            for k in d:
                if d[k]:
                    change_msg += f"  -\t{k} changed from `{d[k][0]}` to `{d[k][1]}`\n"
            if no_change:
                change_msg += "All fields were the exact same as previous version!"
            
            df = df.drop(dup_i)
            try:
                dup_addrs = '; '.join([x for x in dup_cc + dup_receiver if x]) # filter out empty addresses and join them into one string  
                update_i = df[df.job_number==dup_job_number].index
                df.loc[update_i,'cc_email'] = df.loc[update_i,'cc_email'] + '; ' + dup_addrs
            except TypeError:
                pass
        df.to_sql('company_projects', conn, if_exists='replace', index=False)  # we're replacing here instead of appending because of the 2 previous lines
        if instant_scan:
            dilfo_query = "SELECT * FROM company_projects WHERE job_number=?"
            with create_connection() as conn:
                company_projects = pd.read_sql(dilfo_query, conn, params=[job_number])
            hist_query = "SELECT * FROM dcn_certificates ORDER BY pub_date DESC LIMIT 2000"
            with create_connection() as conn:
                df_web = pd.read_sql(hist_query, conn)
            results = match(company_projects=company_projects, df_web=df_web, test=False)
            if len(results[results.pred_match==1]) == 0:
                new_msg = (
                    f"However, no corresponding certificates in recent "
                    f"history were matched to it. "
                    f"Going forward, the Daily Commercial News website will be "
                    f"scraped on a daily basis in search of your project. You "
                    f"will be notified if a possible match has been detected."
                )
                message = (
                    f"From: HBR Bot"
                    f"\n"
                    f"To: {receiver_email}"
                    f"\n"
                    f"Subject: Successful Project Sign-Up: #{job_number}"
                    f"\n\n"
                    f"Hi {receiver_email.split('.')[0].title()},"
                    f"\n\n"
                    f"Your information for project #{job_number} was "
                    f"{'updated' if was_prev_logged else 'logged'} "
                    f"successfully."
                    f"\n\n"
                    f"{change_msg if was_prev_logged else new_msg}"
                    f"\n\n"
                    f"Thanks,\n"
                    f"HBR Bot\n"
                )
                send_email(receiver_email, message, test)

def process_as_reply(email_obj):
    job_number = email_obj['subject'].split(': #')[1]
    feedback = re.findall("^[\W]*([Oo\d]){1}(?=[\W]*)", email_obj['content'].replace('#','').replace('link', ''))[0]
    feedback = int(0 if feedback == ('O' or 'o') else feedback)
    dcn_key = re.findall('\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', email_obj['content'])[0]
    logger.info(f"got feedback `{feedback}` for job #`{job_number}`")
    with create_connection() as conn:
        try:
            was_prev_closed = pd.read_sql("SELECT * FROM company_projects WHERE job_number=?", conn, params=[job_number]).iloc[0].closed
        except IndexError:
            logger.info("job must have been deleted from company_projects at some point... skipping.")
            return
    if was_prev_closed:
        logger.info("job was already matched successfully and logged as `closed`... skipping.")
        return
    if feedback == 1:
        logger.info(f"got feeback that DCN key {dcn_key} was correct")
        update_status_query = "UPDATE company_projects SET closed = 1 WHERE job_number = ?"
        with create_connection() as conn:
            conn.cursor().execute(update_status_query, [job_number])
        logger.info(f"updated company_projects to show `closed` status for job #{job_number}")
    with create_connection() as conn:
        df = pd.read_sql("SELECT * FROM attempted_matches", conn)
        match_dict_input = {
            'job_number': job_number,
            'dcn_key': dcn_key,
            'ground_truth': 1 if feedback == 1 else 0,
            'multi_phase': 1 if feedback == 2 else 0,
            'verifier': email_obj["sender"],
            'source': 'feedback',
            'log_date': str(datetime.datetime.now().date()),
            'validate': 0,
        }
        df = df.append(match_dict_input, ignore_index=True)
        df = df.drop_duplicates(subset=["job_number", "dcn_key"], keep='last')
        df.to_sql('attempted_matches', conn, if_exists='replace', index=False)
        logger.info(
            f"DCN key `{dcn_key}` was a "
            f"{'successful match' if feedback == 1 else 'mis-match'} for job "
            f"#{job_number}"
        )

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
    server = imaplib.IMAP4_SSL(imap_ssl_host, imap_ssl_port)
    server.login(username, password)
    server.select('INBOX')
    _, data = server.search(None, 'UNSEEN')  # UNSEEN or ALL
    mail_ids = data[0]
    id_list = mail_ids.split()
    results = []
    if len(id_list):
        for i, email_id in enumerate(id_list, 1):
            _, data = server.fetch(email_id, '(RFC822)')
            logger.info(f'parsing new email {i} of {len(id_list)}')
            sender, subject, date, content = parse_email(data)
            results.append({"sender": sender, "subject": subject, "date": date, "content": content})
    server.logout()
    return results

def scan_inbox():
    for user_email in get_job_input_data():
        try:    
            if user_email['subject'].startswith("DO NOT MODIFY"):  # e-mail generated by html form
                logger.info(f'processing e-mail from {user_email["sender"]} as user input via html form...')
                process_as_form(user_email)
            elif len(re.findall('\d', user_email['content'])) >= 1:  # True if it's a response to a match notification email
                logger.info(f'processing e-mail from {user_email["sender"]} as user feedback via email response...')
                process_as_reply(user_email)            
        except (IndexError, AttributeError) as e:
            logger.info(e)
            logger.info(traceback.format_exc())
            logger.warning(f'Could not process e-mail from {user_email["sender"]}')

if __name__=="__main__":
    for filename in ['cert_db.sqlite3', 'rf_model.pkl', 'rf_features.pkl']:
        try:
            os.rename('temp_'+filename, filename)
        except:
            pass
    scan_inbox()
