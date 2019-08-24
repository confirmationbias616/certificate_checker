#!/usr/bin/env python

from flask import Flask, render_template, url_for, request, redirect
from datetime import datetime
from db_tools import create_connection
from communicator import send_email
from matcher import match
import pandas as pd
import logging
import sys


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


app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def index():
    lookup_url = "https://canada.constructconnect.com/dcn/certificates-and-notices/"
    receiver_email = 'alex.roy616@gmail.com'  # temporary fix
    if request.method == 'POST':
        with create_connection() as conn:
            try:
                row = pd.read_sql("SELECT * FROM company_projects WHERE job_number=?", conn, params=[request.form['job_number']]).iloc[0]
                was_prev_closed = row.closed
                was_prev_logged = 1
            except IndexError:
                was_prev_closed = 0
                was_prev_logged = 0
        try:
            request.form['instant_scan']
            instant_scan = True
        except (IndexError, KeyError):
            instant_scan = False
        if was_prev_closed:
            logger.info("job was already matched successfully and logged as `closed`. Sending e-mail!")
            # Send email to inform of previous match
            with create_connection() as conn:
                prev_match = pd.read_sql(
                    "SELECT * FROM attempted_matches WHERE job_number=? AND ground_truth=1",
                    conn, params=[request.form['job_number']]).iloc[0]
            verifier = prev_match.verifier
            log_date = prev_match.log_date
            dcn_key = prev_match.dcn_key
            return f"Here's your <a href='{lookup_url}{dcn_key}'>certificate</a>!"  # need already_match page!
        with create_connection() as conn:
            df = pd.read_sql("SELECT * FROM company_projects", conn)
            df = df.append(dict(request.form), ignore_index=True)
            #loop through duplicates to drop the first records but retain their contacts
            for dup_i in df[df.duplicated(subset=["job_number"], keep='last')].index:
                dup_job_number = df.iloc[dup_i].job_number
                dup_receiver = df.iloc[dup_i].receiver_email
                dup_cc = df.iloc[dup_i].cc_email
                # next few lines below will need to be refctored big time for clarity!
                a = df.iloc[dup_i].to_dict()
                b = df[df.job_number==request.form['job_number']].iloc[1].to_dict()
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
            if was_prev_logged:
                return change_msg  # need update page!
            else:
                if not instant_scan:
                    return "blahh you're not interested" # need new_job confirmation page!
                dilfo_query = "SELECT * FROM company_projects WHERE job_number=?"
                with create_connection() as conn:
                    company_projects = pd.read_sql(dilfo_query, conn, params=[request.form['job_number']])
                hist_query = "SELECT * FROM dcn_certificates ORDER BY pub_date DESC LIMIT 2000"
                with create_connection() as conn:
                    df_web = pd.read_sql(hist_query, conn)
                results = match(company_projects=company_projects, df_web=df_web, test=False)
                if len(results[results.pred_match==1]) > 0:
                    return "Go see your inbox, there's a potential match!"
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
                    f"Subject: Successful Project Sign-Up: #{request.form['job_number']}"
                    f"\n\n"
                    f"Hi {receiver_email.split('.')[0].title()},"
                    f"\n\n"
                    f"Your information for project #{request.form['job_number']} was "
                    f"{'updated' if was_prev_logged else 'logged'} "
                    f"successfully."
                    f"\n\n"
                    f"{change_msg if was_prev_logged else new_msg}"
                    f"\n\n"
                    f"Thanks,\n"
                    f"HBR Bot\n"
                )
                send_email(receiver_email, message, False)
                return message
    else:
        with open('index.html', 'r') as a:
            return(a.read())

if __name__ == "__main__":
    app.run(debug=True)
