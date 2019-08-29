#!/usr/bin/env python

from flask import Flask, render_template, url_for, request, redirect, session
from datetime import datetime
from db_tools import create_connection
from communicator import send_email
from matcher import match
import pandas as pd
import logging
import sys


app = Flask(__name__)
app.config['SECRET_KEY'] = 'e5ac358c-f0bf-11e5-9e39-d3b532c10a28'

lookup_url = "https://canada.constructconnect.com/dcn/certificates-and-notices/"
receiver_email = 'alex.roy616@gmail.com'

@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        new_entry = dict(request.form)
        if [True for value in new_entry.values() if type(value) == list]:  # strange little fix
            new_entry = {key:new_entry[key][0] for key in new_entry.keys()}
        session['new_entry'] = new_entry
        with create_connection() as conn:
            try:
                row = pd.read_sql("SELECT * FROM company_projects WHERE job_number=?", conn, params=[new_entry['job_number']]).iloc[0]
                was_prev_closed = row.closed
                if not was_prev_closed:  # case where `closed` column is empty
                    was_prev_closed = 0
                    new_entry['closed'] = 0
                was_prev_logged = 1
            except IndexError:
                was_prev_closed = 0
                new_entry['closed'] = 0
                was_prev_logged = 0
        try:
            new_entry['instant_scan']
            instant_scan = True
        except (IndexError, KeyError):
            instant_scan = False
        if was_prev_closed:
            return redirect(url_for('already_matched'))
        with create_connection() as conn:
            df = pd.read_sql("SELECT * FROM company_projects", conn)
            df = df.append(new_entry, ignore_index=True)
            #loop through duplicates to drop the first records but retain their contacts
            for dup_i in df[df.duplicated(subset="job_number", keep='last')].index:
                dup_job_number = df.iloc[dup_i].job_number
                dup_receiver = df.iloc[dup_i].receiver_email
                dup_cc = df.iloc[dup_i].cc_email
                # next few lines below will need to be refctored big time for clarity!
                a = df.iloc[dup_i].to_dict()
                b = df[df.job_number==new_entry['job_number']].iloc[1].to_dict()
                c = {k: [a[k], b[k]] for k in a if k in b and a[k] != b[k]}
                d = {k: c.get(k,None) for k in ['title', 'city', 'address', 'contractor', 'engineer', 'owner']}
                change_msg = 'Here are the changes you made compared to the prior version:\n'
                no_change = True
                for k in d:
                    if d[k]:
                        change_msg += f"  -\t{k} changed from `{d[k][0]}` to `{d[k][1]}`\n"
                        no_change = False
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
                session['change_msg'] = change_msg
                return redirect(url_for('update'))
            if not instant_scan:
                return redirect(url_for('signup_no_action'))
            dilfo_query = "SELECT * FROM company_projects WHERE job_number=?"
            with create_connection() as conn:
                company_projects = pd.read_sql(dilfo_query, conn, params=[new_entry['job_number']])
            hist_query = "SELECT * FROM dcn_certificates ORDER BY pub_date DESC LIMIT 2000"
            with create_connection() as conn:
                df_web = pd.read_sql(hist_query, conn)
            results = match(company_projects=company_projects, df_web=df_web, test=True)
            if len(results[results.pred_match==1]) > 0:
                session['dcn_key'] = results.iloc[0].dcn_key
                return redirect(url_for('potential_match'))
            return redirect(url_for('nothing_yet'))
    else:
        try:
            return render_template('index.html', **{key:request.args.get(key) for key in request.args})
        except NameError:
            return render_template('index.html')

@app.route('/already_matched', methods=['POST', 'GET'])
def already_matched():
    if request.method == 'POST':
        return redirect(url_for('index'))
    job_number = session['new_entry']['job_number']
    with create_connection() as conn:
        prev_match = pd.read_sql(
            "SELECT * FROM attempted_matches WHERE job_number=? AND ground_truth=1",
            conn, params=[job_number]).iloc[0]
    dcn_key = prev_match.dcn_key
    return render_template('already_matched.html', link=lookup_url+dcn_key, job_number=job_number)

@app.route('/nothing_yet', methods=['POST', 'GET'])
def nothing_yet():
    if request.method == 'POST':
        return redirect(url_for('index'))
    job_number = session['new_entry']['job_number']
    new_msg = (
        f"No corresponding certificates in recent "
        f"history were found as a match. "
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
        f"logged successfully."
        f"\n\n"
        f"However, {new_msg}"
        f"\n\n"
        f"Thanks,\n"
        f"HBR Bot\n"
    )
    send_email(receiver_email, message, True)
    return render_template('nothing_yet.html', job_number=job_number, message=new_msg)

@app.route('/potential_match', methods=['POST', 'GET'])
def potential_match():
    if request.method == 'POST':
        return redirect(url_for('index'))
    job_number = session['new_entry']['job_number']
    dcn_key = session['dcn_key']
    return render_template('potential_match.html', job_number=job_number, link=lookup_url+dcn_key)

@app.route('/update', methods=['POST', 'GET'])
def update():
    # if request.method == 'POST':
    #     return redirect(url_for('index'))
    job_number = session['new_entry']['job_number']
    change_msg = session['change_msg']
    return render_template('update.html', job_number=job_number, change_msg=change_msg)

@app.route('/signup_no_action', methods=['POST', 'GET'])
def signup_no_action():
    if request.method == 'POST':
        return redirect(url_for('index'))
    job_number = session['new_entry']['job_number']
    return render_template('signup_no_action.html', job_number=job_number)

@app.route('/summary_table')
def summary_table():
    closed_query = """
            SELECT
                company_projects.job_number,
                company_projects.city,
                company_projects.address,
                company_projects.title,
                company_projects.owner,
                company_projects.contractor,
                company_projects.engineer,
                attempted_matches.dcn_key
            FROM company_projects
            LEFT JOIN
                attempted_matches
            ON
                company_projects.job_number=attempted_matches.job_number
            WHERE
                company_projects.closed=1
            AND
                attempted_matches.ground_truth=1
        """
    open_query = """
            SELECT
                company_projects.job_number,
                company_projects.city,
                company_projects.address,
                company_projects.title,
                company_projects.owner,
                company_projects.contractor,
                company_projects.engineer
            FROM company_projects
            WHERE
                company_projects.closed=0
        """
    with create_connection() as conn:
        pd.set_option('display.max_colwidth', -1)
        df_closed = pd.read_sql(closed_query, conn).sort_values('job_number')
        df_closed['action'] = df_closed.apply(lambda row: f'''<a href="{lookup_url+row.dcn_key}">view</a>''', axis=1)
        df_closed = df_closed.drop('dcn_key', axis=1)
        df_open = pd.read_sql(open_query, conn).sort_values('job_number')
        df_open['action'] = df_open.apply(lambda row: f'''<a href="{url_for('index', **row)}">modify</a> / <a href="{url_for('delete_job', job_number=row.job_number)}">delete</a>''', axis=1)
        col_order = ['action', 'job_number', 'title', 'contractor', 'engineer', 'owner', 'address', 'city']
    return render_template(
        'summary_table.html',
        df_closed=df_closed.to_html(index=False, columns=col_order, justify='center', escape=False),
        df_open=df_open.to_html(index=False, columns=col_order, justify='center', escape=False)
    )

@app.route('/delete_job')
def delete_job():
    delete_job_query = """
            DELETE FROM company_projects
            WHERE job_number=?
        """
    job_number = request.args.get('job_number')
    with create_connection() as conn:
        conn.cursor().execute(delete_job_query, [job_number])
    return redirect(url_for('summary_table'))


if __name__ == "__main__":
    app.run(debug=True)
