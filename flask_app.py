#!/usr/bin/env python

from flask import Flask, render_template, url_for, request, redirect
from datetime import datetime
from dateutil.parser import parse as parse_date
from db_tools import create_connection
from matcher import match
from inbox_scanner import process_as_feedback
import pandas as pd
import logging
import sys
import os
import re


app = Flask(__name__)
app.config['SECRET_KEY'] = 'e5ac358c-f0bf-11e5-9e39-d3b532c10a28'

lookup_url = "https://canada.constructconnect.com/dcn/certificates-and-notices/"

@app.context_processor
def override_url_for():
    return dict(url_for=dated_url_for)

def dated_url_for(endpoint, **values):
    if endpoint == 'static':
        filename = values.get('filename', None)
        if filename:
            file_path = os.path.join(app.root_path,
                                 endpoint, filename)
            values['q'] = int(os.stat(file_path).st_mtime)
    return url_for(endpoint, **values)

@app.route('/', methods=['POST', 'GET'])
def index():
    all_contacts_query = "SELECT * FROM contacts"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn)
    if request.method == 'POST':
        selected_contact_ids = request.form.getlist("contacts")
        selected_contacts_query = f"SELECT name, email_addr FROM contacts WHERE id in ({','.join('?'*len(selected_contact_ids))})"
        with create_connection() as conn:
            selected_contacts = pd.read_sql(selected_contacts_query, conn, params=[*selected_contact_ids])
        receiver_emails_dump = str({row['name']: row['email_addr'] for _,row in selected_contacts.iterrows()})
        new_entry = dict(request.form)
        new_entry.pop('contacts')  #useless
        if [True for value in new_entry.values() if type(value) == list]:  # strange little fix
            new_entry = {key:new_entry[key][0] for key in new_entry.keys()}
        new_entry.update({'receiver_emails_dump': receiver_emails_dump})
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
        if was_prev_closed:
            return redirect(url_for('already_matched'))
        with create_connection() as conn:
            df = pd.read_sql("SELECT * FROM company_projects", conn)
            df = df.append(new_entry, ignore_index=True)
            #loop through duplicates to drop the first records but retain their contacts
            for dup_i in df[df.duplicated(subset="job_number", keep='last')].index:
                dup_job_number = df.iloc[dup_i].job_number
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
            df.to_sql('company_projects', conn, if_exists='replace', index=False)  # we're replacing here instead of appending because of the 2 previous lines
            if was_prev_logged:
                return redirect(url_for('update', job_number=new_entry['job_number'], change_msg=change_msg))
            return redirect(url_for('signup_confirmation', job_number=new_entry['job_number']))
    else:
        try:
            return render_template('index.html', home=True, all_contacts=all_contacts, **{key:request.args.get(key) for key in request.args})
        except NameError:
            return render_template('index.html', home=True, all_contacts=all_contacts)

@app.route('/already_matched', methods=['POST', 'GET'])
def already_matched():
    if request.method == 'POST':
        return redirect(url_for('index'))
    job_number = request.args.get('job_number')
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
    job_number = request.args.get('job_number')
    new_msg = (
        f"No corresponding certificates in recent "
        f"history were found as a match for project {job_number}. "
        f"Going forward, the Daily Commercial News website will be "
        f"scraped on a daily basis in search of your project. You "
        f"will be notified if a possible match has been detected."
    )
    return render_template('nothing_yet.html', message=new_msg)

@app.route('/potential_match', methods=['POST', 'GET'])
def potential_match():
    if request.method == 'POST':
        return redirect(url_for('index'))
    job_number = request.args.get('job_number')
    dcn_key = request.args.get('dcn_key')
    return render_template('potential_match.html', job_number=job_number, lookup_url=lookup_url, dcn_key=dcn_key)

@app.route('/update', methods=['POST', 'GET'])
def update():
    # if request.method == 'POST':
    #     return redirect(url_for('index'))
    job_number = request.args.get('job_number')
    change_msg = request.args.get('change_msg')
    return render_template('update.html', job_number=job_number, change_msg=change_msg)

@app.route('/signup_confirmation', methods=['POST', 'GET'])
def signup_confirmation():
    job_number = request.args.get('job_number')
    return render_template('signup_confirmation.html', job_number=job_number)

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
                attempted_matches.dcn_key,
                dcn.pub_date
            FROM (SELECT * FROM dcn_certificates ORDER BY cert_id DESC LIMIT 16000) as dcn
            LEFT JOIN
                attempted_matches
            ON
                dcn.dcn_key = attempted_matches.dcn_key
            LEFT JOIN
                company_projects
            ON
                attempted_matches.job_number=company_projects.job_number
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
        df_closed = pd.read_sql(closed_query, conn).sort_values('job_number', ascending=False)
        df_closed['pub_date'] = df_closed.apply(lambda row: f'''<a href="{lookup_url+row.dcn_key}">{row.pub_date}</a>''', axis=1)
        df_closed = df_closed.drop('dcn_key', axis=1)
        df_open = pd.read_sql(open_query, conn).sort_values('job_number', ascending=False)
        df_open['action'] = df_open.apply(lambda row: f'''<a href="{url_for('index', **row)}">modify</a> / <a href="{url_for('delete_job', job_number=row.job_number)}">delete</a>''', axis=1)
        col_order = ['job_number', 'title', 'contractor', 'engineer', 'owner', 'address', 'city']
        def highlight_pending(s):
            days_old = (datetime.now().date() - parse_date(re.findall('\d{4}-\d{2}-\d{2}',s.pub_date)[0]).date()).days
            if days_old < 245:
                row_colour = ''
            elif days_old < 300:
                row_colour = '#696714'
            else:
                row_colour = '#6b2515'
            return [f'color: {row_colour}' for i in range(len(s))]
        df_closed = df_closed[['pub_date']+col_order].style.set_table_styles([{'selector': 'th','props': [('background-color', 'rgb(122, 128, 138)'),('color', 'black')]}]).set_table_attributes('border="1"').set_properties(**{'font-size': '10pt', 'background-color':'rgb(171, 173, 173)'}).hide_index().apply(highlight_pending, axis=1)
        df_open = df_open[['action']+col_order].style.set_table_styles([{'selector': 'th','props': [('background-color', 'rgb(122, 128, 138)'),('color', 'black')]}]).set_table_attributes('border="1"').set_properties(**{'font-size': '10pt', 'background-color':'rgb(190, 153, 138)'}).hide_index()
    return render_template(
        'summary_table.html',
        df_closed=df_closed.render(escape=False),
        df_open=df_open.render(escape=False)
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

@app.route('/instant_scan', methods=['POST', 'GET'])
def instant_scan():
    if request.method == 'POST':
        job_number = request.args.get('job_number')
        lookback = request.args.get('lookback')
        if lookback=='2_months':
            lookback_cert_count = 3000
        elif lookback=='1_month':
            lookback_cert_count = 1500
        else:  # also applies for `2_weeks`
            lookback_cert_count = 750
        dilfo_query = "SELECT * FROM company_projects WHERE job_number=?"
        with create_connection() as conn:
            company_projects = pd.read_sql(dilfo_query, conn, params=[job_number])
        hist_query = "SELECT * FROM dcn_certificates ORDER BY pub_date DESC LIMIT ?"
        with create_connection() as conn:
            df_web = pd.read_sql(hist_query, conn, params=[lookback_cert_count])
        results = match(company_projects=company_projects, df_web=df_web, test=False)
        if len(results[results.pred_match==1]) > 0:
            dcn_key = results.iloc[0].dcn_key
            return redirect(url_for('potential_match', job_number=job_number, dcn_key=dcn_key))
        return redirect(url_for('nothing_yet'))

@app.route('/process_feedback', methods=['POST', 'GET'])
def process_feedback():
    # if request.method == 'GET':
    process_as_feedback(request.args)
    return redirect(url_for('thanks_for_feedback', job_number=request.args['job_number'], response=request.args['response']))

@app.route('/thanks_for_feedback', methods=['POST', 'GET'])
def thanks_for_feedback():
    return render_template('thanks_for_feedback.html', job_number=request.args['job_number'], response=int(request.args['response']))

@app.route('/about', methods=['POST', 'GET'])
def about():
    return render_template('about.html', about=True, hide_helper_links=True)

@app.route('/contact_config', methods=['POST', 'GET'])
def contact_config():
    contact = request.args
    if [True for value in contact.values() if type(value) == list]:  # strange little fix
        contact = {key:contact[key][0] for key in contact.keys()}
    all_contacts_query = "SELECT * FROM contacts"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn)
    all_contacts['action'] = all_contacts.apply(lambda row: f'''<a href="{url_for('update_contact', **row)}">modify</a> / <a href="{url_for('delete_contact', **row)}">delete</a>''', axis=1)
    all_contacts = all_contacts[['name', 'email_addr', 'action']]
    all_contacts = all_contacts.style.set_table_attributes('border="1"').set_properties(**{'font-size': '10pt'}).hide_index()
    return render_template('contact_config.html', all_contacts=all_contacts.render(escape=False), contact=contact, config=True, hide_helper_links=True)

@app.route('/delete_contact')
def delete_contact():
    delete_contact_query = """
            DELETE FROM contacts
            WHERE id=?
        """
    contact = request.args
    with create_connection() as conn:
        conn.cursor().execute(delete_contact_query, [contact.get('id')])
    return redirect(url_for('contact_config'))

@app.route('/update_contact', methods=['POST', 'GET'])
def update_contact():
    contact = request.args
    add_contact_query = """
        INSERT INTO contacts
        (name, email_addr, id) VALUES(?, ?, ?)
    """
    update_contact_query = """
        UPDATE contacts
        SET (name, email_addr)=(?, ?)
        WHERE id=?
    """
    if request.method == "POST":
        contact = request.form
        with create_connection() as conn:
            conn.cursor().execute(update_contact_query, [contact.get('name'), contact.get('email_addr'), contact.get('id')])
    return redirect(url_for('contact_config', **contact))

@app.route('/add_contact', methods=['POST', 'GET'])
def add_contact():
    contact = request.args
    get_contact_ids = """
        SELECT id FROM contacts
    """
    with create_connection() as conn:
        contact_ids = pd.read_sql(get_contact_ids, conn).sort_values('id')
    add_contact_query = """
        INSERT INTO contacts
        (name, email_addr, id) VALUES(?, ?, ?)
    """
    if request.method == "POST":
        contact = request.form
        if len(contact_ids):
            new_contact_id = int(contact_ids.iloc[-1]+1)
        else:
            new_contact_id = 1
        with create_connection() as conn:
            conn.cursor().execute(add_contact_query, [contact.get('name'), contact.get('email_addr'), new_contact_id])
    return redirect(url_for('contact_config'))


if __name__ == "__main__":
    app.run(debug=True)
