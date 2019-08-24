#!/usr/bin/env python

from flask import Flask, render_template, url_for, request, redirect
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from db_tools import create_connection
from communicator import send_email
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
    if request.method == 'POST':
        # with create_connection() as conn:
        #     try:
        #         was_prev_closed = pd.read_sql("SELECT * FROM company_projects WHERE job_number=?", conn, params=[request.form['job_number']]).iloc[0].closed
        #         print(was_prev_closed)
        #     except IndexError:
        #         print('..!!..')
        with create_connection() as conn:
            try:
                row = pd.read_sql("SELECT * FROM company_projects WHERE job_number=?", conn, params=[request.form['job_number']]).iloc[0]
                was_prev_closed = row.closed
                was_prev_logged = 1
            except IndexError:
                was_prev_closed = 0
                was_prev_logged = 0
        receiver_email = 'alex.roy616@gmail.com'
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
            message = (
            f"From: HBR Bot"
            f"\n"
            f"To: {receiver_email}"
            f"\n"
            f"Subject: Previously Matched: #{request.form['job_number']}"
            f"\n\n"
            f"Hi {receiver_email.split('.')[0].title()},"
            f"\n\n"
            f"It looks like "
            f"job #{request.form['job_number']} corresponds to the following certificate:\n"
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
            send_email(receiver_email, message, False)
        return f"Here's your certificate: \n{lookup_url}{dcn_key}"
    else:
        with open('index.html', 'r') as a:
            return(a.read())

# @app.route('/', methods=['POST', 'GET'])
# def index():
#     if request.method == 'POST':
#         task_content = request.form['content']
#         new_task = Todo(content=task_content)

#         try:
#             db.session.add(new_task)
#             db.session.commit()
#             return redirect('/')
#         except:
#             return 'There was an issue adding your task'

#     else:
#         tasks = Todo.query.order_by(Todo.date_created).all()
#         return render_template('index.html', tasks=tasks)


@app.route('/delete/<int:id>')
def delete(id):
    task_to_delete = Todo.query.get_or_404(id)

    try:
        db.session.delete(task_to_delete)
        db.session.commit()
        return redirect('/')
    except:
        return 'There was a problem deleting that task'

@app.route('/update/<int:id>', methods=['GET', 'POST'])
def update(id):
    task = Todo.query.get_or_404(id)

    if request.method == 'POST':
        task.content = request.form['content']

        try:
            db.session.commit()
            return redirect('/')
        except:
            return 'There was an issue updating your task'

    else:
        return render_template('update.html', task=task)


if __name__ == "__main__":
    app.run(debug=True)
