import smtplib, ssl
import datetime
import sys
import logging
import pandas as pd
import ast
from utils import create_connection
import pandas as pd
import json
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From, To
import mysql.connector

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
try:
    with open(".secret.json") as f:
        password = json.load(f)["gmail_password"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    pass

try:
    with open(".secret.json") as f:
        pws = json.load(f)
        mysql_pw = pws["mysql"]
        paw_pw = pws["pythonanywhere"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    pass

def send_email(receiver_email, text_message, job_number, test=False):
    """Sends an e-mail from `hbr.bot.notifier@gmail.com over SMTP protocol.
    
    Parameters:
     - `receiver_email` (dict {str:str}): keys are contact names and values are contact
     email addresses. This specifies the recipient(s) of the email.
     - `html_message` (str): text contained in the email.
     - `job_number` (str): user's job number.
     - `test`: if set to `True`, will short-circuits out of function without doing anything.
    
    """
    message = Mail(
        to_emails=[*receiver_email.values()],
        subject=f"Upcoming Holdback Release: #{job_number}",
        html_content=text_message
    )
    message.from_email = From('notifications@joblert.me', 'HBR-Bot Notifier')
    message.to_email = To([*receiver_email.values()])
    if test:
        return  # escape early
    try:
        with open(".secret.json") as f:
            api_key = json.load(f)["sendgrid_key"]
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)


def communicate(single_web_cert, single_project, test=False):
    """Constructs email message with info pertaining to match of company project and web
    CSP certificate. Sends email accordingly.
    
    Parameters:
     - `single_web_cert` (pd.DataFrame): single-row dataframe containg info of successfully
     matched web CSP certificate.
     - `single_project` (pd.Series): series containg info of successfully matched
     company_project.
     - `test`: if set to `True`, will short-circuits out of function without doing anything.
    
    """
    if len(single_web_cert) > 1:
        raise ValueError(
            f"dataframe passed was suppose to conatin only 1 single row - "
            f"one of them contained {len(single_project)} rows instead."
        )
    if type(single_project) != pd.Series:
        raise TypeError(
            f"`single_project` was supposed to be a pandas series object type."
        )
    receiver_emails_dump = single_project.receiver_emails_dump
    receiver_email = ast.literal_eval(receiver_emails_dump)
    source = single_web_cert.iloc[0].source
    source_base_url_query = "SELECT base_url FROM base_urls WHERE source=%s"
    with create_connection() as conn:
        base_url = pd.read_sql(source_base_url_query, conn, params=[source]).iloc[0].base_url
    url_key = single_web_cert.iloc[0].url_key
    pub_date = datetime.datetime(
        *[int(single_web_cert.iloc[0].pub_date.split("-")[x]) for x in range(3)]
    ).date()
    due_date = lambda delay: pub_date + datetime.timedelta(days=delay)
    with create_connection() as conn:
        project_title = pd.read_sql("SELECT * FROM company_projects WHERE project_id=%s", conn, params=[single_project.project_id]).iloc[0].title
    intro_msg = (
        f"Hi {', '.join(receiver_email.keys())},"
        f"<br><br>"
        f"It looks like your project #{single_project.job_number} "
        f"({project_title}) might be almost ready for holdback release!"
        f"<br>"
    )
    cert_msg = (
        f"Please <a href='{base_url}{url_key}'>click here</a> to make sure "
        f"HBR-Bot correctly matched your project. <b>Don't forget to come back"
        f"when you're done!</b>"
    )
    timing_msg = (
        f"If it's the right project, then the certificate was just published "
        f"on {datetime.datetime.strftime(pub_date,'%B %e, %Y')}. This means a "
        f"valid holdback release invoice could be submitted as of:<br>"
        f"A)&#9;{datetime.datetime.strftime(due_date(45),'%B %e, %Y')} "
        f"if the contract was signed before October 1, 2019 or;<br>"
        f"B)&#9;{datetime.datetime.strftime(due_date(60),'%B %e, %Y')} "
        f"if the contract was signed since then."
        f"<br>"
    )
    link_constructor = "https://www.hbr-bot.ca/process_feedback?project_id={}&job_number={}&response={}&source={}&cert_id={}"
    feedback_msg = (
        f"Your feedback will be required so that HBR Bot can properly "
        f"handle this ticket, whether that means closing it out or keep "
        f"searching for new matches. It will also help improve the "
        f"matching algorithm for future projects.<br>"
        f"<br>"
        f"<a href='{link_constructor.format(single_project.project_id, single_project.job_number, 0, source, single_web_cert.iloc[0].cert_id)}'>link does not relate to my project</a>"
        f"<br>"
        f"<a href='{link_constructor.format(single_project.project_id, single_project.job_number, 1, source, single_web_cert.iloc[0].cert_id)}'>link is accurate match for my project</a>"
        f"<br>"
        f"<a href='{link_constructor.format(single_project.project_id, single_project.job_number, 2, source, single_web_cert.iloc[0].cert_id)}'>link is close but seems to relate to a different phase or stage</a>"
        f"<br><br>"
    )
    disclaimer_msg = (
        "Fianlly, please be aware this is a fully automated message. "
        "The info presented above could be erroneous."
        "<br>"
    )
    closeout_msg = "Thanks,<br>" "HBR Bot<br>"
    message = "<br>".join(
        [intro_msg, cert_msg, timing_msg, feedback_msg, disclaimer_msg, closeout_msg]
    )
    send_email(receiver_email, message, single_project.job_number, test=test)


def process_as_feedback(feedback):
    """Takes in user feedback from web app or clicked emailed link and updates the database accordingly.
    
    Parameters:
    feedback (dict): request.args coming from url clicked by user to submit feedback with
    regards to the quality of the match. User click either comes from the web app or a
    potential match email that would have been sent out.
    
    """
    project_id = feedback["project_id"]
    job_number = feedback["job_number"]
    response = int(feedback["response"])
    source = feedback["source"]
    cert_id = feedback["cert_id"]
    logger.info(f"got feedback `{response}` for job #`{job_number}`")
    with create_connection() as conn:
        try:
            was_prev_closed = (
                pd.read_sql(
                    "SELECT * FROM company_projects WHERE project_id=%s",
                    conn,
                    params=[project_id],
                )
                .iloc[0]
                .closed
            )
        except IndexError:
            logger.info(
                "job must have been deleted from company_projects at some point... skipping."
            )
            return "deleted"
    if was_prev_closed:
        logger.info(
            "job was already matched successfully and logged as `closed`... skipping."
        )
        return "already_closed"
    if response == 1:
        logger.info(f"got feeback that cert_id {cert_id} from {source} was correct")
        update_status_query = (
            "UPDATE company_projects SET closed = 1 WHERE project_id = %s"
        )
        with create_connection() as conn:
            conn.cursor().execute(update_status_query, [project_id])
            conn.commit()
        logger.info(
            f"updated company_projects to show `closed` status for job #{job_number}"
        )
    with create_connection() as conn:
        df = pd.read_sql("SELECT * FROM attempted_matches", conn)
        match_dict_input = {
            "project_id": project_id,
            "cert_id": cert_id,
            "ground_truth": 1 if response == 1 else 0,
            "multi_phase": 1 if response == 2 else 0,
            "log_date": str(datetime.datetime.now().date()),
            "validate": 0,
        }
        df = df.append(match_dict_input, ignore_index=True)
        df = df.drop_duplicates(subset=["project_id", "cert_id"], keep="last")
        df.to_sql("attempted_matches", conn, if_exists="replace", index=False)
        logger.info(
            f"cert_id`{cert_id}` from {source} was a "
            f"{'successful match' if response == 1 else 'mis-match'} for job "
            f"{project_id} (#{job_number})"
        )
