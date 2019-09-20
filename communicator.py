import smtplib, ssl
import datetime
import sys
import logging
import ast
from db_tools import create_connection
import pandas as pd


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
    with open(".password.txt") as file:
        password = file.read()
except FileNotFoundError:  # no password if running in CI
    pass


def send_email(receiver_email, message, test=False):
    """Sends an e-mail from `hbr.bot.notifier@gmail.com over SMTP protocol.
    
    Parameters:
     - `receiver_email` (dict {str:str}): keys are contact names and values are contact
     email addresses. This specifies the recipient(s) of the email.
     - `message` (str): text contained in the email.
     - `test`: if set to `True`, will short-circuits out of function without doing anything.
    
    """
    port = 465  # for SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "hbr.bot.notifier"
    if test:
        return  # escape early
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, [*receiver_email.values()], message)
        logger.info(f"Successfully sent an email to {', '.join(receiver_email.keys())}")
    except (FileNotFoundError, NameError):
        logger.info("password not available -> could not send e-mail")


def communicate(single_web_cert_df, single_project_df, test=False):
    """Constructs email message with info pertaining to match of company project and web
    CSP certificate. Sends email accordingly.
    
    Parameters:
     - `single_web_cert_df` (pd.DataFrame): single-row dataframe containg info of successfully
     matched web CSP certificate.
     - `single_project_df` (pd.DataFrame): single-row dataframe containg info of successfully
     matched company_project.
     - `test`: if set to `True`, will short-circuits out of function without doing anything.
    
    """
    if (len(single_web_cert_df) > 1) or (len(single_project_df) > 1):
        raise ValueError(
            f"`company_projects` dataframe was suppose to conatin only 1 single row - "
            f"it contained {len(single_project_df)} rows instead."
        )
    receiver_emails_dump = single_project_df.receiver_emails_dump
    receiver_email = ast.literal_eval(receiver_emails_dump)
    source = single_web_cert_df.iloc[0].source
    source_base_url_query = "SELECT base_url FROM base_urls WHERE source=?"
    with create_connection() as conn:
        base_url = conn.cursor().execute(source_base_url_query, [source]).fetchone()[0]
    url_key = single_web_cert_df.iloc[0].url_key
    pub_date = datetime.datetime(
        *[int(single_web_cert_df.iloc[0].pub_date.split("-")[x]) for x in range(3)]
    ).date()
    due_date = lambda delay: pub_date + datetime.timedelta(days=delay)

    intro_msg = (
        f"From: HBR Bot"
        f"\n"
        f"To: {', '.join(receiver_email.values())}"
        f"\n"
        f"Subject: Upcoming Holdback Release: #{single_project_df.job_number}"
        f"\n\n"
        f"Hi {', '.join(receiver_email.keys())},"
        f"\n\n"
        f"It looks like your project #{single_project_df.job_number} "
        f"({single_project_df.title.title()}) might be almost ready for holdback release!"
        f"\n"
    )
    cert_msg = (
        f"Before going any further, please follow the link below to make sure the "
        f"algorithm correctly matched project in question:\n{base_url}{url_key}\n"
    )
    timing_msg = (
        f"If it's the right project, then the certificate was just published "
        f"on {datetime.datetime.strftime(pub_date,'%B %e, %Y')}. This means a "
        f"valid holdback release invoice could be submitted as of:\n"
        f"A)\t{datetime.datetime.strftime(due_date(45),'%B %e, %Y')} "
        f"if the contract was signed before October 1, 2019 or;\n"
        f"B)\t{datetime.datetime.strftime(due_date(60),'%B %e, %Y')} "
        f"if the contract was signed since then."
        f"\n"
    )
    link_constructor = "https://www.hbr-bot.com/process_feedback?job_number={}&response={}&source={}&url_key={}"
    feedback_msg = (
        f"Your feedback will be required so that HBR Bot can properly "
        f"handle this ticket, whether that means closing it out or keep "
        f"searching for new matches. It will also help improve the "
        f"matching algorithm for future projects.\n"
        f"\n"
        f"Please click on 1 of the 3 links below to submit your response "
        f"with regards to this match.\n\n"
        f"\t - link does not relate to my project:\n"
        f"\t{link_constructor.format(single_project_df.job_number, 0, source, url_key)}\n\n"
        f"\t - link is accurate match for my project:\n"
        f"\t{link_constructor.format(single_project_df.job_number, 1, source, url_key)}\n\n"
        f"\t - link is close but seems to relate to a different phase or "
        f"stage:\n"
        f"\t{link_constructor.format(single_project_df.job_number, 2, source, url_key)}\n\n"
        f"\n\n"
    )
    disclaimer_msg = (
        "Fianlly, please be aware this is a fully automated message. "
        "The info presented above could be erroneous."
        "\n"
    )
    closeout_msg = "Thanks,\n" "HBR Bot\n"
    message = "\n".join(
        [
            intro_msg,
            cert_msg,
            timing_msg,
            feedback_msg,
            disclaimer_msg,
            closeout_msg,
        ]
    )
    send_email(receiver_email, message, test=test)

def process_as_feedback(feedback):
    """Takes in user feedback from web app or clicked emailed link and updates the database accordingly.
    
    Parameters:
    feedback (dict): request.args coming from url clicked by user to submit feedback with
    regards to the quality of the match. User click either comes from the web app or a
    potential match email that would have been sent out.
    
    """
    imap_ssl_host = "imap.gmail.com"
    imap_ssl_port = 993
    username = "hbr.bot.notifier"
    job_number = feedback["job_number"]
    response = int(feedback["response"])
    source = feedback["source"]
    url_key = feedback["url_key"]
    logger.info(f"got feedback `{response}` for job #`{job_number}`")
    with create_connection() as conn:
        try:
            was_prev_closed = (
                pd.read_sql(
                    "SELECT * FROM company_projects WHERE job_number=?",
                    conn,
                    params=[job_number],
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
        logger.info(f"got feeback that url key {url_key} from {source} was correct")
        update_status_query = (
            "UPDATE company_projects SET closed = 1 WHERE job_number = ?"
        )
        with create_connection() as conn:
            conn.cursor().execute(update_status_query, [job_number])
        logger.info(
            f"updated company_projects to show `closed` status for job #{job_number}"
        )
    with create_connection() as conn:
        df = pd.read_sql("SELECT * FROM attempted_matches", conn)
        match_dict_input = {
            "job_number": job_number,
            "url_key": url_key,
            "ground_truth": 1 if response == 1 else 0,
            "multi_phase": 1 if response == 2 else 0,
            "log_date": str(datetime.datetime.now().date()),
            "validate": 0,
        }
        df = df.append(match_dict_input, ignore_index=True)
        df = df.drop_duplicates(subset=["job_number", "url_key"], keep="last")
        df.to_sql("attempted_matches", conn, if_exists="replace", index=False)
        logger.info(
            f"URL key `{url_key}` from {source} was a "
            f"{'successful match' if response == 1 else 'mis-match'} for job "
            f"#{job_number}"
        )
