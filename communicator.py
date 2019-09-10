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

def send_email(receiver_email, message, test):
	try:
		with open(".password.txt") as file: 
			password = file.read()
	except FileNotFoundError:  # no password if running in CI
		pass
	port = 465 # for SSL
	smtp_server = "smtp.gmail.com"
	sender_email = "dilfo.hb.release"
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

def communicate(web_df, dilfo_row, test=False):
	receiver_emails_dump = dilfo_row.receiver_emails_dump
	# receiver_emails_dump = "{'Alex': 'alex.roy616@gmail.com', 'ARoy':'alex.roy616@me.com'}"
	receiver_email = ast.literal_eval(receiver_emails_dump)
	# if (not receiver_email.endswith('@dilfo.com')) and (receiver_email not in[
	# 	'alex.roy616@gmail.com', 'alex.roy616@icloud.com', 'alex.roy616@me.com']):
	# 	logger.info('given user e-mail address has not been white listed (from dilfo.com '\
	# 		'domain or from Alex Roy address)')
	# 	return 1
	cc_email = dilfo_row.cc_email
	if cc_email:
		if cc_email.endswith('@dilfo.com') or (cc_email in[
			'alex.roy616@gmail.com', 'alex.roy616@icloud.com']):
			pass
		else:
			logger.info('given user e-mail address for cc has not been white listed (from dilfo.com '\
				'domain or from Alex Roy address)')
			cc_email = ''
	else:
		cc_email = ''
	source = web_df.iloc[0].source
	source_base_url_query = "SELECT base_url FROM base_urls WHERE source=?"
	with create_connection() as conn:
		base_url = conn.cursor().execute(source_base_url_query, [source]).fetchone()[0]
	url_key = web_df.iloc[0].url_key
			
	def send_match():
		pub_date = datetime.datetime(
				*[int(web_df.iloc[0].pub_date.split('-')[x]) for x in range(3)]).date()
		due_date = lambda delay: pub_date + datetime.timedelta(days=delay)
		
		intro_msg = (
		    f"From: HBR Bot"
		    f"\n"
		    f"To: {', '.join(receiver_email.values())}"
		    f"\n"
		    f"Subject: Upcoming Holdback Release: #{dilfo_row.job_number}"
		    f"\n\n"
		    f"Hi {', '.join(receiver_email.keys())},"
		    f"\n\n"
		    f"It looks like your project #{dilfo_row.job_number} "
			f"({dilfo_row.title.title()}) might be almost ready for holdback release!"
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
			f"\t{link_constructor.format(dilfo_row.job_number, 0, source, url_key)}\n\n"
			f"\t - link is accurate match for my project:\n"
			f"\t{link_constructor.format(dilfo_row.job_number, 1, source, url_key)}\n\n"
			f"\t - link is close but seems to relate to a different phase or "
			f"stage:\n"
			f"\t{link_constructor.format(dilfo_row.job_number, 2, source, url_key)}\n\n"
			f"\n\n"
		)
		disclaimer_msg = (
		    "Fianlly, please be aware this is a fully automated message. "
		    "The info presented above could be erroneous."
			"\n"
		)
		closeout_msg = (
			"Thanks,\n"
			"HBR Bot\n"
		)
		message = '\n'.join([intro_msg, cert_msg, timing_msg, feedback_msg, disclaimer_msg, closeout_msg])
		send_email(receiver_email, message, test)

	send_match()
