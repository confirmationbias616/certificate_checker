import smtplib, ssl
import datetime
import sys
import logging


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
			server.sendmail(sender_email, [receiver_email], message)
		logger.info(f"Successfully sent an email to {receiver_email}")
	except (FileNotFoundError, NameError):
		logger.info("password not available -> could not send e-mail")

def communicate(web_df, dilfo_row, test=False):
	receiver_email = 'alex.roy616@gmail.com'  # temporary fix
	if (not receiver_email.endswith('@dilfo.com')) and (receiver_email not in[
		'alex.roy616@gmail.com', 'alex.roy616@icloud.com', 'alex.roy616@me.com']):
		logger.info('given user e-mail address has not been white listed (from dilfo.com '\
			'domain or from Alex Roy address)')
		return 1
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
			
	def send_match():
		lookup_url = "https://canada.constructconnect.com/dcn/certificates-and-notices/"
		pub_date = datetime.datetime(
				*[int(web_df.iloc[0].pub_date.split('-')[x]) for x in range(3)]).date()
		due_date = lambda delay: pub_date + datetime.timedelta(days=delay)
		
		intro_msg = (
		    f"From: HBR Bot"
		    f"\n"
		    f"To: {receiver_email}"
			f"\n"
		    f"CC: {cc_email}"
		    f"\n"
		    f"Subject: Upcoming Holdback Release: #{dilfo_row.job_number}"
		    f"\n\n"
		    f"Hi {receiver_email.split('.')[0].title()},"
		    f"\n\n"
		    f"It looks like your project #{dilfo_row.job_number} - "
			f"{dilfo_row.title.title()} might be almost ready for holdback release!"
		    f"\n"
		)
		cert_msg = (
			f"Before going any further, please follow the link below to make sure the "
			f"algorithm correctly matched the project in question:\n{lookup_url}/{web_df.dcn_key.iloc[0]}\n"
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
		feedback_msg = (
			"Your feedback will be required so the service can properly "
			"handle this ticket, whether that means closing it out or keep "
			"searching for new matches. It will also help improve the "
			"matching algorithm for future projects.\n"
			"\n"
			"Please respond to this email with one of the numbers from below, "
			"which corrsponds to your situation with regards to the proposed "
			"link:\n"
			"0)\tlink does not relate to my project\n"
			"1)\tlink is accurate match for my project\n"
			"2)\tlink is close but seems to relate to a different phase or "
			"stage.\n"
			"\n"
			"Whether you were the main receiver or just in CC, your"
			"feedback is greatly apreciated."
			"\n"	
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
