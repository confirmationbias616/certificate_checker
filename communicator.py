import smtplib, ssl
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

def communicate(web_df, dilfo_row, test=False):
	port = 465 # for SSL
	smtp_server = "smtp.gmail.com"
	sender_email = "dilfo.hb.release"

	receiver_email = dilfo_row.receiver_email
	if (not receiver_email.endswith('@dilfo.com')) and (receiver_email not in[
		'alex.roy616@gmail.com', 'alex.roy616@icloud.com']):
		logger.info('given user e-mail address has not been white listed (from dilfo.com '\
			'domain or from Alex Roy address)')
		return 1
	cc_email = dilfo_row.cc_email
	print(cc_email)
	if cc_email:
		if cc_email.endswith('@dilfo.com') or (cc_email in[
			'alex.roy616@gmail.com', 'alex.roy616@icloud.com']):
			cc_phrase = f" You also chose to copy {' '.join([name.capitalize() for name in cc_email.strip('@dilfo.com').split('.')])}."
		else:
			cc_phrase = ''
			logger.info('given user e-mail address for cc has not been white listed (from dilfo.com '\
				'domain or from Alex Roy address)')
			cc_email = ''
			

	def send_email():
		pud_date = datetime.datetime(
				*[int(web_df.iloc[0].pub_date.split('-')[x]) for x in range(3)]).date()
		due_date = lambda delay: pud_date + datetime.timedelta(days=delay)
		
		intro_msg = (
		    f"From: Dilfo HBR Bot"
		    f"\n"
		    f"To: {receiver_email}"
			f"\n"
		    f"CC: {cc_email}"
		    f"\n"
		    f"Subject: #{dilfo_row.job_number} - Upcoming Holdback Release"
		    f"\n\n"
		    f"Hi {receiver_email.split('.')[0].title()},"
		    f"\n\n"
		    f"You're receiving this e-mail notification because you added the project "
		    f"#{dilfo_row.job_number} - {dilfo_row.title} to the watchlist of upcoming "
		    f"holdback releases.{cc_phrase}"
		    f"\n"
		)
		if len(web_df) == 1:
			enum_msg = (
				f"Before going any further, please follow the link below to make sure the "
		    	f"algorithm correctly matched the project in question:\n{web_row.cert_url}\n"
			)
		else:
			enum_msg = "More than one possible match came up. Please check out each one to see if it's the right one.\n"
			for i, link in enumerate(web_df.cert_url):
				enum_msg += f'\tlink #{i+1}:\t{link}\n'
		timing_msg = (
		    f"If it's the right project, then the certificate was just "
		    f"published this past {datetime.datetime.strftime(pud_date,'%A')} "
		    f"on {datetime.datetime.strftime(pud_date,'%B %e, %Y')}. This means a "
		    f"valid holdback release invoice could be submitted as of:"
		    f"\nA)\t{datetime.datetime.strftime(due_date(45),'%B %e, %Y')} "
		    f"if the contract was signed before October 1, 2019 or;"
		    f"\nB)\t{datetime.datetime.strftime(due_date(60),'%B %e, %Y')} "
		    f"if the contract was signed since then."
		    f"\n"
		)
		disclaimer_msg = (
		    "Please be aware this is a fully automated message. "
		    "The info presented above could be erroneous."
		)
		if len(web_df) == 1:
			feedback_msg = (
				"You can help improve the matching algorithms by replying to "
				"this e-mail with a simple `1` or `0` to confirm whether or "
				"not the linked certificate represents the project in question."
				"\n"
			)
		else:
			feedback_msg = (
				"You can help improve the matching algorithms by replying to "
				"this e-mail with the number from above which corresponds to "
				"the correct link for your project. For example, just reply `1` "
				"if the first link was the right one. If none of the links were "
				"correct matches, please reply `0`."
				"\n"
			)
		closeout_msg = (
			"Thanks,\n"
			"Dilfo HBR Bot\n"
		)

		message = '\n'.join([intro_msg, enum_msg, timing_msg, disclaimer_msg, feedback_msg, closeout_msg])

		if not test:
			try:
				context = ssl.create_default_context()
				with open(".password.txt") as file: 
					password = file.read()
				with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
				    server.login(sender_email, password)
				    server.sendmail(sender_email, [receiver_email, cc_email], message)
				logger.info(f"Successfully sent an email to {receiver_email}")
			except FileNotFoundError:
				logger.info("password not available -> could not send e-mail")

	send_email()
