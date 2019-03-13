import smtplib, ssl
import datetime

def communicate(web_row, dilfo_row, test=False):
	port = 465 # for SSL
	smtp_server = "smtp.gmail.com"
	sender_email = "dilfo.hb.release"

	receiver_email = dilfo_row.receiver_email
	if (not receiver_email.endswith('@dilfo.com')) and (receiver_email not in[
		'alex.roy616@gmail.com', 'alex.roy616@icloud.com']):
		print('given user e-mail address has not been white listed (from dilfo.com '\
			'domain or from Alex Roy address)')
		return 1
	cc = dilfo_row.cc_email + '@dilfo.com' if dilfo_row.cc_email not in ['',' '] else ''

	url = 'https://canada.constructconnect.com/dcn/certificates-and-notices?perpage=1000&phrase=&sort=publish_date&owner=&contractor=&date=past_7&date_from=&date_to=#results'

	def send_email():
		
		pud_date = datetime.datetime(
				*[int(web_row.pub_date.split('-')[x]) for x in range(3)]).date()
		due_date = lambda delay: pud_date + datetime.timedelta(days=delay)
		
		message = (
		    f"CC: {cc}"
		    f"\n"
		    f"Subject: Alert for Holdback Release on Dilfo Project "
		    f"#{dilfo_row.job_number} - {dilfo_row.title}"
		    f"\n\n"
		    f"Hi {receiver_email.split('.')[0].title()},"
		    f"\n"
		    f"You're receiving this e-mail notification because either you or another "
		    f"Dilfo employee added the project #{dilfo_row.job_number} - "
		    f"{dilfo_row.title} to the watchlist of upcoming holdback releases."
		    f"\n"
		    f"Since this project's certificate was just recently published this past "
		    f"{datetime.datetime.strftime(pud_date,'%A')} on "
		    f"{datetime.datetime.strftime(pud_date,'%B %e, %Y')}, a valid holdback "
		    f"release invoice could be submitted as of:"
		    f"\n1.\t{datetime.datetime.strftime(due_date(45),'%B %e, %Y')} "
		    f"if the contract was signed before October 1, 2019 or;"
		    f"\n2.\t{datetime.datetime.strftime(due_date(60),'%B %e, %Y')} "
		    f"if the contract was signed since then."
		    f"\n"
		    f"Follow link posted by the Daily Commercial News for more details:\n"
		    f"{web_row.cert_url}"
		    f"\n"
		    f"Please be aware this is a fully automated message. "
		    f"The info contained here could be erroneous."
		)

		if not test:
			try:
				context = ssl.create_default_context()
				with open(".password.txt") as file: 
					password = file.read()
				with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
				    server.login(sender_email, password)
				    server.sendmail(sender_email, [receiver_email, cc], message)
				print(f"Sccessfully sent an email to {receiver_email}")
			except FileNotFoundError:
				print("password not available -> could not send e-mail")
		else:
			print("for testing purposes only, let's see that e-mail draft:\n\n")
			print(message)

	send_email()
