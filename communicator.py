import smtplib, ssl

def communicate(web_row, dilfo_row):
	port = 465 # for SSL
	smtp_server = "smtp.gmail.com"
	sender_email = "dilfo.hb.release"

	receiver_email = dilfo_row.receiver_email

	url = 'https://canada.constructconnect.com/dcn/certificates-and-notices?perpage=1000&phrase=&sort=publish_date&owner=&contractor=&date=past_7&date_from=&date_to=#results'

	def send_email():
		message = (
		    f"Subject: Alert for possible HB Release!!!"
		    f"\n\n"
		    f"See below for details:"
		    f"{web_row}"
		    f"\n\n"
		    f"Go check the website {dilfo_row.cert_url}"
		)

		context = ssl.create_default_context()

		try:
			with open(".password.txt") as file: 
				password = file.read()
			with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
			    server.login(sender_email, password)
			    server.sendmail(sender_email, receiver_email, message)
			print(f"Sccessfully sent an email to {receiver_email}")
		except FileNotFoundError:
			print("password not available -> could not send e-mail")

	send_email()

if __name__=="__main__":
	communicate()