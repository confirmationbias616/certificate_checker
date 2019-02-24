import smtplib, ssl

def communicate(pandas_row):
	port = 465 # for SSL
	smtp_server = "smtp.gmail.com"
	sender_email = "alex.roy616@gmail.com"

	receiver_email = "alex.roy616@gmail.com"

	url = 'https://canada.constructconnect.com/dcn/certificates-and-notices?perpage=1000&phrase=&sort=publish_date&owner=&contractor=&date=past_7&date_from=&date_to=#results'

	def send_email(pandas_row):
		message = (
		    f"Subject: Alert for possible HB Release!!!"
		    f"\n\n"
		    f"See below for details:"
		    f"{pandas_row}"
		    f"\n\n"
		    f"Go check the website {url}"
		)

		context = ssl.create_default_context()

		try:
			with open(".password.txt") as file: 
				password = file.read()
			with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
			    server.login(sender_email, password)
			    server.sendmail(sender_email, receiver_email, message)
		except FileNotFoundError:
			print("password not available -> could not send e-mail")

	send_email(web_row)

if __name__=="__main__":
	communicate()