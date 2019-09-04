import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from communicator import send_email
from time import sleep


receiver_email = 'alex.roy616@gmail.com'
base_url = 'https://www.shopify.ca/'
query = 'careers/search?locations%5B%5D=1&keywords=&sort='

while True:
    try:
        response = requests.get(base_url+query)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        print(e)
        pass

    results = {x.get_text().lower() : base_url+x.get('href') for x in soup.find_all('a', {'aria-label':'view job posting:'})}
    filtered_results = {x:results[x] for x in results if re.findall('winter|intern|coop|spring', x)}
    try:
        existing_filtered_results = {x.position : x.link for _, x in pd.read_csv('shop_jobs.csv').iterrows()}
        new_results = {k : filtered_results[k] for k in set(filtered_results) - set(existing_filtered_results)}
    except FileNotFoundError:
        new_results = filtered_results

    for job in new_results:
        message = (
        		f"From: Shop Jobs Bot"
    		    f"\n"
    		    f"To: {receiver_email}"
    		    f"\n"
    		    f"Subject: New Job Alert: {job.title()}"
    		    f"\n\n"
    		    f"{new_results[job]}"
        )
        send_email(receiver_email, message, False)

    if new_results:
        pd.DataFrame({'position':list(filtered_results.keys()),'link':list(filtered_results.values())}).to_csv('shop_jobs.csv', index=False)

    sleep(300)