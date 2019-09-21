import add_parent_to_path
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from time import sleep


# NEED TO ADD OCN LOGIC
base_url = "https://canada.constructconnect.com/dcn/certificates-and-notices\
            ?perpage=1000&phrase=&sort=publish_date&owner=&contractor="


def get_number_of_results_today(date):
    date_param_url = f"&date=custom&date_from={date}&date_to={date}#results"
    response = requests.get(base_url + date_param_url)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")
    number_of_matches = int(
        re.compile("\d\d*").findall(
            (soup.find("h4", {"class": "search-total h-normalize"}).get_text())
        )[0]
    )
    return number_of_matches


prev_count = 0
while True:
    try:
        if datetime.datetime.now().day > current_date:
            prev_count = 0
            current_date = now.day
    except NameError:
        pass
    now = datetime.datetime.now()
    try:
        number_of_results_today = get_number_of_results_today(now.date())
    except Exception as e:
        print(repr(e))
        sleep(300)
    if number_of_results_today > prev_count:
        df = pd.read_csv("cert_count_timelog.csv")
        print(
            f"number of results so far today @ {str(now.time()).split('.')[0]}: {number_of_results_today}"
        )
        df = df.append(
            {
                "date": now.date(),
                "time": str(now.time()).split(".")[0],
                "count": number_of_results_today,
            },
            ignore_index=True,
        )
        df.to_csv("cert_count_timelog.csv", index=False)
    prev_count = number_of_results_today
    sleep(60)
