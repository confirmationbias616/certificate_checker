import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import argparse


def scrape(limit=False):
    pub_date = []
    city = []
    address = []
    title = []
    owner = []
    contractor = []
    engineer = []

    url = 'https://canada.constructconnect.com/dcn/certificates-and-notices?perpage=1000&phrase=&sort=publish_date&owner=&contractor=&date=past_7&date_from=&date_to=#results'

    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")

    def get_details(entry):
        project_details = 'https://canada.constructconnect.com' + entry.find("a")["href"]
        response = requests.get(project_details)
        html = response.content
        entry_soup = BeautifulSoup(html, "html.parser")
        pub_date.append(entry_soup.find("time").get_text())
        city.append(entry_soup.find("div",{"class":"content-left"}).find("h4").get_text())
        address.append(entry_soup.find("p",{"class":"print-visible"}).get_text())
        title.append(entry_soup.find_all("section",{"class":"content"})[3].find("p").get_text())
        company_soup = entry_soup.find_all("section",{"class":"content"})[4]
        company_results = {
            key.get_text():value.get_text() for key, value in zip(
                company_soup.find_all("dt"), company_soup.find_all("dd"))}
        lookup = {"Name of Owner": owner, "Name of Contractor": contractor, "Name of Certifier": engineer}
        for key in list(lookup.keys()):
            lookup[key].append(company_results.get(key, np.nan))

    for i, entry in enumerate(soup.find_all("article", {"class":"cards-item"}), 1):
        print(f'getting entry #{i}...')
        get_details(entry)
        if (limit) and (i >= limit):
            break


    df_web = pd.DataFrame(
        data={
            "pub_date": pub_date,
            "city": city,
            "address": address,
            "title": title,
            "owner": owner,
            "contractor": contractor,
            "engineer": engineer
        }
    )

    df_web.astype('str').to_csv(f'./data/raw_web_certs_{datetime.datetime.now().date()}.csv', index=False)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="scrapes DCM website and returns \
        certificates in the form of a pandas dataframe")
    parser.add_argument(
        "-l", "--limit",
        type=int,
        help="limits the amount of certificates to be scraped. Default is no limit.",
    )
    args = parser.parse_args()
    scrape(limit=args.limit)
