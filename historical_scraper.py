import datetime
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import argparse
import progressbar

def scrape(start='', finish='', limit=False, test=False):

    pub_date, city, address, title, owner, contractor, engineer, cert_url = [
    [] for _ in range(8)
    ]

    search_url = f'https://canada.constructconnect.com/dcn/certificates-and-notices?perpage=50000&phrase=&sort=publish_date&owner=&contractor=&date=custom&date_from={start}&date_to={finish}#results'

    response = requests.get(search_url)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")

    number_of_matches = int(re.compile('\d\d*').findall(
        (soup.find('h4', {'class':'search-total h-normalize'}).get_text()))[0])

    def get_details(entry):
        url = 'https://canada.constructconnect.com' + entry.find("a")["href"]
        cert_url.append(url)
        while True:
            try:
                response = requests.get(url)
                break
            except requests.exceptions.ConnectionError:
                sleep(1)
                continue
        html = response.content
        entry_soup = BeautifulSoup(html, "html.parser")
        try:
            pub_date.append(entry_soup.find("time").get_text())
        except AttributeError:
            pub_date.append(np.nan)
        try:
            city.append(entry_soup.find("div",{"class":"content-left"}).find("h4").get_text())
        except AttributeError:
            city.append(np.nan)
        try:
            address.append(entry_soup.find("p",{"class":"print-visible"}).get_text())
        except AttributeError:
            address.append(np.nan)
        try:
            title.append(entry_soup.find_all("section",{"class":"content"})[3].find("p").get_text())
        except AttributeError:
            title.append(np.nan)
        try:
            company_soup = entry_soup.find_all("section",{"class":"content"})[4]
            company_results = {
            key.get_text():value.get_text() for key, value in zip(
                company_soup.find_all("dt"), company_soup.find_all("dd"))}
        except AttributeError:
            company_results = {}
        lookup = {"Name of Owner": owner, "Name of Contractor": contractor, "Name of Certifier": engineer}
        for key in list(lookup.keys()):
            lookup[key].append(company_results.get(key, np.nan))
    print(f'\nscraping all of {number_of_matches} new certificates from{start} to {finish}...')
    
    bar = progressbar.ProgressBar(maxval=number_of_matches+1, \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()

    for i, entry in enumerate(soup.find_all("article", {"class":"cards-item"}), 1):
        get_details(entry)
        bar.update(i+1)
    bar.finish()
    print("saving to df_web dataframe.")
    df_web = pd.DataFrame(
        data={
            "pub_date": pub_date,
            "city": city,
            "address": address,
            "title": title,
            "owner": owner,
            "contractor": contractor,
            "engineer": engineer,
            "cert_url": cert_url,
        }
    )

    df_web.astype('str').to_csv(f'./data/raw_web_certs_{start}_to_{finish}.csv', index=False)
    return df_web

#specify dates to be scraped down here (could make this into CLI later!)
scrape(start='2011-09-01', finish='2011-12-31')
scrape(start='2011-05-01', finish='2011-08-31')
scrape(start='2011-01-01', finish='2011-04-30')
scrape(start='2010-09-01', finish='2010-12-31')
scrape(start='2010-05-01', finish='2010-08-31')
scrape(start='2010-01-01', finish='2010-04-30')
scrape(start='2009-09-01', finish='2009-12-31')
scrape(start='2009-05-01', finish='2009-08-31')
scrape(start='2009-01-01', finish='2009-04-30')
