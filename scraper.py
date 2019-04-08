import datetime
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import argparse
import progressbar
from time import sleep
import sqlite3
from sqlite3 import Error


database = 'cert_db'

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None

def scrape(limit=False, test=False, ref=False):

    pub_date, city, address, title, owner, contractor, engineer, cert_url = [
        [] for _ in range(8)]

    search_url = 'https://canada.constructconnect.com/dcn/certificates-and-notices\
    ?perpage=1000&phrase=&sort=publish_date&owner=&contractor=&date=past_7&\
    date_from=&date_to=#results'

    response = requests.get(search_url)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")

    number_of_matches = int(re.compile('\d\d*').findall(
        (soup.find('h4', {'class':'search-total h-normalize'}).get_text()))[0])

    def get_details(entry):
            
        if isinstance(ref, pd.DataFrame):
            url = entry
        else:
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
        pub_date.append(entry_soup.find("time").get_text())
        city.append(
            entry_soup.find("div",{"class":"content-left"}).find("h4").get_text())
        address.append(
            entry_soup.find("p",{"class":"print-visible"}).get_text())
        title.append(
            entry_soup.find_all("section",{"class":"content"})[3].find("p").get_text())
        company_soup = entry_soup.find_all("section",{"class":"content"})[4]
        company_results = {
            key.get_text():value.get_text() for key, value in zip(
                company_soup.find_all("dt"), company_soup.find_all("dd"))}
        lookup = {
            "Name of Owner": owner,
            "Name of Contractor": contractor,
            "Name of Certifier": engineer
        }
        for key in list(lookup.keys()):
            lookup[key].append(company_results.get(key, np.nan))
    if isinstance(ref, pd.DataFrame):
        for link in ref.link_to_cert:
            get_details(link)
    else:
        print(f'\nscraping all of {number_of_matches} new certificates for this week...')
        bar = progressbar.ProgressBar(maxval=number_of_matches+1, \
            widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar.start()
        for i, entry in enumerate(soup.find_all("article", {"class":"cards-item"}), 1):
            get_details(entry)
            if limit and (i >= limit):
                print("limit reached - breaking out of loop.")
                break
            bar.update(i+1)
        bar.finish()      
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
    # make date into actual datetime object
    df_web['pub_date'] = df_web.pub_date.apply(
            lambda x: re.findall('\d{4}-\d{2}-\d{2}', x)[0])

    if not test and not isinstance(ref, pd.DataFrame):
        conn = create_connection(database)
        with conn:
            df_web.to_sql('hist_certs', conn, if_exists='append')
    else:
        return df_web


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
