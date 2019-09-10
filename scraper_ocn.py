import datetime
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import argparse
import progressbar
from time import sleep
from db_tools import create_connection
import sys
import logging
import dateutil.parser


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

def scrape(limit=False, test=False, ref=False, since='last_record'):

    pub_date, city, address, title, owner, contractor, engineer, url_key = [
        [] for _ in range(8)]
    now = datetime.datetime.now().date()
    base_url = "https://ontarioconstructionnews.com/certificates/?per_page=1000&certificates_page=1&search=&form_id=&owner_name_like=&contractor_name_like="
    if since == 'week_ago':
        date_param_url = "&date_published=last_seven_days&date_published_from=&date_published_to="
    elif since == 'last_record':
        hist_query = "SELECT pub_date FROM web_certificates WHERE source='ocn' ORDER BY pub_date DESC LIMIT 1"
        with create_connection() as conn:
            cur = conn.cursor()
            cur.execute(hist_query)
            last_date = cur.fetchone()[0]
            ld_year = int(last_date[:4])
            ld_month = int(last_date[5:7])
            ld_day = int(last_date[8:])
            since = (datetime.datetime(ld_year, ld_month, ld_day) + datetime.timedelta(1)).date()
    else:
        valid_since_date = re.search("\d{4}-\d{2}-\d{2}", since)
        if not valid_since_date:
            raise ValueError("`since` parameter should be in the format yyyy-mm-dd if not default value of `week_ago`")
    date_param_url = f'&date_published=custom&date_published_from={since}&date_published_to={now}'
    response = requests.get(base_url + date_param_url)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")
    number_of_matches = int(
        (soup.find_all('span', {'class':'items-found'})[1].get_text().split(' of ')[1]))

    def get_details(entry):
        if isinstance(ref, pd.DataFrame):
            entry = 'https://ontarioconstructionnews.com/certificates/' + entry
        url_key.append(entry.split('https://ontarioconstructionnews.com/certificates/')[1])
        while True:
            try:
                response = requests.get(entry)
                break
            except requests.exceptions.ConnectionError:
                sleep(1)
                continue
        html = response.content
        entry_soup = BeautifulSoup(html, "html.parser")
        pub_date.append(str(dateutil.parser.parse(entry_soup.find("date").get_text()).date()))
        city.append(entry_soup.find("h2",{"class":"ocn-subheading"}).get_text())
        address.append(entry_soup.find("div",{"class":"ocn-certificate"}).find('p').get_text())
        title.append(entry_soup.find("h2", {"class":"ocn-heading"}).find_next_sibling('p').get_text())
        company_soup = entry_soup.find('div', {'class':'ocn-participant-wrap'})
        company_results = {
            key.get_text():value.get_text() for key, value in zip(
                company_soup.find_all("div", {'class':'participant-type'})[::2], company_soup.find_all("div", {'class':'participant-name-wrap'}))}
        lookup = {
            "Name of Owner": owner,
            "Name of Contractor": contractor,
            "Name of Payment Certifier": engineer
        }
        for key in lookup:
            lookup[key].append(company_results.get(key, np.nan))
    if isinstance(ref, pd.DataFrame):
        logger.info(f"fetching web certificate info for previously matched projects...")
        for key in ref['url_key']:
            get_details(key)
    else:
        if not number_of_matches:
            logger.info('Nothing new to scrape in timeframe specified - exiting scrape function.')
            return False # signaling that scrape returned nothing
        logger.info(f"scraping all of {number_of_matches} new certificates since {since}...")
        bar = progressbar.ProgressBar(maxval=number_of_matches+1, \
            widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar.start()
        entries = [x.find('a').get('href') for x in soup.find_all('td', {'class':'col-location'})]
        for i, entry in enumerate(entries, 1):
            get_details(entry)
            if limit and (i >= limit):
                logger.info("limit reached - breaking out of loop.")
                break
            bar.update(i+1)
        bar.finish()      
    with create_connection() as conn:
        last_cert_id = pd.read_sql("SELECT * from web_certificates ORDER BY cert_id DESC LIMIT 1", conn).iloc[0].cert_id
    df_web = pd.DataFrame(
        data={
            "pub_date": pub_date,
            "city": city,
            "address": address,
            "title": title,
            "owner": owner,
            "contractor": contractor,
            "engineer": engineer,
            "url_key": url_key,
            "source": "ocn"
        }
    )
    df_web = df_web.sort_values('pub_date', ascending=True)
    df_web['cert_id'] = [int(x) for x in range(last_cert_id+1,last_cert_id+1+len(df_web))]
    # make date into actual datetime object
    df_web['pub_date'] = df_web.pub_date.apply(
            lambda x: re.findall('\d{4}-\d{2}-\d{2}', x)[0])

    if not test and not isinstance(ref, pd.DataFrame):
        attrs = [
            'cert_id',
            'pub_date',
            'city',
            'address',
            'title',
            'owner',
            'contractor',
            'engineer',
            'url_key',
            'source'
        ]
        query=f''' 
            INSERT INTO web_certificates 
            ({', '.join(attrs)}) VALUES ({','.join(['?']*len(attrs))})
        '''
        new_certs = [[row[attr] for attr in attrs] for _, row in df_web.iterrows()]
        with create_connection() as conn:
            conn.cursor().executemany(query, new_certs)
        return True  # signaling that something scrape did return some results
    else:
        return df_web


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="scrapes certificate websites and returns \
        certificates in the form of a pandas dataframe")
    parser.add_argument(
        "-l", "--limit",
        type=int,
        help="limits the amount of certificates to be scraped. Default is no limit.",
    )
    args = parser.parse_args()
    # scrape(limit=args.limit)
    scrape(since='2019-03-30')
