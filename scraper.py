import datetime
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import argparse
import progressbar
from time import sleep
from utils import create_connection
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


def scrape(source="dcn", provided_url_key=False, limit=False, since="last_record", test=False):
    """Extracts new certificates by scraping CSP websites and writes data to the web_certificates table in the database.
    
    Parameters:
     - `source` (str): Specifies source webstie being scraped for CSP's. Can be either `dcn` for Daily Commercial News or `ocn` for Ontario Construction News.
     - `provided_url_key` (str of False): provided_url_key that is to be scraped. False by default.
     - `limit` (int): Specifies a limit for the amount of certificates to be scraped. Default is no limit.
     - `since` (str): Specifies date from when to begin looking for new CSP's. Can be either `last_record` or `yyyy-mm-dd` string format.
     - `test` (bool): Set to True to cancel writing to the database and return DataFrame of scraped certificates instead.

    Returns:
     - `True` if 1 or more certificates were scraped
     - `False` if no certificates were scraped
     - a Pandas DataFrame containing new certificates if Test=True

    """
    # Initialize string and lambda functions based on source :
    def get_details(entry):
        entry = base_url + entry
        url_key.append(entry.split(base_aug_url)[1])
        while True:
            try:
                response = requests.get(entry)
                break
            except requests.exceptions.ConnectionError:
                sleep(1)
                continue
        html = response.content
        entry_soup = BeautifulSoup(html, "html.parser")
        pub_date.append(get_pub_date(entry_soup))
        city.append(get_city(entry_soup))
        address.append(get_address(entry_soup))
        title.append(get_title(entry_soup))
        company_soup = get_company_soup(entry_soup)
        company_results = get_company_results(company_soup)
        lookup = {
            company_term["owner"]: owner,
            company_term["contractor"]: contractor,
            company_term["engineer"]: engineer,
        }
        for key in lookup:
            lookup[key].append(company_results.get(key, np.nan))

    if source == "dcn":
        base_search_url = "https://canada.constructconnect.com/dcn/certificates-and-\
                notices?perpage=1000&phrase=&sort=publish_date&owner=&contractor="
        custom_param_url = "&date=custom&date_from={}&date_to={}#results"
        get_number_of_matches = lambda soup: int(
            re.compile("\d\d*").findall(
                (soup.find("h4", {"class": "search-total h-normalize"}).get_text())
            )[0]
        )
        get_pub_date = lambda entry_soup: entry_soup.find("time").get_text()
        get_city = (
            lambda entry_soup: entry_soup.find("div", {"class": "content-left"})
            .find("h4")
            .get_text()
        )
        get_address = lambda entry_soup: entry_soup.find(
            "p", {"class": "print-visible"}
        ).get_text()
        get_title = (
            lambda entry_soup: entry_soup.find_all("section", {"class": "content"})[3]
            .find("p")
            .get_text()
        )
        get_company_soup = lambda entry_soup: entry_soup.find_all(
            "section", {"class": "content"}
        )[4]
        get_company_results = lambda company_soup: {
            key.get_text(): value.get_text()
            for key, value in zip(
                company_soup.find_all("dt"), company_soup.find_all("dd")
            )
        }
        company_term = {
            "owner": "Name of Owner",
            "contractor": "Name of Contractor",
            "engineer": "Name of Certifier",
        }
        get_entries = lambda soup: [
            x.find("a").get("href")
            for x in soup.find_all("article", {"class": "cards-item"})
        ]
        base_url = "https://canada.constructconnect.com"
        base_aug_url = (
            "https://canada.constructconnect.com/dcn/certificates-and-notices/"
        )
    elif source == "ocn":
        base_search_url = "https://ontarioconstructionnews.com/certificates/?\
            per_page=1000&certificates_page=1&search=&form_id=&owner_name_like\
                =&contractor_name_like="
        custom_param_url = (
            "&date_published=custom&date_published_from={}&date_published_to={}"
        )
        get_number_of_matches = lambda soup: int(
            (
                soup.find_all("span", {"class": "items-found"})[1]
                .get_text()
                .split(" of ")[1]
            )
        )
        get_pub_date = lambda entry_soup: str(
            dateutil.parser.parse(entry_soup.find("date").get_text()).date()
        )
        get_city = lambda entry_soup: entry_soup.find(
            "h2", {"class": "ocn-subheading"}
        ).get_text()
        get_address = (
            lambda entry_soup: entry_soup.find("div", {"class": "ocn-certificate"})
            .find("p")
            .get_text()
        )
        get_title = (
            lambda entry_soup: entry_soup.find("h2", {"class": "ocn-heading"})
            .find_next_sibling("p")
            .get_text()
        )
        get_company_soup = lambda entry_soup: entry_soup.find(
            "div", {"class": "ocn-participant-wrap"}
        )
        get_company_results = lambda company_soup: {
            key.get_text(): value.get_text()
            for key, value in zip(
                company_soup.find_all("div", {"class": "participant-type"})[::2],
                company_soup.find_all("div", {"class": "participant-name-wrap"}),
            )
        }
        company_term = {
            "owner": "Name of Owner",
            "contractor": "Name of Contractor",
            "engineer": "Name of Payment Certifier",
        }
        get_entries = lambda soup: [
            x.find("a").get("href")
            for x in soup.find_all("td", {"class": "col-location"})
        ]
        base_url = ""
        base_aug_url = "https://ontarioconstructionnews.com/certificates/"
    else:
        raise ValueError("Must specify CSP source.")
    pub_date, city, address, title, owner, contractor, engineer, url_key = [
        [] for _ in range(8)
    ]
    now = datetime.datetime.now().date()
    if provided_url_key:
        get_details(provided_url_key)
        return pd.DataFrame(
            data={
                "pub_date": pub_date,
                "city": city,
                "address": address,
                "title": title,
                "owner": owner,
                "contractor": contractor,
                "engineer": engineer,
                "url_key": url_key,
                "source": source,
            }
        )
    if since == "last_record":
        hist_query = """
            SELECT pub_date 
            FROM web_certificates 
            WHERE source=? 
            ORDER BY pub_date DESC LIMIT 1
        """
        with create_connection() as conn:
            cur = conn.cursor()
            cur.execute(hist_query, [source])
            last_date = cur.fetchone()[0]
            ld_year = int(last_date[:4])
            ld_month = int(last_date[5:7])
            ld_day = int(last_date[8:])
            since = (
                datetime.datetime(ld_year, ld_month, ld_day) + datetime.timedelta(1)
            ).date()
    else:
        valid_since_date = re.search("\d{4}-\d{2}-\d{2}", since)
        if not valid_since_date:
            raise ValueError(
                "`since` parameter should be in the format yyyy-mm-dd if not a "
                "predefined term."
            )
    date_param_url = custom_param_url.format(since, now)
    response = requests.get(base_search_url + date_param_url)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")
    number_of_matches = get_number_of_matches(soup)
    if not number_of_matches:
        logger.info(
            "Nothing new to scrape in timeframe specified - exiting scrape function."
        )
        return False  # signaling that scrape returned nothing
    logger.info(
        f"scraping all of {number_of_matches} new certificates since {since}..."
    )
    bar = progressbar.ProgressBar(
        maxval=number_of_matches + 1,
        widgets=[progressbar.Bar("=", "[", "]"), " ", progressbar.Percentage()],
    )
    bar.start()
    entries = get_entries(soup)
    for i, entry in enumerate(entries, 1):
        get_details(entry)
        if limit and (i >= limit):
            logger.info("limit reached - breaking out of loop.")
            break
        bar.update(i + 1)
    bar.finish()
    with create_connection() as conn:
        last_cert_id = (
            pd.read_sql(
                "SELECT * from web_certificates ORDER BY cert_id DESC LIMIT 1", conn
            )
            .iloc[0]
            .cert_id
        )
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
            "source": source,
        }
    )
    df_web = df_web.sort_values("pub_date", ascending=True)
    df_web["cert_id"] = [
        int(x) for x in range(last_cert_id + 1, last_cert_id + 1 + len(df_web))
    ]
    # make date into actual datetime object
    df_web["pub_date"] = df_web.pub_date.apply(
        lambda x: re.findall("\d{4}-\d{2}-\d{2}", x)[0]
    )
    if test:
        return df_web
    attrs = [
        "cert_id",
        "pub_date",
        "city",
        "address",
        "title",
        "owner",
        "contractor",
        "engineer",
        "url_key",
        "source",
    ]
    query = f""" 
        INSERT INTO web_certificates 
        ({', '.join(attrs)}) VALUES ({','.join(['?']*len(attrs))})
    """
    new_certs = [[row[attr] for attr in attrs] for _, row in df_web.iterrows()]
    with create_connection() as conn:
        conn.cursor().executemany(query, new_certs)
    return True  # signaling that something scrape did return some results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--source",
        type=str,
        help="specifies source webstie being scraped for CSP's",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        help="limits the amount of certificates to be scraped. Default is no limit.",
    )
    parser.add_argument(
        "--since", type=str, help="datefrom when to begin looking for new CSP's"
    )
    args = parser.parse_args()
    kwargs = {}
    if args.source:
        kwargs["source"] = args.source
    if args.since:
        kwargs["since"] = args.since
    if args.limit:
        kwargs["limit"] = args.limit
    scrape(**kwargs)
