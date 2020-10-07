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
from geocoder import geocode
import sys
import logging
import dateutil.parser
from dateutil.parser import parse as parse_date


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


def scrape(
    source="dcn", provided_url_key=False, limit=False, since="last_record", until="now", test=False
):
    """Extracts new certificates by scraping CSP websites and writes data to the web_certificates table in the database.
    
    Parameters:
     - `source` (str): Specifies source webstie being scraped for CSP's. Can be either `dcn` for Daily Commercial News or `ocn` for Ontario Construction News.
     - `provided_url_key` (str of False): provided_url_key that is to be scraped. False by default.
     - `limit` (int): Specifies a limit for the amount of certificates to be scraped. Default is no limit.
     - `since` (str): Specifies date from when to begin looking for new CSP's. Can be either `last_record` or `yyyy-mm-dd` string format.
     - `until` (str): Specifies date for when to end the search for new CSP's. Can be either `now` or `yyyy-mm-dd` string format.
     - `test` (bool): Set to True to cancel writing to the database and return DataFrame of scraped certificates instead.

    Returns:
     - `True` if 1 or more certificates were scraped
     - `False` if no certificates were scraped
     - a Pandas DataFrame containing new certificates if Test=True

    """
    # Initialize string and lambda functions based on source :
    def get_details(entry):
        entry = base_url + entry
        url_key = entry.split(base_aug_url)[1]
        while True:
            try:
                response = requests.get(entry)
                break
            except requests.exceptions.ConnectionError:
                sleep(1)
                continue
        if response.status_code == 404:
            return
        html = response.content
        entry_soup = BeautifulSoup(html, "html.parser")
        if source == "dcn":
            pub_date = entry_soup.find("time").get_text()
            cert_type = entry_soup.find("h1").get_text()
            if cert_type == "Certificates and Notices":
                cert_type = (
                    "csp"
                )  # old style -> assume csp by default even if it might not be true
                city = (
                    entry_soup.find("div", {"class": "content-left"})
                    .find("h4")
                    .get_text()
                )
                address = entry_soup.find("p", {"class": "print-visible"}).get_text()
                title = (
                    entry_soup.find_all("section", {"class": "content"})[3]
                    .find("p")
                    .get_text()
                )
            else:
                cert_type = (
                    "csp"
                    if cert_type == "Certificate of Substantial Performance"
                    else cert_type
                )
                city = entry_soup.find_all("dl")[0].find("dt").get_text()
                address = entry_soup.find_all("dl")[1].find("dt").get_text()
                title = entry_soup.find_all("dl")[2].find("dd").get_text()
                if address.startswith(
                    "This is to certify"
                ):  # no address available. chnage sequence going forward
                    address = ""
                    title = entry_soup.find_all("dl")[1].find("dd").get_text()
            company_results = {
                key.get_text(): value.get_text()
                for key, value in zip(
                    entry_soup.find_all("dt"), entry_soup.find_all("dd")
                )
            }
            owner = company_results.get(
                "Name of owner:", company_results.get("Name of Owner", np.nan)
            )
            contractor = company_results.get(
                "Name of contractor:", company_results.get("Name of Contractor", np.nan)
            )
            engineer = company_results.get(
                "Name of payment certifier:",
                company_results.get(
                    "Name of Certifier",
                    company_results.get("Name of certifier:", np.nan),
                ),
            )
        elif source == "ocn":
            if (
                "Non-Payment"
                in entry_soup.find("h1", {"class": "entry-title"}).get_text()
            ):
                cert_type = "np"
            elif (
                "Notice of Termination"
                in entry_soup.find("h2", {"class": "ocn-heading"}).find_next_sibling("p").get_text()
            ):
                cert_type = "term"    
            else:
                cert_type = "csp"
            pub_date = str(
                dateutil.parser.parse(entry_soup.find("date").get_text()).date()
            )
            city = (
                entry_soup.find("h2", {"class": "ocn-subheading"}).get_text().split(":")[0]
            )
            if cert_type == "csp":
                address = (
                    entry_soup.find("div", {"class": "ocn-certificate"})
                    .find("p")
                    .get_text()
                )
                title = (
                    entry_soup.find("h2", {"class": "ocn-heading"})
                    .find_next_sibling("p")
                    .get_text()
                )
                company_soup = entry_soup.find("div", {"class": "ocn-participant-wrap"})
                company_results = {
                    key.get_text(): value.get_text()
                    for key, value in zip(
                        company_soup.find_all("div", {"class": "participant-type"})[
                            ::2
                        ],
                        company_soup.find_all(
                            "div", {"class": "participant-name-wrap"}
                        ),
                    )
                }
                owner = company_results.get("Name of Owner", np.nan)
                contractor = company_results.get("Name of Contractor", np.nan)
                engineer = company_results.get("Name of Payment Certifier", np.nan)
            elif cert_type == "np":
                address = (
                    entry_soup.find("h4", {"class": "ocn-subheading"})
                    .find_next("p")
                    .get_text()
                )
                title = address  # temporary until we see more of these
                for x in entry_soup.find_all("strong"):
                    try:
                        if x.get_text() == "Name of owner:":
                            owner = x.find_parent().get_text().split(": ")[1]
                        if x.get_text() == "Name of contractor:":
                            contractor = x.find_parent().get_text().split(": ")[1]
                    except AttributeError:
                        pass
                engineer = np.nan
            elif cert_type == "term":
                address = (
                    entry_soup.find("h1", {"class": "entry-title"})
                    .get_text()
                )
                title = address  # temporary until we see more of these
                for x in entry_soup.find_all("strong"):
                    try:
                        if x.get_text() == "Name of owner:":
                            owner = x.find_parent().get_text().split(": ")[1]
                        if x.get_text() == "Name of contractor:":
                            contractor = x.find_parent().get_text().split(": ")[1]
                    except AttributeError:
                        pass
                engineer = np.nan
        elif source == "l2b":
            cert_type_text = entry_soup.find("h2").get_text()
            #cert_type = ("csp" if "Form 9" in cert_type_text else cert_type_text)
            if "Form 9" in cert_type_text:
                cert_type = "csp"
            elif "Form 10" in cert_type_text:
                cert_type = "ccs"
            else:
                cert_type = cert_type_text
            attr_pairs = {}
            fields = entry_soup.find_all('p', {'class':'mb-25'})
            for field in fields:
                try:
                    attr_pair = [s for s in re.findall('[^\t^\n^\r]*', field.get_text()) if s]
                    attr_pairs.update({attr_pair[0]: attr_pair[1]})
                except IndexError:
                    pass
            response = requests.get(base_url)
            html = response.content
            soup = BeautifulSoup(html, "html.parser")
            pub_date = [str(parse_date(entry.find_all('td')[1].get_text()).date()) for entry in soup.find('tbody').find_all('tr') if url_key in str(entry)][0]
            if cert_type == 'ccs':
                city = attr_pairs.get('Of premises at', np.nan)
                address = attr_pairs.get('Of premises at', np.nan)
                title = ' '.join((attr_pairs.get('The subcontract provided for the supply of the following services or materials', ''), attr_pairs.get('To the following improvement', '')))
                title = np.nan if title in ('', ' ') else title
            else:
                city = attr_pairs.get('Where the Premises is Situated', np.nan)
                address = attr_pairs.get('Where the Premises is Located', np.nan)
                title = attr_pairs.get('This is to certify that the contract for the following improvement', np.nan)
            owner = attr_pairs.get('Name of Owner', np.nan)
            contractor = attr_pairs.get('Name of Contractor', np.nan)
            engineer = attr_pairs.get('Name of Payment Certifier', np.nan)
        return (
            pub_date,
            city,
            address,
            title,
            owner,
            contractor,
            engineer,
            url_key,
            cert_type,
            source,
        )

    pub_date, city, address, title, owner, contractor, engineer, url_key, cert_type = [
        [] for _ in range(9)
    ]
    if until == "now":
        until = datetime.datetime.now().date()
    else:
        try:
            until = re.findall("\d{4}-\d{2}-\d{2}", until)[0]
        except KeyError:
            raise ValueError(
                "`until` parameter should be in the format yyyy-mm-dd if not a key_word"
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
            since = datetime.datetime(ld_year, ld_month, ld_day).date()
    else:
        valid_since_date = re.search("\d{4}-\d{2}-\d{2}", since)
        if not valid_since_date:
            raise ValueError(
                "`since` parameter should be in the format yyyy-mm-dd if not a "
                "predefined term."
            )
    if source == "dcn":
        base_url = "https://canada.constructconnect.com"
        base_aug_url = (
            "https://canada.constructconnect.com/dcn/certificates-and-notices/"
        )
        base_search_url = "https://canada.constructconnect.com/dcn/certificates-and-\
                notices?perpage=1000&phrase=&sort=publish_date&owner=&contractor="
        custom_param_url = "&date=custom&date_from={}&date_to={}#results"
        get_number_of_matches = lambda soup: int(
            re.compile("\d\d*").findall(
                (soup.find("span", {"class": "search-results__total"}).get_text())
            )[0]
        )
        get_entries = lambda soup: [
            x.find("a").get("href")
            for x in soup.find_all("article", {"class": "cards-item"})
        ]
    elif source == "ocn":
        base_url = ""
        base_aug_url = "https://ontarioconstructionnews.com/certificates/"
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
        get_entries = lambda soup: [
            x.find("a").get("href")
            for x in soup.find_all("td", {"class": "col-location"})
        ]
    elif source == "l2b":
        base_url = "https://certificates.link2build.ca/"
        base_aug_url = "Search/Detail/"
        base_search_url = "https://certificates.link2build.ca/"
        custom_param_url = ""
        since = str(since)
        until = str(until)
        get_entries = lambda soup: [
            entry.find('a').get('href') for entry in soup.find('tbody').find_all('tr') if parse_date(since) <= parse_date(entry.find_all('td')[1].get_text()) <= parse_date(until)]
        get_number_of_matches = lambda soup: len(get_entries(soup))
    else:
        raise ValueError("Must specify CSP source.")
    if provided_url_key:
        details = get_details(provided_url_key)
        return pd.DataFrame(
            data={
                "pub_date": details[0],
                "city": details[1],
                "address": details[2],
                "title": details[3],
                "owner": details[4],
                "contractor": details[5],
                "engineer": details[6],
                "url_key": details[7],
                "cert_type": details[8],
                "source": [source] * len(details[0]),
            }
        )
    date_param_url = custom_param_url.format(since, until)
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
    logged_key_query = """
        SELECT url_key 
        FROM web_certificates 
        WHERE source=? 
    """
    with create_connection() as conn:
        logged_url_keys = list(pd.read_sql(logged_key_query, conn, params=[source]).url_key)
    entries = get_entries(soup)
    for i, entry in enumerate(entries, 1):
        check_url_key = (base_url + entry).split(base_aug_url)[1]
        if not test and check_url_key in logged_url_keys:
            logger.info(f"entry for {check_url_key} was already logged - continuing with the next one (if any)...")
            continue
        for cumulative, item in zip(
            [
                pub_date,
                city,
                address,
                title,
                owner,
                contractor,
                engineer,
                url_key,
                cert_type,
            ],
            get_details(entry),
        ):
            cumulative.append(item)
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
            "cert_type": cert_type,
            "source": [source] * len(pub_date),
        }
    )
    if not len(df_web):
        return False
    df_web = df_web.sort_values("pub_date", ascending=True)
    df_web["cert_id"] = [
        int(x) for x in range(last_cert_id + 1, last_cert_id + 1 + len(df_web))
    ]
    # make date into actual datetime object
    df_web["pub_date"] = df_web.pub_date.apply(
        lambda x: str(parse_date(str(x)).date()) if (x and str(x) != 'nan') else np.nan
    )
    logger.info("Fetching geocode information...")
    df_web = geocode(df_web)
    if test:
        return df_web
    attrs = [
        "cert_id",
        "pub_date",
        "city",
        "address_lat",
        "address_lng",
        "city_lat",
        "city_lng",
        "city_size",
        "address",
        "title",
        "owner",
        "contractor",
        "engineer",
        "url_key",
        "cert_type",
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
        "--since", type=str, help="date from when to begin looking for new CSP's"
    )
    parser.add_argument(
        "--until", type=str, help="date for when to stop search for new CSP's"
    )
    args = parser.parse_args()
    kwargs = {}
    if args.source:
        kwargs["source"] = args.source
    if args.since:
        kwargs["since"] = args.since
    if args.since:
        kwargs["until"] = args.until
    if args.limit:
        kwargs["limit"] = args.limit
    scrape(**kwargs)
