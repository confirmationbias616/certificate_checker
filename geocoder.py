import argparse
import logging
import sys
import pandas as pd
import requests
import json
import numpy as np
from statistics import mean
from utils import create_connection, persistant_cache
import mysql.connector


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

try:
    with open(".secret.json") as f:
        api_key = json.load(f)["geo_api_key"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    api_key = None

try:
    with open(".secret.json") as f:
        pws = json.load(f)
        mysql_pw = pws["mysql"]
        paw_pw = pws["pythonanywhere"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    pass

def api_call(address_param):
    if not api_key:
        return {}
    api_request = "https://maps.googleapis.com/maps/api/geocode/json?address={}, Ontario, Canada&bounds=41.6765559,-95.1562271|56.931393,-74.3206479&key={}"
    response = requests.get(api_request.format(address_param, api_key))
    results_list = json.loads(response.content)['results']
    for result in results_list:
        if 'Ontario' in str(result) or 'ontario' in str(result):
            return result
    return {}

def get_address_latlng(address_input, city_input):
    if (not address_input) or (address_input == 'null'):
        return {}
    info = api_call(f"{address_input}, {city_input}")
    if info:
        return info['geometry']['location']
    return {}

@persistant_cache('cache_geocode_city.json')
def get_city_latlng(city_input):
    if (not city_input) or (city_input == 'null'):
        return {}, np.nan
    info = api_call(city_input)
    if info:
        bounds = info['geometry']['viewport']
        return get_city_centre(bounds), get_city_size(bounds)
    return {}, np.nan

def get_city_centre(bounds):
    location = {}
    location['lat'] = mean([x['lat'] for x in bounds.values()])
    location['lng'] = mean([x['lng'] for x in bounds.values()])
    return location

def get_city_size(bounds):
    lat_diff = bounds['northeast']['lat'] - bounds['southwest']['lat']
    lng_diff = bounds['northeast']['lng'] - bounds['southwest']['lng']
    area = abs(lat_diff * lng_diff)
    return area

def geocode(df, retry_na=False):
    if not len(df):
        return df
    if (not retry_na) and ('address' not in df.columns or 'city' not in df.columns):
        raise ValueError("Input DataFrame does not contain all required columns (`city` and `address`)")
    df['address_latlng'] = df.apply(lambda row: get_address_latlng(row.address, row.city), axis=1)
    df['city_latlng_size'] = df.city.apply(lambda x: get_city_latlng(str(x).lower()))
    df['address_lat'] = df.address_latlng.apply(lambda x: x.get('lat', np.nan))
    df['address_lng'] = df.address_latlng.apply(lambda x: x.get('lng', np.nan))
    df['city_lat'] = df.city_latlng_size.apply(lambda x: x[0].get('lat', np.nan))
    df['city_lng'] = df.city_latlng_size.apply(lambda x: x[0].get('lng', np.nan))
    df['city_size'] = df.city_latlng_size.apply(lambda x: x[1])
    df.drop(['address_latlng', 'city_latlng_size'], axis=1, inplace=True)
    return df

def geo_update_db_table(table_name, start_date=None, end_date=None, limit=None):
    update_geo_data = """
        UPDATE {}
        SET address_lat = %s,
            address_lng = %s,
            city_lat = %s,
            city_lng = %s,
            city_size = %s
        WHERE {} = %s
    """
    if table_name == 'company_projects':
        fetch_jobs = """
            SELECT * from company_projects ORDER BY project_id DESC
        """
        match_id = 'project_id'
        limit_params = []
        update_geo_data = update_geo_data.format(table_name, match_id)
    elif table_name == 'web_certificates':
        fetch_jobs = """
            SELECT * from web_certificates WHERE pub_date BETWEEN %s AND %s ORDER BY pub_date DESC
        """
        match_id = 'cert_id'
        limit_params = [start_date, end_date]
        update_geo_data = update_geo_data.format(table_name, match_id)
    else:
        raise ValueError("Invalid input `table_name`. Choice of `web_certificates` or `company_projects`")
    if limit:
        fetch_jobs = fetch_jobs + f" LIMIT {limit}"
    with create_connection() as conn:
        df = pd.read_sql(fetch_jobs, conn, params=limit_params)
    for i, row in df.iterrows():
        if any([True if str(x) not in ['nan', 'None']  else False for x in row.loc[['address_lat', 'city_lat']]]):
            logger.info(f"Job {row.loc[match_id]} ({row.pub_date.iloc[0]}) already has geo data - skipping out")
            continue 
        row = pd.DataFrame(row).transpose()
        row = geocode(row)
        def nullify(x):
            if np.isnan(x):
                return None
            else:
                return x
        with create_connection() as conn:
            conn.cursor().execute(update_geo_data, [
                nullify(row.loc[i, 'address_lat']),
                nullify(row.loc[i, 'address_lng']),
                nullify(row.loc[i, 'city_lat']),
                nullify(row.loc[i, 'city_lng']),
                nullify(row.loc[i, 'city_size']),
                nullify(row.loc[i, match_id])
            ])
            conn.commit()
        logger.info(f"Job {row.loc[i, match_id]} ({row.pub_date.iloc[0] if table_name == 'web_certificates' else ''})  has been updated with geo data")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('table_name',
        type=str,
        help="pass in the name of a table containg text columns for `city` and `address` and it will be rewritten with geocode data",
    )
    parser.add_argument(
        "-s",
        "--start_date",
        nargs='?',
        type=str,
        help="limits the amount of jobs to process, starting with the latest one.",
    )
    parser.add_argument(
        "-e",
        "--end_date",
        nargs='?',
        type=str,
        help="limits the amount of jobs to process, starting with the latest one.",
    )
    parser.add_argument(
        "-l",
        "--limit",
        nargs='?',
        type=str,
        help="limits the amount of jobs to process, starting with the latest one.",
    )
    args = parser.parse_args()
    try:
        geo_update_db_table(args.table_name, args.start_date, args.end_date, limit=args.limit)
        
    except AttributeError:
        geo_update_db_table(args.table_name)
