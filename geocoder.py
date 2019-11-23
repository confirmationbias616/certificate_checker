import pandas as pd
import requests
import json
import numpy as np
from statistics import mean
from functools import lru_cache

try:
    with open(".api_key.txt") as file:
        api_key = file.read()
except FileNotFoundError:  # no key if running in CI
    pass

def api_call(address_param):
    api_request = "https://maps.googleapis.com/maps/api/geocode/json?address={}&bounds=41.6765559,-95.1562271|56.931393,-74.3206479&key={}"
    response = requests.get(api_request.format(address_param, api_key))
    return json.loads(response.content)

@lru_cahce()
def get_address_latlng(address_input):
    if address_input == '':
        return {}
    info = api_call(address_input)
    if info['status'] == 'OK':
        return info['results'][0]['geometry']['location']
    return {}  

@lru_cache()
def get_city_latlng(city_input):
    if city_input == '':
        return {}
    info = api_call(city_input)
    if info['status'] == 'OK':
        bounds = info['results'][0]['geometry']['viewport']
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

def geocode(df):
    if 'address' not in df.columns or 'city' not in df.columns:
        raise ValueError("Input DataFrame does not contain all required columns (`city` and `address`)")
    df['address_latlng'] = df.address.apply(lambda x: get_address_latlng(x))
    df['city_latlng_size'] = df.city.apply(lambda x: get_city_latlng(x))
    df['address_lat'] = df.address_latlng.apply(lambda x: x.get('lat', np.nan))
    df['address_lng'] = df.address_latlng.apply(lambda x: x.get('lng', np.nan))
    df['city_lat'] = df.city_latlng_size.apply(lambda x: x[0].get('lat', np.nan))
    df['city_lng'] = df.city_latlng_size.apply(lambda x: x[0].get('lng', np.nan))
    df['city_size'] = df.city_latlng_size.apply(lambda x: x[1])
    df.drop(['address_latlng', 'city_latlng_size'], axis=1, inplace=True)
    return df

