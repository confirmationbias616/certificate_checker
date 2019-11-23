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

def get_address_latlng(address_input):
    if address_input == '':
        return {}
    info = api_call(address_input)
    if info['status'] == 'OK':
        return info['results'][0]['geometry']['location']
    return {}  

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

