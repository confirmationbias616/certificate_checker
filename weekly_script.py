import datetime
import pandas as pd
from scraper import scrape
from wrangler import wrangle
from matcher import match

scrape()
try:
	pd.read_csv(f'./data/raw_web_certs_{datetime.datetime.now().date()}.csv')
except FileNotFoundError:
	scrape()
wrangle()
match()