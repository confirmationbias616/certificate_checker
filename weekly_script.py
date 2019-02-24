import datetime
import pandas as pd
from scraper import scrape
from wrangler import wrangle
from matcher import match

try:
	pd.read_csv(f'./data/raw_web_certs_{datetime.datetime.now().date()}.csv')
	print("Already scraped today. Skipping on to wrangling!")
except FileNotFoundError:
	scrape()
wrangle()
match()