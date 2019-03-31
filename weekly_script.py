import datetime
import pandas as pd
from log_user_input import log_user_input
from scraper import scrape
from wrangler import wrangle
from matcher import match
from build_train_set import build_train_set
from ml import train_model

log_user_input()
try:
    pd.read_csv(f'./data/raw_web_certs_{datetime.datetime.now().date()}.csv')
    print("Already scraped today. Skipping on to wrangling!")
except FileNotFoundError:
    scrape()
build_train_set()
train_model()
wrangle()
match()
