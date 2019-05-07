from log_user_input import log_user_input
from scraper import scrape
from matcher import match
from ml import build_train_set, train_model

log_user_input()
scrape()
build_train_set()
train_model()
match()  #test=True to mute sending of e-mails
