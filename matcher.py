from fuzzywuzzy import fuzz
import pandas as pd
import datetime
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from wrangler import wrangle
from communicator import communicate
import pickle
import sqlite3
from sqlite3 import Error


database = 'cert_db'

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None

def load_model():
    with open("./rf_model.pkl", "rb") as input_file:
        return pickle.load(input_file)

def load_feature_list():
    with open("./rf_features.pkl", "rb") as input_file:
        return pickle.load(input_file)

def predict_match(sample):
	clf = load_model()
	cols = load_feature_list()
	match = clf.predict(sample[cols].values.reshape(1, -1))[0]
	return match

def predict_prob(sample):
    clf = load_model()
    cols = load_feature_list()
    prob = clf.predict_proba(sample[cols].values.reshape(1, -1))[0][1]
    return prob

def match(df_dilfo=False, df_web=False, test=False, min_score_thresh=0.66):
	if not isinstance(df_dilfo, pd.DataFrame):  # df_dilfo == False
		open_query = "SELECT * FROM dilfo_open"
		conn = create_connection(database)
		with conn:
			df_dilfo = pd.read_sql(open_query, conn).drop('index', axis=1)
		df_dilfo = wrangle(df_dilfo)
	if not isinstance(df_web, pd.DataFrame):  # df_web == False
		week_ago = (datetime.datetime.now()-datetime.timedelta(7)).date()
		hist_query = "SELECT * FROM hist_certs WHERE pub_date>=? ORDER BY pub_date"
		conn = create_connection(database)
		with conn:
			df_web = pd.read_sql(hist_query, conn, params=[week_ago]).drop('index', axis=1)
		df_web = wrangle(df_web)
	for i in range(len(df_dilfo)):
		print(f"searching for potential match for project #{df_dilfo.iloc[i].job_number}...")
		def attr_score(row, i, attr, seg='full'):
			if row in ["", " ", "NaN", "nan", np.nan]:  # should not be comparing empty fields because empty vs empty is an exact match!
				return 0
			try:
				if seg=='full':
					return fuzz.ratio(row, df_dilfo.iloc[i][attr])
				else:
					return fuzz.partial_ratio(row, df_dilfo.iloc[i][attr])
			except TypeError:
				return 0
		scoreable_attrs = ['contractor', 'street_name', 'street_number', 'title', 'city', 'owner']
		for attr in scoreable_attrs:
				df_web[f'{attr}_score'] = df_web[attr].apply(
					lambda row: attr_score(row, i, attr, seg='full'))
				df_web[f'{attr}_pr_score'] = df_web[attr].apply(
					lambda row: attr_score(row, i, attr, seg='partial'))
		def compile_score(row):
		    scores = row[[f'{attr}_score' for attr in scoreable_attrs]]
		    scores = [x/100 for x in scores if type(x)==int]
		    countable_attrs = len([x for x in scores if x > 0])
		    total_score = sum(scores)/countable_attrs if countable_attrs > 2 else 0
		    return total_score
		df_web['total_score'] = df_web.apply(lambda row: compile_score(row), axis=1)
		results = df_web.copy()
		# LOGICAL BREAK IN FUNCTION? TIME TO SPLIT FUNC INTO 2?!?!?!
		results['pred_match'] = results.apply(lambda row: predict_match(row), axis=1)
		results['pred_prob'] = results.apply(lambda row: predict_prob(row), axis=1)
		results = results.sort_values('pred_prob', ascending=False)
		matches = results[results.pred_match==1]
		msg = 	"\t-> Found {} match with probability of {}!" +\
				"\t-> Dilfo job details: {}" +\
				"\t-> web job details: {}"
		try:
			top = matches.iloc[0]
			print(msg.format('a', top.pred_prob, df_dilfo.iloc[i], top))
			print("\tgetting ready to send notification...")
			communicate(top, df_dilfo.iloc[i], test=test)
			if len(matches) > 1:
				for _, row in matches[1:].iterrows():
					print(msg.format('another possible', row['pred_prob'], df_dilfo.iloc[i], row))
			else:
				print('no secondary matches found')
		except IndexError:
			print('no matches found')
	if test:
		return results

if __name__=="__main__":
	match()