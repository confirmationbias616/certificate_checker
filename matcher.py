from fuzzywuzzy import fuzz
import pandas as pd
import datetime
import numpy as np
from wrangler import wrangle
from communicator import communicate
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

def match(df_dilfo=False, df_web=False, test=False, min_score_thresh=0.66):
	if not isinstance(df_dilfo, pd.DataFrame):  # df_dilfo == False
		open_query = "SELECT * FROM dilfo_open"
		conn = create_connection(database)
		with conn:
			df_dilfo = pd.read_sql(open_query, conn).drop('index', axis=1)
		df_dilfo = wrangle(df_dilfo)
	if not isinstance(df_web, pd.DataFrame):  # df_web == False
		7_days_ago = (datetime.datetime.now()-datetime.timedelta(7)).date()
		hist_query = "SELECT * FROM hist_certs WHERE pub_date>=? ORDER BY pub_date"
		conn = create_connection(database)
        with conn:
			df_web = pd.read_sql(hist_query, conn, params=[7_days_ago]).drop('index', axis=1)
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
		ranked = df_web.sort_values('total_score', ascending=False)
		top_score = ranked.iloc[0]['total_score']
		if top_score > min_score_thresh:
			print(
				f"\t-> Found a match with score of {top_score}!"
				f"\t-> Dilfo job details: {df_dilfo.iloc[i]}"
				f"\t-> web job details: {ranked.iloc[0]}"
				f"\n\tgetting ready to send notification..."
			)
			communicate(ranked.iloc[0], df_dilfo.iloc[i], test=test)
		else:
			print("\t-> nothing found.")
			if test:
				return ranked.drop(ranked.index)  # short-circuit out of loop of best match is not good enough
	if test:
		return ranked

if __name__=="__main__":
	match()