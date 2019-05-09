import pandas as pd
import datetime
from wrangler import wrangle
from communicator import communicate
from scorer import compile_score, attr_score
from matcher_build import match_build
import pickle
from db_tools import create_connection
import re


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

def match(df_dilfo=False, df_web=False, test=False, since='week_ago'):
	if not isinstance(df_dilfo, pd.DataFrame):  # df_dilfo == False
		open_query = "SELECT * FROM dilfo_open"
		with create_connection() as conn:
			df_dilfo = pd.read_sql(open_query, conn)
	df_dilfo = wrangle(df_dilfo)
	if not isinstance(df_web, pd.DataFrame):  # df_web == False
		if since == 'week_ago':
			since = (datetime.datetime.now()-datetime.timedelta(7)).date()
		else:
			valid_since_date = re.search("\d{4}-\d{2}-\d{2}", since)
			if not valid_since_date:
				raise ValueError("`since` parameter should be in the format yyyy-mm-dd if not default value of `week_ago`")
		hist_query = "SELECT * FROM hist_certs WHERE pub_date>=? ORDER BY pub_date"
		with create_connection() as conn:
			df_web = pd.read_sql(hist_query, conn, params=[since])
		if len(df_web) == 0:  # SQL query retunred nothing so no point of going any further
			print("Nothing has been collected from Daily commercial News in the past week. Breaking out of match function.")
			return 0
	df_web = wrangle(df_web)
	for _, dilfo_row in df_dilfo.iterrows():
		results = match_build(dilfo_row.to_frame().transpose(), df_web)  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
		# LOGICAL BREAK IN FUNCTION? TIME TO SPLIT FUNC INTO 2?!?!?!
		print(f"searching for potential match for project #{dilfo_row['job_number']}...")
		results['pred_match'] = results.apply(lambda row: predict_match(row), axis=1)
		results['pred_prob'] = results.apply(lambda row: predict_prob(row), axis=1)
		results = results.sort_values('pred_prob', ascending=False)
		matches = results[results.pred_prob>=0.5]
		msg = 	"\n-> Found {} match with probability of {}!" +\
				"-> Dilfo job details:\n{}" +\
				"-> web job details:\n{}"
		try:
			top = matches.iloc[0]
			print(msg.format('a', top.pred_prob, dilfo_row, top))
			print("\tgetting ready to send notification...")
			communicate(top, dilfo_row, test=test)
			if len(matches) > 1:
				for _, row in matches[1:].iterrows():
					print(msg.format('another possible', row['pred_prob'], dilfo_row, row))
			else:
				print('no secondary matches found')
		except IndexError:
			print('no matches found')
		try:
			results_master = results_master.append(results)
		except NameError:
			results_master = results

	if test:
		return results_master

if __name__=="__main__":
	match()
