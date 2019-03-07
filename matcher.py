from fuzzywuzzy import fuzz
import pandas as pd
import datetime
from communicator import communicate


def match(df_dilfo=False, df_web=False, test=False, threshold=0.9):
	if not isinstance(df_dilfo, pd.DataFrame):
		df_dilfo = pd.read_csv('./data/clean_dilfo_certs.csv')
	if not isinstance(df_web, pd.DataFrame):
		df_web = pd.read_csv(f'./data/clean_web_certs_{datetime.datetime.now().date()}.csv')
	match_count = 0
	for i in range(len(df_dilfo)):
		print(f"searching for potential match for project #{df_dilfo.iloc[i].job_number}...")
		def attr_score(row, i, attr):
			try:
				return fuzz.ratio(row, df_dilfo.iloc[i][attr])
			except TypeError:
				return 0
		scoreable_attrs = ['contractor', 'street_name', 'street_number', 'title', 'city', 'owner']
		for attr in scoreable_attrs:
				df_web[f'{attr}_score'] = df_web[attr].apply(lambda row: attr_score(row, i, attr))
		def compile_score(row):
		    scores = row[[f'{attr}_score' for attr in scoreable_attrs]]
		    scores = [x/100 for x in scores if type(x)==int]
		    return sum(scores)/len(scores)
		df_web['total_score'] = df_web.apply(lambda row: compile_score(row), axis=1)
		ranked = df_web.sort_values('total_score', ascending=False)
		top_score = ranked.iloc[0]['total_score']
		if top_score > threshold:
			print(
				f"\t-> Found a match with score of {top_score}!"
				f"\n\tgetting ready to send notification..."
			)
			communicate(ranked.iloc[0], df_dilfo.iloc[i], test=test)
			match_count += 1
		else:
			print("\t-> nothing found.")
	return match_count

if __name__=="__main__":
	match()