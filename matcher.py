from fuzzywuzzy import fuzz
import pandas as pd
import datetime
from communicator import communicate


def match():
	df_dilfo = pd.read_csv('./data/clean_dilfo_certs.csv')
	df_web = pd.read_csv(f'./data/clean_web_certs_{datetime.datetime.now().date()}.csv')

	for i in range(len(df_dilfo)):
		print(f"searching for potential match for project #{df_dilfo.iloc[i].job_number}...")
		df_web['contractor_match'] = df_web['contractor'].apply(lambda row: fuzz.ratio(row, df_dilfo.iloc[i]['contractor']))
		ranked = df_web.sort_values('contractor_match', ascending=False)
		if ranked.iloc[0]['contractor_match'] > 90:
			print('\t-> Found a match! -> getting ready to send notification...')
			communicate(ranked.iloc[0], df_dilfo.iloc[i])
		else:
			print("\t-> nothing found.")

if __name__=="__main__":
	match()