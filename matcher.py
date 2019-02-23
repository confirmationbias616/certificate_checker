from fuzzywuzzy import fuzz
import pandas as pd
import datetime
from communicator import communicate


def match():
	df_dilfo = pd.read_csv('./data/clean_dilfo_certs.csv')
	df_web = pd.read_csv(f'./data/clean_web_certs_{datetime.datetime.now().date()}.csv')

	for i in range(len(df_dilfo)):
		df_web['contractor_match'] = df_web['contractor'].apply(lambda row: fuzz.ratio(row, df_dilfo.iloc[i]['contractor']))
		ranked = df_web.sort_values('contractor_match', ascending=False)
		for i in range(len(ranked)):
			if ranked.iloc[i]['contractor_match'] > 90:
				send_email(ranked.iloc[i])
			else:
				break

if __name__=='__main__':
	match()