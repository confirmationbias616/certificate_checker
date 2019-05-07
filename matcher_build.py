from scorer import compile_score, attr_score


def match_build(df_dilfo, df_web):
	scoreable_attrs = ['contractor', 'street_name', 'street_number', 'title', 'city', 'owner']
	for _, dilfo_row in df_dilfo.iterrows():  # nescessary even when df_dilfo is single row because it's still DataFrame instead of Series
		for attr in scoreable_attrs:
			for attr_suffix, match_style in zip(['score', 'pr_score'], ['full', 'partial']):
				df_web[f'{attr}_{attr_suffix}'] = df_web.apply(lambda web_row: attr_score(
						web_row[attr], dilfo_row[attr], match_style=match_style), axis=1)
		df_web['total_score'] = df_web.apply(lambda row: compile_score(
			row, scoreable_attrs), axis=1)
		if len(df_dilfo) == 1:  # if single row was passed instead of actual dataframe
			return df_web
