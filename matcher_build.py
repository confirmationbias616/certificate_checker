

def match_build(df_dilfo=False, df_web=False):
	for i in range(len(df_dilfo)):
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
	return df_web
