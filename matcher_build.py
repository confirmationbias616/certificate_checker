from scorer import compile_score, compile_score_add, attr_score


def match_build(company_projects, df_web):
	scoreable_attrs = ['contractor', 'street_name', 'street_number', 'title', 'city', 'owner']
	for _, company_project_row in company_projects.iterrows():  # nescessary even when company_projects is single row because it's still DataFrame instead of Series
		for attr in scoreable_attrs:
			for attr_suffix, match_style in zip(['score', 'pr_score'], ['full', 'partial']):
				df_web[f'{attr}_{attr_suffix}'] = df_web.apply(lambda web_row: attr_score(
						web_row[attr], company_project_row[attr], match_style=match_style), axis=1)
		df_web['total_score'] = df_web.apply(lambda row: compile_score(
			row, scoreable_attrs), axis=1)
		if len(company_projects) == 1:  # if single row was passed instead of actual dataframe
			return df_web
