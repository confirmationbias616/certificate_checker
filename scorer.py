from fuzzywuzzy import fuzz
import numpy as np


def compile_score(row, scoreable_attrs, style):
    scores = row[[f"{attr}_score" for attr in scoreable_attrs]]
    scores = [x / 100 for x in scores if type(x) == int]
    if style == 'multiply':
        countable_attrs = len([x for x in scores if x > 0])
        score = sum(scores) / countable_attrs if countable_attrs > 2 else 0
    elif style =='add':
        score = sum(scores)
    else:
        raise ValueError(f"style parameter {style} not recognized")
    return score

def attr_score(web_str, company_project_str, match_style="full"):
    if web_str in [
        "",
        " ",
        "NaN",
        "nan",
        np.nan,
    ]:  # should not be comparing empty fields because empty vs empty is an exact match!
        return 0
    try:
        if match_style == "full":
            return fuzz.ratio(web_str, company_project_str)
        else:
            return fuzz.partial_ratio(web_str, company_project_str)
    except TypeError:
        return 0

def build_match_score(single_project_df, web_df):
    if len(single_project_df) > 1:
        raise ValueError(
            f"`company_projects` dataframe was suppose to conatin only 1 single row - "
            f"it contained {len(single_project_df)} rows instead."
        )
    scoreable_attrs = [
        "contractor",
        "street_name",
        "street_number",
        "title",
        "city",
        "owner",
    ]
    possible_matches_scored = web_df  # renaming for clarity
    for (
        _,
        company_project_row,
    ) in (
        single_project_df.iterrows()
    ):  # nescessary even when company_projects is single row because it's still DataFrame instead of Series
        for attr in scoreable_attrs:
            for attr_suffix, match_style in zip(
                ["score", "pr_score"], ["full", "partial"]
            ):
                possible_matches_scored[f"{attr}_{attr_suffix}"] = possible_matches_scored.apply(
                    lambda web_row: attr_score(
                        web_row[attr],
                        company_project_row[attr],
                        match_style=match_style,
                    ),
                    axis=1,
                )
        possible_matches_scored["total_score"] = possible_matches_scored.apply(
            lambda row: compile_score(row, scoreable_attrs, 'multiply'), axis=1
        )
        return possible_matches_scored
