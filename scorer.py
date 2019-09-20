from fuzzywuzzy import fuzz
import numpy as np


def compile_score(row, scoreable_attrs):
    scores = row[[f"{attr}_score" for attr in scoreable_attrs]]
    scores = [x / 100 for x in scores if type(x) == int]
    countable_attrs = len([x for x in scores if x > 0])
    total_score = sum(scores) / countable_attrs if countable_attrs > 2 else 0
    return total_score


def compile_score_add(row, scoreable_attrs):
    scores = row[[f"{attr}_score" for attr in scoreable_attrs]]
    scores = [x / 100 for x in scores if type(x) == int]
    total_add_score = sum(scores)
    return total_add_score


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

def build_match_score(company_projects, df_web):
    scoreable_attrs = [
        "contractor",
        "street_name",
        "street_number",
        "title",
        "city",
        "owner",
    ]
    for (
        _,
        company_project_row,
    ) in (
        company_projects.iterrows()
    ):  # nescessary even when company_projects is single row because it's still DataFrame instead of Series
        for attr in scoreable_attrs:
            for attr_suffix, match_style in zip(
                ["score", "pr_score"], ["full", "partial"]
            ):
                df_web[f"{attr}_{attr_suffix}"] = df_web.apply(
                    lambda web_row: attr_score(
                        web_row[attr],
                        company_project_row[attr],
                        match_style=match_style,
                    ),
                    axis=1,
                )
        df_web["total_score"] = df_web.apply(
            lambda row: compile_score(row, scoreable_attrs), axis=1
        )
        if (
            len(company_projects) == 1
        ):  # if single row was passed instead of actual dataframe
            return df_web
