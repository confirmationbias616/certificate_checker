from fuzzywuzzy import fuzz
import numpy as np
from utils import create_connection


def compile_score(row, scoreable_attrs, style):
    """Compiles the total score of each column in the row that was individually scored
    against its attribute counterpart and sports the `_score` suffix."""
    scores = row[[f"{attr}_score" for attr in scoreable_attrs]]
    scores = [x / 100 for x in scores if type(x) == int]
    if style == "multiply":
        countable_attrs = len([x for x in scores if x > 0])
        score = sum(scores) / countable_attrs if countable_attrs > 2 else 0
    elif style == "add":
        score = sum(scores)
    else:
        raise ValueError(f"style parameter {style} not recognized")
    return score


def attr_score(web_str, company_project_str, match_style="full"):
    """Compares strings from both a company project entry and web certificate entry for
    the same attribute (column) and returns a fuzzy match score as a float ranging
    between 0 and 1. `match_style` can be `"full"` or `"partial"`. Refer to fuzzywuzzy
    docs for more info."""
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

def use_fresh_certs_only(single_project_row, web_df):
    """if True, `last_cert_id_check` will be read for assembling only fresh
    web certificates for given project and databse will be updated afterwards as well.
    
    Parameters:
     - `single_project_row` (pd.Series): row of company project to match.
     - `web_df` (pd.DataFrame): dataframe of CSP certificates to match to the
     company project.

    Returns:
     - a Pandas DataFrame containing fresh certificates for given project. This will 
     be a subset of initial `web_df` input.
    
    """
    try:
        possible_matches_scored = web_df[web_df.cert_id > int(single_project_row.last_cert_id_check)]
    except (TypeError, ValueError):  # last_cert_id_check was `NULL`
        possible_matches_scored = web_df
    update_query = """ 
        UPDATE company_projects 
        SET last_cert_id_check=?
        WHERE project_id=?
    """
    with create_connection() as conn:
        conn.cursor().execute(update_query, [max(possible_matches_scored.cert_id), single_project_row.project_id])
    return web_df

def build_match_score(single_project_df, web_df, fresh_cert_limit=True):
    """Builds a possible match dataframe of one-to many relationship between specified
    company project and all web certificates along with many added columns of engineered
    features that the Random Forest Classifier will be looking for.
    
    Parameters:
     - `single_project_df` (pd.DataFrame): specify dataframe of company project to match.
     Must be a single-row dataframe containing only one project, due to legacy code.
     - `web_df` (pd.DataFrame): specify dataframe of CSP certificates to match to the
     company project.
     - `fresh_cert_limit` (bool): specify whether or not to apply `use_fresh_certs_only()`

    Returns:
     - a Pandas DataFrame containing new certificates if Test=True

    """
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
    for (
        _,
        company_project_row,
    ) in (
        single_project_df.iterrows()
    ):  # nescessary even when company_projects is single row because it's still DataFrame instead of Series
        if fresh_cert_limit:
            possible_matches_scored = use_fresh_certs_only(company_project_row, web_df)
        for attr in scoreable_attrs:
            for attr_suffix, match_style in zip(
                ["score", "pr_score"], ["full", "partial"]
            ):
                possible_matches_scored[
                    f"{attr}_{attr_suffix}"
                ] = possible_matches_scored.apply(
                    lambda web_row: attr_score(
                        web_row[attr],
                        company_project_row[attr],
                        match_style=match_style,
                    ),
                    axis=1,
                )
        possible_matches_scored["total_score"] = possible_matches_scored.apply(
            lambda row: compile_score(row, scoreable_attrs, "multiply"), axis=1
        )
        return possible_matches_scored
