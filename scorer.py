from fuzzywuzzy import fuzz
import numpy as np


def compile_score(row, scoreable_attrs):
    scores = row[[f'{attr}_score' for attr in scoreable_attrs]]
    scores = [x/100 for x in scores if type(x)==int]
    countable_attrs = len([x for x in scores if x > 0])
    total_score = sum(scores)/countable_attrs if countable_attrs > 2 else 0
    return total_score

def attr_score(web_str, dilfo_str, match_style='full'):
    if web_str in ["", " ", "NaN", "nan", np.nan]:  # should not be comparing empty fields because empty vs empty is an exact match!
        return 0
    try:
        if match_style == 'full':
            return fuzz.ratio(web_str, dilfo_str)
        else:
            return fuzz.partial_ratio(web_str, dilfo_str)
    except TypeError:
        return 0