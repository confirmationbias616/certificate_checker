import datetime
import pandas as pd
import numpy as np
from cleanco import cleanco
import unidecode
import re


def clean_job_number(raw):
    try:
        job_number = re.findall("\d{4}", str(raw))[0]
        return job_number
    except IndexError:
        return ""


def clean_pub_date(raw):
    try:
        date = re.findall("\d{4}\-\d{2}\-\d{2}", str(raw))[0]
        return date
    except IndexError:
        return ""


def clean_city(raw):
    if raw == " ":
        return ""
    raw = unidecode.unidecode(raw)
    city = raw.lower()
    for variant in [" ste ", " ste. ", " ste-", " st ", " saint ", " sainte ", " st-"]:
        city = city.replace(variant, " st ")
    city = city.replace("of the ", "")
    if "of " in city:
        city = city.split("of ")[1]
    if ("county, " in city) and ("county, on" not in city):
        city = city.split("county, ")[1]
    for sep in (",", " - "):
        if sep in city:
            city = city.split(sep)[0]
    city = city.replace("-", " ")
    for word in [
        "city",
        "county",
        "municipality",
        "district",
        "ward",
        "township",
        "greater",
        "region",
    ]:
        city = city.replace(word, " ")
    city = city.rstrip(" ").lstrip(" ")
    if city.endswith(" on", -3):
        city = city[:-3]
    elif "ontario" in city:
        city = city.strip("ontario")
    city = "".join([x.rstrip("'s") for x in city.split(" ")])
    city = city.replace("/", "&")
    if city == "ottawacarleton":
        city = "ottawa"
    return city


def clean_company_name(raw):
    if raw in (" ", "None", None):
        return ""
    name = unidecode.unidecode(raw)
    try:
        name = re.findall("o/a (.*)", name, flags=re.I)[0]
    except IndexError:
        pass
    try:
        name = re.findall("c/o (.*)", name, flags=re.I)[0]
    except IndexError:
        pass
    try:
        name = re.findall("(.*) for ", name, flags=re.I)[0]
    except IndexError:
        pass
    name = cleanco(name).clean_name()
    name = name.lower()
    for stopword in ["of", "d'", "l'"]:
        name = name.replace(stopword, "")
    name = name.replace("and", "&")
    for punct in ["-", ".", ",", "(", ")"]:
        name = name.replace(punct, " ")
    for punct in ["'"]:
        name = name.replace(punct, "")
    if (not name.startswith("s ")) and (not " s " in name):
        name = " ".join([word.rstrip("s") for word in name.split(" ")])
    name = "".join([word for word in name.split(" ")])
    for word in [
        "constructor",
        "construction",
        "contracting",
        "contractor",
        "mechanical",
        "plumbing",
        "heating",
        "mech",
        "electrical",
        "electric",
        "development",
        "interior" "builders",
        "building",
        "enterprise",
        "infrastructure",
        "management",
        "excavating",
        "trucking",
        "company",
        "restoration",
        "service",
        "servicing",
        "hvac",
        "system",
        "paving",
        "industrie",
        "industry",
        "engineering",
        "consulting",
        "consultant",
        "solution",
        "commercial",
        "group",
        "insulation",
        "insulators",
        "ontario",
        "canada",
    ]:
        name = name.replace(word, "")
    return name


def get_acronyms(raw):
    acronyms = re.findall("[A-Z\-\&]{3,}", str(raw))
    return acronyms


def get_street_number(raw):
    if raw == " ":
        return ""
    try:
        number = re.findall(" ?(\d+) \w", raw)[0]
    except IndexError:
        return ""
    try:
        int(number)
        return number
    except ValueError:
        return ""


def get_street_name(raw):
    if raw == " " or (raw == None):
        return ""
    raw = raw.lower()
    raw = unidecode.unidecode(raw)
    try:
        num = re.findall(" ?(\d+) \w", raw)[0]
    except IndexError:
        return ""
    rest = re.findall(f"{num} (.*)", raw)[0]
    if any(
        [rest.startswith(x) for x in ["apt ", "apt. ", "apartment ", "unit ", "suite "]]
    ):
        if "," in rest:
            rest = rest.split(",")[1].lstrip(" ")
        else:
            return ""
    for saint_word in ["st ", "st. ", "saint ", "st-", "saint-"]:
        if rest.startswith(saint_word):
            rest = rest.replace(saint_word, "")
            break
    try:
        name = rest.split(" ")[0]
    except IndexError:
        pass

    if name.isalpha() or "-" in name:
        return name
    else:
        return ""


def clean_title(raw):
    if raw == " ":
        return ""
    raw = unidecode.unidecode(raw)
    title = raw.lower()
    return title


def wrangle(df):
    def wrangle_coord(df):
        clean_ops = {
            "job_number": clean_job_number,
            "pub_date": clean_pub_date,
            "city": clean_city,
            "title": clean_title,
            "owner": clean_company_name,
            "contractor": clean_company_name,
        }
        for attr in clean_ops:
            try:
                df[attr] = df[attr].apply(clean_ops[attr])
            except (KeyError, AttributeError):
                pass
        get_address_ops = {
            "street_number": get_street_number,
            "street_name": get_street_name,
        }
        for attr in get_address_ops:
            df[attr] = df["address"].astype("str").apply(get_address_ops[attr])
        for attr in ["title", "owner", "contractor"]:
            df[f"{attr}_acronyms"] = df[attr].apply(get_acronyms)
        return df

    if isinstance(df, pd.DataFrame):
        return wrangle_coord(df)
    else:
        raise Exception("Need to pass in a DataFrame!")
