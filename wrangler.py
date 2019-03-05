import datetime 
import pandas as pd
import numpy as np
from cleanco import cleanco
import unidecode
import re


def clean_job_number(raw):
    try:
        job_number = re.compile('\d{4}').findall(str(raw))[0]
        return job_number
    except IndexError:
        return ""

def clean_pub_date(raw):
    try:
        date = re.compile('\d{4}\-\d{2}\-\d{2}').findall(str(raw))[0]
        return date
    except IndexError:
        return ""

def clean_city(raw):
    if raw == " ":
        return ""
    city = raw.lower()
    for variant in [
        " ste ",
        " ste. ",
        " ste-",
        " st ",
        " saint ",
        " sainte ",
        " st-"
        ]: city = city.replace(variant," st ")
    city = city.replace("of the ","")
    if "of " in city:
        city = city.split("of ")[1]
    if ("county, " in city) and ("county, on" not in city):
        city = city.split("county, ")[1]
    for sep in (",", " - "):
        if sep in city:
            city = city.split(sep)[0]
    city = city.replace("-"," ")
    for word in [
        "city",
        "county",
        "municipality",
        "district",
        "ward",
        "township",
        "greater",
        "region"
        ]: city = city.replace(word," ")
    city = city.rstrip(" ").lstrip(" ")
    if city.endswith(" on", -3):
        city = city[:-3]
    elif "ontario" in city:
        city = city.strip("ontario")
    city = "".join([x.rstrip("'s") for x in city.split(" ")])
    city = city.replace("/","&")
    return city

def clean_company_name(raw):
    if raw == " ":
        return ""
    name = unidecode.unidecode(raw)
    name = cleanco(name).clean_name()
    name = name.lower()
    for stopword in [
        "of", 
        "d'",
        "l'"
    ]:
        name = name.replace(stopword,"")
    name = name.replace("and","&")
    for punct in ["-", ".", ","]:
        name = name.replace(punct," ")
    for punct in ["'"]:
        name = name.replace(punct,"")
    if (not name.startswith('s ')) and (not " s " in name):
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
        "interior"
        "builders",
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
        "insulators"
    ]: name = name.replace(word,"")
    return name

def get_acronyms(raw):
    acronyms = re.compile('[A-Z\-\&]{3,}').findall(str(raw))
    return acronyms


def get_street_number(raw):
    if raw == " ":
        return ""
    raw = raw.lower()
    if any([raw.startswith(x) for x in ["apt", "unit", "suite"]]):
    	raw = raw.split(",")[1].lstrip(" ")
    elif "-" in raw[:3]:
    	raw = raw.split("-")[1]
    number = raw.split(' ')[0]
    try:
        int(number)
        return number
    except ValueError:
        return ""

def get_street_name(raw):
    if raw == " ":
        return ""
    raw = raw.lower()
    raw = unidecode.unidecode(raw)
    if any([raw.startswith(x) for x in ["apt", "unit", "suite"]]):
    	raw = raw.split(",")[1].lstrip(" ")
    elif "," in raw and any([x in raw.split(",")[0] for x in ["apt", "unit", "suite"]]):
        raw = raw.split(",")[1]
    try:
        name = raw.split(' ')[1]
    except IndexError:
        return ""
    for unit_word in ["apt", "unit", "suite"]:
    	name = name.replace(unit_word, "")
    if name.isalpha():
    	return name
    else:
    	return ""

def clean_title(raw):
    if raw == " ":
        return ""
    title = raw
    return title

def wrangle(ref=False, filenames=['./data/raw_dilfo_certs.csv', f'./data/raw_web_certs_{datetime.datetime.now().date()}.csv']):
    def wrangle_coord(df):
        clean_ops = {
        'job_number': clean_job_number,
        'pub_date': clean_pub_date,
        'city': clean_city,
        'title': clean_title,
        'owner': clean_company_name,
        'contractor': clean_company_name,
        }
        for attr in clean_ops:
            try:
                df[attr] = df[attr].apply(clean_ops[attr])
            except (KeyError, AttributeError):
                pass
        get_address_ops = {
        'street_number': get_street_number,
        'street_name': get_street_name,
        }
        for attr in get_address_ops:
            df[attr] = df['address'].astype('str').apply(get_address_ops[attr])
        for attr in ["title","owner","contractor"]:
            df[f'{attr}_acronyms'] = df[attr].apply(get_acronyms)
        return df
    if isinstance(ref, pd.DataFrame):
        return wrangle_coord(ref)
    else:
        for filename in filenames:
            df = pd.read_csv(filename, dtype={x:"str" for x in ["pub_date", "address", "title", "owner", "contractor", "engineer"]})
            df = df.fillna(" ")
            wrangle_coord(df)
            df.to_csv(filename.replace("raw","clean"), index=False)

if __name__=="__main__":
    wrangle()
