import datetime 
import pandas as pd
import numpy as np
from cleanco import cleanco
import unidecode


def clean_pub_date(raw):
    date = raw.replace("\n", "").replace(" ", "")
    return date

def clean_city(raw):
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
    try:
        text = ''.join([x for x in raw if ((not x.isdigit()) and (x not in ',()-:;.'))])
        text = text.replace("/"," ").replace("&", " ")
        accronyms =  [x for x in text.split(" ") if len(x)>2 and x.isupper()]
        if len(accronyms) == 0:
            return []
        return accronyms
    except TypeError:
        return []

def get_street_number(raw):
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
    raw = raw.lower()
    raw = unidecode.unidecode(raw)
    if any([raw.startswith(x) for x in ["apt", "unit", "suite"]]):
    	raw = raw.split(",")[1].lstrip(" ")
    elif any([x in raw.split(",")[0] for x in ["apt", "unit", "suite"]]):
    	raw = raw.split(",")[1]
    name = raw.split(' ')[1]
    for unit_word in ["apt", "unit", "suite"]:
    	name = name.replace(unit_word, "")
    if name.isalpha():
    	return name
    else:
    	return ""

def clean_title(raw):
    title = raw
    return title

def wrangle_coord(df):
    clean_ops = {
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
        try:
            df[attr] = df['address'].apply(get_address_ops[attr])
        except KeyError:
            pass
    for attr in ["title","owner","contractor"]:
        df[f'{attr}_acronyms'] = df[attr].apply(get_acronyms)
    return df

def wrangle():
    for filename in (
        './data/raw_dilfo_certs.csv', 
        f'./data/raw_web_certs_{datetime.datetime.now().date()}.csv'
        ):
        df = pd.read_csv(filename)
        wrangle_coord(df)
        df.to_csv(filename.replace("raw","clean"), index=False)

if __name__=="__main__":
    wrangle()
