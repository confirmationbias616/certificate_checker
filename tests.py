import unittest
import pandas as pd
import numpy as np
import datetime
from ddt import ddt, data, unpack
from scraper import scrape
from wrangler import clean_job_number, clean_pub_date, clean_city, clean_company_name, get_acronyms, get_street_number, get_street_name, clean_title, wrangle
from matcher import match
from communicator import communicate
from ml import build_train_set, train_model, evaluate
from db_tools import create_connection
import os


def setUpModule():
    # the import statement below runs some code automatically
    from test import test_setup
    for filename in ['cert_db', 'rf_model.pkl', 'rf_features.pkl']:
        try:  # if not running on CI build
            os.rename(filename, 'temp_'+filename)
        except:  # if running on CI build
            pass
     for filename in ['cert_db', 'rf_model.pkl', 'rf_features.pkl']:
        try:  # if not running on CI build           
            os.rename('test_'+filename, filename)
        except:  # if running on CI build
            pass
 
def tearDownModule():
    for filename in ['cert_db', 'rf_model.pkl', 'rf_features.pkl']:
        try:  # if not running on CI build
            os.rename('temp_'+filename, filename)
        except:  # if running on CI build
            pass
    
    
@ddt
class TestWrangleFuncs(unittest.TestCase):

    @data(
        (" ", ""),
        ("\n  #2404\n", "2404"),
        ("no. 2404", "2404"),
        ("# 2404", "2404"),
        ("#2404", "2404"),
        ("2404", "2404"),
    )
    @unpack
    def test_clean_job_number(self, input_string, desired_string):
        output_string = clean_job_number(input_string)
        self.assertEqual(desired_string, output_string)

    @data(
        (" ", ""),
        ("\n  2019-02-20\n", "2019-02-20"),
        ("2019-02-20", "2019-02-20"),
        (" 2019-02-20 ", "2019-02-20"),
    )
    @unpack
    def test_clean_pub_date(self, input_string, desired_string):
        output_string = clean_pub_date(input_string)
        self.assertEqual(desired_string, output_string)

    @data(
        (" ", ""),
        ("Timmins ON, South Porcupine ON, Temagami ON & New Liskeard ON", "timmin"),
        ("Ottawa, Ontario", "ottawa"),
        ("Frontenac County, City of Kingston, Selma Subdivision, Ontario", "kingston"),
        ("Kingston", "kingston"),
        ("Kingston, Ontario", "kingston"),
        ("Kingston Ontario", "kingston"),
        ("Kingston ON", "kingston"),
        ("Kingston", "kingston"),
        ("Township of South Algonquin", "southalgonquin"),
        ("City of Greater Sudbury", "sudbury"),
        ("Cochrane District, City of Timmins", "timmin"),
        ("Brampton/Mississauga", "brampton&mississauga"),
        ("City of Kitchener - Building Department", "kitchener"),
        ("City of Ottawa, Ontario, Canada", "ottawa"),
        ("Essex County", "essex"),
        ("Etobicoke - City of Toronto", "toronto"),
        ("Halton Region, City of Burlington", "burlington"),
        ("Municipality of Chatham-Kent", "chathamkent"),
        ("Municipality of Chatham Kent", "chathamkent"),
        ("Municipality of Chatham  Kent", "chathamkent"),
        ("Municipality of ChathamKent", "chathamkent"),
        ("York Region", "york"),
        ("Corporation of the City of North Bay", "northbay"),
        ("Elgin County/City of St. Thomas", "st.thoma"),
        ("Durham Region, Town of Whitby", "whitby"),
        ("Hastings County, Municipality Faraday", "faraday"),
        ("Regional Municipality of Windsor", "windsor"),
        ("Niagara Region, City of St. Catharines, Canada", "st.catharine"),
        ("York Region, Town of Markham", "markham"),
        ("Town of Wasaga Beach", "wasagabeach"),
        ("Ottawa-Carleton", "ottawa")
    )
    @unpack
    def test_clean_city(self, input_string, desired_string):
        output_string = clean_city(input_string)
        self.assertEqual(desired_string, output_string)

    @data(
        (" ", ""),
        ("Frecon", "frecon"),
        ("Frecon", "frecon"),
        ("PCL Constructors", "pcl"),
        ("Université d'Ottawa", "universiteottawa"),
        ("Dilfo Mechanical Ltd.", "dilfo"),
        ("Dilfo Mechanical Ltd", "dilfo"),
        ("Dilfo Mechanical Limited", "dilfo"),
        ("Graceview Enterprises Inc.", "graceview"),
        ("Dilfo HVAC Services Inc", "dilfo"),
        ("S&R Mechanical", "s&r"),
        ("s and r mech", "s&r"),
        ("srmech", "sr"),
        ("G&L Insulation", "g&l"),
        ("8906785 Canada Inc. O/A R.E. Hein Construction (Ontario)", "rehein"),
        ('PCL Constructors Canada Inc. for GAL Power Systems', 'pcl')
    )
    @unpack
    def test_clean_company_name(self, input_string, desired_string):
        output_string = clean_company_name(input_string)
        self.assertEqual(desired_string, output_string)

    @data(
        (" ", []),
        ("Ron Eastern Construction Ltd. (RECL)", ["RECL"]),
        ("RECL", ["RECL"]),
        ("Ellis Don for BGIS", ["BGIS"]),
        ("ED/BGIS", ["BGIS"]),
        ("Ron Eastern Construction Limited (RECL) for PWGSC", ["RECL", "PWGSC"]),
        ("Ron Eastern Construction Limited", []),
    )
    @unpack
    def test_get_acronyms(self, input_string, desired_string):
        output_string = get_acronyms(input_string)
        self.assertEqual(desired_string, output_string)

    address_test_data = (
        ("123 Fake St.", "123", "fake"),
        ("12 Carrière Rd", "12", "carriere"),
        ("8-1230 marenger street", "1230", "marenger"),
        ("apt. 8, 1230 marenger street", "1230", "marenger"),
        ("8-1230 marenger street, apt. 8, ", "1230", "marenger"),
        ("1230 apt. 8, marenger street", "1230", "marenger"),
        ("1010 talbot st. unit #1", "1010", "talbot"),
        ("6250 st albans court", "6250", "albans"),
        ("6250 saint albans court", "6250", "albans"),
        ("6250 st. albans", "6250", "albans"),
        ("6250 st-albans CRT", "6250", "albans"),
        ("University of Ottawa, Faculty of Medicine and Faculty of Health Sciences, Roger Guindon Hall, 451 Smyth Road, Ottawa, Ontario K1H 8L1", "451", "smyth"),
        ("145 Jean-Jacques Lussier", "145", "jean-jacques"),
        ("Edwardsburgh/Cardinal", "", ""),
    )
    @data(*address_test_data)
    @unpack
    def test_get_street_number(self, input_string, desired_string1, desired_string2):
        output_string = get_street_number(input_string)
        self.assertEqual(desired_string1, output_string)
    @data(*address_test_data)
    @unpack
    def test_get_street_name(self, input_string, desired_string1, desired_string2):
        output_string = get_street_name(input_string)
        self.assertEqual(desired_string2, output_string)

    @data(
        (" ", ""),
        ("test", "test"),
    )
    @unpack
    def test_clean_title(self, input_string, desired_string):
        output_string = clean_title(input_string)
        self.assertEqual(desired_string, output_string)

@ddt
class IntegrationTests(unittest.TestCase):
        
    def test_scarpe_to_communicate(self):
        test_limit = 3
        web_df = scrape(limit=test_limit, test=True)
        self.assertEqual(len(web_df), test_limit)
        web_row = web_df.iloc[0]
        match_first_query = "SELECT * FROM dilfo_open LIMIT 1"
        with create_connection() as conn:
            dilfo_row = pd.read_sql(match_first_query, conn).iloc[0]
        communicate(web_row, dilfo_row, test=True)

    def test_truth_table(self):

        prob_thresh = 0.65
        
        build_train_set()
        train_model(prob_thresh=prob_thresh)
        match_query = "SELECT * FROM dilfo_matched"
        with create_connection() as conn:
            test_df_dilfo = pd.read_sql(match_query, conn)
        test_web_df = scrape(ref=test_df_dilfo)
        results = match(df_dilfo=test_df_dilfo, df_web=test_web_df, test=True, prob_thresh=prob_thresh)
        
        # confrim 100% recall with below assert
        qty_actual_matches = int(len(results)**0.5)
        qty_found_matches = results[results.pred_match == 1].title.nunique()
        self.assertTrue(qty_found_matches == qty_actual_matches, msg=f"qty_found_matches({qty_found_matches}) not equal qty_actual_matches({qty_actual_matches})")
        
        # make sure not more than 10% false positives with below assert
        false_positives = len(results[results.pred_match == 1]) - qty_found_matches
        self.assertTrue(false_positives <= round(qty_actual_matches*0.1,1), msg=f"found too many false positives ({false_positives}) out of total test projects ({qty_actual_matches})")

if __name__ == '__main__':
    for filename in ['cert_db', 'rf_model.pkl', 'rf_features.pkl']:
        try:
            os.rename('temp_'+filename, filename)
        except:
            pass
    unittest.main(verbosity=2)
