import unittest
import pandas as pd
import datetime
from ddt import ddt, data, unpack
from scraper import scrape
from wrangler import clean_job_number, clean_pub_date, clean_city, clean_company_name, get_acronyms, get_street_number, get_street_name, clean_title, wrangle
from matcher import match
from communicator import communicate


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
            ("Town of Wasaga Beach", "wasagabeach")
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
            ("8-1230, apt. 8, marenger street", "", ""),
            ("1010 talbot st. unit #1", "1010", "talbot")
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
            dilfo_row = pd.read_csv('./data/raw_dilfo_certs.csv').iloc[0]
            communicate(web_row, dilfo_row, test=True)

        def test_true_positives(self):
            test_df_dilfo = pd.read_csv('./data/test_raw_dilfo_certs.csv')
            test_web_df = scrape(ref=test_df_dilfo)
            test_df_dilfo, test_web_df = wrangle(ref=test_df_dilfo), wrangle(ref=test_web_df)
            match_count = match(test_df_dilfo, test_web_df, threshold=0.1, test=True)
            self.assertEqual(len(test_df_dilfo), match_count)



if __name__ == '__main__':
        unittest.main(verbosity=2)
