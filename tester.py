import unittest
from ddt import ddt, data, unpack
from wrangler import *


@ddt
class TestName(unittest.TestCase):

        @data(
            ("\n  2019-02-20\n", "2019-02-20"),
        )
        @unpack
        def test_clean_pub_date(self, input_string, desired_string):
            output_string = clean_pub_date(input_string)
            self.assertEqual(desired_string, output_string)

        @data(
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
            ("G&L Insulation", "g&l")
        )
        @unpack
        def test_clean_company_name(self, input_string, desired_string):
            output_string = clean_company_name(input_string)
            self.assertEqual(desired_string, output_string)

        @data(
            ("Ron Eastern Construction Ltd. (RECL)", ["RECL"]),
        )
        @unpack
        def test_get_acronyms(self, input_string, desired_string):
            output_string = get_acronyms(input_string)
            self.assertEqual(desired_string, output_string)

        @data(
            ("123 Fake St.", "123"),
        )
        @unpack
        def test_get_street_number(self, input_string, desired_string):
            output_string = get_street_number(input_string)
            self.assertEqual(desired_string, output_string)

        @data(
            ("123 Fake St.", "fake"),
            ("12 Carrière Rd", "carriere"),
        )
        @unpack
        def test_get_street_name(self, input_string, desired_string):
            output_string = get_street_name(input_string)
            self.assertEqual(desired_string, output_string)

        @data(
            ("test", "test"),
        )
        @unpack
        def test_clean_title(self, input_string, desired_string):
            output_string = clean_title(input_string)
            self.assertEqual(desired_string, output_string)


if __name__ == '__main__':
        unittest.main(verbosity=2)
