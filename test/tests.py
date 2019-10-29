import add_parent_to_path
import unittest
import pandas as pd
import numpy as np
import mechanize
import requests
from bs4 import BeautifulSoup
import urllib
import re
import datetime
from ddt import ddt, data, unpack
from scraper import scrape
from wrangler import (
    clean_job_number,
    clean_pub_date,
    clean_city,
    clean_company_name,
    get_acronyms,
    get_street_number,
    get_street_name,
    clean_title,
    wrangle,
)
from matcher import match
from communicator import communicate, process_as_feedback
from ml import build_train_set, train_model, validate_model
from utils import create_connection, load_config
from test.test_setup import create_test_db
from flask_app import app
import os


prob_thresh = load_config()["machine_learning"]["prboability_thresholds"]["general"]


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
        ("Ottawa-Carleton", "ottawa"),
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
        ("PCL Constructors Canada Inc. for GAL Power Systems", "pcl"),
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
        (
            "University of Ottawa, Faculty of Medicine and Faculty of Health Sciences, Roger Guindon Hall, 451 Smyth Road, Ottawa, Ontario K1H 8L1",
            "451",
            "smyth",
        ),
        ("145 Jean-Jacques Lussier", "145", "jean"),
        ("145 Jean Jacques Lussier", "145", "jean"),
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
        ("testé l'apostrophe", "testelapostrophe"),
        ("u. of o...", "uofo"),
        ("PDV: Fit-Up;", "pdvfitup"),
    )
    @unpack
    def test_clean_title(self, input_string, desired_string):
        output_string = clean_title(input_string)
        self.assertEqual(desired_string, output_string)


@ddt
class InputTests(unittest.TestCase):
    def setUp(self):
        for filename in ["cert_db.sqlite3", "results.json"]:
            try:
                os.rename(filename, "temp_" + filename)
            except FileNotFoundError:
                pass
        create_test_db()
        for filename in ["cert_db.sqlite3", "rf_model.pkl", "rf_features.pkl", "results.json"]:
            try:
                os.rename("test_" + filename, filename)
            except FileNotFoundError:
                pass

    def tearDown(self):
        for filename in ["cert_db.sqlite3", "results.json"]:
            try:
                os.rename("temp_" + filename, filename)
            except FileNotFoundError:
                try:
                    os.remove(filename)
                except FileNotFoundError:
                    pass

    @data(("dcn",), ("ocn",))
    @unpack
    def test_scarpe(self, source):
        test_limit = 3
        web_df = scrape(
            source=source,
            limit=test_limit,
            test=True,
            since=str(datetime.datetime.now().date() - datetime.timedelta(7)),
        )
        self.assertEqual(len(web_df), test_limit)
        web_df = scrape(source=source, limit=test_limit, test=True, since="2019-09-17")
        self.assertEqual(len(web_df), test_limit)
        # need more assertions here to endure quality of scraped data

    @data(
        (
            "/dcn/certificates-and-notices/247FE31E-933A-4632-80FD-B98521CD0E69",
            "dcn",
            "csp",
            "\n  2019-10-01\n",
            "Regional Municipality of Metropolitan Toronto",
            "St. Edmund Campion Catholic School, 30 Highcastle Road, Toronto, Ontario",
            "Foundation Repairs",
            "Toronto Catholic District School Board",
            "Colonial Building Restoration",
            "TSS Building Science Inc.",
        )
    )
    @unpack
    def test_scarpe_with_provided_key(
        self,
        url_key,
        source,
        cert_type,
        pub_date,
        city,
        address,
        title,
        owner,
        contractor,
        engineer,
    ):
        scraped_data = scrape(source=source, provided_url_key=url_key, test=True).iloc[
            0
        ]
        self.assertEqual(cert_type, scraped_data["cert_type"])
        self.assertEqual(pub_date, scraped_data["pub_date"])
        self.assertEqual(city, scraped_data["city"])
        self.assertEqual(address, scraped_data["address"])
        self.assertEqual(title, scraped_data["title"])
        self.assertEqual(owner, scraped_data["owner"])
        self.assertEqual(contractor, scraped_data["contractor"])
        self.assertEqual(engineer, scraped_data["engineer"])

    @data(("9999", True, True, True), ("9998", False, False, False))
    @unpack
    def test_input_project(
        self,
        test_job_number,
        select_checkbox,
        expected_submit_success,
        expected_logged_success,
    ):
        br = mechanize.Browser()
        base_url = "http://127.0.0.1:5000"
        br.open(base_url)
        br.select_form("job_entry")
        br.form["job_number"] = test_job_number
        for field_name in [
            "title",
            "city",
            "address",
            "contractor",
            "owner",
            "engineer",
        ]:
            br.form[field_name] = "test"
        br.find_control("contacts").items[0].selected = select_checkbox
        submit_success = False
        try:
            br.submit()
            submit_success = True
        except urllib.error.HTTPError:
            pass
        summary_html = requests.get(base_url + "/summary_table").content
        logged_success = any(re.findall(test_job_number, str(summary_html)))
        self.assertEqual(expected_submit_success, submit_success)
        self.assertEqual(expected_logged_success, logged_success)
        if (
            expected_logged_success
        ):  # test delete link only if project was expected to be logged.
            expected_delete_success = True
            summary_page_soup = BeautifulSoup(summary_html, "html.parser")
            summary_page_links = [
                x.get("href") for x in summary_page_soup.find_all("a")
            ]
            delete_link = [
                x for x in summary_page_links if test_job_number in x and "delete" in x
            ][0]
            requests.get(base_url + delete_link)
            summary_html = requests.get(base_url + "/summary_table").content
            delete_success = not any(re.findall(test_job_number, str(summary_html)))
            self.assertEqual(expected_delete_success, delete_success)

    def test_exact_match_project(self):
        scrape(
            source="dcn", limit=1, test=False
        )  # to get recent cert in database from within in case test csv's are outdated
        build_train_set()
        train_model(prob_thresh=prob_thresh)
        for filename in ["rf_model.pkl", "rf_features.pkl"]:
            try:
                os.rename("new_" + filename, filename)
            except FileNotFoundError:
                pass
        get_latest_web_cert = """
            SELECT * 
            FROM web_certificates 
            ORDER BY cert_id DESC
            LIMIT 1
        """
        with create_connection() as conn:
            latest_web_cert = pd.read_sql(get_latest_web_cert, conn).iloc[0]
        br = mechanize.Browser()
        base_url = "http://127.0.0.1:5000"
        br.open(base_url)
        br.select_form("job_entry")
        br.form["job_number"] = "9999"
        for field_name in [
            "title",
            "city",
            "address",
            "contractor",
            "owner",
            "engineer",
        ]:
            br.form[field_name] = str(latest_web_cert[field_name])
        br.find_control("contacts").items[0].selected = True
        try:
            br.submit()
        except urllib.error.HTTPError:
            pass
        br.select_form(nr=0)
        br.submit()
        self.assertEqual(
            re.findall("(?<=url_key=).*", br.geturl())[0], latest_web_cert["url_key"]
        )


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        for filename in ["cert_db.sqlite3", "results.json"]:
            try:
                os.rename(filename, "temp_" + filename)
            except FileNotFoundError:
                pass
        create_test_db()
        for filename in ["cert_db.sqlite3", "rf_model.pkl", "rf_features.pkl", "results.json"]:
            try:
                os.rename("test_" + filename, filename)
            except FileNotFoundError:
                pass

    def tearDown(self):
        for filename in ["cert_db.sqlite3", "results.json"]:
            try:
                os.rename("temp_" + filename, filename)
            except FileNotFoundError:
                try:
                    os.remove(filename)
                except FileNotFoundError:
                    pass

    def test_truth_table(self):
        build_train_set()
        train_model(prob_thresh=prob_thresh)
        match_query = """
            SELECT
                company_projects.*,
                web_certificates.url_key
            FROM 
                web_certificates
            LEFT JOIN
                attempted_matches
            ON
                web_certificates.url_key = attempted_matches.url_key
            LEFT JOIN
                company_projects
            ON
                attempted_matches.job_number=company_projects.job_number
            LEFT JOIN
                base_urls
            ON
                base_urls.source=web_certificates.source
            WHERE 
                company_projects.closed=1
            AND
                attempted_matches.ground_truth=1
            AND 
                attempted_matches.multi_phase=0
            AND 
                attempted_matches.validate=0
        """
        corr_web_certs_query = """
            SELECT
                web_certificates.*
            FROM 
                web_certificates
            LEFT JOIN
                attempted_matches
            ON
                web_certificates.url_key = attempted_matches.url_key
            LEFT JOIN
                company_projects
            ON
                attempted_matches.job_number=company_projects.job_number
            LEFT JOIN
                base_urls
            ON
                base_urls.source=web_certificates.source
            WHERE 
                company_projects.closed=1
            AND
                attempted_matches.ground_truth=1
            AND 
                attempted_matches.multi_phase=0
            AND 
                attempted_matches.validate=0
        """

        with create_connection() as conn:
            test_company_projects = pd.read_sql(match_query, conn)
            test_web_df = pd.read_sql(corr_web_certs_query, conn)
        test_web_df = wrangle(test_web_df)
        results = match(
            company_projects=test_company_projects,
            df_web=test_web_df,
            test=True,
            prob_thresh=prob_thresh,
            version="new",
        )

        # confrim 100% recall with below assert
        qty_actual_matches = int(len(results) ** 0.5)
        qty_found_matches = results[results.pred_match == 1].title.nunique()
        self.assertTrue(
            qty_found_matches == qty_actual_matches,
            msg=f"qty_found_matches({qty_found_matches}) not equal qty_actual_matches({qty_actual_matches})",
        )

        # make sure not more than 25% false positives with below assert
        false_positives = len(results[results.pred_match == 1]) - qty_found_matches
        self.assertTrue(
            false_positives <= round(qty_actual_matches * 0.25, 1),
            msg=f"found too many false positives ({false_positives}) out of total test projects ({qty_actual_matches})",
        )

        # test single sample
        sample_company = pd.DataFrame(
            {
                "job_number": "2387",
                "city": "Ottawa",
                "address": "2562 Del Zotto Ave., Ottawa, Ontario",
                "title": "DWS Building Expansion",
                "owner": "Douglas Stalker",
                "contractor": "GNC",
                "engineer": "Goodkey",
                "receiver_emails_dump": "{'alex': 'alex.roy616@gmail.com'}",
                "closed": "0",
            },
            index=range(1),
        )
        sample_web = pd.DataFrame(
            {
                "pub_date": "2019-03-06",
                "city": "Ottawa-Carleton",
                "address": "2562 Del Zotto Avenue, Gloucester, Ontario",
                "title": "Construct a 1 storey storage addition to a 2 storey office/industrial building",
                "owner": "Doug Stalker, DWS Roofing",
                "contractor": "GNC Constructors Inc.",
                "engineer": None,
                "url_key": "B0046A36-3F1C-11E9-9A87-005056AA6F02",
                "source": "dcn",
            },
            index=range(1),
        )
        is_match, prob = match(
            company_projects=sample_company, df_web=sample_web, test=True, version="new"
        ).iloc[0][["pred_match", "pred_prob"]]
        self.assertTrue(
            is_match,
            msg=f"Project #{sample_company.job_number} did not match successfully. Match probability returned was {prob}.",
        )

        # test same sample but using db retreival
        results = match(
            company_projects=sample_company,
            since="2019-03-05",
            until="2019-03-07",
            test=True,
            version="new",
        )
        prob_from_db_cert = (
            results[results.contractor == "gnc"].iloc[0].pred_prob
        )  #'gnc' is what is returned from the wrangling funcs
        self.assertTrue(round(prob, 2) == round(prob_from_db_cert, 2))

        # make sure validation runs
        validate_model(prob_thresh=prob_thresh, test=True)


if __name__ == "__main__":
    for filename in ["cert_db.sqlite3", "rf_model.pkl", "rf_features.pkl", "results.json"]:
        try:
            os.rename("temp_" + filename, filename)
        except:
            pass
    unittest.main(verbosity=2)
