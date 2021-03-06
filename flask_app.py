#!/usr/bin/env python

from flask import Flask, render_template, url_for, request, redirect, session
from flask_session import Session
import datetime
from dateutil.parser import parse as parse_date
import dateutil.relativedelta
from utils import create_connection, load_config
from wrangler import wrangle
from matcher import match
from scraper import scrape
from communicator import process_as_feedback
from geocoder import geocode, geo_update_db_table, get_city_latlng
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
import logging
import sys
import os
import time
import re
import ast
import folium
from folium.plugins import MarkerCluster, HeatMapWithTime, HeatMap, LocateControl
import json
import sqlite3
from flask_login import (
    LoginManager,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from oauthlib.oauth2 import WebApplicationClient
import requests
# from db import init_db_command
from user import User
import stripe
from wordcloud_generator import generate_wordcloud
from functools import lru_cache
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import matplotlib
matplotlib.use('Agg')
from redislite import Redis
from redislite.client import RedisLiteException
import mysql.connector


try:
    redis_connection = Redis('/dev/shm/limiter.db')
except RedisLiteException:
    redis_connection = Redis('limiter.db')
redis_connection.flushdb()

logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s "
        "- line %(lineno)d"
    )
)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

app = Flask(__name__)

#set up Flask-Sessions
app.config.from_object(__name__)
app.config['SESSION_TYPE'] = 'filesystem'

try:
    with open(".secret.json") as f:
        app.config['SECRET_KEY'] = json.load(f)["flask_session_key"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    app.config['SECRET_KEY'] = "JUSTTESTING"

Session(app)

try:
    with open(".secret.json") as f:
        pws = json.load(f)
        mysql_pw = pws["mysql"]
        paw_pw = pws["pythonanywhere"]
except FileNotFoundError:  # no `.secret.json` file if running in CI
    pass

# trick from SO for properly relaoding CSS
app.config['TEMPLATES_AUTO_RELOAD'] = True

set_default_company_id = False
try:
    with open(".secret.json") as f:
        cred = json.load(f)["oauth_cred"]
    GOOGLE_CLIENT_ID = cred.get("GOOGLE_CLIENT_ID", None)
    GOOGLE_CLIENT_SECRET = cred.get("GOOGLE_CLIENT_SECRET", None)
    client = WebApplicationClient(GOOGLE_CLIENT_ID)  # OAuth 2 client setup
except FileNotFoundError:  # CI server
    set_default_company_id = True  # to enable tests on CI server

GOOGLE_DISCOVERY_URL = (
    "https://accounts.google.com/.well-known/openid-configuration"
)

# User session management setup
# https://flask-login.readthedocs.io/en/latest
login_manager = LoginManager()
login_manager.init_app(app)

# Flask-Login helper to retrieve a user from our db
@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)

def request_limit_reached():
    max_free_requests = load_config()['flask_app']['max_free_requests']
    free_timeout_length = load_config()['flask_app']['free_timeout_length']
    free_session_length = load_config()['flask_app']['free_session_length']
    full_user_query = "SELECT id FROM users WHERE account_type='full'"
    with create_connection() as conn:
        valid_user_ids = pd.read_sql(full_user_query, conn)
    if session.get('company_id') in list(valid_user_ids.id):
        return False  # unlimited access for full accounts
    user_ip = str(request.headers.get('X-Real-IP'))
    # if user_ip == 'None':
    #     return False  # test or dev server
    if user_ip in redis_connection:
        time_since_first_access = datetime.datetime.now() - parse_date(redis_connection.hgetall(user_ip).get(b'time_first_access'))
    else:
        time_since_first_access = datetime.timedelta(minutes=0)
    if time_since_first_access > datetime.timedelta(minutes=free_session_length):
        redis_connection.delete(user_ip)  # reset access_count for user due to staleness
    access_count = int(redis_connection.hgetall(user_ip).get(b'access_count')) if user_ip in redis_connection else 0
    if not access_count:
        redis_connection.hmset(user_ip, {
            'access_count': 1,
            'time_first_access': str(datetime.datetime.now())
        })
        return False
    elif access_count < max_free_requests:
        redis_connection.hmset(user_ip, {
            'access_count': access_count + 1,
            'time_first_access': redis_connection.hgetall(user_ip).get(b'time_first_access')  # carry over
        })
        return False
    elif access_count == max_free_requests:
        redis_connection.hmset(user_ip, {
            'access_count': access_count + 1,
            'time_first_access': redis_connection.hgetall(user_ip).get(b'time_first_access')  # carry over
        })
        redis_connection.expire(user_ip, 120)
        session['limit_expiry'] = datetime.datetime.now() + datetime.timedelta(minutes=free_timeout_length) + datetime.timedelta(seconds=60, hours=-4)
        return True
    else:
        return True

def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

def get_zoom_level(region_size):
    zoom_params = (
        (0.0002, 15),
        (0.001, 13),
        (0.005, 13),
        (0.01, 13),
        (0.1, 12),
        (0.5, 11),
        (1, 10),
        (2.5, 8.5),
    )
    for param_size, zoom_level in zoom_params:
        if region_size < param_size:
            return zoom_level
    return 5

# this function works in conjunction with `dated_url_for` to make sure the browser uses
# the latest version of css stylesheet when modified and reloaded during testing
@app.context_processor
def override_url_for():
    return dict(url_for=dated_url_for)


def dated_url_for(endpoint, **values):
    if endpoint == "static":
        filename = values.get("filename", None)
        if filename:
            file_path = os.path.join(app.root_path, endpoint, filename)
            values["q"] = int(os.stat(file_path).st_mtime)
    return url_for(endpoint, **values)

@lru_cache(maxsize=32)
def get_current_coords():
    try:
        ip_add = request.headers['X-Real-IP']  #special for PythonAnywhere
    except KeyError:
        logger.info("Couldn't get IP address. Running local server?")
        return 'nan', 'nan', 'nan'
    logger.info(f"IP address: {ip_add}")
    response = requests.get(f"https://ipgeolocation.com/{ip_add}")
    if response.status_code == 200:
        if response.json()['region'] != 'Ontario':
            logger.info("User located outside Ontario")
            return 'nan', 'nan', 'nan'
        lat, lng = [float(x) for x in response.json()['coords'].split(',')]
        return lat, lng, response.json()['city']
    else:
        logger.info("Invalid IP address.")
        return 'nan', 'nan', 'nan'

def load_user():
    if session.get('company_id'):
        return
    username = current_user.name if current_user.is_authenticated else None
    if current_user.is_authenticated:
        session['company_id'] = current_user.id
        session['company_name'] = current_user.name
        session['company_email'] = current_user.email
        if session.get('company_id'):
            with create_connection() as conn:
                session['account_type'] = pd.read_sql("""
                SELECT * 
                FROM users 
                WHERE id=%s
            """, conn, params=[current_user.id]).iloc[0].account_type
        if session.get('account_type') != "full":
            return redirect(url_for("payment"))
    elif set_default_company_id:  # for CI server
        session['company_id'] = 1
        session['company_name'] = "Testing123"
        session['account_type'] = "full"
    else:  # for for dev and prod servers
        session['company_id'] = None

def get_web_certs(east_lat, west_lat, south_lng, north_lng, end_date, select_source, limit_count, text_search=False):
    web_query = """
        SELECT 
            web_certificates.*, 
            CONCAT(base_urls.base_url, web_certificates.url_key) AS link,
            COALESCE(web_certificates.address_lat, web_certificates.city_lat) as lat,
            COALESCE(web_certificates.address_lng, web_certificates.city_lng) as lng,
            base_urls.long_name as source_name
        FROM 
            web_certificates
        JOIN 
            base_urls
        ON 
            web_certificates.source=base_urls.source
        WHERE
            cert_type = "csp"
        AND
            COALESCE(web_certificates.address_lat, web_certificates.city_lat) > %s
        AND
            COALESCE(web_certificates.address_lat, web_certificates.city_lat) < %s
        AND
            COALESCE(web_certificates.address_lng, web_certificates.city_lng) > %s
        AND
            COALESCE(web_certificates.address_lng, web_certificates.city_lng) < %s
        AND
            pub_date <= %s
        AND
            web_certificates.source LIKE %s
        {}
        ORDER BY 
            pub_date
        DESC LIMIT %s
    """
    add_fts_query = """
        AND
            web_certificates.cert_id IN (SELECT cert_id FROM cert_search WHERE text MATCH %s)
    """
    web_query = web_query.format(add_fts_query) if text_search else web_query.format('')
    with create_connection() as conn:
        if text_search:
            df = pd.read_sql(web_query , conn, params=[east_lat, west_lat, south_lng, north_lng, end_date, select_source, text_search, limit_count*2])
        else:
            df = pd.read_sql(web_query , conn, params=[east_lat, west_lat, south_lng, north_lng, end_date, select_source, limit_count*2])
        return df

@app.route("/", methods=["POST", "GET"])
def index():
    load_user()
    if session.get('account_type') == 'full' or not session.get('company_id'):
        return render_template('landing_page.html')
    else:
        return redirect(url_for("payment"))


@app.route("/project_entry", methods=["POST", "GET"])
def project_entry():
    load_user()
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    all_contacts_query = "SELECT * FROM contacts WHERE company_id=%s"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn, params=[session.get('company_id')])
    if request.method == "POST":
        selected_contact_ids = request.form.getlist("contacts")
        selected_contacts_query = (
            f"SELECT name, email_address FROM contacts WHERE id in "
            f"({','.join([' %s']*len(selected_contact_ids))}) AND company_id=%s"
        )
        with create_connection() as conn:
            selected_contacts = pd.read_sql(
                selected_contacts_query, conn, params=[*selected_contact_ids, session.get('company_id')]
            )
        receiver_emails_dump = str(
            {row["name"]: row["email_address"] for _, row in selected_contacts.iterrows()}
        )
        new_entry = dict(request.form)
        new_entry.pop("contacts")  # useless
        if [
            True for value in new_entry.values() if type(value) == list
        ]:  # strange little fix
            new_entry = {key: new_entry[key][0] for key in new_entry.keys()}
        new_entry.update({"receiver_emails_dump": receiver_emails_dump})
        with create_connection() as conn:
            try:
                row = pd.read_sql(
                    "SELECT * FROM company_projects WHERE job_number=%s and company_id=%s",
                    conn,
                    params=[new_entry["job_number"], session.get('company_id')],
                ).iloc[0]
                was_prev_closed = row.closed
                if not was_prev_closed:  # case where `closed` column is empty
                    was_prev_closed = 0
                    new_entry["closed"] = 0
                was_prev_logged = 1
            except IndexError:
                was_prev_closed = 0
                new_entry["closed"] = 0
                was_prev_logged = 0
        if was_prev_closed:
            return redirect(
                url_for("already_matched",
                job_number=new_entry["job_number"],
                )
            )
        if was_prev_logged:
            with create_connection() as conn:
                conn.cursor().execute(f"""
                    DELETE FROM company_projects WHERE job_number=%s AND company_id=%s
                """, [new_entry["job_number"], session.get('company_id')])
                conn.commit()
        with create_connection() as conn:
            conn.cursor().execute(f"""
                INSERT INTO company_projects (company_id, {', '.join(list(new_entry.keys()))}) VALUES (%s, {','.join([' %s']*len(new_entry))})
            """, [session.get('company_id')] + list(new_entry.values()))
            conn.commit()
        geo_update_db_table('company_projects', limit=1)
        if not was_prev_logged:
            return render_template(
                "signup_confirmation.html", 
                job_number=new_entry["job_number"],
            )
        return render_template(
            "update.html",
            job_number=new_entry["job_number"],
            recorded_change=True,
        )
    else:
        try:
            return render_template(
                "project_entry.html",
                all_contacts=all_contacts,
                **{key: request.args.get(key) for key in request.args},
            )
        except NameError:
            return render_template("project_entry.html", all_contacts=all_contacts)


@app.route("/login")
def login():
    # Find out what URL to hit for Google login
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]

    # Use library to construct the request for Google login and provide
    # scopes that let you retrieve user's profile from Google
    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=request.base_url + "/callback",
        scope=["openid", "email", "profile"],
    )
    return redirect(request_uri)


@app.route("/login/callback")
def callback():
    # Get authorization code Google sent back to you
    code = request.args.get("code")

    # Find out what URL to hit to get tokens that allow you to ask for
    # things on behalf of a user
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]

    # Prepare and send request to get tokens! Yay tokens!
    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=request.base_url,
        code=code,
    )
    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    )

    # Parse the tokens!
    client.parse_request_body_response(json.dumps(token_response.json()))

    # Now that we have tokens (yay) let's find and hit URL
    # from Google that gives you user's profile information,
    # including their Google Profile Image and Email
    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body)

    # We want to make sure their email is verified.
    # The user authenticated with Google, authorized our
    # app, and now we've verified their email through Google!
    if userinfo_response.json().get("email_verified"):
        unique_id = userinfo_response.json()["sub"]
        users_email = userinfo_response.json()["email"]
        picture = userinfo_response.json()["picture"]
        users_name = userinfo_response.json()["given_name"]
    else:
        return "User email not available or not verified by Google.", 400

    # Create a user in our db with the information provided
    # by Google
    user = User(
        id_=unique_id, name=users_name, email=users_email, profile_pic=picture
    )

    # Doesn't exist? Add to database
    if not User.get(unique_id):
        User.create(unique_id, users_name, users_email, picture)

    # Begin user session by logging the user in
    login_user(user)

    # Send user back to homepage
    return redirect(url_for("index"))


@app.route("/logout")
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for("index"))

@app.route("/already_matched", methods=["POST", "GET"])
def already_matched():
    if request.method == "POST":
        return redirect(url_for("index"))
    job_number = request.args.get("job_number")
    link_query = """
        SELECT
            CONCAT(base_urls.base_url, web_certificates.url_key) AS link
        FROM
            attempted_matches
        LEFT JOIN
            web_certificates
        ON
            web_certificates.cert_id = attempted_matches.cert_id
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web_certificates.source
        WHERE
            job_number=%s
        AND
            company_id=%s
        AND
            attempted_matches.ground_truth = 1
    """
    with create_connection() as conn:
        link = pd.read_sql(link_query, conn, params=[job_number, session.get('company_id')]).iloc[0].link
    return render_template("already_matched.html", link=link, job_number=job_number)


@app.route("/potential_match", methods=["POST", "GET"])
def potential_match():
    if request.method == "POST":
        return redirect(url_for("index"))
    project_id = request.args.get("project_id")
    cert_id = request.args.get("cert_id")
    job_number = request.args.get("job_number")
    url_key = request.args.get("url_key")
    source = request.args.get("source")
    source_base_url_query = "SELECT base_url FROM base_urls WHERE source=%s"
    with create_connection() as conn:
        base_url = pd.read_sql(source_base_url_query, conn, params=[source]).iloc[0].base_url
    return render_template(
        "potential_match.html",
        project_id=project_id,
        cert_id=cert_id,
        job_number=job_number,
        base_url=base_url,
        url_key=url_key,
        source=source,
    )


@app.route("/summary_table")
def summary_table():
    load_user()
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    def highlight_pending(s):
        days_old = (
            datetime.datetime.now().date()
            - parse_date(re.findall("\d{4}-\d{2}-\d{2}", s.pub_date)[0]).date()
        ).days
        if days_old < 60:  # fresh - within lien period
            row_colour = "rgb(97, 62, 143)"
        else:
            row_colour = ""
        return [f"color: {row_colour}" for i in range(len(s))]
    col_order = [
        "job_number",
        "title",
        "contractor",
        "engineer",
        "owner",
        "address",
        "city",
    ]
    closed_query = """
            SELECT
                company_projects.project_id,
                company_projects.job_number,
                company_projects.city,
                company_projects.address,
                company_projects.title,
                company_projects.owner,
                company_projects.contractor,
                company_projects.engineer,
                web.url_key,
                web.pub_date,
                CONCAT(base_urls.base_url, web.url_key) AS link
            FROM (SELECT * FROM web_certificates ORDER BY cert_id DESC LIMIT 16000) as web
            LEFT JOIN
                attempted_matches
            ON
                web.cert_id = attempted_matches.cert_id
            LEFT JOIN
                company_projects
            ON
                attempted_matches.project_id = company_projects.project_id
            LEFT JOIN
                base_urls
            ON
                base_urls.source = web.source
            WHERE
                company_projects.closed=1
            AND
                company_id=%s
            AND
                attempted_matches.ground_truth=1
        """
    open_query = """
            SELECT *
            FROM company_projects
            WHERE company_projects.closed=0
            AND company_id=%s
        """
    with create_connection() as conn:
        df_closed = pd.read_sql(closed_query, conn, params=[session.get('company_id')]).sort_values(
            "job_number", ascending=False
        )
        df_open = pd.read_sql(open_query, conn, params=[session.get('company_id')]).sort_values(
            "job_number", ascending=False
        )
    pd.set_option("display.max_colwidth", -1)
    if not len(df_closed):
        df_closed = None
    else:
        df_closed["job_number"] = df_closed.apply(
            lambda row: f"""<a href="{row.link}">{row.job_number}</a>""", axis=1
        )
        df_closed = df_closed.drop("url_key", axis=1)
        df_closed = (
            df_closed[["pub_date"] + col_order]
            .style.set_table_styles(
                [
                    {
                        "selector": "th",
                        "props": [
                            ("background-color", "rgb(122, 128, 138)"),
                            ("color", "black"),
                        ],
                    }
                ]
            )
            .set_table_attributes('border="1"')
            .set_properties(
                **{"font-size": "10pt", "background-color": "rgb(168, 185, 191)"}
            )
            .set_properties(subset=["action", "job_number"], **{"text-align": "center"})
            .hide_index()
            .apply(highlight_pending, axis=1)
        )
        df_closed = df_closed.render(escape=False)
    if not len(df_open):
        df_open = None
    else:
        df_open["action"] = df_open.apply(
            lambda row: (
                f"""<a href="{url_for('project_entry', **row)}">modify</a> / """
                f"""<a href="{url_for('delete_job', project_id=row.project_id)}">delete</a>"""
            ),
            axis=1,
        )
        df_open["contacts"] = df_open.apply(
            lambda row: ", ".join(ast.literal_eval(row.receiver_emails_dump).keys()),
            axis=1,
        )
        df_open = (
            df_open[["action"] + col_order + ["contacts"]]
            .style.set_table_styles(
                [
                    {
                        "selector": "th",
                        "props": [
                            ("background-color", "rgb(122, 128, 138)"),
                            ("color", "black"),
                        ],
                    }
                ]
            )
            .set_table_attributes('border="1"')
            .set_properties(
                **{"background-color": "rgb(168, 185, 191)"}
            )
            .set_properties(
                subset=["action", "job_number", "contacts"], **{"text-align": "center"}
            )
            .hide_index()
        )
        df_open = df_open.render(escape=False)
    return render_template(
        "summary_table.html",
        df_closed=df_closed,
        df_open=df_open,
    )


@app.route("/delete_job")
def delete_job():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    delete_job_query = """
            DELETE FROM company_projects
            WHERE project_id=%s
        """
    delete_match_query = """
            DELETE FROM attempted_matches
            WHERE project_id=%s
        """
    project_id = request.args.get("project_id")
    with create_connection() as conn:
        conn.cursor().execute(delete_job_query, [project_id])
        conn.commit()
        conn.cursor().execute(delete_match_query, [project_id])
        conn.commit()
    return redirect(url_for("summary_table"))


@app.route("/instant_scan", methods=["POST", "GET"])
def instant_scan():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    if request.method == "POST":
        job_number = request.args.get("job_number")
        lookback = request.args.get("lookback")
        if lookback == "2_months":
            lookback_cert_date = str(datetime.datetime.now().date() - datetime.timedelta(days=62))
        elif lookback == "1_month":
            lookback_cert_date = str(datetime.datetime.now().date() - datetime.timedelta(days=31))
        else:  # also applies for `2_weeks`
            lookback_cert_date = str(datetime.datetime.now().date() - datetime.timedelta(days=14))
        company_query = "SELECT * FROM company_projects WHERE job_number=%s AND company_id=%s"
        with create_connection() as conn:
            company_projects = pd.read_sql(company_query, conn, params=[job_number, session.get('company_id')])
        hist_query = """ 
            SELECT *
            FROM (
                SELECT * FROM web_certificates 
                WHERE pub_date > %s
            ) AS s
            WHERE 
                address_lat IS NULL 
                OR (
                    abs(address_lat - %s) < 0.5
                    AND
                    abs(address_lng - %s) < 0.5
                )
        """
        with create_connection() as conn:
            df_web = pd.read_sql(hist_query, conn, params=[lookback_cert_date, company_projects.iloc[0].address_lat, company_projects.iloc[0].address_lng])
        results = match(
            company_projects=company_projects,
            df_web=df_web,
            prob_thresh=load_config()["machine_learning"]["prboability_thresholds"][
                "general"
            ],
            multi_phase_proned_thresh=load_config()["machine_learning"][
                "prboability_thresholds"
            ]["multi_phase"],
            test=load_config()["flask_app"]["test"],
        )
        if isinstance(results, pd.DataFrame) and (
            len(results[results.pred_match == 1]) > 0
        ):
            url_key = results.iloc[0].url_key
            source = results.iloc[0].source
            cert_id = results.iloc[0].cert_id
            return redirect(
                url_for(
                    "potential_match",
                    project_id=company_projects.iloc[0].project_id,
                    job_number=job_number,
                    source=source,
                    url_key=url_key,
                    cert_id=cert_id
                )
            )
        return render_template("nothing_yet.html", job_number=job_number)


@app.route("/process_feedback", methods=["POST", "GET"])
def process_feedback():
    load_user()
    status = process_as_feedback(request.args)
    return render_template(
        "thanks_for_feedback.html",
        project_id=request.args["project_id"],
        cert_id=request.args["cert_id"],
        job_number=request.args["job_number"],
        response=request.args["response"],
        status=status,
    )


@app.route("/about", methods=["POST", "GET"])
def about():
    return render_template("about.html")


@app.route("/plan_info", methods=["POST", "GET"])
def plan_info():
    load_user()
    if session.get('company_id') and session.get('account_type') != "full":
        return redirect(url_for('payment'))
    return render_template("plan_info.html")


@app.route("/payment", methods=["POST", "GET"])
def payment():
    load_user()
    # Set your secret key: remember to change this to your live secret key in production
    # See your keys here: https://dashboard.stripe.com/account/apikeys
    stripe.api_key = 'sk_test_d2uR7P9xdhu8MW6akC8KTNEd00ArjxicJW'
    stripe_session = stripe.checkout.Session.create(
    payment_method_types=['card'],
    subscription_data={
        'items': [{
        'plan': 'plan_GbSnJ1D9h7JrYl',
        }],
    },
    success_url=url_for('thanks_for_payment', _external=True)+'?session_id={CHECKOUT_SESSION_ID}',
    cancel_url=url_for('payment', _external=True),
    )
    return render_template("payment.html", session_id=stripe_session.id)

@app.route("/thanks_for_payment", methods=["POST", "GET"])
def thanks_for_payment():
    update_account_type_query = (
        "UPDATE users SET account_type = 'full' WHERE id = %s"
    )
    with create_connection() as conn:
        conn.cursor().execute(update_account_type_query, [session.get('company_id')])
        conn.commit()
    session['account_type'] = 'full'
    return render_template("thanks_for_payment.html")

@app.route("/limit", methods=["POST", "GET"])
def limit():
    return render_template("limit.html")

@app.route("/user_account", methods=["POST", "GET"])
def user_account():
    load_user()
    return render_template("user_account.html")

@app.route("/admin", methods=["POST", "GET"])
def admin():
    with create_connection() as conn:
        df_users = pd.read_sql("""
            SELECT
                CONCAT(SUBSTR(date_added,0,5),'-',SUBSTR(date_added,6,2)) as yearmonth,
                COUNT(*) as count
            FROM users
            WHERE date_added > '2018-01-01'
            GROUP BY 1
            ORDER BY date_added
        """, conn)
    all_possible_yearmonths = [f"{x[0]}-{x[1]}" for x in list(zip(np.repeat(range(2020, 2100),12), [str(x).zfill(2) for x in range(1,13)]*36))]
    all_yearmonths = [x for x in all_possible_yearmonths if x <= list(df_users['yearmonth'])[-1]]
    df_agg = pd.merge(pd.DataFrame({'yearmonth' : all_yearmonths}), df_users, how='left').fillna(0)
    plt.bar(df_agg.yearmonth, df_agg['count'], align='center', color=(112/255, 94/255, 134/255, 1))
    ax = plt.axes()
    x_axis = ax.axes.get_xaxis()
    x_label = x_axis.get_label()
    x_label.set_visible(False)
    for spine in ax.spines:
        ax.spines[spine].set_visible(False)
    ax.tick_params(axis=u'both', which=u'both',length=0)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    possibly_valid_xticks = (f'{x[0]}-{x[1]}' for x in zip(np.repeat(range(2018, 2100),6), [str(x).zfill(2) for x in range(1,13)] * 100))
    valid_xticks = [x for x in possibly_valid_xticks if x in list(df_agg.yearmonth.unique())]
    plt.xticks(valid_xticks)
    plt.locator_params(axis='x', nbins=20)
    legend = plt.legend(frameon=1, prop={'size': 20})
    frame = legend.get_frame()
    frame.set_alpha(0)
    plt.title("New sign-ups\n")
    plt.savefig("static/new_users.png", transparent=True)
    return render_template("admin.html")

@app.route("/terminate_account", methods=["POST", "GET"])
def terminate_account():
    load_user()
    if request.args.get('confirmed'):
        update_account_type_query = (
            "UPDATE users SET account_type = '' WHERE id = %s"
        )
        with create_connection() as conn:
            conn.cursor().execute(update_account_type_query, [session.get('company_id')])
            conn.commit()
        session.clear()
        return redirect(url_for("index"))
    return render_template("terminate_account.html")

@app.route("/contact_config", methods=["POST", "GET"])
def contact_config():
    load_user()
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    contact = request.args
    all_contacts_query = "SELECT * FROM contacts WHERE company_id=%s"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn, params=[session.get('company_id')])
    if not len(all_contacts):
        all_contacts = None
    else:
        all_contacts["action"] = all_contacts.apply(
            lambda row: (
                f"""<a href="{url_for('update_contact', update=True, id=row['id'], name=row['name'], email_address=row['email_address'])}">modify</a> /"""
                f"""<a href="{url_for('delete_contact', id=row['id'], name=row['name'], email_address=row['email_address'])}">delete</a>"""
            ),
            axis=1,
        )
        all_contacts = all_contacts[["name", "email_address", "action"]]
        all_contacts = (
            all_contacts.style.set_table_attributes('border="1"')
            .set_table_styles(
                [
                    {
                        "selector": "th",
                        "props": [
                            ("background-color", "rgb(122, 128, 138)"),
                            ("color", "black"),
                        ],
                    }
                ]
            )
            .set_properties(
                **{"font-size": "10pt", "background-color": "rgb(168, 185, 191)"}
            )
            .hide_index()
        )
        all_contacts = all_contacts.render(escape=False)
    return render_template(
        "contact_config.html",
        all_contacts=all_contacts,
        config=True,
        **contact,
    )


@app.route("/delete_contact")
def delete_contact():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    delete_contact_query = """
            DELETE FROM contacts
            WHERE id=%s
            AND company_id=%s
        """
    contact = request.args
    with create_connection() as conn:
        conn.cursor().execute(delete_contact_query, [contact.get("id"), session.get('company_id')])
        conn.commit()
    return redirect(url_for("contact_config", **contact))


@app.route("/update_contact", methods=["POST", "GET"])
def update_contact():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    contact = request.args
    update_contact_query = """
        UPDATE contacts
        SET name = %s,  email_address = %s
        WHERE id=%s
        AND company_id=%s
    """
    if request.method == "POST":
        contact = request.form
        with create_connection() as conn:
            conn.cursor().execute(
                update_contact_query,
                [contact.get("name"), contact.get("email_address"), contact.get("id"), session.get('company_id')],
            )
            conn.commit()
    return redirect(url_for("contact_config", **contact))


@app.route("/add_contact", methods=["POST", "GET"])
def add_contact():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    contact = request.args
    add_contact_query = """
        INSERT INTO contacts
        (company_id, name, email_address) VALUES(%s, %s, %s)
    """
    if request.method == "POST":
        contact = request.form
        with create_connection() as conn:
            conn.cursor().execute(
                add_contact_query,
                [session.get('company_id'), contact.get("name"), contact.get("email_address")],
            )
            conn.commit()
    return redirect(url_for("contact_config", **contact))


@app.route("/interact", methods=["POST", "GET"])
def interact():
    if request.method == "POST":
        
        form = dict(request.form)
        if [
            True for value in form.values() if type(value) == list
        ]:  # strange little fix
            form = {key: form[key][0] for key in form.keys()}
        
        if form.get("cert_link") or form.get("job_number"):
            try:
                url_key = form.get("cert_link").split(
                    "https://canada.constructconnect.com"
                )[1]
                b = scrape(source="dcn", provided_url_key=url_key, test=True)
            except IndexError:
                url_key = form.get("cert_link")
                if "ontarioconstructionnews.com" in url_key:
                    b = scrape(source="ocn", provided_url_key=url_key, test=True)
                else:
                    pass
            try:
                scraped_cert_info = {
                    "cert_" + key: b.iloc[0][key]
                    for key in [
                        "title",
                        "address",
                        "city",
                        "contractor",
                        "owner",
                        "engineer",
                    ]
                }
            except (NameError, UnboundLocalError):
                scraped_cert_info = {}
            try:
                with create_connection() as conn:
                    comp_df = pd.read_sql(
                        "SELECT * FROM company_projects WHERE job_number=%s AND company_id=%s",
                        conn,
                        params=[form.get("job_number"), session.get('company_id')],
                    )
                comp_info = {
                    "comp_" + key: comp_df.iloc[0][key]
                    for key in [
                        "title",
                        "address",
                        "city",
                        "contractor",
                        "owner",
                        "engineer",
                    ]
                }
            except IndexError:
                comp_info = {}
            return redirect(
                url_for(
                    "interact",
                    **{key: scraped_cert_info.get(key) for key in scraped_cert_info},
                    **{key: comp_info.get(key) for key in comp_info},
                    cert_link=form.get("cert_link"),
                    job_number=form.get("job_number"),
                )
            )
        elif any(form.values()):
            a = pd.DataFrame(
                {
                    key.split("comp_")[1]: [form.get(key)]
                    for key in form
                    if key.startswith("comp_")
                }
            )
            a["job_number"] = 9999  # this attribute is required by match()
            b = pd.DataFrame(
                {
                    key.split("cert_")[1]: [form.get(key)]
                    for key in form
                    if key.startswith("cert_")
                }
            )
            a_wrangled_df = wrangle(a)
            b_wrangled_df = wrangle(b)
            a_wrangled_df = (
                a_wrangled_df.style.set_table_styles(
                    [
                        {
                            "selector": "th",
                            "props": [
                                ("background-color", "rgb(122, 128, 138)"),
                                ("color", "black"),
                            ],
                        }
                    ]
                )
                .set_table_attributes('border="1"')
                .set_properties(
                    **{"font-size": "10pt", "background-color": "rgb(168, 185, 191)"}
                )
                .set_properties(
                    subset=["action", "job_number"], **{"text-align": "center"}
                )
                .hide_index()
            )
            b_wrangled_df = (
                b_wrangled_df.style.set_table_styles(
                    [
                        {
                            "selector": "th",
                            "props": [
                                ("background-color", "rgb(122, 128, 138)"),
                                ("color", "black"),
                            ],
                        }
                    ]
                )
                .set_table_attributes('border="1"')
                .set_properties(
                    **{"font-size": "10pt", "background-color": "rgb(168, 185, 191)"}
                )
                .hide_index()
            )
            b['cert_id'] = 999999
            match_result = match(company_projects=a, df_web=b, test=True).iloc[0]
            pred_prob = match_result.pred_prob
            pred_match = match_result.pred_match
            a_wrangled_df = a_wrangled_df.render(escape=False)
            b_wrangled_df = b_wrangled_df.render(escape=False)
            return redirect(
                url_for(
                    "interact",
                    **{key: form.get(key) for key in form},
                    pred_prob=pred_prob,
                    pred_match=pred_match,
                    a_wrangled_df=a_wrangled_df,
                    b_wrangled_df=b_wrangled_df,
                )
            )
        else:
            return redirect(
                url_for(
                    "interact", **{key: form.get(key) for key in form}
                )
            )

    else:
        return render_template(
            "interact.html",
            interact=True,
            **{key: request.args.get(key) for key in request.args},
        )

@app.route('/rewind', methods=["POST", "GET"])
def rewind():
    result_limit = request.args.get('result_limit')
    location_string = request.args.get('location_string')
    text_search = request.args.get('text_search')
    select_source = request.args.get('select_source')
    skip = request.args.get('skip')
    if skip == 'd':
        end_date = str(parse_date(request.args.get('start_date')).date() - dateutil.relativedelta.relativedelta(days=1))
    elif skip == 'w':
        end_date = str(parse_date(request.args.get('start_date')).date() - dateutil.relativedelta.relativedelta(weeks=1))
    elif skip == 'm':
        end_date = str(parse_date(request.args.get('start_date')).date() - dateutil.relativedelta.relativedelta(months=1))
    elif skip == 'y':
        end_date = str(parse_date(request.args.get('start_date')).date() - dateutil.relativedelta.relativedelta(years=1))
    else:
        end_date = str(parse_date(request.args.get('start_date')).date() - dateutil.relativedelta.relativedelta(days=1))
    start_coords_lat = request.args.get('start_coords_lat')
    start_coords_lng = request.args.get('start_coords_lng')
    start_zoom = request.args.get('start_zoom', 6)
    region_size = request.args.get('region_size', 500)
    return redirect(url_for("map", end_date=end_date, start_coords_lat=start_coords_lat, start_coords_lng=start_coords_lng, start_zoom=start_zoom, region_size=region_size, result_limit=result_limit, location_string=location_string, text_search=text_search, select_source=select_source))

@app.route('/set_location', methods=["POST", "GET"])
def set_location():
    logger.debug(f"set_location just got called: {datetime.datetime.now()}")
    location_string = request.form.get('location_string')
    result_limit = request.form.get('result_limit')
    text_search = request.form.get('text_search')
    text_search = ' '.join(re.findall('[A-z0-9çéâêîôûàèùëïü() ]*', text_search)[:-1])  # strip out disallowed charcters
    text_search = ' '.join([x.lower() if x not in ('OR', 'AND', 'NOT') else x for x in text_search.split(' ')])
    select_source = request.form.get('select_source')
    start_coords, region_size = get_city_latlng(location_string.title())
    if not start_coords:
        # location_string = None
        current_lat, current_lng, current_city = get_current_coords()
        start_coords = {'lat':current_lat, 'lng':current_lng}
        region_size = 500 if current_lat == 'nan' else 1
    start_zoom = get_zoom_level(float(region_size))
    return redirect(url_for("map", start_coords_lat=start_coords['lat'], start_coords_lng=start_coords['lng'], start_zoom=start_zoom, region_size=region_size, result_limit=result_limit, location_string=location_string, text_search=text_search, select_source=select_source))

@app.route('/map', methods=["POST", "GET"])
def map():
    logger.debug(f"map just got called: {datetime.datetime.now()}")
    load_user()
    if request_limit_reached():
        return redirect(url_for("limit"))
    logger.debug(f"done loading user - start building page: {datetime.datetime.now()}")
    closed_query = """
        SELECT
            company_projects.job_number,
            company_projects.city,
            company_projects.address,
            company_projects.title,
            company_projects.owner,
            company_projects.contractor,
            company_projects.engineer,
            web.url_key,
            web.pub_date,
            CONCAT(base_urls.base_url, web.url_key) AS link,
            web.source,
            COALESCE(web.address_lat, web.city_lat) as lat,
            COALESCE(web.address_lng, web.city_lng) as lng,
            base_urls.long_name as source_name
        FROM (SELECT * FROM web_certificates ORDER BY cert_id DESC LIMIT 16000) as web
        LEFT JOIN
            attempted_matches
        ON
            web.cert_id = attempted_matches.cert_id
        LEFT JOIN
            company_projects
        ON
            attempted_matches.project_id = company_projects.project_id
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web.source
        WHERE
            company_projects.closed=1
        AND
            attempted_matches.ground_truth=1
        AND
            company_id=%s
    """
    open_query = """
        SELECT
            company_projects.job_number,
            company_projects.city,
            company_projects.address,
            company_projects.title,
            company_projects.owner,
            company_projects.contractor,
            company_projects.engineer,
            company_projects.receiver_emails_dump,
            COALESCE(company_projects.address_lat, company_projects.city_lat) as lat,
            COALESCE(company_projects.address_lng, company_projects.city_lng) as lng
        FROM company_projects
        WHERE
            company_projects.closed=0
        AND
            company_id=%s
    """
    current_lat, current_lng, current_city = get_current_coords()
    get_lat = request.args.get('start_coords_lat', current_lat)
    get_lat = 45.41117 if get_lat == 'nan' else float(get_lat)
    get_lng = request.args.get('start_coords_lng', current_lng)
    get_lng = -75.69812 if get_lng == 'nan' else float(get_lng)
    region_size = request.args.get('region_size', 500 if current_lat == 'nan' else 1)
    pad = (float(region_size) ** 0.5)/1.3
    text_search = request.args.get('text_search', None)
    location_string = request.args.get('location_string', current_city)
    location_string = None if location_string == 'nan' else location_string
    today = datetime.datetime.now().date()
    end_date = request.args.get('end_date', str(today))
    select_source = request.args.get('select_source', '%')
    result_limit = request.args.get('result_limit')
    limit_count = 200
    logger.debug(f"done with all map set-up - start getting running SQL queries: {datetime.datetime.now()}")
    with create_connection() as conn:
        logger.debug(f"getting open project: {datetime.datetime.now()}")
        df_cp_open = pd.read_sql(open_query, conn, params=[session.get('company_id')])
        logger.debug(f"getting closed project: {datetime.datetime.now()}")
        df_cp_closed = pd.read_sql(closed_query, conn, params=[session.get('company_id')])
    logger.debug(f"closed db connection after retreiving open and closed: {datetime.datetime.now()}")
    df_cp_open.dropna(axis=0, subset=['lat'], inplace=True)
    df_cp_closed.dropna(axis=0, subset=['lat'], inplace=True)
    logger.debug(f"getting web_certs: {datetime.datetime.now()}")
    df_wc = get_web_certs(get_lat - pad, get_lat + pad, get_lng - pad, get_lng + pad, end_date, select_source, limit_count*2, text_search=text_search)
    logger.debug(f"got web_certs: {datetime.datetime.now()}")
    if len(df_wc) > limit_count:
        last_date = df_wc.iloc[limit_count].pub_date
    elif len(df_wc):
        last_date = list(df_wc.pub_date)[-1]
    else:
        last_date = "1990-06-2"
    df_wc = df_wc[df_wc.pub_date >= last_date]
    logger.info('SQL queries successful!')
    df_wc.dropna(axis=0, subset=['lat'], inplace=True)
    rows_remaining = df_wc.head(1) if result_limit == 'daily' else df_wc
    if (not result_limit) or (result_limit == 'daily'):
        non_specified_start_date = list(rows_remaining.pub_date)[0] if len(rows_remaining) else '2000-01-01'
    else:
        non_specified_start_date = list(rows_remaining.pub_date)[-1] if len(rows_remaining) else '2000-01-01'
    start_date = request.args.get('start_date', non_specified_start_date)
    def select_df_wc_window(start_date, end_date):
        return df_wc[(start_date <= df_wc.pub_date) & (df_wc.pub_date <= end_date)]
    df_wc = select_df_wc_window(start_date, end_date)
    start_zoom = request.args.get('start_zoom', get_zoom_level(float(region_size)))
    start_coords_lat = request.args.get('start_coords_lat', df_cp_open.lat.mean())
    start_coords_lng = request.args.get('start_coords_lng', df_cp_open.lng.mean())
    logger.debug(f"start building map: {datetime.datetime.now()}")
    m = folium.Map(tiles='cartodbpositron', location=(get_lat, get_lng), zoom_start=start_zoom, min_zoom=5, height='71%')
    mc = MarkerCluster()
    feature_group = folium.FeatureGroup(name='Closed Projects')
    for _, row in df_cp_closed.iterrows():
        popup=folium.map.Popup(html=f"""
            <style>
                h4 {{
                    text-align: center;
                    font-family: 'Montserrat', sans-serif
                }}
                h5 {{
                    text-align: center;
                    color: rgb(112, 94, 134);
                    font-family: 'Montserrat', sans-serif
                }}
                table {{
                    border-collapse:separate; 
                    border-spacing:1em;
                }}  
                th {{
                    text-align: right;
                    vertical-align: top;
                    font-family: 'Montserrat', sans-serif;
                    font-size: 90%;
                }}
                td {{
                    text-align: left;
                    vertical-align: top;
                    font-family: 'Montserrat', sans-serif;
                    font-size: 90%;
                }}
            </style>
            <h4>{row.title}</h4>
            <h5><b>@ {row.address}</b></h5>
            <hr style="border: 1px solid black">
            <table style="width:100%">
                <tr>
                    <th><b>Date published</b></th>
                    <td>{row.pub_date}</td>
                </tr>
                <tr>
                    <th><b>Contractor</b></th> 
                    <td><a href={ url_for('insights', text_search=row.contractor) }>{row.contractor}</a></td>
                </tr>
                <tr>
                    <th><b>Owner</b></th>
                    <td><a href={ url_for('insights', text_search=row.owner) }>{row.owner}</a></td>
                </tr>
                <tr>
                    <th><b>Certifier</b></th>
                    <td><a href={ url_for('insights', text_search=row.engineer) }>{row.engineer}</a></td>
                </tr>
                <tr>
                    <th><b>Source</b></th>
                    <td><a href="{row.link}" "target="_blank">{row.source_name}</a></td>
                </tr>
            </table>
            <hr style="border: 1px solid black">
            <h5><em>project was successfully matched</em></h5>
            <br>
        """, max_width='250', max_height='200')
        folium.Marker(
            [row.lat, row.lng],
            popup=popup,
            # tooltip=f"{row.title[:25]}{'...' if len(row.title) >= 25 else ''}",
            icon=folium.Icon(prefix='fa', icon='check', color='gray')
        ).add_to(feature_group)
    if session.get('account_type') == "full":
        feature_group.add_to(m)

    feature_group = folium.FeatureGroup(name='Open Projects')
    for _, row in df_cp_open.iterrows():
        popup=folium.map.Popup(html=f"""
            <style>
                h4 {{
                    text-align: center;
                    font-family: 'Montserrat', sans-serif
                }}
                h5 {{
                    text-align: center;
                    color: rgb(112, 94, 134);
                    font-family: 'Montserrat', sans-serif
                }}
                table {{
                    border-collapse:separate; 
                    border-spacing:1em;
                }}  
                th {{
                    text-align: right;
                    vertical-align: top;
                    font-family: 'Montserrat', sans-serif;
                    font-size: 90%;
                }}
                td {{
                    text-align: left;
                    vertical-align: top;
                    font-family: 'Montserrat', sans-serif;
                    font-size: 90%;
                }}
            </style>
            <h4>{row.title}</h4>
            <h5><b>@ {row.address}</b></h5>
            <hr style="border: 1px solid black">
            <table style="width:100%">
                <tr>
                    <th><b>Contractor</b></th> 
                    <td><a href={ url_for('insights', text_search=row.contractor) }>{row.contractor}</a></td>
                </tr>
                <tr>
                    <th><b>Owner</b></th>
                    <td><a href={ url_for('insights', text_search=row.owner) }>{row.owner}</a></td>
                </tr>
                <tr>
                    <th><b>Certifier</b></th>
                    <td><a href={ url_for('insights', text_search=row.engineer) }>{row.engineer}</a></td>
                </tr>
            </table>
            <hr style="border: 1px solid black">
            <h5><em>actively searching for matches</em></h5>
            <br>
        """, max_width='250', max_height='200')
        folium.Marker(
            [row.lat, row.lng],
            popup=popup,
            # tooltip=f"{row.title[:25]}{'...' if len(row.title) >= 25 else ''}",
            icon=folium.Icon(prefix='fa', icon='search', color='black')
        ).add_to(feature_group)
    if session.get('account_type') == "full":
        feature_group.add_to(m)

    feature_group = folium.FeatureGroup(name="Web CSP's")
    for _, row in df_wc.iterrows():
        popup=folium.map.Popup(html=f"""
            <style>
                h4 {{
                    text-align: center;
                    font-family: 'Montserrat', sans-serif
                    display: table-cell;
                    white-space: nowrap;
                    text-overflow: ellipsis;
                    font-family: 'Montserrat', sans-serif;
                    overflow: hidden;
                    display: -webkit-box;
                    -webkit-line-clamp: 3;
                    -webkit-box-orient: vertical;
                }}
                h5 {{
                    text-align: center;
                    color: rgb(112, 94, 134);
                    font-family: 'Montserrat', sans-serif
                }}
                table {{
                    border-collapse:separate; 
                    border-spacing:1em;
                }}  
                th {{
                    text-align: right;
                    vertical-align: top;
                    font-family: 'Montserrat', sans-serif;
                    font-size: 90%;
                }}
                td {{
                    text-align: left;
                    vertical-align: top;
                    font-family: 'Montserrat', sans-serif;
                    font-size: 90%;
                }}
            </style>
            <h4>{row.title}</h4>
            <h5><b>@ {row.address}</b></h5>
            <hr style="border: 1px solid var(--highlight)">
            <table style="width:100%">
                <tr>
                    <th><b>Contractor</b></th>
                    <td><a href={ url_for('insights', text_search=row.contractor) }>{row.contractor.replace("`",' ')}</a></td>
                </tr>
                <tr>
                    <th><b>Owner</b></th>
                    <td><a href={ url_for('insights', text_search=row.owner) }>{row.owner}</a></td>
                </tr>
                <tr>
                    <th><b>Certifier</b></th>
                    <td><a href={ url_for('insights', text_search=row.engineer) }>{row.engineer}</a></td>
                </tr>
                <tr>
                    <th><b>Source</b></th>
                    <td><a href="{row.link}" "target="_blank">{row.source_name}</a></td>
                </tr>
            </table>
            <hr style="border: 1px solid var(--highlight);">
            <h5 style="color: var(--background);"><em>Web Certificate of Substantial Performance</em></h5>
            <br>
        """, max_width='250', max_height='200')
        mc.add_child(folium.Marker(
            [row.lat, row.lng],
            popup=popup,
            # tooltip=f"{str(row.title)[:25]}{'...' if len(str(row.title)) >= 25 else ''}",
            icon=folium.Icon(prefix='fa', icon='circle', color='lightgray')
        ))
    feature_group.add_child(mc)
    feature_group.add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    LocateControl().add_to(m)
    m.save('templates/map_widget.html')
    logger.debug(f"done building map - start editing html: {datetime.datetime.now()}")
    with open('templates/map_widget.html', 'r+') as f:
        html = f.read()
        for line in [
            """<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css"/>""",
            """<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css"/>""",
        ]:  # delete in folium-generated html that interfere with this project's `style.css`.
            html = html.replace(line,'')
        html = html.replace(
            """font-awesome/4.6.3/css/font-awesome.min.css""", 
            """font-awesome/4.7.0/css/font-awesome.min.css"""
        )  # update font-awesome in folium-generated html to match this project's `style.css`.
        html = html.replace(
            """initial-scale=1.0, maximum-scale=1.0,""",
            """initial-scale=0.95, maximum-scale=0.95,"""
        )
        html = html.replace(
            """width: 100.0%;""",
            """width: 95.0%;"""
        )
        html = html.replace(
            """position: relative;""",
            """position: center;"""
        )
        html = html.replace(
            """left: 0.0%;""",
            """left: 2.5%;"""
        )
        html = html.replace(
            """</head>""",
            """<style>
                    .marker-cluster-small {
                      background-color: rgba(130, 117, 147, 0.6);
                    }
                    .marker-cluster-small div {
                      background-color: rgba(130, 117, 147, 0.6);
                    }

                    .marker-cluster-medium {
                      background-color: rgba(112, 94, 134, 0.6);
                    }
                    .marker-cluster-medium div {
                      background-color: rgba(112, 94, 134, 0.6);
                    }

                    .marker-cluster-large {
                      background-color: rgba(91, 71, 116, 0.6);
                    }
                    .marker-cluster-large div {
                      background-color: rgba(112, 94, 134, 0.6);
                    }
                  </style>
            """
        )
        f.seek(0)
        f.write(html)
        f.truncate()
        logger.debug(f"done editing html - rendering page: {datetime.datetime.now()}")
    return render_template('map.html', map=True, start_date=start_date, end_date=end_date, start_coords_lat=start_coords_lat, start_coords_lng=start_coords_lng, start_zoom=start_zoom, region_size=region_size, cert_count=len(df_wc), result_limit=result_limit, location_string=location_string, text_search=text_search, select_source=select_source)

@app.route('/insights', methods=["POST", "GET"])
def insights():
    load_user()
    if request_limit_reached():
        return redirect(url_for("limit"))
    wc_count = None
    wc_search_type = None
    text_search = request.form.get('text_search', request.args.get('text_search', ''))
    if text_search:
        query = """
            SELECT pub_date
            FROM web_certificates
            WHERE cert_id in (
                SELECT cert_id 
                FROM cert_search 
                WHERE text MATCH %s
            )
            AND cert_type = 'csp'
        """
        with create_connection() as conn:
            df_raw = pd.read_sql(query, conn, params=[text_search])
        df_raw["pub_date"] = df_raw["pub_date"].astype("datetime64")
        df_actual = df_raw.groupby(df_raw["pub_date"].dt.year).count().rename(columns={'pub_date': 'count'})
        present = datetime.datetime.now()
        df2 = pd.DataFrame(index=np.arange(2001,2021))
        df_actual = df2.join(df_actual)
        df_actual.fillna(0, inplace=True)
        df_forecast = df_actual.copy()
        try:
            day_of_year = present.timetuple().tm_yday
            forecast_current_year = df_actual.loc[present.year]['count'] * 365 / day_of_year
            if day_of_year < 15:
                forecast_current_year = max(df_actual.loc[present.year - 1]['count'] * 1.5 , forecast_current_year)
            df_forecast.loc[present.year]['count'] = forecast_current_year
        except KeyError:
            pass
        df_ema = df_forecast.copy()
        df_ema['EMA_5'] = df_ema.iloc[:,0][::-1].ewm(span=5, adjust=True).mean()[::-1]
        df_ema['SMOOTH_EMA_5'] = df_ema.EMA_5.interpolate(method='cubic')
        if len(df_raw):
            plt.figure(figsize=[15,10])
            plt.rcParams.update({'font.size': 22})
            plt.bar(df_actual.index, df_actual.iloc[:,0], align='center', alpha=0.2, label='projects completed per year', color='blue')
            if int(forecast_current_year):
                plt.bar(df_forecast.index, df_forecast.iloc[:,0], align='center', alpha=0.4, label='projected completions this year', color='gray')
            if len(df_raw) > 10:
                plt.plot(df_ema.iloc[:-3]['SMOOTH_EMA_5'],label='calculated project load', color='purple', linewidth=5)
                plt.plot(df_ema.iloc[-4:-1]['SMOOTH_EMA_5'], color='purple', linewidth=5, linestyle='--')
            ax = plt.axes()
            x_axis = ax.axes.get_xaxis()
            x_label = x_axis.get_label()
            x_label.set_visible(False)
            for spine in ax.spines:
                ax.spines[spine].set_visible(False)
            ax.tick_params(axis=u'both', which=u'both',length=0)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.xaxis.set_major_locator(MaxNLocator(integer=True))
            plt.xticks([2005, 2010, 2015, 2020])
            plt.locator_params(axis='x', nbins=20)
            legend = plt.legend(frameon=1, prop={'size': 20})
            frame = legend.get_frame()
            frame.set_alpha(0)
            plt.savefig(f"static/timeline_{text_search.replace(' ', '_')}.png", transparent=True)
            time.sleep(5)
        query = """
            SELECT *
            FROM web_certificates
            WHERE cert_id in (
                SELECT cert_id 
                FROM cert_search 
                WHERE text MATCH %s
            )
            AND cert_type = 'csp'
        """
        with create_connection() as conn:
            df = pd.read_sql(query, conn, params=[text_search])
        df.pickup_datetime = pd.to_datetime(df.pub_date, format='%Y-%m-%d')
        df['year'] = df.pickup_datetime.apply(lambda x: x.year)
        df['month'] = df.pickup_datetime.apply(lambda x: x.month)
        df['count'] = 1
        def generateBaseMap(default_location=[48.5, -83], default_zoom_start=4):
            m = folium.Map(tiles='cartodbpositron', location=default_location, control_scale=True, zoom_start=default_zoom_start)
            LocateControl().add_to(m)
            return m
        m = generateBaseMap()
        agg_data = df[['address_lat', 'address_lng', 'count']].groupby(['address_lat', 'address_lng']).sum().reset_index().values.tolist()
        HeatMap(data=agg_data, radius=12, use_local_extrema=False, gradient={0.2: 'blue', 0.4: 'blue', 0.6: 'purple', 1: 'purple'}).add_to(m)
        m.save('templates/agg_heatmap.html')
        m = generateBaseMap()
        df_year_list = []
        years = df.year.sort_values().unique()
        for year in years:
            df_year_list.append(df.loc[df.year == year, ['address_lat', 'address_lng', 'count']].groupby(['address_lat', 'address_lng']).sum().reset_index().values.tolist())
        HeatMapWithTime(df_year_list, index=list(years), auto_play=True, radius=15, use_local_extrema=True, gradient={0.2: 'blue', 0.4: 'blue', 0.6: 'purple', 1: 'purple'}).add_to(m)
        m.save('templates/year_lapse_heatmap.html')
        text_search = ' '.join(re.findall('[A-z0-9çéâêîôûàèùëïü() ]*', text_search)[:-1])  # strip out disallowed charcters
        text_search = ' '.join([x.lower() if x not in ('OR', 'AND', 'NOT') else x for x in text_search.split(' ')])
        wc_count, _ = generate_wordcloud(f"{text_search}_contractor")
        field_results = [(field, generate_wordcloud(f"{text_search}_{field}")[1]) for field in ('contractor', 'engineer', 'owner', 'city')]
        sorted_field_results = sorted(field_results, key=lambda field_results:field_results[1])
        if sorted_field_results[0][1] < 0.25:
            wc_search_type = sorted_field_results[0][0]
        print(sorted_field_results)
    return render_template('insights.html', text_search=text_search, wc_id=text_search.replace(' ', '_') if text_search else '', wc_count=wc_count, wc_search_type=wc_search_type)

@app.route('/get_year_lapse_heatmap', methods=["POST", "GET"])
def get_year_lapse_heatmap():
    print('yo')
    return render_template('year_lapse_heatmap.html')

@app.route('/get_agg_heatmap', methods=["POST", "GET"])
def get_agg_heatmap():
    print('yo')
    return render_template('agg_heatmap.html')


if __name__ == "__main__":
    if load_config()["flask_app"]["adhoc_ssl"]:
        app.run(debug=load_config()["flask_app"]["debug"], ssl_context="adhoc")
    else:
        app.run(debug=load_config()["flask_app"]["debug"])
