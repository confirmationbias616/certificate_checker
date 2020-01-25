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
import logging
import sys
import os
import re
import ast
import folium
from folium.plugins import MarkerCluster
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

def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

# list of tuples determining which upper limit of region size (left) should correspond
# to which level of zoom (right) for the follium map
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
                WHERE id=?
            """, conn, params=[current_user.id]).iloc[0].account_type
        if session.get('account_type') != "full":
            return redirect(url_for("payment"))
    elif set_default_company_id:  # for CI server
        session['company_id'] = 1
        session['company_name'] = "Testing123"
        session['account_type'] = "full"
    else:  # for for dev and prod servers
        session['company_id'] = None

@app.route("/", methods=["POST", "GET"])
def index():
    load_user()
    if session.get('account_type') == 'full' or not session.get('company_id'):
        return redirect(url_for("map"))
    else:
        return redirect(url_for("payment"))


@app.route("/project_entry", methods=["POST", "GET"])
def project_entry():
    load_user()
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    all_contacts_query = "SELECT * FROM contacts WHERE company_id=?"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn, params=[session.get('company_id')])
    if request.method == "POST":
        selected_contact_ids = request.form.getlist("contacts")
        selected_contacts_query = (
            f"SELECT name, email_address FROM contacts WHERE id in "
            f"({','.join('?'*len(selected_contact_ids))}) AND company_id=?"
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
                    "SELECT * FROM company_projects WHERE job_number=? and company_id=?",
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
                    DELETE FROM company_projects WHERE job_number=? AND company_id=?
                """, [new_entry["job_number"], session.get('company_id')])
        with create_connection() as conn:
            conn.cursor().execute(f"""
                INSERT INTO company_projects (company_id, {', '.join(list(new_entry.keys()))}) VALUES (?, {','.join(['?']*len(new_entry))})
            """, [session.get('company_id')] + list(new_entry.values()))
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
            (base_urls.base_url || web_certificates.url_key) AS link
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
            job_number=?
        AND
            company_id=?
        AND
            attempted_matches.ground_truth = 1
    """
    with create_connection() as conn:
        link = conn.cursor().execute(link_query, [job_number, session.get('company_id')]).fetchone()[0]
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
    source_base_url_query = "SELECT base_url FROM base_urls WHERE source=?"
    with create_connection() as conn:
        base_url = conn.cursor().execute(source_base_url_query, [source]).fetchone()[0]
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
                (base_urls.base_url || web.url_key) AS link
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
                company_id=?
            AND
                attempted_matches.ground_truth=1
        """
    open_query = """
            SELECT *
            FROM company_projects
            WHERE company_projects.closed=0
            AND company_id=?
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
                **{"font-size": "10pt", "background-color": "rgb(138, 175, 190)"}
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
            WHERE project_id=?
        """
    delete_match_query = """
            DELETE FROM attempted_matches
            WHERE project_id=?
        """
    project_id = request.args.get("project_id")
    with create_connection() as conn:
        conn.cursor().execute(delete_job_query, [project_id])
        conn.cursor().execute(delete_match_query, [project_id])
    return redirect(url_for("summary_table"))


@app.route("/instant_scan", methods=["POST", "GET"])
def instant_scan():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    if request.method == "POST":
        job_number = request.args.get("job_number")
        lookback = request.args.get("lookback")
        if lookback == "2_months":
            lookback_cert_count = 3000
        elif lookback == "1_month":
            lookback_cert_count = 1500
        else:  # also applies for `2_weeks`
            lookback_cert_count = 750
        company_query = "SELECT * FROM company_projects WHERE job_number=? AND company_id=?"
        with create_connection() as conn:
            company_projects = pd.read_sql(company_query, conn, params=[job_number, session.get('company_id')])
        hist_query = "SELECT * FROM web_certificates ORDER BY pub_date DESC LIMIT ?"
        with create_connection() as conn:
            df_web = pd.read_sql(hist_query, conn, params=[lookback_cert_count])
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
    return render_template("about.html", about=True)


@app.route("/plan_info", methods=["POST", "GET"])
def plan_info():
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
        "UPDATE users SET account_type = 'full' WHERE id = ?"
    )
    with create_connection() as conn:
        conn.cursor().execute(update_account_type_query, [session.get('company_id')])
    session['account_type'] = 'full'
    return render_template("thanks_for_payment.html")

@app.route("/user_account", methods=["POST", "GET"])
def user_account():
    load_user()
    return render_template("user_account.html")

@app.route("/contact_config", methods=["POST", "GET"])
def contact_config():
    load_user()
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    contact = request.args
    all_contacts_query = "SELECT * FROM contacts WHERE company_id=?"
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
                **{"font-size": "10pt", "background-color": "rgb(138, 175, 190)"}
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
            WHERE id=?
            AND company_id=?
        """
    contact = request.args
    with create_connection() as conn:
        conn.cursor().execute(delete_contact_query, [contact.get("id"), session.get('company_id')])
    return redirect(url_for("contact_config", **contact))


@app.route("/update_contact", methods=["POST", "GET"])
def update_contact():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    contact = request.args
    update_contact_query = """
        UPDATE contacts
        SET name = ?,  email_address = ?
        WHERE id=?
        AND company_id=?
    """
    if request.method == "POST":
        contact = request.form
        with create_connection() as conn:
            conn.cursor().execute(
                update_contact_query,
                [contact.get("name"), contact.get("email_address"), contact.get("id"), session.get('company_id')],
            )
    return redirect(url_for("contact_config", **contact))


@app.route("/add_contact", methods=["POST", "GET"])
def add_contact():
    if session.get('account_type') != "full":
        return redirect(url_for("payment"))
    contact = request.args
    add_contact_query = """
        INSERT INTO contacts
        (company_id, name, email_address) VALUES(?, ?, ?)
    """
    if request.method == "POST":
        contact = request.form
        with create_connection() as conn:
            conn.cursor().execute(
                add_contact_query,
                [session.get('company_id'), contact.get("name"), contact.get("email_address")],
            )
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
                        "SELECT * FROM company_projects WHERE job_number=? AND company_id=?",
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
    limit_daily = request.args.get('limit_daily')
    location_string = request.args.get('location_string')
    text_search = request.args.get('text_search')
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
    return redirect(url_for("map", end_date=end_date, start_coords_lat=start_coords_lat, start_coords_lng=start_coords_lng, start_zoom=start_zoom, region_size=region_size, limit_daily=limit_daily, location_string=location_string, text_search=text_search))

@app.route('/set_location', methods=["POST", "GET"])
def set_location():
    location_string = request.form.get('location_string')
    limit_daily = request.form.get('limit_daily')
    text_search = request.form.get('text_search')
    text_search = ' '.join(re.findall('[A-z0-9çéâêîôûàèùëïü ]*', text_search))  # strip out disallowed charcters
    start_coords, region_size = get_city_latlng(location_string.title())
    if not start_coords:
        location_string = 'Ontario'
        start_coords, region_size = get_city_latlng('ontario')
    for size, zoom_level in zoom_params:
        if region_size < size:
            start_zoom = zoom_level
            break
        start_zoom = 5
    return redirect(url_for("map", start_coords_lat=start_coords['lat'], start_coords_lng=start_coords['lng'], start_zoom=start_zoom, region_size=region_size, limit_daily=limit_daily, location_string=location_string, text_search=text_search))

@app.route('/map', methods=["POST", "GET"])
def map():
    load_user()
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
            (base_urls.base_url || web.url_key) AS link,
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
            company_id=?
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
            company_id=?
    """
    web_query = """
        SELECT 
            web_certificates.*, 
            (base_urls.base_url || web_certificates.url_key) AS link,
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
            lat > ?
        AND
            lat < ?
        AND
            lng > ?
        AND
            lng < ?
        AND
            pub_date < ?
        {}
        ORDER BY 
            pub_date
        DESC LIMIT ?
    """
    add_fts_query = """
        AND
            web_certificates.cert_id IN (SELECT cert_id FROM cert_search WHERE text MATCH ?)
    """
    get_lat = request.args.get('start_coords_lat', 'nan')
    get_lat = 45.41117 if get_lat == 'nan' else float(get_lat)
    get_lng = request.args.get('start_coords_lng', 'nan')
    get_lng = -75.69812 if get_lng == 'nan' else float(get_lng)
    region_size = request.args.get('region_size', 500)
    pad = (float(region_size) ** 0.5)/1.3
    text_search = request.args.get('text_search', None)
    location_string = request.args.get('location_string')
    today = datetime.datetime.now().date()
    end_date = request.args.get('end_date', str(today))
    limit_daily = request.args.get('limit_daily')
    limit_count = 200
    while True:
        try:
            with create_connection() as conn:
                df_cp_open = pd.read_sql(open_query, conn, params=[session.get('company_id')])
                df_cp_closed = pd.read_sql(closed_query, conn, params=[session.get('company_id')])
                break
        except pd.io.sql.DatabaseError:
            logger.info('Database is locked. Retrying SQL queries...')
    df_cp_open.dropna(axis=0, subset=['lat'], inplace=True)
    df_cp_closed.dropna(axis=0, subset=['lat'], inplace=True)
    while True:
        with create_connection() as conn:
            if text_search:
                df_wc = pd.read_sql(web_query.format(add_fts_query) , conn, params=[get_lat - pad, get_lat + pad, get_lng - pad, get_lng + pad, end_date, text_search, limit_count*2])
            else:
                df_wc = pd.read_sql(web_query.format(''), conn, params=[get_lat - pad, get_lat + pad, get_lng - pad, get_lng + pad, end_date,limit_count*2])
        if len(df_wc) > limit_count:
            last_date = df_wc.iloc[limit_count].pub_date
        elif len(df_wc):
            last_date = list(df_wc.pub_date)[-1]
        else:
            last_date = "1990-06-2"
        df_wc = df_wc[df_wc.pub_date >= last_date]
        logger.info('SQL queries successful!')
        break
    df_wc.dropna(axis=0, subset=['lat'], inplace=True)
    rows_remaining = df_wc.head(1) if limit_daily else df_wc
    if limit_daily:
        non_specified_start_date = list(rows_remaining.pub_date)[0] if len(rows_remaining) else '2000-01-01'
    else:
        non_specified_start_date = list(rows_remaining.pub_date)[-1] if len(rows_remaining) else '2000-01-01'
    start_date = request.args.get('start_date', non_specified_start_date)
    def select_df_wc_window(start_date, end_date):
        return df_wc[(start_date <= df_wc.pub_date) & (df_wc.pub_date <= end_date)]
    df_wc = select_df_wc_window(start_date, end_date)
    start_zoom = request.args.get('start_zoom', 6)
    start_coords_lat = request.args.get('start_coords_lat', df_cp_open.lat.mean())
    start_coords_lng = request.args.get('start_coords_lng', df_cp_open.lng.mean())
    m = folium.Map(tiles='cartodbpositron', location=(get_lat, get_lng), zoom_start=start_zoom, min_zoom=5, height='71%')
    folium.TileLayer('stamentoner').add_to(m)
    folium.TileLayer('cartodbdark_matter').add_to(m)
    folium.TileLayer('OpenStreetMap').add_to(m)
    folium.TileLayer('stamenwatercolor').add_to(m)
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
                    color: rgb(104, 76, 141);
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
            <hr style="border: 1px solid black">
            <table style="width:100%">
                <tr>
                    <th><b>Date published</b></th>
                    <td>{row.pub_date}</td>
                </tr>
                <tr>
                    <th><b>Contractor</b></th> 
                    <td>{row.contractor}</td> 
                </tr>
                <tr>
                    <th><b>Owner</b></th>
                    <td>{row.owner}</td>
                </tr>
                <tr>
                    <th><b>Source</b></th>
                    <td><a href="{row.link}" "target="_blank">{row.source_name}</a></td>
                </tr>
            </table>
            <hr style="border: 1px solid black">
            <h5><em>Web Certificate of Substantial Performance</em></h5>
        """, max_width='250', max_height='200')
        folium.Marker(
            [row.lat, row.lng],
            popup=popup,
            tooltip=f"{row.title[:25]}{'...' if len(row.title) >= 25 else ''}",
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
                    color: rgb(104, 76, 141);
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
            <hr style="border: 1px solid black">
            <table style="width:100%">
                <tr>
                    <th><b>Contractor</b></th> 
                    <td>{row.contractor}</td> 
                </tr>
                <tr>
                    <th><b>Owner</b></th>
                    <td>{row.owner}</td>
                </tr>
            </table>
            <hr style="border: 1px solid black">
            <h5><em>Web Certificate of Substantial Performance</em></h5>
        """, max_width='250', max_height='200')
        folium.Marker(
            [row.lat, row.lng],
            popup=popup,
            tooltip=f"{row.title[:25]}{'...' if len(row.title) >= 25 else ''}",
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
                }}
                h5 {{
                    text-align: center;
                    color: rgb(104, 76, 141);
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
            <hr style="border: 1px solid black">
            <table style="width:100%">
                <tr>
                    <th><b>Date published</b></th>
                    <td>{row.pub_date}</td>
                </tr>
                <tr>
                    <th><b>Contractor</b></th> 
                    <td>{row.contractor}</td> 
                </tr>
                <tr>
                    <th><b>Owner</b></th>
                    <td>{row.owner}</td>
                </tr>
                <tr>
                    <th><b>Source</b></th>
                    <td><a href="{row.link}" "target="_blank">{row.source_name}</a></td>
                </tr>
            </table>
            <hr style="border: 1px solid black">
            <h5><em>Web Certificate of Substantial Performance</em></h5>
        """, max_width='250', max_height='200')
        mc.add_child(folium.Marker(
            [row.lat, row.lng],
            popup=popup,
            tooltip=f"{str(row.title)[:25]}{'...' if len(str(row.title)) >= 25 else ''}",
            icon=folium.Icon(prefix='fa', icon='circle', color='green')
        ))
    feature_group.add_child(mc)
    feature_group.add_to(m)
    folium.LayerControl(collapsed=True).add_to(m)
    m.save('templates/map_widget.html')
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
        f.seek(0)
        f.write(html)
        f.truncate()
    return render_template('map.html', map=True, start_date=start_date, end_date=end_date, start_coords_lat=start_coords_lat, start_coords_lng=start_coords_lng, start_zoom=start_zoom, region_size=region_size, cert_count=len(df_wc), limit_daily=limit_daily, location_string=location_string, text_search=text_search)


if __name__ == "__main__":
    if load_config()["flask_app"]["adhoc_ssl"]:
        app.run(debug=load_config()["flask_app"]["debug"], ssl_context="adhoc")
    else:
        app.run(debug=load_config()["flask_app"]["debug"])
