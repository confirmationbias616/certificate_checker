#!/usr/bin/env python

from flask import Flask, render_template, url_for, request, redirect
from datetime import datetime
from dateutil.parser import parse as parse_date
from utils import create_connection, load_config
from wrangler import wrangle
from matcher import match
from scraper import scrape
from communicator import process_as_feedback
import pandas as pd
import logging
import sys
import os
import re
import ast


app = Flask(__name__)
app.config["SECRET_KEY"] = "e5ac358c-f0bf-11e5-9e39-d3b532c10a28"


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


@app.route("/", methods=["POST", "GET"])
def index():
    all_contacts_query = "SELECT * FROM contacts"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn)
    if request.method == "POST":
        selected_contact_ids = request.form.getlist("contacts")
        selected_contacts_query = (
            f"SELECT name, email_addr FROM contacts WHERE id in "
            f"({','.join('?'*len(selected_contact_ids))})"
        )
        with create_connection() as conn:
            selected_contacts = pd.read_sql(
                selected_contacts_query, conn, params=[*selected_contact_ids]
            )
        receiver_emails_dump = str(
            {row["name"]: row["email_addr"] for _, row in selected_contacts.iterrows()}
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
                    "SELECT * FROM company_projects WHERE job_number=?",
                    conn,
                    params=[new_entry["job_number"]],
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
                url_for("already_matched", job_number=new_entry["job_number"])
            )
        df = pd.read_sql("SELECT * FROM company_projects", conn)
        df = df.append(new_entry, ignore_index=True)
        if not was_prev_logged:
            df.to_sql("company_projects", conn, if_exists="replace", index=False)
            return render_template(
                "signup_confirmation.html", job_number=new_entry["job_number"]
            )
        recorded_change = (
            True if any(list(df.duplicated(subset="job_number"))) else False
        )
        df = df.drop_duplicates(subset="job_number", keep="last")
        df.to_sql("company_projects", conn, if_exists="replace", index=False)
        return render_template(
            "update.html",
            job_number=new_entry["job_number"],
            recorded_change=recorded_change,
        )
    else:
        try:
            return render_template(
                "index.html",
                home=True,
                all_contacts=all_contacts,
                **{key: request.args.get(key) for key in request.args},
            )
        except NameError:
            return render_template("index.html", home=True, all_contacts=all_contacts)


@app.route("/already_matched", methods=["POST", "GET"])
def already_matched():
    if request.method == "POST":
        return redirect(url_for("index"))
    job_number = request.args.get("job_number")
    link_query = """
        SELECT
            (base_urls.base_url || attempted_matches.url_key) AS link
        FROM
            attempted_matches
        LEFT JOIN
            web_certificates
        ON
            web_certificates.url_key = attempted_matches.url_key
        LEFT JOIN
            base_urls
        ON
            base_urls.source = web_certificates.source
        WHERE
            job_number=?
        AND
            attempted_matches.ground_truth = 1
    """
    with create_connection() as conn:
        link = conn.cursor().execute(link_query, [job_number]).fetchone()[0]
    return render_template("already_matched.html", link=link, job_number=job_number)


@app.route("/potential_match", methods=["POST", "GET"])
def potential_match():
    if request.method == "POST":
        return redirect(url_for("index"))
    job_number = request.args.get("job_number")
    url_key = request.args.get("url_key")
    source = request.args.get("source")
    source_base_url_query = "SELECT base_url FROM base_urls WHERE source=?"
    with create_connection() as conn:
        base_url = conn.cursor().execute(source_base_url_query, [source]).fetchone()[0]
    return render_template(
        "potential_match.html",
        job_number=job_number,
        base_url=base_url,
        url_key=url_key,
        source=source,
    )


@app.route("/summary_table")
def summary_table():
    closed_query = """
            SELECT
                company_projects.job_number,
                company_projects.city,
                company_projects.address,
                company_projects.title,
                company_projects.owner,
                company_projects.contractor,
                company_projects.engineer,
                attempted_matches.url_key,
                web.pub_date,
                (base_urls.base_url || attempted_matches.url_key) AS link
            FROM (SELECT * FROM web_certificates ORDER BY cert_id DESC LIMIT 16000) as web
            LEFT JOIN
                attempted_matches
            ON
                web.url_key = attempted_matches.url_key
            LEFT JOIN
                company_projects
            ON
                attempted_matches.job_number=company_projects.job_number
            LEFT JOIN
                base_urls
            ON
                base_urls.source=web.source
            WHERE
                company_projects.closed=1
            AND
                attempted_matches.ground_truth=1
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
                company_projects.receiver_emails_dump
            FROM company_projects
            WHERE
                company_projects.closed=0
        """
    with create_connection() as conn:
        pd.set_option("display.max_colwidth", -1)
        df_closed = pd.read_sql(closed_query, conn).sort_values(
            "job_number", ascending=False
        )
        df_closed["job_number"] = df_closed.apply(
            lambda row: f"""<a href="{row.link}">{row.job_number}</a>""", axis=1
        )
        df_closed = df_closed.drop("url_key", axis=1)
        df_open = pd.read_sql(open_query, conn).sort_values(
            "job_number", ascending=False
        )
        df_open["action"] = df_open.apply(
            lambda row: (
                f"""<a href="{url_for('index', **row)}">modify</a> / """
                f"""<a href="{url_for('delete_job', job_number=row.job_number)}">delete</a>"""
            ),
            axis=1,
        )
        df_open["contacts"] = df_open.apply(
            lambda row: ", ".join(ast.literal_eval(row.receiver_emails_dump).keys()),
            axis=1,
        )
        col_order = [
            "job_number",
            "title",
            "contractor",
            "engineer",
            "owner",
            "address",
            "city",
        ]

        def highlight_pending(s):
            days_old = (
                datetime.now().date()
                - parse_date(re.findall("\d{4}-\d{2}-\d{2}", s.pub_date)[0]).date()
            ).days
            if days_old < 60:  # fresh - within lien period
                row_colour = "rgb(97, 62, 143)"
            else:
                row_colour = ""
            return [f"color: {row_colour}" for i in range(len(s))]

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
    return render_template(
        "summary_table.html",
        df_closed=df_closed.render(escape=False),
        df_open=df_open.render(escape=False),
    )


@app.route("/delete_job")
def delete_job():
    delete_job_query = """
            DELETE FROM company_projects
            WHERE job_number=?
        """
    delete_match_query = """
            DELETE FROM attempted_matches
            WHERE job_number=?
        """
    job_number = request.args.get("job_number")
    with create_connection() as conn:
        conn.cursor().execute(delete_job_query, [job_number])
        conn.cursor().execute(delete_match_query, [job_number])
    return redirect(url_for("summary_table"))


@app.route("/instant_scan", methods=["POST", "GET"])
def instant_scan():
    if request.method == "POST":
        job_number = request.args.get("job_number")
        lookback = request.args.get("lookback")
        if lookback == "2_months":
            lookback_cert_count = 3000
        elif lookback == "1_month":
            lookback_cert_count = 1500
        else:  # also applies for `2_weeks`
            lookback_cert_count = 750
        company_query = "SELECT * FROM company_projects WHERE job_number=?"
        with create_connection() as conn:
            company_projects = pd.read_sql(company_query, conn, params=[job_number])
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
            return redirect(
                url_for(
                    "potential_match",
                    job_number=job_number,
                    source=source,
                    url_key=url_key,
                )
            )
        return render_template("nothing_yet.html", job_number=job_number)


@app.route("/process_feedback", methods=["POST", "GET"])
def process_feedback():
    # if request.method == 'GET':
    status = process_as_feedback(request.args)
    return render_template(
        "thanks_for_feedback.html",
        job_number=request.args["job_number"],
        response=request.args["response"],
        status=status,
    )


@app.route("/about", methods=["POST", "GET"])
def about():
    return render_template("about.html", about=True, hide_helper_links=True)


@app.route("/contact_config", methods=["POST", "GET"])
def contact_config():
    contact = request.args
    all_contacts_query = "SELECT * FROM contacts"
    with create_connection() as conn:
        all_contacts = pd.read_sql(all_contacts_query, conn)
    all_contacts["action"] = all_contacts.apply(
        lambda row: (
            f"""<a href="{url_for('update_contact', **row)}">modify</a> /"""
            f""" <a href="{url_for('delete_contact', **row)}">delete</a>"""
        ),
        axis=1,
    )
    all_contacts = all_contacts[["name", "email_addr", "action"]]
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
    return render_template(
        "contact_config.html",
        all_contacts=all_contacts.render(escape=False),
        contact=contact,
        config=True,
        hide_helper_links=True,
    )


@app.route("/delete_contact")
def delete_contact():
    delete_contact_query = """
            DELETE FROM contacts
            WHERE id=?
        """
    contact = request.args
    with create_connection() as conn:
        conn.cursor().execute(delete_contact_query, [contact.get("id")])
    return redirect(url_for("contact_config"))


@app.route("/update_contact", methods=["POST", "GET"])
def update_contact():
    contact = request.args
    add_contact_query = """
        INSERT INTO contacts
        (name, email_addr, id) VALUES(?, ?, ?)
    """
    update_contact_query = """
        UPDATE contacts
        SET name = ?,  email_addr = ?
        WHERE id=?
    """
    if request.method == "POST":
        contact = request.form
        with create_connection() as conn:
            conn.cursor().execute(
                update_contact_query,
                [contact.get("name"), contact.get("email_addr"), contact.get("id")],
            )
    return redirect(url_for("contact_config", **contact))


@app.route("/add_contact", methods=["POST", "GET"])
def add_contact():
    contact = request.args
    get_contact_ids = """
        SELECT id FROM contacts
    """
    with create_connection() as conn:
        contact_ids = pd.read_sql(get_contact_ids, conn).sort_values("id")
    add_contact_query = """
        INSERT INTO contacts
        (name, email_addr, id) VALUES(?, ?, ?)
    """
    if request.method == "POST":
        contact = request.form
        if len(contact_ids):
            new_contact_id = int(contact_ids.iloc[-1] + 1)
        else:
            new_contact_id = 1
        with create_connection() as conn:
            conn.cursor().execute(
                add_contact_query,
                [contact.get("name"), contact.get("email_addr"), new_contact_id],
            )
    return redirect(url_for("contact_config"))


@app.route("/interact", methods=["POST", "GET"])
def interact():
    if request.method == "POST":
        if request.form.get("cert_link") or request.form.get("job_number"):
            try:
                url_key = request.form.get("cert_link").split(
                    "https://canada.constructconnect.com"
                )[1]
                b = scrape(source="dcn", provided_url_key=url_key, test=True)
            except IndexError:
                url_key = request.form.get("cert_link")
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
                        "SELECT * FROM company_projects WHERE job_number=?",
                        conn,
                        params=[request.form.get("job_number")],
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
                    cert_link=request.form.get("cert_link"),
                    job_number=request.form.get("job_number"),
                )
            )
        elif any(request.form.values()):
            print(request.form.values())
            a = pd.DataFrame(
                {
                    key.split("comp_")[1]: [request.form.get(key)]
                    for key in request.form
                    if key.startswith("comp_")
                }
            )
            a["job_number"] = 9999  # this attribute is required by match()
            b = pd.DataFrame(
                {
                    key.split("cert_")[1]: [request.form.get(key)]
                    for key in request.form
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
            match_result = match(company_projects=a, df_web=b, test=True)
            return redirect(
                url_for(
                    "interact",
                    **{key: request.form.get(key) for key in request.form},
                    pred_prob=match_result.iloc[0].pred_prob,
                    pred_match=match_result.iloc[0].pred_match,
                    a_wrangled_df=a_wrangled_df.render(escape=False),
                    b_wrangled_df=b_wrangled_df.render(escape=False),
                )
            )
        else:
            return redirect(
                url_for(
                    "interact", **{key: request.form.get(key) for key in request.form}
                )
            )

    else:
        return render_template(
            "interact.html",
            interact=True,
            **{key: request.args.get(key) for key in request.args},
        )


if __name__ == "__main__":
    app.run(debug=load_config()["flask_app"]["debug"])
