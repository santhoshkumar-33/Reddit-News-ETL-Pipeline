import pandas as pd
import requests
import os
import mysql.connector
import news_config as cfg
import logging
from datetime import date

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

current_dt = date.today()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE")
}

def get_mysql_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)


def mysql_load(table_name: str, df: pd.DataFrame):

    conn = get_mysql_conn()
    cursor = conn.cursor()

    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))

    insert_sql = f"""
        INSERT INTO {table_name} ({cols})
        VALUES ({placeholders})
    """

    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()

    cursor.close()
    conn.close()

def get_reddit_token():

    auth = requests.auth.HTTPBasicAuth(
        os.getenv("REDDIT_CLIENT_ID"),
        os.getenv("REDDIT_CLIENT_SECRET")
    )

    data = {
        "grant_type": "client_credentials",
        "username": os.getenv("REDDIT_USERNAME"),
        "password": os.getenv("REDDIT_PASSWORD")
    }

    headers = {"User-Agent": "News/0.0.1"}

    request = requests.post(
        os.getenv("REDDIT_BASE_ACCESS_URL"),
        auth=auth,
        data=data,
        headers=headers
    )

    token = request.json()["access_token"]
    headers["Authorization"] = f"bearer {token}"

    return headers

def make_request(url: str):
    headers = get_reddit_token()
    return requests.get(url, headers=headers, params={"limit": "100"})

def format_df(end_point):
    rows = []
    for post in end_point.json()["data"]["children"]:
        rows.append({
            "title": post["data"]["title"],
            "upvote_ratio": post["data"]["upvote_ratio"],
            "score": post["data"]["score"],
            "ups": post["data"]["ups"],
            "domain": post["data"]["domain"],
            "num_comments": post["data"]["num_comments"]
        })
    return pd.DataFrame(rows)

def delete_today_data():

    conn = get_mysql_conn()
    cursor = conn.cursor()

    for table in cfg.tables:
        cursor.execute(
            f"DELETE FROM {table} WHERE DATE(dt_updated) = CURDATE()"
        )

    conn.commit()
    cursor.close()
    conn.close()

def main():

    logging.info("Fetching Reddit data...")

    sources = [
        cfg.r_news,
        cfg.not_the_onion,
        cfg.offbeat,
        cfg.the_news,
        cfg.us_news,
        cfg.full_news,
        cfg.quality_news,
        cfg.uplifting_news,
        cfg.in_the_news
    ]

    data_frames = []

    for src in sources:
        response = make_request(src)
        df = format_df(response)
        df["dt_updated"] = pd.Timestamp.now()
        data_frames.append(df)

    logging.info("Deleting today's existing data...")
    delete_today_data()

    logging.info("Loading data into MySQL...")
    for table, df in zip(cfg.tables, data_frames):
        mysql_load(table, df)

    logging.info(f"All tables loaded successfully for {current_dt}")

if __name__ == "__main__":
    main()
