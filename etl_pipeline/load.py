# etl_pipeline/load.py

from __future__ import annotations
from typing import Dict

import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


# ========= KONFIGURASI KONEKSI MYSQL =========
MYSQL_USER = "root"          # sesuaikan dengan phpMyAdmin
MYSQL_PASSWORD = ""          # kosong jika XAMPP default
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "dw_sales"        # nama database warehouse


# ========= PATH & LOGGING =========
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "etl_load.log"

logger = logging.getLogger("etl_load")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_engine():
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )
    engine = create_engine(url, echo=False)
    return engine


# ========= BUAT STAR SCHEMA (DIM + FACT) =========
def create_star_schema(engine):
    """
    Membuat tabel dim_date, dim_country, dim_item, dim_channel, fact_sales.
    """
    with engine.begin() as conn:
        # dim_date
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_id INT AUTO_INCREMENT PRIMARY KEY,
                    order_date DATE NOT NULL,
                    order_year INT,
                    order_month INT,
                    UNIQUE KEY uq_dim_date_order_date (order_date)
                );
                """
            )
        )

        # dim_country
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dim_country (
                    country_id INT AUTO_INCREMENT PRIMARY KEY,
                    region VARCHAR(100),
                    country VARCHAR(100),
                    UNIQUE KEY uq_dim_country_region_country (region, country)
                );
                """
            )
        )

        # dim_item
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dim_item (
                    item_id INT AUTO_INCREMENT PRIMARY KEY,
                    item_type VARCHAR(100),
                    UNIQUE KEY uq_dim_item_item_type (item_type)
                );
                """
            )
        )

        # dim_channel
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dim_channel (
                    channel_id INT AUTO_INCREMENT PRIMARY KEY,
                    sales_channel VARCHAR(50),
                    UNIQUE KEY uq_dim_channel_sales_channel (sales_channel)
                );
                """
            )
        )

        # fact_sales
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS fact_sales (
                    sales_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    order_id BIGINT,
                    date_id INT,
                    country_id INT,
                    item_id INT,
                    channel_id INT,
                    units_sold DOUBLE,
                    unit_price DOUBLE,
                    unit_cost DOUBLE,
                    total_revenue DOUBLE,
                    total_cost DOUBLE,
                    total_profit DOUBLE,
                    profit_per_unit DOUBLE,
                    revenue_per_unit DOUBLE,
                    profit_margin_ratio DOUBLE,
                    shipping_days INT,
                    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
                    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
                    FOREIGN KEY (item_id) REFERENCES dim_item(item_id),
                    FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id)
                );
                """
            )
        )


# ========= RESET DATA (FULL LOAD) =========
def reset_star_schema(engine):
    """
    Kosongkan tabel fact & dim sebelum load (full refresh),
    dengan mematikan foreign_key_checks sementara.
    """
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        conn.execute(text("DELETE FROM fact_sales;"))
        conn.execute(text("DELETE FROM dim_date;"))
        conn.execute(text("DELETE FROM dim_country;"))
        conn.execute(text("DELETE FROM dim_item;"))
        conn.execute(text("DELETE FROM dim_channel;"))

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))


# ========= LOAD DIMENSIONS =========
def load_dimensions(df: pd.DataFrame, engine) -> Dict[str, pd.DataFrame]:
    """
    Mengisi dim_date, dim_country, dim_item, dim_channel.
    Return: dict nama_dim -> dataframe dimensi di DB (dengan ID).
    """
    dim_frames: Dict[str, pd.DataFrame] = {}
    cols = df.columns

    # --- dim_date ---
    if {"order_date", "order_year", "order_month"}.issubset(cols):
        dim_date = (
            df[["order_date", "order_year", "order_month"]]
            .drop_duplicates()
            .sort_values("order_date")
        )
        dim_date["order_date"] = pd.to_datetime(dim_date["order_date"])
        dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
        dim_date_db = pd.read_sql("SELECT * FROM dim_date", engine)
        dim_frames["dim_date"] = dim_date_db

    # --- dim_country ---
    if {"region", "country"}.issubset(cols):
        dim_country = df[["region", "country"]].drop_duplicates()
        dim_country.to_sql("dim_country", engine, if_exists="append", index=False)
        dim_country_db = pd.read_sql("SELECT * FROM dim_country", engine)
        dim_frames["dim_country"] = dim_country_db

    # --- dim_item ---
    if "item_type" in cols:
        dim_item = df[["item_type"]].drop_duplicates()
        dim_item.to_sql("dim_item", engine, if_exists="append", index=False)
        dim_item_db = pd.read_sql("SELECT * FROM dim_item", engine)
        dim_frames["dim_item"] = dim_item_db

    # --- dim_channel ---
    if "sales_channel" in cols:
        dim_channel = df[["sales_channel"]].drop_duplicates()
        dim_channel.to_sql("dim_channel", engine, if_exists="append", index=False)
        dim_channel_db = pd.read_sql("SELECT * FROM dim_channel", engine)
        dim_frames["dim_channel"] = dim_channel_db

    return dim_frames


# ========= LOAD FACT TABLE =========
def load_fact_sales(df: pd.DataFrame, engine, dim_frames: Dict[str, pd.DataFrame]):
    """
    Join dataframe hasil transform dengan dimensi untuk dapat FK, lalu insert ke fact_sales.
    """
    fact = df.copy()

    # pastikan order_date datetime
    if "order_date" in fact.columns:
        fact["order_date"] = pd.to_datetime(fact["order_date"])

    # join ke dim_date
    if "dim_date" in dim_frames:
        dim_date = dim_frames["dim_date"].copy()
        if "order_date" in dim_date.columns:
            dim_date["order_date"] = pd.to_datetime(dim_date["order_date"])

        fact = fact.merge(
            dim_date[["date_id", "order_date"]],
            how="left",
            on="order_date",
        )

    # join ke dim_country
    if "dim_country" in dim_frames:
        dim_country = dim_frames["dim_country"].copy()
        fact = fact.merge(
            dim_country[["country_id", "region", "country"]],
            how="left",
            on=["region", "country"],
        )

    # join ke dim_item
    if "dim_item" in dim_frames:
        dim_item = dim_frames["dim_item"].copy()
        fact = fact.merge(
            dim_item[["item_id", "item_type"]],
            how="left",
            on="item_type",
        )

    # join ke dim_channel
    if "dim_channel" in dim_frames:
        dim_channel = dim_frames["dim_channel"].copy()
        fact = fact.merge(
            dim_channel[["channel_id", "sales_channel"]],
            how="left",
            on="sales_channel",
        )

    fact_cols = [
        "order_id",
        "date_id",
        "country_id",
        "item_id",
        "channel_id",
        "units_sold",
        "unit_price",
        "unit_cost",
        "total_revenue",
        "total_cost",
        "total_profit",
        "profit_per_unit",
        "revenue_per_unit",
        "profit_margin_ratio",
        "shipping_days",
    ]

    fact_cols_existing = [c for c in fact_cols if c in fact.columns]
    fact_to_load = fact[fact_cols_existing]

    fact_to_load.to_sql("fact_sales", engine, if_exists="append", index=False)

def run_analytic_queries(engine):
    """
    Menjalankan minimal 8 query analitik untuk verifikasi & eksplorasi.
    Hasil ringkas dicatat ke log dan ditampilkan ke console.
    """
    queries = {
        "q1_total_revenue_all": """
            SELECT SUM(total_revenue) AS total_revenue_all
            FROM fact_sales;
        """,
        "q2_total_revenue_per_year": """
            SELECT d.order_year, SUM(f.total_revenue) AS total_revenue_year
            FROM fact_sales f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.order_year
            ORDER BY d.order_year;
        """,
        "q3_top5_country_profit": """
            SELECT c.country, SUM(f.total_profit) AS total_profit_country
            FROM fact_sales f
            JOIN dim_country c ON f.country_id = c.country_id
            GROUP BY c.country
            ORDER BY total_profit_country DESC
            LIMIT 5;
        """,
        "q4_units_sold_per_item": """
            SELECT i.item_type, SUM(f.units_sold) AS total_units_sold
            FROM fact_sales f
            JOIN dim_item i ON f.item_id = i.item_id
            GROUP BY i.item_type
            ORDER BY total_units_sold DESC;
        """,
        "q5_avg_margin_per_channel": """
            SELECT ch.sales_channel, AVG(f.profit_margin_ratio) AS avg_profit_margin_ratio
            FROM fact_sales f
            JOIN dim_channel ch ON f.channel_id = ch.channel_id
            GROUP BY ch.sales_channel;
        """,
        "q6_revenue_per_region_year": """
            SELECT c.region, d.order_year, SUM(f.total_revenue) AS total_revenue
            FROM fact_sales f
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY c.region, d.order_year
            ORDER BY c.region, d.order_year;
        """,
        "q7_top10_order_profit": """
            SELECT f.order_id, f.total_revenue, f.total_cost, f.total_profit
            FROM fact_sales f
            ORDER BY f.total_profit DESC
            LIMIT 10;
        """,
        "q8_avg_shipping_days_country": """
            SELECT c.country, AVG(f.shipping_days) AS avg_shipping_days
            FROM fact_sales f
            JOIN dim_country c ON f.country_id = c.country_id
            GROUP BY c.country
            ORDER BY avg_shipping_days;
        """,
    }

    with engine.connect() as conn:
        for name, sql in queries.items():
            start = time.time()
            result = conn.execute(text(sql))
            rows = result.fetchall()
            elapsed = round(time.time() - start, 3)

            # log ringkas
            logger.info(
                f"[ANALYTIC] {name} | rows={len(rows)} | exec_time_sec={elapsed}"
            )

            # TAMPILKAN KE CONSOLE (print)
            print("=" * 60)
            print(f"RESULT {name} (rows={len(rows)}, exec_time={elapsed}s)")
            if rows:
                # tampilkan maksimal 5 baris pertama
                max_rows = min(5, len(rows))
                # ambil nama kolom
                columns = result.keys()
                print(" | ".join(columns))
                for i in range(max_rows):
                    print(" | ".join(str(rows[i][j]) for j in range(len(columns))))
            else:
                print("No rows returned.")
            print("=" * 60)


# ========= FUNGSI UTAMA LOAD =========
def load_to_warehouse(df_transformed: pd.DataFrame):
    """
    Memuat data hasil transform ke MySQL (star schema) dan mencatat log.
    Sekaligus menjalankan query analitik untuk verifikasi.
    """
    start_time = time.time()
    logger.info("Start LOAD to MySQL data warehouse")

    engine = get_engine()
    create_star_schema(engine)
    reset_star_schema(engine)  # full refresh

    dim_frames = load_dimensions(df_transformed, engine)
    load_fact_sales(df_transformed, engine, dim_frames)

    # jalankan query analitik
    run_analytic_queries(engine)

    elapsed = round(time.time() - start_time, 3)
    logger.info(f"LOAD completed successfully | exec_time_sec={elapsed}")


# ========= TEST CEPAT =========
if __name__ == "__main__":
    from extract import extract_etl_source1
    from extract_api import extract_etl_source2
    from transform import transform_sales

    df_local = extract_etl_source1()
    df_api = extract_etl_source2()
    df_tr = transform_sales(df_local, df_api)

    load_to_warehouse(df_tr)
