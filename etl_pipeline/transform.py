# etl_pipeline/transform.py

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd


@dataclass
class TransformConfig:
    id_col: str = "Order ID"
    order_date_col: str = "Order Date"
    ship_date_col: str = "Ship Date"
    numeric_cols: Tuple[str, ...] = (
        "Units Sold",
        "Unit Price",
        "Unit Cost",
        "Total Revenue",
        "Total Cost",
        "Total Profit",
    )
    categorical_cols: Tuple[str, ...] = (
        "Region",
        "Country",
        "Item Type",
        "Sales Channel",
        "Order Priority",
    )


config = TransformConfig()


# ============ HELPER: OUTLIER DENGAN IQR ============
def handle_outliers_iqr(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Tangani outlier dengan metode IQR:
    - Hitung Q1, Q3, IQR
    - Nilai di luar [Q1-1.5*IQR, Q3+1.5*IQR] di-clipping ke batas bawah/atas.
    """
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            continue
        if not np.issubdtype(df[col].dtype, np.number):
            continue

        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        df[col] = df[col].clip(lower=lower, upper=upper)

    return df


# ============ HELPER: NORMALISASI MIN-MAX ============
def min_max_scale(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            continue
        col_min = df[col].min()
        col_max = df[col].max()
        if col_max == col_min:
            df[f"{col}_norm"] = 0.0
        else:
            df[f"{col}_norm"] = (df[col] - col_min) / (col_max - col_min)
    return df


# ============ STANDARDISASI NAMA KOLOM ============
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


# ============ DATA QUALITY CHECKS ============
def run_data_quality_checks(
    df: pd.DataFrame, pk_col: str, numeric_cols: List[str]
) -> Dict[str, object]:
    """
    Minimal 6 aturan validasi:
    - uniqueness check (PK)
    - null check
    - range check
    - datatype consistency
    - referential integrity (pk tidak null)
    - distribusi data (summary statistik)
    """
    report: Dict[str, object] = {}

    # 1) Uniqueness check
    if pk_col in df.columns:
        dup_count = df[pk_col].duplicated().sum()
        report["uniqueness_pk_duplicates"] = int(dup_count)

    # 2) Null check
    report["null_counts_per_column"] = df.isnull().sum().to_dict()

    # 3) Range check (numeric tidak negatif)
    range_issues = {}
    for col in numeric_cols:
        if col in df.columns:
            bad_rows = df[df[col] < 0].shape[0]
            range_issues[col] = int(bad_rows)
    report["range_negative_values"] = range_issues

    # 4) Datatype consistency
    dtypes = df.dtypes.astype(str).to_dict()
    report["dtypes"] = dtypes

    # 5) Referential integrity (pk tidak null)
    if pk_col in df.columns:
        null_pk = df[pk_col].isnull().sum()
        report["referential_integrity_pk_null"] = int(null_pk)

    # 6) Distribusi data (summary statistik numerik)
    desc = df[numeric_cols].describe().to_dict()
    report["distribution_numeric_describe"] = desc

    return report


# ============ TRANSFORM UTAMA ============
def transform_sales(df_local: pd.DataFrame, df_api: pd.DataFrame) -> pd.DataFrame:
    """
    Melakukan seluruh tahapan Transform:
    a. Data cleaning
    b. Standardisasi data
    c. Enrichment & feature engineering
    d. Validasi kualitas data
    """

    # --------- 1. GABUNGKAN SUMBER (ENRICHMENT DASAR) ---------
    df_local = standardize_column_names(df_local)
    df_api = standardize_column_names(df_api)

    df_all = pd.concat([df_local, df_api], ignore_index=True)

    # 1b. Bersihkan kolom kategori untuk dimensi (trim spasi)
    for col in ["region", "country", "item_type", "sales_channel"]:
        if col in df_all.columns:
            df_all[col] = df_all[col].astype(str).str.strip()

    # --------- 2. DATA CLEANING ---------
    # 2.1 Hapus duplikat berdasarkan primary key
    if config.id_col.lower().replace(" ", "_") in df_all.columns:
        pk_col = config.id_col.lower().replace(" ", "_")
    else:
        pk_col = "order_id"

    df_all = df_all.drop_duplicates(subset=[pk_col])

    # 2.2 Tangani missing values
    for col in df_all.columns:
        if df_all[col].dtype.kind in "biufc":  # numeric
            median_val = df_all[col].median()
            df_all[col] = df_all[col].fillna(median_val)
        else:
            df_all[col] = df_all[col].fillna("Unknown")

    # 2.3 Standarkan format tanggal/waktu
    if config.order_date_col.lower().replace(" ", "_") in df_all.columns:
        od_col = config.order_date_col.lower().replace(" ", "_")
        df_all[od_col] = pd.to_datetime(df_all[od_col], errors="coerce")
        # buang baris dengan order_date rusak
        df_all = df_all[df_all[od_col].notna()]
    else:
        od_col = None

    if config.ship_date_col.lower().replace(" ", "_") in df_all.columns:
        sd_col = config.ship_date_col.lower().replace(" ", "_")
        df_all[sd_col] = pd.to_datetime(df_all[sd_col], errors="coerce")
    else:
        sd_col = None

    # 2.4 Tangani outlier (IQR)
    numeric_cols_std = [c.lower().replace(" ", "_") for c in config.numeric_cols]
    numeric_cols_std = [c for c in numeric_cols_std if c in df_all.columns]
    df_all = handle_outliers_iqr(df_all, numeric_cols_std)

    # --------- 3. STANDARDISASI DATA ---------
    # 3.2 Normalisasi dua kolom numerik
    cols_to_normalize = []
    for original in ["Units Sold", "Total Revenue"]:
        col_std = original.lower().replace(" ", "_")
        if col_std in df_all.columns:
            cols_to_normalize.append(col_std)
    df_all = min_max_scale(df_all, cols_to_normalize)

    # 3.3 Encoding kolom kategorikal (tanpa menghapus kolom dimensi)
    dim_keep = ["region", "country", "item_type", "sales_channel"]
    cat_cols_std = [c.lower().replace(" ", "_") for c in config.categorical_cols]
    cat_cols_std = [
        c for c in cat_cols_std
        if c in df_all.columns and c not in dim_keep
    ]
    df_all = pd.get_dummies(df_all, columns=cat_cols_std, drop_first=True)

    # 3.4 Pastikan tipe data konsisten
    for col in numeric_cols_std:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
    for col in [od_col, sd_col]:
        if col and col in df_all.columns:
            df_all[col] = pd.to_datetime(df_all[col], errors="coerce")

    # --------- 4. DATA ENRICHMENT & FEATURE ENGINEERING ---------
    if "total_profit" in df_all.columns and "units_sold" in df_all.columns:
        df_all["profit_per_unit"] = df_all["total_profit"] / df_all[
            "units_sold"
        ].replace(0, np.nan)

    if "total_revenue" in df_all.columns and "units_sold" in df_all.columns:
        df_all["revenue_per_unit"] = df_all["total_revenue"] / df_all[
            "units_sold"
        ].replace(0, np.nan)

    if "total_revenue" in df_all.columns and "total_profit" in df_all.columns:
        df_all["profit_margin_ratio"] = df_all["total_profit"] / df_all[
            "total_revenue"
        ].replace(0, np.nan)

    if od_col and sd_col and od_col in df_all.columns and sd_col in df_all.columns:
        df_all["shipping_days"] = (df_all[sd_col] - df_all[od_col]).dt.days

    if od_col and od_col in df_all.columns:
        df_all["order_year"] = df_all[od_col].dt.year
        df_all["order_month"] = df_all[od_col].dt.month

    # --------- 5. VALIDASI KUALITAS DATA ---------
    validation_report = run_data_quality_checks(df_all, pk_col, numeric_cols_std)

    print("===== DATA QUALITY REPORT =====")
    for k, v in validation_report.items():
        print(f"{k}: {v}")

    return df_all

