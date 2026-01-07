# etl_pipeline/extract_api.py

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import time
import os
import zipfile
import pandas as pd

# 0) Set kredensial Kaggle via environment
os.environ["KAGGLE_USERNAME"] = "darrylsatria"
os.environ["KAGGLE_KEY"] = "KGAT_cddd433d10496ac9a6d08d2f9602bd62"

import kaggle


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "logs"
RAW_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "etl_extract_api.log"

logger = logging.getLogger("etl_extract_api")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def extract_etl_source2(
    kaggle_dataset: str = "okhiriadaveoseghale/100000-sales-records",
):
    """
    EXTRACT sumber ke-2 (API Kaggle):
    - Set credential Kaggle via environment (tanpa file kaggle.json)
    - Download dataset (zip) ke raw/
    - Unzip, baca CSV ke DataFrame
    """
    start_time = time.time()
    logger.info(f"Start EXTRACT source2_kaggle | dataset={kaggle_dataset}")

    # 1) Auth ke Kaggle API
    kaggle.api.authenticate()

    # 2) Download semua file dataset sebagai zip ke raw/
    kaggle.api.dataset_download_files(
        kaggle_dataset,
        path=str(RAW_DIR),
        force=True,
    )

    # 3) Ambil file zip yang baru ke-download
    zip_files = list(RAW_DIR.glob("*.zip"))
    if not zip_files:
        raise FileNotFoundError("Zip hasil download Kaggle tidak ditemukan di raw/")
    zip_path = zip_files[0]

    # 4) Extract isi zip ke raw/
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(RAW_DIR)

    zip_path.unlink()  # opsional: hapus zip

    # 5) Cari file CSV di raw/
    csv_files = list(RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("Tidak ada file CSV ditemukan di folder raw/")
    csv_path = csv_files[0]

    # 6) Baca CSV mentah ke DataFrame
    df = pd.read_csv(csv_path)

    # 7) Logging metadata
    n_rows, n_cols = df.shape
    file_size_bytes = os.path.getsize(csv_path)
    file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
    elapsed = round(time.time() - start_time, 3)

    logger.info(
        f"EXTRACT summary | source=source2_kaggle_api | "
        f"dataset={kaggle_dataset} | file={csv_path.name} | "
        f"rows={n_rows} | cols={n_cols} | "
        f"file_size_mb={file_size_mb} | exec_time_sec={elapsed}"
    )

    return df

