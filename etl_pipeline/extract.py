# etl_pipeline/extract.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import time
import os
import pandas as pd

# ----- PATH DASAR -----
BASE_DIR = Path(__file__).resolve().parents[1]      # ke bigdata_final_project/
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "logs"
RAW_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ----- LOGGING SETUP -----
LOG_FILE = LOG_DIR / "etl_extract.log"

logger = logging.getLogger("etl_extract")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def extract_etl_source1(
    filename: str = "100000_Sales_Records_Raw.csv",
):
    """
    Extract dari file CSV lokal di folder raw.
    Hanya baca ke DataFrame dan catat log (tanpa cleaning/transform).
    """
    start = time.time()

    file_path = RAW_DIR / filename
    logger.info(f"Start EXTRACT source1_local | file={file_path}")

    # baca data mentah ke DataFrame
    df = pd.read_csv(file_path)

    # hitung metadata
    rows, cols = df.shape
    size_bytes = os.path.getsize(file_path)
    size_mb = round(size_bytes / (1024 * 1024), 2)
    elapsed = round(time.time() - start, 3)

    logger.info(
        f"EXTRACT summary | source=source1_local | "
        f"file={file_path.name} | rows={rows} | cols={cols} | "
        f"size_mb={size_mb} | exec_sec={elapsed}"
    )

    return df
