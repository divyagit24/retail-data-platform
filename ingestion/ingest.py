"""
ingest.py
---------
Ingests Olist E-Commerce CSV files into Snowflake RAW schema.
Supports full load and incremental load modes.

Usage:
    python ingest.py --table orders --mode full
    python ingest.py --table all --mode full
"""

import os
import argparse
import logging
from datetime import datetime
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/ingest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":    os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":       os.getenv("SNOWFLAKE_USER"),
    "password":   os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse":  os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
    "database":   os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DB"),
    "schema":     os.getenv("SNOWFLAKE_SCHEMA",   "RAW"),
    "role":       os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN"),
}

DATA_DIR = os.getenv("DATA_DIR", "data/raw")

# Maps logical table name → CSV filename and Snowflake target table
TABLE_MAP = {
    "orders":       ("olist_orders_dataset.csv",          "RAW_ORDERS"),
    "order_items":  ("olist_order_items_dataset.csv",     "RAW_ORDER_ITEMS"),
    "products":     ("olist_products_dataset.csv",        "RAW_PRODUCTS"),
    "customers":    ("olist_customers_dataset.csv",       "RAW_CUSTOMERS"),
    "reviews":      ("olist_order_reviews_dataset.csv",   "RAW_ORDER_REVIEWS"),
    "sellers":      ("olist_sellers_dataset.csv",         "RAW_SELLERS"),
}


# ── Snowflake connection ───────────────────────────────────────────────────────
def get_connection():
    """Create and return a Snowflake connection."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logger.info("Snowflake connection established.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_csv(filename: str) -> pd.DataFrame:
    """Read a CSV file from the data directory into a DataFrame."""
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Source file not found: {filepath}")
    
    logger.info(f"Reading file: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df):,} rows from {filename}")
    return df


def clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Apply common cleaning steps to a DataFrame before loading."""
    # Uppercase column names — Snowflake convention
    df.columns = [col.upper() for col in df.columns]

    # Add audit columns
    df["_INGESTED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["_SOURCE_FILE"] = table_name

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Replace empty strings with None
    df.replace("", None, inplace=True)

    logger.info(f"Cleaned DataFrame: {len(df):,} rows, {len(df.columns)} columns")
    return df


def truncate_table(conn, table_name: str):
    """Truncate a Snowflake table before full load."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
        logger.info(f"Truncated table: {table_name}")
    except Exception as e:
        logger.error(f"Error truncating {table_name}: {e}")
        raise
    finally:
        cursor.close()


def load_to_snowflake(conn, df: pd.DataFrame, table_name: str, mode: str = "full"):
    """
    Load a DataFrame into a Snowflake table.
    mode='full'        → truncate then insert
    mode='incremental' → append only
    """
    if mode == "full":
        truncate_table(conn, table_name)

    logger.info(f"Loading {len(df):,} rows into {table_name} (mode={mode})...")

    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        auto_create_table=True,       # creates table if it doesn't exist
        overwrite=False,
        quote_identifiers=False,
    )

    if success:
        logger.info(f"Successfully loaded {num_rows:,} rows into {table_name} ({num_chunks} chunks)")
    else:
        raise RuntimeError(f"Failed to load data into {table_name}")

    return num_rows


def log_pipeline_run(conn, table_name: str, rows_loaded: int, status: str, error: str = None):
    """Log each pipeline run to a Snowflake audit table."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO RAW.PIPELINE_AUDIT_LOG
                (TABLE_NAME, ROWS_LOADED, STATUS, ERROR_MESSAGE, RUN_TIMESTAMP)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (table_name, rows_loaded, status, error))
        logger.info(f"Audit log written for {table_name}")
    except Exception as e:
        logger.warning(f"Could not write audit log: {e}")
    finally:
        cursor.close()


# ── Main ingestion logic ───────────────────────────────────────────────────────
def ingest_table(conn, table_key: str, mode: str = "full"):
    """Ingest a single table from CSV into Snowflake."""
    if table_key not in TABLE_MAP:
        raise ValueError(f"Unknown table key: '{table_key}'. Valid options: {list(TABLE_MAP.keys())}")

    csv_file, snowflake_table = TABLE_MAP[table_key]
    rows_loaded = 0
    status = "SUCCESS"
    error_msg = None

    try:
        df = read_csv(csv_file)
        df = clean_dataframe(df, table_key)
        rows_loaded = load_to_snowflake(conn, df, snowflake_table, mode)
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        logger.error(f"Ingestion failed for {table_key}: {e}")
        raise
    finally:
        log_pipeline_run(conn, snowflake_table, rows_loaded, status, error_msg)

    return rows_loaded


def ingest_all(conn, mode: str = "full"):
    """Ingest all tables."""
    results = {}
    for table_key in TABLE_MAP:
        try:
            rows = ingest_table(conn, table_key, mode)
            results[table_key] = {"status": "SUCCESS", "rows": rows}
        except Exception as e:
            results[table_key] = {"status": "FAILED", "error": str(e)}

    # Summary
    logger.info("=" * 50)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 50)
    for table, result in results.items():
        if result["status"] == "SUCCESS":
            logger.info(f"  ✅ {table:<20} {result['rows']:>10,} rows loaded")
        else:
            logger.error(f"  ❌ {table:<20} FAILED — {result['error']}")
    logger.info("=" * 50)

    return results


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Retail Data Platform — Ingestion Script")
    parser.add_argument("--table", default="all", help="Table to ingest (or 'all')")
    parser.add_argument("--mode",  default="full", choices=["full", "incremental"],
                        help="Load mode: full (truncate+load) or incremental (append)")
    args = parser.parse_args()

    logger.info(f"Starting ingestion | table={args.table} | mode={args.mode}")

    os.makedirs("logs", exist_ok=True)

    conn = get_connection()
    try:
        if args.table == "all":
            ingest_all(conn, args.mode)
        else:
            ingest_table(conn, args.table, args.mode)
    finally:
        conn.close()
        logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()
