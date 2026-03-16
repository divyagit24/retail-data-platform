import os
import logging
import pandas as pd
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/ingest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        logger.info("Snowflake connection established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def load_csv(filepath: str) -> pd.DataFrame:
    """Load CSV file into a DataFrame with error handling."""
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Loaded {len(df)} records from {filepath}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file {filepath}: {e}")
        raise


def validate_dataframe(df: pd.DataFrame, required_columns: list) -> bool:
    """Validate that required columns exist and no nulls in key fields."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        return False
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"Null values detected:\n{null_counts[null_counts > 0]}")
    logger.info("DataFrame validation passed.")
    return True


def ingest_to_snowflake(df: pd.DataFrame, table_name: str, conn) -> int:
    """Ingest DataFrame records into Snowflake table."""
    success_count = 0
    cursor = conn.cursor()
    try:
        for _, row in df.iterrows():
            try:
                placeholders = ", ".join(["%s"] * len(row))
                columns = ", ".join(row.index.tolist())
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row))
                success_count += 1
            except Exception as row_error:
                logger.warning(f"Skipping row due to error: {row_error}")
                continue
        conn.commit()
        logger.info(f"Successfully ingested {success_count}/{len(df)} records into {table_name}.")
    except Exception as e:
        logger.error(f"Ingestion failed for table {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
    return success_count


def run_ingestion(filepath: str, table_name: str, required_columns: list):
    """Main ingestion orchestrator."""
    logger.info(f"Starting ingestion pipeline — {datetime.now().isoformat()}")
    conn = None
    try:
        df = load_csv(filepath)
        if not validate_dataframe(df, required_columns):
            raise ValueError("DataFrame validation failed. Aborting ingestion.")
        conn = get_snowflake_connection()
        count = ingest_to_snowflake(df, table_name, conn)
        logger.info(f"Ingestion complete. {count} records loaded into {table_name}.")
    except Exception as e:
        logger.error(f"Ingestion pipeline failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    run_ingestion(
        filepath="data/orders.csv",
        table_name="RAW.ORDERS",
        required_columns=["order_id", "customer_id", "order_date", "amount"]
    )
