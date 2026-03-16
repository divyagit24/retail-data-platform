from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import snowflake.connector
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "divya",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}


def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def archive_old_orders(**context):
    """Archive orders older than 90 days into ARCHIVE schema."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        archive_sql = """
            INSERT INTO ARCHIVE.ORDERS
            SELECT *, CURRENT_TIMESTAMP() AS archived_at
            FROM RAW.ORDERS
            WHERE order_date < DATEADD(day, -90, CURRENT_DATE())
            AND order_id NOT IN (SELECT order_id FROM ARCHIVE.ORDERS)
        """
        cursor.execute(archive_sql)
        archived = cursor.rowcount
        logger.info(f"Archived {archived} old order records.")

        delete_sql = """
            DELETE FROM RAW.ORDERS
            WHERE order_date < DATEADD(day, -90, CURRENT_DATE())
        """
        cursor.execute(delete_sql)
        deleted = cursor.rowcount
        logger.info(f"Deleted {deleted} records from RAW.ORDERS after archiving.")
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Archive orders failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def archive_old_customers(**context):
    """Archive inactive customers with no orders in last 180 days."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        archive_sql = """
            INSERT INTO ARCHIVE.CUSTOMERS
            SELECT c.*, CURRENT_TIMESTAMP() AS archived_at
            FROM RAW.CUSTOMERS c
            LEFT JOIN RAW.ORDERS o ON c.customer_id = o.customer_id
                AND o.order_date >= DATEADD(day, -180, CURRENT_DATE())
            WHERE o.customer_id IS NULL
            AND c.customer_id NOT IN (SELECT customer_id FROM ARCHIVE.CUSTOMERS)
        """
        cursor.execute(archive_sql)
        archived = cursor.rowcount
        logger.info(f"Archived {archived} inactive customer records.")
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Archive customers failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def vacuum_staging_tables(**context):
    """Truncate staging tables after successful archival."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        tables = ["STAGING.STG_ORDERS", "STAGING.STG_CUSTOMERS", "STAGING.STG_PRODUCTS"]
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            logger.info(f"Truncated {table}")
        conn.commit()
        logger.info("Staging tables vacuumed successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Vacuum staging failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def log_cleanup_summary(**context):
    """Log cleanup run summary to audit table."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO AUDIT.PIPELINE_RUNS (pipeline_name, run_date, status)
            VALUES ('cleanup_retail_dag', CURRENT_TIMESTAMP(), 'SUCCESS')
        """)
        conn.commit()
        logger.info("Cleanup summary logged to audit table.")
    except Exception as e:
        logger.warning(f"Could not log audit entry: {e}")
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="cleanup_retail_dag",
    default_args=default_args,
    description="Archive old retail data and vacuum staging tables",
    schedule_interval="0 2 * * 0",  # Every Sunday at 2am
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["retail", "cleanup", "archival"],
) as dag:

    start = EmptyOperator(task_id="start")

    archive_orders = PythonOperator(
        task_id="archive_old_orders",
        python_callable=archive_old_orders,
    )

    archive_customers = PythonOperator(
        task_id="archive_old_customers",
        python_callable=archive_old_customers,
    )

    vacuum_staging = PythonOperator(
        task_id="vacuum_staging_tables",
        python_callable=vacuum_staging_tables,
    )

    log_summary = PythonOperator(
        task_id="log_cleanup_summary",
        python_callable=log_cleanup_summary,
    )

    end = EmptyOperator(task_id="end")

    start >> [archive_orders, archive_customers] >> vacuum_staging >> log_summary >> end
