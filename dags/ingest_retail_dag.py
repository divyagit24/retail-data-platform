"""
ingest_retail_dag.py
--------------------
Airflow DAG for the Retail Data Platform ingestion pipeline.
Runs daily at 6AM UTC.

Schedule:
    - Checks source files exist
    - Validates raw data pre-load
    - Loads all tables into Snowflake RAW schema
    - Runs post-load data quality checks
    - Sends alert on success or failure

Usage:
    Place this file in your Airflow dags/ folder.
    Enable the DAG from the Airflow UI at http://localhost:8080
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import os
import sys
import logging

# Add project root to path so we can import our scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.ingest import get_connection, ingest_all, ingest_table
from ingestion.validate import validate_all, validate_table

logger = logging.getLogger(__name__)

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "divya",
    "depends_on_past":  False,
    "email":            [os.getenv("ALERT_EMAIL", "divya.purli@gmail.com")],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5m, 10m on retries
    "execution_timeout": timedelta(hours=2),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="ingest_retail_dag",
    description="Retail Data Platform — daily ingestion pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",       # every day at 6AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                        # don't backfill missed runs
    max_active_runs=1,                    # prevent concurrent runs
    tags=["retail", "ingestion", "snowflake"],
    doc_md="""
## Retail Ingestion DAG

Orchestrates the daily load of Olist e-commerce data into Snowflake.

### Steps
1. **check_source_files** — verify CSV files exist in data/raw/
2. **pre_validate** — sanity check on source file row counts
3. **load_*** — parallel load of all 6 tables into Snowflake RAW
4. **post_validate** — data quality checks on loaded tables
5. **pipeline_success / pipeline_failure** — terminal alert tasks

### SLA
Each run should complete within 60 minutes. Airflow will alert if breached.
    """,
) as dag:

    # ── Task functions ─────────────────────────────────────────────────────────

    def check_source_files(**context):
        """Verify all required CSV files exist before starting the pipeline."""
        data_dir = os.getenv("DATA_DIR", "data/raw")
        required_files = [
            "olist_orders_dataset.csv",
            "olist_order_items_dataset.csv",
            "olist_products_dataset.csv",
            "olist_customers_dataset.csv",
            "olist_order_reviews_dataset.csv",
            "olist_sellers_dataset.csv",
        ]

        missing = []
        for f in required_files:
            path = os.path.join(data_dir, f)
            if not os.path.exists(path):
                missing.append(f)
            else:
                size_mb = os.path.getsize(path) / (1024 * 1024)
                logger.info(f"  ✅ Found: {f} ({size_mb:.1f} MB)")

        if missing:
            raise FileNotFoundError(
                f"Missing source files: {missing}\n"
                f"Expected in: {data_dir}"
            )

        logger.info(f"All {len(required_files)} source files found.")
        # Push file count to XCom for downstream tasks
        context["ti"].xcom_push(key="file_count", value=len(required_files))


    def pre_validate_sources(**context):
        """Quick pre-load validation — check files are non-empty."""
        import pandas as pd
        data_dir = os.getenv("DATA_DIR", "data/raw")

        files = {
            "orders":      "olist_orders_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "products":    "olist_products_dataset.csv",
            "customers":   "olist_customers_dataset.csv",
            "reviews":     "olist_order_reviews_dataset.csv",
            "sellers":     "olist_sellers_dataset.csv",
        }

        summary = {}
        for name, filename in files.items():
            df = pd.read_csv(os.path.join(data_dir, filename), nrows=1)
            full_count = sum(1 for _ in open(os.path.join(data_dir, filename))) - 1
            if full_count == 0:
                raise ValueError(f"Source file is empty: {filename}")
            summary[name] = full_count
            logger.info(f"  {name}: {full_count:,} rows")

        context["ti"].xcom_push(key="source_row_counts", value=summary)
        logger.info("Pre-validation passed. All source files have data.")


    def load_table(table_key: str, **context):
        """Generic loader for a single table — used by all load_* tasks."""
        conn = get_connection()
        try:
            rows = ingest_table(conn, table_key, mode="full")
            context["ti"].xcom_push(key=f"{table_key}_rows_loaded", value=rows)
            logger.info(f"Loaded {rows:,} rows for {table_key}")
        finally:
            conn.close()


    def post_validate_snowflake(**context):
        """Run data quality checks on all Snowflake RAW tables after load."""
        conn = get_connection()
        try:
            passed = validate_all(conn)
            if not passed:
                raise ValueError(
                    "Post-load validation FAILED. Check logs for details. "
                    "Pipeline will not proceed to transformation."
                )
            logger.info("Post-load validation passed.")
        finally:
            conn.close()


    def on_pipeline_success(**context):
        """Log pipeline success summary."""
        ti = context["ti"]
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"DAG run: {context['run_id']}")
        logger.info(f"Execution date: {context['execution_date']}")
        logger.info("=" * 60)

        # Log row counts from XCom
        for table in ["orders", "order_items", "products", "customers", "reviews", "sellers"]:
            rows = ti.xcom_pull(key=f"{table}_rows_loaded", task_ids=f"load_{table}")
            if rows:
                logger.info(f"  {table:<20} {rows:>10,} rows")


    def on_pipeline_failure(**context):
        """Log pipeline failure details."""
        logger.error("=" * 60)
        logger.error("PIPELINE FAILED")
        logger.error(f"DAG run: {context['run_id']}")
        logger.error(f"Failed task: {context.get('task_instance').task_id}")
        logger.error("Check task logs above for details.")
        logger.error("=" * 60)


    # ── Tasks ──────────────────────────────────────────────────────────────────

    start = EmptyOperator(
        task_id="start",
        doc="Pipeline entry point"
    )

    check_files = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
        sla=timedelta(minutes=5),
        doc_md="Verify all 6 source CSV files exist in data/raw/"
    )

    pre_validate = PythonOperator(
        task_id="pre_validate_sources",
        python_callable=pre_validate_sources,
        sla=timedelta(minutes=10),
        doc_md="Check all source files are non-empty before loading"
    )

    # Parallel load tasks — one per table
    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_table,
        op_kwargs={"table_key": "orders"},
        sla=timedelta(minutes=30),
    )

    load_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_table,
        op_kwargs={"table_key": "order_items"},
        sla=timedelta(minutes=30),
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_table,
        op_kwargs={"table_key": "products"},
        sla=timedelta(minutes=15),
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_table,
        op_kwargs={"table_key": "customers"},
        sla=timedelta(minutes=15),
    )

    load_reviews = PythonOperator(
        task_id="load_reviews",
        python_callable=load_table,
        op_kwargs={"table_key": "reviews"},
        sla=timedelta(minutes=20),
    )

    load_sellers = PythonOperator(
        task_id="load_sellers",
        python_callable=load_table,
        op_kwargs={"table_key": "sellers"},
        sla=timedelta(minutes=10),
    )

    post_validate = PythonOperator(
        task_id="post_validate_snowflake",
        python_callable=post_validate_snowflake,
        sla=timedelta(minutes=15),
        doc_md="Run data quality checks on all RAW tables after load"
    )

    pipeline_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=on_pipeline_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    pipeline_failure = PythonOperator(
        task_id="pipeline_failure",
        python_callable=on_pipeline_failure,
        trigger_rule=TriggerRule.ONE_FAILED,   # runs if ANY upstream task fails
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
        doc="Pipeline exit point — always runs"
    )

    # ── DAG dependency graph ───────────────────────────────────────────────────
    #
    #  start
    #    └── check_source_files
    #          └── pre_validate_sources
    #                ├── load_orders ──────┐
    #                ├── load_order_items ─┤
    #                ├── load_products ────┤── post_validate ── pipeline_success ── end
    #                ├── load_customers ───┤                 \_ pipeline_failure ──┘
    #                ├── load_reviews ─────┤
    #                └── load_sellers ─────┘

    start >> check_files >> pre_validate

    # Fan out — all load tasks run in parallel
    load_tasks = [
        load_orders,
        load_order_items,
        load_products,
        load_customers,
        load_reviews,
        load_sellers,
    ]
    pre_validate >> load_tasks >> post_validate

    # Terminal tasks
    post_validate >> [pipeline_success, pipeline_failure] >> end
