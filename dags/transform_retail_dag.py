"""
transform_retail_dag.py
-----------------------
Airflow DAG that triggers Databricks transformation notebooks after ingestion.
Runs after ingest_retail_dag completes successfully.

Notebooks triggered (in order):
    1. 01_staging.py      — RAW → STAGING
    2. 02_analytics.py    — STAGING → ANALYTICS
    3. 03_data_quality.py — Analytics quality checks

Note: Uses DatabricksRunNowOperator for Databricks Community Edition.
      In production, replace with DatabricksSubmitRunOperator for job clusters.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
import subprocess
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "divya",
    "depends_on_past":  False,
    "email":            [os.getenv("ALERT_EMAIL", "divya.purli@gmail.com")],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}


# ── Simulate Databricks run (for local/Community Edition) ─────────────────────
# In a real Databricks workspace, replace these with:
#   from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
#
# Example production task:
#   run_staging = DatabricksRunNowOperator(
#       task_id="run_staging_notebook",
#       databricks_conn_id="databricks_default",
#       job_id=12345,   # Your Databricks job ID
#   )

def run_notebook_locally(notebook_path: str, **context):
    """
    Run a Databricks-style Python notebook locally.
    In production Databricks, replace with DatabricksRunNowOperator.
    """
    logger.info(f"Running notebook: {notebook_path}")
    result = subprocess.run(
        ["python", notebook_path],
        capture_output=True,
        text=True,
        env={**os.environ}
    )

    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info(f"[notebook] {line}")

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(
            f"Notebook failed: {notebook_path}\n"
            f"Error: {result.stderr[-500:]}"
        )

    logger.info(f"Notebook completed successfully: {notebook_path}")
    context["ti"].xcom_push(key=f"{os.path.basename(notebook_path)}_status", value="SUCCESS")


with DAG(
    dag_id="transform_retail_dag",
    description="Retail Data Platform — Databricks transformation pipeline",
    default_args=default_args,
    schedule_interval="0 8 * * *",      # runs at 8AM UTC — 2hrs after ingestion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "transformation", "databricks"],
    doc_md="""
## Retail Transform DAG

Runs Databricks notebooks to transform RAW data into analytics-ready tables.

### Dependencies
Waits for `ingest_retail_dag` to complete before starting.

### Steps
1. **wait_for_ingestion** — sensor waits for ingest DAG success
2. **run_staging** — executes 01_staging.py (RAW → STAGING)
3. **run_analytics** — executes 02_analytics.py (STAGING → ANALYTICS)
4. **run_data_quality** — executes 03_data_quality.py (quality checks)
5. **pipeline_success / pipeline_failure** — terminal tasks
    """,
) as dag:

    NOTEBOOKS_DIR = os.path.join(os.path.dirname(__file__), "..", "transformations")

    start = EmptyOperator(task_id="start")

    # Wait for ingestion DAG to finish before transforming
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="ingest_retail_dag",
        external_task_id="end",
        allowed_states=["success"],
        timeout=7200,               # wait up to 2 hours
        poke_interval=60,           # check every 60 seconds
        mode="reschedule",          # free up worker slot while waiting
        doc_md="Wait for ingest_retail_dag to complete before transforming"
    )

    run_staging = PythonOperator(
        task_id="run_staging_notebook",
        python_callable=run_notebook_locally,
        op_kwargs={"notebook_path": os.path.join(NOTEBOOKS_DIR, "01_staging.py")},
        sla=timedelta(minutes=60),
        doc_md="Execute 01_staging.py — transforms RAW → STAGING in Databricks"
    )

    run_analytics = PythonOperator(
        task_id="run_analytics_notebook",
        python_callable=run_notebook_locally,
        op_kwargs={"notebook_path": os.path.join(NOTEBOOKS_DIR, "02_analytics.py")},
        sla=timedelta(minutes=60),
        doc_md="Execute 02_analytics.py — computes business metrics in Databricks"
    )

    run_data_quality = PythonOperator(
        task_id="run_data_quality_notebook",
        python_callable=run_notebook_locally,
        op_kwargs={"notebook_path": os.path.join(NOTEBOOKS_DIR, "03_data_quality.py")},
        sla=timedelta(minutes=20),
        doc_md="Execute 03_data_quality.py — validates analytics table outputs"
    )

    def on_success(**context):
        logger.info("="*60)
        logger.info("TRANSFORM PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Run ID: {context['run_id']}")
        logger.info("Analytics tables ready in Snowflake ANALYTICS schema:")
        logger.info("  - DAILY_REVENUE")
        logger.info("  - TOP_PRODUCTS")
        logger.info("  - CUSTOMER_RETENTION")
        logger.info("  - SELLER_PERFORMANCE")
        logger.info("  - CATEGORY_REVENUE")
        logger.info("  - ORDER_FULFILLMENT")
        logger.info("="*60)

    def on_failure(**context):
        logger.error("="*60)
        logger.error("TRANSFORM PIPELINE FAILED")
        logger.error(f"Run ID: {context['run_id']}")
        logger.error(f"Failed task: {context.get('task_instance').task_id}")
        logger.error("Check task logs for details.")
        logger.error("="*60)

    pipeline_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=on_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    pipeline_failure = PythonOperator(
        task_id="pipeline_failure",
        python_callable=on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DAG graph ──────────────────────────────────────────────────────────────
    #
    #  start → wait_for_ingestion → run_staging → run_analytics
    #                                                  └── run_data_quality
    #                                                          ├── pipeline_success
    #                                                          └── pipeline_failure
    #                                                                    └── end

    (
        start
        >> wait_for_ingestion
        >> run_staging
        >> run_analytics
        >> run_data_quality
        >> [pipeline_success, pipeline_failure]
        >> end
    )
