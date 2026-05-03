"""
ecommerce_pipeline_dag.py
--------------------------
Airflow DAG that orchestrates the three pipeline stages:

  [ingest_raw_data] → [transform_data] → [load_warehouse] → [run_analytics_check]

Place this file in your $AIRFLOW_HOME/dags/ directory.

Prerequisites:
  pip install apache-airflow apache-airflow-providers-postgres
  export AIRFLOW_HOME=~/airflow
  airflow db init
  airflow users create --role Admin --username admin ...
  airflow webserver & airflow scheduler
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator

# ── Allow imports from project root when Airflow picks up this DAG ──────────
import sys
PROJECT_ROOT = str(Path(__file__).parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.settings import get_config

cfg = get_config()

# ─────────────────────────────────────────────────────────────────────────────
# Default task arguments
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner"           : "data_engineering",
    "depends_on_past" : False,
    "start_date"      : datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : cfg["airflow"]["retries"],
    "retry_delay"     : timedelta(minutes=cfg["airflow"]["retry_delay_minutes"]),
}


# ─────────────────────────────────────────────────────────────────────────────
# Callable task functions
# ─────────────────────────────────────────────────────────────────────────────

def task_ingest(**context):
    """Trigger the producer to write a new batch to the data lake."""
    from ingestion.producer import produce
    produce()


def task_transform(**context):
    """Run the transformation pipeline on today's raw data."""
    from processing.transform import run_transformation
    run_transformation()


def task_load(**context):
    """Load processed data into the PostgreSQL data warehouse."""
    from warehouse.loader import run_load
    run_load()


def task_analytics_check(**context):
    """
    Lightweight data-quality check: assert row counts > 0 in each table.
    Extend this with Great Expectations for production-grade validation.
    """
    from sqlalchemy import create_engine, text
    from config.settings import get_config

    cfg    = get_config()
    db     = cfg["database"]
    engine = create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )
    checks = [
        "SELECT COUNT(*) FROM dw.dim_category",
        "SELECT COUNT(*) FROM dw.dim_product",
        "SELECT COUNT(*) FROM dw.fact_product_listing",
    ]
    with engine.connect() as conn:
        for q in checks:
            result = conn.execute(text(q)).scalar()
            assert result > 0, f"Quality check FAILED — zero rows: {q}"
    print("All data-quality checks passed ✓")


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id          = cfg["airflow"]["dag_id"],
    default_args    = default_args,
    description     = "E-Commerce end-to-end data pipeline",
    schedule_interval = cfg["airflow"]["schedule_interval"],
    catchup         = False,
    tags            = ["ecommerce", "data_engineering", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest = PythonOperator(
        task_id         = "ingest_raw_data",
        python_callable = task_ingest,
    )

    transform = PythonOperator(
        task_id         = "transform_data",
        python_callable = task_transform,
    )

    load = PythonOperator(
        task_id         = "load_warehouse",
        python_callable = task_load,
    )

    quality = PythonOperator(
        task_id         = "run_analytics_check",
        python_callable = task_analytics_check,
    )

    end = EmptyOperator(task_id="end")

    # ── Task dependencies ────────────────────────────────────────────────
    start >> ingest >> transform >> load >> quality >> end
