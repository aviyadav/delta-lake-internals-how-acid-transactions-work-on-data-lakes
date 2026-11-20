"""Shop-A-Zone lakehouse pipeline.

    validate -> bronze -> silver -> catalog -> dbt gold -> dbt tests
             -> quality gate -> publish + verify through Trino

Every task is idempotent enough to re-run: Bronze appends a new snapshot,
Silver MERGEs on order_id, Gold rebuilds deterministically, and the catalog
registration is CREATE TABLE IF NOT EXISTS.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.hooks.trino import TrinoHook

JOBS = "/opt/jobs"
DBT_DIR = "/opt/dbt"
DBT_BIN = "/opt/dbt-venv/bin/dbt"

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def spark_step(task_id: str, script: str, args: list[str] | None = None):
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"{JOBS}/{script}",
        name=f"shopazone-{task_id}",
        deploy_mode="client",
        application_args=args or [],
        conf={"spark.cores.max": "2"},
        verbose=False,
    )


def verify_through_trino(**context) -> None:
    """Read the same Delta files with a different engine - the cross-engine claim."""
    hook = TrinoHook(trino_conn_id="trino_default")
    checks = {
        "silver.orders": "select count(*) from delta.silver.orders",
        "gold.gold_customer_orders": "select count(*) from delta.gold.gold_customer_orders",
        "gold.gold_daily_revenue": "select count(*) from delta.gold.gold_daily_revenue",
    }
    counts = {}
    for label, sql in checks.items():
        rows = list(hook.get_records(sql))
        counts[label] = int(rows[0][0])
        print(f"[trino] {label:<28} rows={counts[label]}")

    if not counts["silver.orders"]:
        raise ValueError("Trino sees no rows in delta.silver.orders")

    top = list(
        hook.get_records(
            "select order_date, total_orders, round(realized_revenue, 2) "
            "from delta.gold.gold_daily_revenue order by order_date desc limit 5"
        )
    )
    print("[trino] latest revenue days:")
    for row in top:
        print(f"[trino]   {row}")

    context["ti"].xcom_push(key="trino_counts", value=counts)


with DAG(
    dag_id="shopazone_lakehouse",
    description="Batch + streaming Delta pipeline with dbt, quality gate and Trino verification",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "delta", "shopazone"],
    doc_md=__doc__,
) as dag:
    validate_source = spark_step("validate_source", "validate_source.py")

    ingest_bronze = spark_step("ingest_bronze", "ingest_batch_orders.py")

    build_silver = spark_step("build_silver", "build_silver.py")

    register_catalog = spark_step("register_catalog", "register_tables.py")

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"cd {DBT_DIR} && {DBT_BIN} run --profiles-dir {DBT_DIR}/profiles "
            "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs "
            "--select gold --target lakehouse"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && {DBT_BIN} test --profiles-dir {DBT_DIR}/profiles "
            "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs "
            "--select gold --target lakehouse"
        ),
    )

    quality_gate = spark_step("quality_gate", "quality_checks.py")

    publish_trino = PythonOperator(
        task_id="publish_verify_trino",
        python_callable=verify_through_trino,
    )

    validate_source >> ingest_bronze >> build_silver >> register_catalog
    register_catalog >> dbt_run_gold >> dbt_test >> quality_gate >> publish_trino
