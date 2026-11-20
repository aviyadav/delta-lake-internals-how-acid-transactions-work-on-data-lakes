"""On-demand Delta maintenance: compact files, force a checkpoint, vacuum."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="lakehouse_maintenance",
    description="OPTIMIZE, checkpoint and VACUUM the Delta tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "delta", "maintenance"],
    doc_md=__doc__,
) as dag:
    compact = SparkSubmitOperator(
        task_id="optimize_and_checkpoint",
        conn_id="spark_default",
        application="/opt/jobs/maintenance.py",
        name="shopazone-maintenance-optimize",
        deploy_mode="client",
        application_args=["--optimize", "--checkpoint", "--tables", "silver.orders"],
        verbose=False,
    )

    vacuum = SparkSubmitOperator(
        task_id="vacuum",
        conn_id="spark_default",
        application="/opt/jobs/maintenance.py",
        name="shopazone-maintenance-vacuum",
        deploy_mode="client",
        application_args=["--vacuum-hours", "0", "--tables", "silver.orders"],
        verbose=False,
    )

    compact >> vacuum
