"""Shared configuration, Delta table registry and Spark helpers.

Every stage of the pipeline uses these helpers so that the storage layout,
catalog registration and Delta settings are declared in exactly one place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
HMS_URI = os.getenv("HMS_URI", "thrift://hive-metastore:9083")
ORDERS_JDBC_URL = os.getenv("ORDERS_JDBC_URL", "jdbc:postgresql://postgres:5432/orders")
ORDERS_JDBC_USER = os.getenv("ORDERS_JDBC_USER", "orders")
ORDERS_JDBC_PASSWORD = os.getenv("ORDERS_JDBC_PASSWORD", "orders")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shopazone.orders.events")


def lake_uri(path: str) -> str:
    """Map a logical lakehouse path to its S3 URI.

    The `s3://` scheme is served by Hadoop's S3A implementation (fs.s3.impl in
    spark-defaults.conf) so Spark and Trino address the same object paths.
    """
    return f"s3://{BUCKET}/{path.strip('/')}"


@dataclass(frozen=True)
class TableSpec:
    name: str
    path: str
    schema: Optional[StructType] = None
    properties: dict = field(default_factory=dict)

    @property
    def uri(self) -> str:
        return lake_uri(self.path)


BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_amount", DecimalType(12, 2)),
        StructField("order_status", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("_ingest_ts", TimestampType()),
        StructField("_source_system", StringType()),
    ]
)

BRONZE_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_amount", DecimalType(12, 2)),
        StructField("order_status", StringType()),
        StructField("event_time", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("_ingest_ts", TimestampType()),
        StructField("_source_system", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_amount", DecimalType(12, 2)),
        StructField("order_status", StringType()),
        StructField("order_date", DateType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("_event_ts", TimestampType()),
        StructField("_source", StringType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

REJECTED_SCHEMA = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_amount", DecimalType(12, 2)),
        StructField("order_status", StringType()),
        StructField("_event_ts", TimestampType()),
        StructField("_source", StringType()),
        StructField("_reject_reason", StringType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

DQ_SCHEMA = StructType(
    [
        StructField("check_name", StringType()),
        StructField("severity", StringType()),
        StructField("passed", BooleanType()),
        StructField("observed", StringType()),
        StructField("detail", StringType()),
        StructField("checked_at", TimestampType()),
    ]
)

TABLES: dict[str, TableSpec] = {
    "bronze.orders": TableSpec(
        name="bronze.orders",
        path="bronze/orders",
        schema=BRONZE_ORDERS_SCHEMA,
        properties={"delta.checkpointInterval": "5"},
    ),
    "bronze.order_events": TableSpec(
        name="bronze.order_events",
        path="bronze/order_events",
        schema=BRONZE_EVENTS_SCHEMA,
        properties={"delta.checkpointInterval": "5"},
    ),
    "silver.orders": TableSpec(
        name="silver.orders",
        path="silver/orders",
        schema=SILVER_ORDERS_SCHEMA,
        properties={
            "delta.enableChangeDataFeed": "true",
            "delta.checkpointInterval": "5",
        },
    ),
    "silver.orders_rejected": TableSpec(
        name="silver.orders_rejected",
        path="silver/orders_rejected",
        schema=REJECTED_SCHEMA,
    ),
    "gold.gold_customer_orders": TableSpec(
        name="gold.gold_customer_orders", path="gold/gold_customer_orders"
    ),
    "gold.gold_daily_revenue": TableSpec(
        name="gold.gold_daily_revenue", path="gold/gold_daily_revenue"
    ),
    "gold.dq_results": TableSpec(
        name="gold.dq_results", path="gold/dq_results", schema=DQ_SCHEMA
    ),
}


def get_spark(app_name: str, extra_conf: Optional[dict] = None) -> SparkSession:
    """Build a Delta-enabled session. Mirrors docker/spark/spark-defaults.conf
    so the jobs also run from a plain `spark-submit` or a notebook."""
    conf = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.hive.metastore.uris": HMS_URI,
        "spark.sql.warehouse.dir": "/tmp/warehouse",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": AWS_KEY,
        "spark.hadoop.fs.s3a.secret.key": AWS_SECRET,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "8",
    }
    conf.update(extra_conf or {})
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def ensure_database(spark: SparkSession, name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")


def has_delta_log(spark: SparkSession, spec: TableSpec) -> bool:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(f"{spec.uri}/_delta_log")
    return path.getFileSystem(conf).exists(path)


def _register_in_catalog(spark: SparkSession, spec: TableSpec) -> None:
    spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS {spec.name} USING DELTA LOCATION '{spec.uri}'")


def table_location(spark: SparkSession, name: str) -> str:
    """Read the LOCATION recorded for a table in the metastore."""
    for row in spark.sql(f"DESCRIBE EXTENDED {name}").collect():
        if (row["col_name"] or "").strip().lower() == "location":
            return (row["data_type"] or "").strip()
    return ""


def _align_catalog_location(spark: SparkSession, spec: TableSpec) -> None:
    """Point the catalog at the canonical URI when it drifted (metadata only)."""
    location = table_location(spark, spec.name)
    if location and location.rstrip("/") != spec.uri:
        print(f"[bootstrap] re-pointing {spec.name}: {location} -> {spec.uri}")
        spark.sql(f"ALTER TABLE {spec.name} SET LOCATION '{spec.uri}'")


def ensure_table(spark: SparkSession, spec: TableSpec) -> None:
    """Create the Delta log first, then the catalog entry that points at it.

    A catalog entry without a Delta log is a broken table, so a stale one (for
    example left behind by an interrupted first run) is dropped and rebuilt.
    """
    database = spec.name.split(".")[0]
    ensure_database(spark, database)

    if spark.catalog.tableExists(spec.name) and not has_delta_log(spark, spec):
        print(f"[bootstrap] dropping stale catalog entry {spec.name} (no Delta log)")
        spark.sql(f"DROP TABLE {spec.name}")

    if not has_delta_log(spark, spec):
        if spec.schema is None:
            raise ValueError(f"{spec.name} has no schema declared; write data first")
        from delta.tables import DeltaTable

        print(f"[bootstrap] creating Delta table {spec.name} at {spec.uri}")
        builder = DeltaTable.createIfNotExists(spark).location(spec.uri)
        for field_ in spec.schema.fields:
            builder = builder.addColumn(field_.name, field_.dataType, field_.nullable)
        for key, value in spec.properties.items():
            builder = builder.property(key, value)
        builder.execute()

    if not spark.catalog.tableExists(spec.name):
        _register_in_catalog(spark, spec)
    else:
        _align_catalog_location(spark, spec)

    if spec.properties:
        # Only commit a property change when it is actually needed: every ALTER is
        # a new Delta version and would otherwise pollute the transaction log.
        existing = {
            row["key"]: row["value"]
            for row in spark.sql(f"SHOW TBLPROPERTIES {spec.name}").collect()
        }
        pending = {k: v for k, v in spec.properties.items() if existing.get(k) != v}
        if pending:
            rendered = ", ".join(f"'{k}' = '{v}'" for k, v in pending.items())
            spark.sql(f"ALTER TABLE {spec.name} SET TBLPROPERTIES ({rendered})")


def current_version(spark: SparkSession, uri: str) -> int:
    """Latest committed Delta version for a table path."""
    from delta.tables import DeltaTable

    rows = DeltaTable.forPath(spark, uri).history(1).select("version").collect()
    return int(rows[0][0]) if rows else -1


def table_version(spark: SparkSession, spec: TableSpec) -> int:
    return current_version(spark, spec.uri)
