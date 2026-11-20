"""Stage 2 - Bronze -> Silver.

Conforms batch (Postgres) and streaming (Kafka) records into one current-state
table, quarantines business-invalid rows, and applies the result with MERGE.
Silver is created with Change Data Feed enabled so downstream readers can see
row-level changes per Delta version.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.lakehouse import TABLES, ensure_table, get_spark, table_version

VALID_STATUSES = ("CREATED", "PAID", "COMPLETED", "CANCELLED", "REFUNDED")

SILVER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_amount",
    "order_status",
    "order_date",
    "created_at",
    "updated_at",
    "_event_ts",
    "_source",
    "_ingested_at",
]


def _from_batch(batch: DataFrame) -> DataFrame:
    return batch.select(
        F.col("order_id").cast("long"),
        F.col("customer_id").cast("long"),
        F.col("order_amount").cast("decimal(12,2)"),
        F.upper(F.trim(F.col("order_status"))).alias("order_status"),
        F.to_date("created_at").alias("order_date"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("updated_at").alias("_event_ts"),
        F.col("_source_system").alias("_source"),
        F.col("_ingest_ts").alias("_ingested_at"),
    )


def _from_events(events: DataFrame) -> DataFrame:
    return events.select(
        F.col("order_id").cast("long"),
        F.col("customer_id").cast("long"),
        F.col("order_amount").cast("decimal(12,2)"),
        F.upper(F.trim(F.col("order_status"))).alias("order_status"),
        F.to_date("created_at").alias("order_date"),
        F.col("created_at"),
        F.col("event_time").alias("updated_at"),
        F.col("event_time").alias("_event_ts"),
        F.col("_source_system").alias("_source"),
        F.col("_ingest_ts").alias("_ingested_at"),
    )


def main() -> None:
    spark = get_spark("shopazone-silver-orders")
    silver = TABLES["silver.orders"]
    rejected = TABLES["silver.orders_rejected"]
    ensure_table(spark, silver)
    ensure_table(spark, rejected)

    batch = spark.read.format("delta").load(TABLES["bronze.orders"].uri)
    events = spark.read.format("delta").load(TABLES["bronze.order_events"].uri)

    combined = _from_batch(batch).unionByName(_from_events(events))

    # One row per order: the newest event wins. Late/duplicate source rows collapse here.
    latest = (
        combined.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("order_id").orderBy(
                    F.col("_event_ts").desc_nulls_last(), F.col("_ingested_at").desc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    reject_reason = (
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id"))
        .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
        .when(
            F.col("order_amount").isNull() | (F.col("order_amount") <= 0),
            F.lit("non_positive_amount"),
        )
        .when(~F.col("order_status").isin(*VALID_STATUSES), F.lit("unknown_order_status"))
    )

    annotated = latest.withColumn("_reject_reason", reject_reason)
    bad = annotated.filter(F.col("_reject_reason").isNotNull())
    clean = annotated.filter(F.col("_reject_reason").isNull()).select(*SILVER_COLUMNS)

    bad_rows = bad.count()
    new_rejections = 0
    if bad_rows:
        quarantined = bad.select(
            "order_id",
            "customer_id",
            "order_amount",
            "order_status",
            "_event_ts",
            "_source",
            "_reject_reason",
            "_ingested_at",
        )
        # Quarantine is an append-only audit log, but re-running the pipeline must
        # not multiply the same rejection: only unseen (order_id, reason) pairs
        # are appended.
        seen = spark.read.format("delta").load(rejected.uri).select(
            "order_id", "_reject_reason"
        )
        fresh = quarantined.join(seen, ["order_id", "_reject_reason"], "left_anti")
        new_rejections = fresh.count()
        if new_rejections:
            fresh.write.format("delta").mode("append").save(rejected.uri)

    before = table_version(spark, silver)

    target = DeltaTable.forPath(spark, silver.uri)
    (
        target.alias("t")
        .merge(clean.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s._event_ts > t._event_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )

    after = table_version(spark, silver)
    current = spark.read.format("delta").load(silver.uri)

    print(f"[silver] quarantined rows      : {bad_rows} (new: {new_rejections}) -> {rejected.uri}")
    print(f"[silver] silver version        : {before} -> {after}")
    print(f"[silver] current orders        : {current.count()}")
    print(f"[silver] distinct order ids    : {current.select('order_id').distinct().count()}")

    metrics = (
        DeltaTable.forPath(spark, silver.uri)
        .history(1)
        .select("version", "operation", "operationMetrics")
        .collect()[0]
    )
    print(f"[silver] last commit           : v{metrics['version']} {metrics['operation']}")
    print(f"[silver] merge metrics         : {metrics['operationMetrics']}")

    print("\n[silver] Change Data Feed for the last commit:")
    spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", after) \
        .load(silver.uri) \
        .select("order_id", "order_status", "_change_type", "_commit_version") \
        .orderBy("order_id") \
        .show(10, truncate=False)


if __name__ == "__main__":
    main()
