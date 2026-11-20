"""Stage 1a - batch ingestion.

Reads the operational Postgres `orders` table over JDBC and appends it as a new
Delta commit in the Bronze layer. Nothing is cleaned here: Bronze is the raw,
replayable record of what the source system said.
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, TimestampType

from common.lakehouse import (
    ORDERS_JDBC_PASSWORD,
    ORDERS_JDBC_URL,
    ORDERS_JDBC_USER,
    TABLES,
    ensure_table,
    get_spark,
    table_version,
)


def main() -> None:
    spark = get_spark("shopazone-bronze-orders")
    spec = TABLES["bronze.orders"]

    source = (
        spark.read.format("jdbc")
        .option("url", ORDERS_JDBC_URL)
        .option("user", ORDERS_JDBC_USER)
        .option("password", ORDERS_JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option(
            "query",
            "SELECT order_id, customer_id, order_amount, order_status, "
            "created_at, updated_at FROM orders",
        )
        .load()
    )

    batch = source.select(
        F.col("order_id").cast(LongType()),
        F.col("customer_id").cast(LongType()),
        F.col("order_amount").cast(DecimalType(12, 2)),
        F.col("order_status").cast(StringType()),
        F.col("created_at").cast(TimestampType()),
        F.col("updated_at").cast(TimestampType()),
        F.current_timestamp().alias("_ingest_ts"),
        F.lit("postgres.orders").alias("_source_system"),
    )

    ensure_table(spark, spec)
    before = table_version(spark, spec)
    batch.write.format("delta").mode("append").save(spec.uri)
    after = table_version(spark, spec)

    print(f"[ingest] rows read from Postgres : {batch.count()}")
    print(f"[ingest] Delta version           : {before} -> {after}")
    print(f"[ingest] table location          : {spec.uri}")
    print("[ingest] the commit is visible only after 0000...{}.json lands".format(after))

    if after <= before:
        sys.exit("ingest did not create a new Delta version")


if __name__ == "__main__":
    main()
