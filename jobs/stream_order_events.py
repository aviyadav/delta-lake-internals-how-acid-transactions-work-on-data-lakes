"""Stage 1b - streaming ingestion.

Kafka -> Spark Structured Streaming -> Bronze Delta table.

The streaming checkpoint lives in object storage next to (but separate from) the
Delta table. The checkpoint tracks Kafka offsets; the Delta log provides
exactly-once table commits for the processed micro-batches.
"""

from __future__ import annotations

import threading
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.lakehouse import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    TABLES,
    ensure_table,
    get_spark,
    lake_uri,
)

CHECKPOINT = lake_uri("checkpoints/bronze_order_events_v2")

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_amount", DecimalType(12, 2)),
        StructField("order_status", StringType()),
        StructField("event_time", TimestampType()),
        StructField("created_at", TimestampType()),
    ]
)


def main() -> None:
    spark = get_spark("shopazone-bronze-events")
    spec = TABLES["bronze.order_events"]
    ensure_table(spark, spec)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select(
            "e.event_id",
            "e.order_id",
            "e.customer_id",
            "e.order_amount",
            "e.order_status",
            "e.event_time",
            "e.created_at",
            "kafka_partition",
            "kafka_offset",
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit("kafka.shopazone.orders.events"))
        .filter(F.col("event_id").isNotNull())
    )

    # Delta is the sink itself: Spark commits each micro-batch into the table and
    # records the Kafka offsets in the checkpoint, which is what makes the write
    # exactly-once across restarts.
    query = (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="15 seconds")
        .start(spec.uri)
    )

    def report() -> None:
        while query.isActive:
            progress = query.lastProgress
            if progress:
                print(
                    f"[stream] batch={progress['batchId']} "
                    f"input_rows={progress['numInputRows']} "
                    f"rate={progress['processedRowsPerSecond']:.1f} rows/s "
                    f"table={spec.name}",
                    flush=True,
                )
            time.sleep(15)

    threading.Thread(target=report, daemon=True).start()

    print(f"[stream] writing {KAFKA_TOPIC} -> {spec.uri}", flush=True)
    print(f"[stream] checkpoint -> {CHECKPOINT}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
