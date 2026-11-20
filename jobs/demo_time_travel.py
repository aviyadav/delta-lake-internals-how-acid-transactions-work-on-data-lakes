"""Demonstration: time travel across the Delta versions of a table."""

from __future__ import annotations

from delta.tables import DeltaTable

from common.lakehouse import TABLES, current_version, get_spark


def show(spark, name: str) -> None:
    spec = TABLES[name]
    latest = current_version(spark, spec.uri)
    print(f"\n== {name} (latest v{latest})")
    print(f"{'version':<9}{'rows':<8}{'statuses'}")
    for version in range(latest + 1):
        frame = spark.read.format("delta").option("versionAsOf", version).load(spec.uri)
        statuses = (
            frame.groupBy("order_status").count().collect() if "order_status" in frame.columns else []
        )
        rendered = ", ".join(f"{row['order_status']}={row['count']}" for row in statuses)
        print(f"{version:<9}{frame.count():<8}{rendered}")

    if latest >= 1:
        old = spark.read.format("delta").option("versionAsOf", 0).load(spec.uri)
        new = spark.read.format("delta").load(spec.uri)
        print(
            f"v0 -> v{latest}: "
            f"{old.select('order_id').subtract(new.select('order_id')).count()} ids removed, "
            f"{new.select('order_id').subtract(old.select('order_id')).count()} ids added"
        )


def main() -> None:
    spark = get_spark("shopazone-time-travel")
    show(spark, "bronze.orders")
    show(spark, "silver.orders")


if __name__ == "__main__":
    main()
