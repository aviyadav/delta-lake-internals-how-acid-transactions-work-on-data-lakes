"""Register Delta directories in the Hive Metastore.

Spark jobs write by path (so a crashed writer can never leave a half-created
catalog entry). This step is what makes those directories visible as tables to
Spark SQL, dbt and Trino - the catalog stores the pointer, the Delta log stores
the truth.

The step is also self-healing: the local S3 emulator does not survive a container
recreation, so a catalog entry can outlive the data it points at. When the Delta
log is gone the entry is dropped (there is no table to protect) and, if a log
exists at the canonical location, re-registered there.
"""

from __future__ import annotations

from common.lakehouse import (
    TABLES,
    ensure_database,
    get_spark,
    has_delta_log,
    table_location,
)

REQUIRED = [
    "bronze.orders",
    "bronze.order_events",
    "silver.orders",
    "silver.orders_rejected",
    "gold.gold_customer_orders",
    "gold.gold_daily_revenue",
]


def register_one(spark, name: str) -> None:
    spec = TABLES[name]

    if spark.catalog.tableExists(name):
        try:
            location = table_location(spark, name)
        except Exception:  # entry points at a path Trino/Spark cannot resolve
            location = ""

        if has_delta_log(spark, spec):
            if location.rstrip("/") == spec.uri:
                print(f"[catalog] exists    {name:<28} -> {location}")
            else:
                spark.sql(f"ALTER TABLE {name} SET LOCATION '{spec.uri}'")
                print(f"[catalog] re-pointed {name:<27} -> {spec.uri}")
            return

        # No Delta log at the canonical path: the data is gone (ephemeral S3).
        print(f"[catalog] repairing {name:<27} (no Delta log at {spec.uri})")
        spark.sql(f"DROP TABLE {name}")

    if not has_delta_log(spark, spec):
        # Nothing has written this table yet (e.g. dbt runs after this task).
        print(f"[catalog] pending   {name:<28} (no Delta log yet)")
        return

    spark.sql(f"CREATE EXTERNAL TABLE {name} USING DELTA LOCATION '{spec.uri}'")
    print(f"[catalog] created   {name:<28} -> {spec.uri}")


def main() -> None:
    spark = get_spark("shopazone-register-catalog")
    for database in ("bronze", "silver", "gold"):
        ensure_database(spark, database)

    for name in REQUIRED:
        register_one(spark, name)

    for database in ("bronze", "silver", "gold"):
        print(f"\n[catalog] SHOW TABLES IN {database}")
        spark.sql(f"SHOW TABLES IN {database}").select("tableName", "isTemporary").show(
            truncate=False
        )


if __name__ == "__main__":
    main()
