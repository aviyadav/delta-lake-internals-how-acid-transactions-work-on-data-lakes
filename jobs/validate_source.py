"""Stage 0 - validate the source before it is allowed into Bronze.

This is the article's "Validate Source Data" step: structural problems (no rows,
null keys, unparseable timestamps) block the run. Business defects (negative
amounts, unknown statuses) are reported and deliberately allowed through, so the
downstream quality gate has something real to quarantine.
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from common.lakehouse import ORDERS_JDBC_PASSWORD, ORDERS_JDBC_URL, ORDERS_JDBC_USER, get_spark

BLOCKING_THRESHOLD = 0


def main() -> None:
    spark = get_spark("shopazone-validate-source")
    orders = (
        spark.read.format("jdbc")
        .option("url", ORDERS_JDBC_URL)
        .option("user", ORDERS_JDBC_USER)
        .option("password", ORDERS_JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "orders")
        .load()
        .cache()
    )

    total = orders.count()
    checks = [
        ("source_has_rows", total > 0, total, "blocking"),
        (
            "order_id_not_null",
            orders.filter(F.col("order_id").isNull()).count() == 0,
            orders.filter(F.col("order_id").isNull()).count(),
            "blocking",
        ),
        (
            "timestamps_parse",
            orders.filter(F.col("updated_at").isNull()).count() == 0,
            orders.filter(F.col("updated_at").isNull()).count(),
            "blocking",
        ),
        (
            "duplicate_order_ids",
            orders.groupBy("order_id").count().filter("count = 1").count()
            == orders.select("order_id").distinct().count(),
            orders.groupBy("order_id").count().filter("count > 1").count(),
            "warning",
        ),
        (
            "negative_amounts",
            orders.filter(F.col("order_amount") < 0).count() == 0,
            orders.filter(F.col("order_amount") < 0).count(),
            "warning",
        ),
        (
            "unknown_statuses",
            orders.filter(
                ~F.upper(F.trim(F.col("order_status"))).isin(
                    "CREATED", "PAID", "COMPLETED", "CANCELLED", "REFUNDED"
                )
            ).count()
            == 0,
            orders.filter(
                ~F.upper(F.trim(F.col("order_status"))).isin(
                    "CREATED", "PAID", "COMPLETED", "CANCELLED", "REFUNDED"
                )
            ).count(),
            "warning",
        ),
    ]

    print(f"{'check':<22}{'severity':<10}{'result':<8}observed")
    print("-" * 62)
    blocking_failures = 0
    for name, passed, observed, severity in checks:
        print(f"{name:<22}{severity:<10}{'PASS' if passed else 'WARN':<8}{observed}")
        if not passed and severity == "blocking":
            blocking_failures += 1

    if blocking_failures > BLOCKING_THRESHOLD:
        sys.exit(f"source validation failed with {blocking_failures} blocking issue(s)")

    print("\n[validate] source is structurally usable; business defects are quarantined in Silver")


if __name__ == "__main__":
    main()
