"""Stage 5 - the data-quality gate.

Delta guarantees a transaction is atomic, not that its contents are correct.
These checks run after dbt and fail the pipeline on business-level defects.
Results are persisted to `gold.dq_results` so quality is observable over time.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Optional

from common.lakehouse import TABLES, ensure_table, get_spark


@dataclass
class Rule:
    name: str
    severity: str
    sql: str
    max_value: Optional[float] = None
    min_value: Optional[float] = None
    detail: str = ""


STATUS_LIST = "'CREATED','PAID','COMPLETED','CANCELLED','REFUNDED'"

RULES = [
    Rule(
        "silver_order_id_unique",
        "blocking",
        "SELECT count(*) AS n FROM (SELECT order_id FROM silver.orders "
        "GROUP BY order_id HAVING count(*) > 1)",
        max_value=0,
        detail="one current row per order_id",
    ),
    Rule(
        "silver_customer_present",
        "blocking",
        "SELECT count(*) AS n FROM silver.orders WHERE customer_id IS NULL",
        max_value=0,
    ),
    Rule(
        "silver_amount_positive",
        "blocking",
        "SELECT count(*) AS n FROM silver.orders WHERE order_amount <= 0",
        max_value=0,
    ),
    Rule(
        "silver_status_in_domain",
        "blocking",
        f"SELECT count(*) AS n FROM silver.orders WHERE order_status NOT IN ({STATUS_LIST})",
        max_value=0,
    ),
    Rule(
        "silver_has_rows",
        "blocking",
        "SELECT count(*) AS n FROM silver.orders",
        min_value=1,
    ),
    Rule(
        "silver_no_future_orders",
        "warning",
        "SELECT count(*) AS n FROM silver.orders WHERE order_date > current_date()",
        max_value=0,
    ),
    Rule(
        "gold_customer_reconciliation",
        "blocking",
        "SELECT abs((SELECT count(DISTINCT customer_id) FROM silver.orders) - "
        "(SELECT count(*) FROM gold.gold_customer_orders)) AS n",
        max_value=0,
        detail="every silver customer appears exactly once in Gold",
    ),
    Rule(
        "gold_revenue_reconciliation",
        "blocking",
        "SELECT abs((SELECT coalesce(sum(order_amount),0) FROM silver.orders "
        "WHERE order_status = 'COMPLETED') - "
        "(SELECT coalesce(sum(realized_revenue),0) FROM gold.gold_daily_revenue)) AS n",
        max_value=0.01,
        detail="Gold revenue equals Silver COMPLETED revenue",
    ),
    Rule(
        "bronze_ingest_fresh",
        "warning",
        "SELECT coalesce(max(age_hours), 0) AS n FROM ("
        "SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(_ingest_ts)) / 3600.0 "
        "AS age_hours FROM bronze.orders)",
        max_value=24,
        detail="latest Bronze ingest is under 24h old",
    ),
    Rule(
        "quarantine_visible",
        "info",
        "SELECT count(*) AS n FROM silver.orders_rejected",
        min_value=1,
        detail="invalid source rows are captured, not silently dropped",
    ),
]


def main() -> None:
    spark = get_spark("shopazone-quality-gate")
    results_spec = TABLES["gold.dq_results"]
    ensure_table(spark, results_spec)

    rows = []
    failed_blocking = 0
    print(f"{'check':<32}{'severity':<10}{'result':<8}{'observed':<12}rule")
    print("-" * 100)

    for rule in RULES:
        observed = float(spark.sql(rule.sql).collect()[0][0] or 0)
        passed = True
        if rule.max_value is not None and observed > rule.max_value:
            passed = False
        if rule.min_value is not None and observed < rule.min_value:
            passed = False
        if not passed and rule.severity == "blocking":
            failed_blocking += 1

        rule_text = []
        if rule.min_value is not None:
            rule_text.append(f">= {rule.min_value:g}")
        if rule.max_value is not None:
            rule_text.append(f"<= {rule.max_value:g}")
        print(
            f"{rule.name:<32}{rule.severity:<10}{'PASS' if passed else 'FAIL':<8}"
            f"{observed:<12g}{' '.join(rule_text)}"
        )
        rows.append(
            (
                rule.name,
                rule.severity,
                passed,
                f"{observed:g}",
                rule.detail or " ".join(rule_text),
            )
        )

    # Insert through Spark SQL: a Python-created DataFrame would need a Python
    # worker on the executors, and only the driver image is pinned to PySpark 3.5.
    def sql_literal(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    values = ", ".join(
        "(" + ", ".join(
            [
                sql_literal(name),
                sql_literal(severity),
                "true" if passed else "false",
                sql_literal(observed),
                sql_literal(detail),
            ]
        ) + ")"
        for name, severity, passed, observed, detail in rows
    )
    spark.sql(
        "INSERT INTO gold.dq_results "
        "SELECT check_name, severity, passed, observed, detail, current_timestamp() "
        f"FROM VALUES {values} "
        "AS t(check_name, severity, passed, observed, detail)"
    )

    print(f"\n[quality] results appended to {results_spec.uri}")
    if failed_blocking:
        sys.exit(f"[quality] {failed_blocking} blocking check(s) failed")
    print("[quality] all blocking checks passed")


if __name__ == "__main__":
    main()
