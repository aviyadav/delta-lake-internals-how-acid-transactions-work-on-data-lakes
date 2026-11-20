"""Demonstration: Schema enforcement.

Delta refuses a write whose schema does not match the table. Schema evolution is
opt-in (`mergeSchema` / `ALTER TABLE`), which is exactly the guarantee the article
describes for transactional tables.
"""

from __future__ import annotations

import sys

from delta.tables import DeltaTable

from common.lakehouse import TABLES, current_version, get_spark


def main() -> None:
    spark = get_spark("shopazone-schema-demo")
    spec = TABLES["silver.orders"]
    before = current_version(spark, spec.uri)

    unexpected = spark.sql(
        """
        SELECT 9999L AS order_id, 501L AS customer_id, 10.00D AS order_amount,
               'PAID' AS order_status, current_date() AS order_date,
               current_timestamp() AS created_at, current_timestamp() AS updated_at,
               current_timestamp() AS _event_ts, 'demo' AS _source,
               current_timestamp() AS _ingested_at,
               'column-that-does-not-exist' AS surprise_column
        """
    )

    print("[schema] writing a row with an unknown column into silver.orders ...")
    try:
        unexpected.write.format("delta").mode("append").save(spec.uri)
    except Exception as exc:  # DeltaAnalysisException / AnalysisException
        print(f"[schema] REJECTED as expected: {type(exc).__name__}")
        print(f"[schema] {str(exc).splitlines()[0][:300]}")
    else:
        sys.exit("[schema] FAIL - Delta accepted a mismatching schema")

    after = current_version(spark, spec.uri)
    print(f"[schema] version unchanged: {before} -> {after}")
    print("[schema] to evolve intentionally: .option('mergeSchema','true') or ALTER TABLE")


if __name__ == "__main__":
    main()
