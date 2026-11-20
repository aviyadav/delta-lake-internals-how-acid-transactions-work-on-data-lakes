"""Table maintenance: compaction, checkpoint creation and VACUUM.

Delta does not rewrite files on its own. Small-file growth and stale files are
operational concerns, which is why the article puts OPTIMIZE/VACUUM in the
production checklist.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable

from common.lakehouse import TABLES, current_version, get_spark

DEFAULT_TABLES = ["silver.orders", "silver.orders_rejected", "bronze.orders"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs="*", default=DEFAULT_TABLES)
    parser.add_argument("--optimize", action="store_true")
    parser.add_argument("--vacuum-hours", type=int)
    parser.add_argument("--checkpoint", action="store_true")
    args = parser.parse_args()

    spark = get_spark(
        "shopazone-maintenance",
        extra_conf={
            "spark.databricks.delta.retentionDurationCheck.enabled": "false"
        },
    )

    for name in args.tables:
        spec = TABLES[name]
        table = DeltaTable.forPath(spark, spec.uri)
        print(f"\n== {name} ({spec.uri}) v{current_version(spark, spec.uri)}")

        if args.checkpoint:
            # With a checkpoint interval of 1, the next commit also writes a checkpoint.
            spark.sql(
                f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.checkpointInterval' = '1')"
            )
            print("  checkpointInterval=1 set")

        if args.optimize:
            result = table.optimize().executeCompaction()
            row = result.selectExpr(
                "metrics.numFilesAdded",
                "metrics.numFilesRemoved",
                "metrics.filesAdded.totalFiles",
            ).collect()[0]
            print(
                "  OPTIMIZE -> files added={}, removed={}, total after={}".format(
                    row[0], row[1], row[2]
                )
            )
            print(f"  version after compaction: {current_version(spark, spec.uri)}")

        if args.vacuum_hours is not None:
            table.vacuum(args.vacuum_hours)
            print(f"  VACUUM removed files older than {args.vacuum_hours} hours")


if __name__ == "__main__":
    main()
