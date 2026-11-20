"""Demonstration: Atomicity.

A writer stages a Parquet file inside the Delta table directory and then "crashes"
before it can write the commit JSON. The table version does not move and readers
never see the staged rows - the commit log is the atomic switch.
"""

from __future__ import annotations

import argparse
import sys
import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common.lakehouse import TABLES, current_version, ensure_table, get_spark


def _parquet_files(spark, spec) -> list[str]:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.Path(spec.uri).getFileSystem(conf)
    statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path(spec.uri))
    return sorted(
        s.getPath().getName() for s in statuses if s.getPath().getName().endswith(".parquet")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash", action="store_true", help="exit non-zero like a failed job")
    args = parser.parse_args()

    spark = get_spark("shopazone-atomicity-demo")
    spec = TABLES["bronze.orders"]
    ensure_table(spark, spec)

    table = DeltaTable.forPath(spark, spec.uri)
    before = current_version(spark, spec.uri)
    before_rows = spark.read.format("delta").load(spec.uri).count()
    before_files = len(_parquet_files(spark, spec))

    staged = spark.range(3).select(
        F.lit("staged-but-uncommitted").alias("status"),
        F.col("id").alias("n"),
    )
    # This is what a writer does before it commits: put data files in place.
    staged.write.mode("append").parquet(spec.uri)

    after_files = len(_parquet_files(spark, spec))
    after = current_version(spark, spec.uri)
    after_rows = spark.read.format("delta").load(spec.uri).count()

    print("[atomicity] simulated: data files written, commit JSON never produced\n")
    print(f"  parquet files in table root : {before_files} -> {after_files}")
    print(f"  Delta version               : {before} -> {after}")
    print(f"  rows visible to readers     : {before_rows} -> {after_rows}")
    print(f"  staged file name            : part-* (unreferenced)\n")

    ok = after == before and after_rows == before_rows and after_files > before_files
    print(f"[atomicity] {'PASS' if ok else 'FAIL'} - uncommitted files are invisible to Delta")
    print("[atomicity] the orphan stays until VACUUM removes it (see make maintain)")

    if args.crash:
        print("[atomicity] exiting 1 to mimic the failed Spark job", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
