"""Demonstration: optimistic concurrency (OCC).

Two independent Spark drivers read the same Silver snapshot, then both try to
rewrite the same rows. Delta serialises the commits: one writer wins, the other
either commits cleanly first or fails with a concurrency exception and has to
retry. Nothing is locked up-front; conflicts are detected at commit time.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common.lakehouse import TABLES, get_spark

WRITERS = 2


def writer(writer_id: int, barrier: str) -> None:
    barrier_path = Path(barrier)

    spark = get_spark(f"occ-writer-{writer_id}", extra_conf={"spark.master": "local[2]"})
    uri = TABLES["silver.orders"].uri
    table = DeltaTable.forPath(spark, uri)

    # Both writers read the same snapshot BEFORE the barrier, so their read sets
    # overlap and Delta has to arbitrate the two commits.
    snapshot = spark.read.format("delta").load(uri).filter(F.col("customer_id") == 501)
    snapshot.cache()
    snapshot.count()

    (barrier_path / f"ready-{writer_id}").touch()
    deadline = time.time() + 60
    while len(list(barrier_path.glob("ready-*"))) < WRITERS and time.time() < deadline:
        time.sleep(0.05)

    result = "COMMITTED"
    try:
        (
            table.alias("t")
            .merge(snapshot.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdate(set={"_source": F.lit(f"occ-writer-{writer_id}")})
            .execute()
        )
    except Exception as exc:
        result = f"CONFLICT {type(exc).__name__}: {str(exc).splitlines()[0][:180]}"

    (barrier_path / f"result-{writer_id}.txt").write_text(result)
    print(f"[occ] writer {writer_id}: {result}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--writer", type=int)
    parser.add_argument("--barrier")
    args = parser.parse_args()

    if args.writer is not None:
        writer(args.writer, args.barrier)
        return

    barrier = tempfile.mkdtemp(prefix="occ-")
    spark_submit = os.path.join(os.getenv("SPARK_HOME", "/opt/spark"), "bin", "spark-submit")
    if shutil.which("spark-submit"):
        spark_submit = shutil.which("spark-submit")

    print(f"[occ] launching {WRITERS} concurrent writers against silver.orders")
    procs = [
        subprocess.Popen(
            [
                spark_submit,
                "--master",
                "local[2]",
                "--name",
                f"shopazone-occ-{i}",
                __file__,
                "--writer",
                str(i),
                "--barrier",
                barrier,
            ]
        )
        for i in range(WRITERS)
    ]
    for proc in procs:
        proc.wait()

    for i in range(WRITERS):
        print(f"[occ] writer {i} outcome: {Path(barrier, f'result-{i}.txt').read_text()}")

    reader = get_spark("shopazone-occ-report", extra_conf={"spark.master": "local[2]"})
    history = DeltaTable.forPath(reader, TABLES["silver.orders"].uri).history(4)
    history.select("version", "timestamp", "operation").show(truncate=False)

    print("[occ] rows for customer 501 after both commits (last writer wins per row):")
    (
        reader.read.format("delta")
        .load(TABLES["silver.orders"].uri)
        .filter("customer_id = 501")
        .select("order_id", "_source")
        .orderBy("order_id")
        .show(6, truncate=False)
    )

    print("[occ] each commit is a separate Delta version; readers keep their snapshot")
    sys.exit(0)


if __name__ == "__main__":
    main()
