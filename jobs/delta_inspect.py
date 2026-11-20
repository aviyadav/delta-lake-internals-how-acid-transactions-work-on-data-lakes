"""Inspect the parts of Delta Lake the article is about.

    python delta_inspect.py --table bronze.orders log
    python delta_inspect.py --table bronze.orders snapshot --version 3
    python delta_inspect.py --table silver.orders changes --version 2
    python delta_inspect.py --table silver.orders travel
    python delta_inspect.py --table silver.orders history
"""

from __future__ import annotations

import argparse
import json
from collections import Counter

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common.lakehouse import TABLES, current_version, get_spark


def _list_log(spark, spec):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(f"{spec.uri}/_delta_log")
    fs = path.getFileSystem(conf)
    entries = fs.listStatus(path)
    return sorted(entry.getPath().getName() for entry in entries)


def _commit_lines(spark, spec):
    logs = _list_log(spark, spec)
    commits = [name for name in logs if name.endswith(".json")]
    rows = (
        spark.read.text(f"{spec.uri}/_delta_log/*.json")
        .withColumn("file", F.input_file_name())
        .collect()
    )
    grouped: dict[str, list[str]] = {name: [] for name in commits}
    for row in rows:
        name = row["file"].rsplit("/", 1)[-1]
        grouped.setdefault(name, []).append(row["value"])
    return logs, grouped


def cmd_log(spark, spec, args) -> None:
    logs, grouped = _commit_lines(spark, spec)
    print(f"table      : {spec.name}")
    print(f"location   : {spec.uri}")
    print(f"_delta_log : {len(logs)} entries\n")

    for name in logs:
        if name.endswith(".json"):
            actions = Counter()
            operation = ""
            for line in grouped.get(name, []):
                payload = json.loads(line)
                for key in payload:
                    actions[key] += 1
                if "commitInfo" in payload:
                    operation = payload["commitInfo"].get("operation", "")
            summary = ", ".join(f"{k}={v}" for k, v in sorted(actions.items()))
            print(f"{name}  {operation:<18} {summary}")
        elif ".checkpoint" in name:
            print(f"{name}  <- reconciled table state")
        elif name == "_last_checkpoint":
            print(f"{name}  <- pointer to the newest checkpoint")

    if args.actions:
        newest = [n for n in logs if n.endswith(".json")][-1]
        print(f"\nraw actions in {newest}:")
        for line in grouped[newest]:
            print(json.dumps(json.loads(line), indent=2)[:1200])

    checkpoint_pointer = [n for n in logs if n == "_last_checkpoint"]
    if checkpoint_pointer:
        # Spark's text source skips files starting with "_", so read via Hadoop FS.
        text = _read_hadoop_text(spark, f"{spec.uri}/_delta_log/_last_checkpoint")
        print(f"\n_last_checkpoint: {text[:300]}")


def _read_hadoop_text(spark, uri: str) -> str:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(conf)
    if not fs.exists(path):
        return ""
    stream = fs.open(path)
    try:
        return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def cmd_snapshot(spark, spec, args) -> None:
    logs, grouped = _commit_lines(spark, spec)
    target = args.version if args.version is not None else current_version(spark, spec.uri)

    active: dict[str, int] = {}
    for name in logs:
        if not name.endswith(".json"):
            continue
        version = int(name.split(".")[0])
        if version > target:
            break
        for line in grouped.get(name, []):
            payload = json.loads(line)
            if "add" in payload:
                active[payload["add"]["path"]] = payload["add"].get("size", 0)
            elif "remove" in payload:
                active.pop(payload["remove"]["path"], None)

    total_bytes = sum(active.values())
    print(f"table                  : {spec.name}")
    print(f"requested version      : {target}")
    print(f"active parquet files   : {len(active)}")
    print(f"active bytes           : {total_bytes:,}")
    print("reconstruction         : replay add/remove actions from version 0")
    print("\nfirst 8 active files:")
    for path, size in list(sorted(active.items()))[:8]:
        print(f"  {path}  {size:,} bytes")


def cmd_history(spark, spec, args) -> None:
    history = DeltaTable.forPath(spark, spec.uri).history(args.limit)
    history.select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    ).show(truncate=60)


def cmd_changes(spark, spec, args) -> None:
    latest = current_version(spark, spec.uri)
    start = args.version if args.version is not None else max(0, latest - 1)
    print(f"reading change data feed for {spec.name} from version {start}")
    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .load(spec.uri)
    )
    changes.select(
        "order_id",
        "order_status",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ).orderBy("_commit_version", "order_id").show(30, truncate=False)


def cmd_travel(spark, spec, args) -> None:
    latest = current_version(spark, spec.uri)
    print(f"table {spec.name} latest version = {latest}\n")
    print(f"{'version':<10}{'rows':<10}columns")
    for version in range(latest + 1):
        frame = (
            spark.read.format("delta").option("versionAsOf", version).load(spec.uri)
        )
        print(f"{version:<10}{frame.count():<10}{len(frame.columns)}")

    if latest >= 1:
        old = spark.read.format("delta").option("versionAsOf", 0).load(spec.uri)
        new = spark.read.format("delta").load(spec.uri)
        removed = old.select("order_id").subtract(new.select("order_id")).count()
        added = new.select("order_id").subtract(old.select("order_id")).count()
        print(f"\nversion 0 -> {latest}: {removed} order ids disappeared, {added} new ids")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, choices=sorted(TABLES))
    sub = parser.add_subparsers(dest="command", required=True)

    log_parser = sub.add_parser("log")
    log_parser.add_argument("--actions", action="store_true")
    log_parser.set_defaults(func=cmd_log)

    snapshot_parser = sub.add_parser("snapshot")
    snapshot_parser.add_argument("--version", type=int)
    snapshot_parser.set_defaults(func=cmd_snapshot)

    history_parser = sub.add_parser("history")
    history_parser.add_argument("--limit", type=int, default=10)
    history_parser.set_defaults(func=cmd_history)

    changes_parser = sub.add_parser("changes")
    changes_parser.add_argument("--version", type=int)
    changes_parser.set_defaults(func=cmd_changes)

    travel_parser = sub.add_parser("travel")
    travel_parser.set_defaults(func=cmd_travel)

    args = parser.parse_args()
    spark = get_spark(f"delta-inspect-{args.command}")
    args.func(spark, TABLES[args.table], args)


if __name__ == "__main__":
    main()
