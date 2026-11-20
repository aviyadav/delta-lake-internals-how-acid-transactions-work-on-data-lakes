# Shop-A-Zone Lakehouse — Delta Lake, end to end

A working, Dockerised implementation of the architecture in
[Delta Lake Architecture Explained: Internals, ACID Transactions, Transaction Log and How It Works](Delta%20Lake%20Architecture%20Explained_%20Internals%2C%20ACID%20Transactions%2C%20Transaction%20Log%20and%20How%20It%20Works.pdf).

The article explains *why* a data lake needs a transaction log. This repository makes
that observable on one machine: batch and streaming ingestion land in Delta tables on
S3-compatible storage, Airflow orchestrates the pipeline, dbt builds the Gold layer, a
quality gate refuses to publish bad data, and Trino reads the same Parquet files Spark
just wrote — no copy, no export, no warehouse load.

```
Postgres orders ──batch──┐                        ┌──> Hive Metastore ──> Trino
                         ├──> Spark ──> S3 ───────┤
Kafka order events ──────┘        │  (Delta)      └──> dbt (Spark SQL) ──> Gold
                                  │
                                  └──> Airflow: validate → bronze → silver
                                       → catalog → dbt gold → dbt tests
                                       → quality gate → Trino verify
```

## What a run proves

A green run is not the point; these are:

* **Transactions are atomic.** A writer can stage Parquet files and "crash" — the table
  version and row count do not move, because no commit JSON was written.
* **One table, two engines.** Spark writes `silver.orders` and `gold.*`; Trino returns
  89 orders and the Gold revenue rollup from the same files via `_delta_log`.
* **History is queryable.** `versionAsOf` returns earlier snapshots; Change Data Feed
  returns row-level changes per commit; checkpoints keep replay cheap.

## Stack, per stage

| Stage | Tool (as named in the article) | In this repo |
|---|---|---|
| Ingestion (batch) | orders database + Spark | Postgres 16 + PySpark JDBC read |
| Ingestion (stream) | Kafka, Spark Structured Streaming | Kafka 3.9 (KRaft) → Spark 3.5.4 |
| Storage | Amazon S3 + Parquet | LocalStack 3.8 S3 API (`s3://lakehouse`) |
| Table format | Delta Lake | Delta Lake 3.2.0 (`delta-spark`) |
| Processing | Apache Spark | Spark standalone master + worker |
| Catalog | Hive Metastore | Hive Metastore 4.0.0 on Postgres |
| Transformation | dbt | dbt-spark 1.11.1 over Spark Thrift |
| Serving | Trino | Trino 476 Delta Lake connector |
| Orchestration | Apache Airflow | Airflow 2.10.5 (LocalExecutor) |
| Quality | dbt tests + checks | dbt tests + a Spark quality gate |
| Ops | — | Delta `_delta_log` inspector, OPTIMIZE/VACUUM |

Driver Python is 3.11 (Airflow image), worker Python is 3.8 (Spark image); jobs therefore
use the DataFrame/SQL API only and never Python UDFs.

## Services and ports

Twelve containers. The infrastructure services are health-checked; the worker, producer
and streaming job run continuously without a health probe. `./scripts/lake.sh ps` shows
the state.

| Service | URL / port | Notes |
|---|---|---|
| Airflow webserver | http://localhost:8080 | `admin` / `admin` |
| Trino | http://localhost:8085 | `docker exec lake-trino trino ...` |
| Spark master UI | http://localhost:8090 | applications, executors |
| Spark worker UI | http://localhost:8091 | cores / memory in use |
| Spark driver UI | http://localhost:4040 | only while a job runs |
| Spark Thrift (HiveServer2) | `localhost:10000` | used by dbt |
| Hive Metastore | `localhost:9083` | name → S3 location |
| LocalStack S3 | http://localhost:4566 | `awslocal s3 ls s3://lakehouse` |
| Kafka (external listener) | `localhost:29092` | topic `shopazone.orders.events` |
| Postgres | `localhost:5432` | `orders`, `airflow`, `metastore` DBs |

## Quick start (WSL / Linux)

```bash
cd ~/codebase/python-base/delta-lake-internals-how-acid-transactions-work-on-data-lakes
./scripts/lake.sh up        # builds the local images, then starts everything
./scripts/lake.sh ps        # wait until every row is healthy
./scripts/lake.sh dag       # trigger the pipeline
./scripts/lake.sh status    # per-task result of that run
./scripts/lake.sh trino     # query Gold through Trino
```

* `./scripts/lake.sh` is the portable CLI; `make <target>` exposes the same commands.
* All `shopazone/*` images are **built from `docker/`**, never pulled from a registry.
  `up` builds first, so a missing local image is rebuilt instead of failing the pull.
* First build downloads base images (Spark, Airflow, Hive, Trino, Kafka, LocalStack,
  Postgres) and takes ~10 minutes; later runs reuse the layer cache.
* **Windows:** run from a WSL distro with Docker Desktop's WSL integration enabled and
  keep the project on the Linux filesystem (`~/...`), not `/mnt/c` — bind mounts from
  `/mnt/c` are very slow.

## The pipeline

`dags/shopazone_lakehouse.py`, eight tasks, all idempotent enough to re-run:

| Task | Operator | What it does |
|---|---|---|
| `validate_source` | Spark | blocks on null keys / no rows; reports duplicates, negative amounts, unknown statuses |
| `ingest_bronze` | Spark | JDBC read → append commit in `bronze.orders` |
| `build_silver` | Spark | union batch + streaming, newest event per order, quarantine invalid rows, `MERGE` into `silver.orders` |
| `register_catalog` | Spark | external Delta tables in the Hive Metastore, self-healing stale entries |
| `dbt_run_gold` | Bash | dbt models `gold_customer_orders`, `gold_daily_revenue` |
| `dbt_test` | Bash | unique / not_null / accepted_values + revenue reconciliation |
| `quality_gate` | Spark | 10 rules, results appended to `gold.dq_results`, blocks on failure |
| `publish_verify_trino` | Python | reads Silver and Gold through Trino, prints counts |

Two long-running services feed it: the order-event producer (1 event/second, with
deliberate duplicates) and the streaming job that appends Kafka micro-batches to
`bronze.order_events` every 15 seconds with exactly-once commits.

## Tables

| Table | Written by | Pattern |
|---|---|---|
| `bronze.orders` | `ingest_batch_orders.py` | append; raw source rows, no cleaning |
| `bronze.order_events` | `stream_order_events.py` | Structured Streaming, checkpoint = Kafka offsets |
| `silver.orders` | `build_silver.py` | MERGE on `order_id`, newest `_event_ts` wins, CDF on |
| `silver.orders_rejected` | `build_silver.py` | quarantine with `_reject_reason` (deduplicated) |
| `gold.gold_customer_orders` | dbt | one row per customer |
| `gold.gold_daily_revenue` | dbt | one row per `order_date` |
| `gold.dq_results` | `quality_checks.py` | quality history, one row per rule per run |

Rejection reasons: `missing_order_id`, `missing_customer_id`, `non_positive_amount`,
`unknown_order_status`. Source statuses are normalised (`completed ` → `COMPLETED`)
before the domain check.

Physical layout is plain Delta: Parquet files plus `_delta_log/` (JSON commits,
`*.checkpoint.parquet`, `_last_checkpoint`), all under
`s3://lakehouse/{bronze,silver,gold}/<table>/`.

## Observe the data flow

```bash
./scripts/lake.sh inspect      # _delta_log commits, checkpoint, _last_checkpoint
./scripts/lake.sh snapshot     # replay add/remove → active files + bytes
./scripts/lake.sh history      # Delta table history
./scripts/lake.sh travel       # row counts per version, plus v0 → latest diff
./scripts/lake.sh changes      # Change Data Feed rows per commit
./scripts/lake.sh atomicity    # staged files without a commit are invisible
./scripts/lake.sh schema       # unknown column is rejected
./scripts/lake.sh concurrency  # two writers → two serialised versions
./scripts/lake.sh maintain     # OPTIMIZE + force checkpoint + VACUUM
./scripts/lake.sh tasklog build_silver   # tail the newest task log
./scripts/s3-tree.sh bronze/orders       # what is actually in the bucket
```

The step-by-step guided tour, with expected output for every stage, is
[docs/PLAYBOOK.md](docs/PLAYBOOK.md). Design decisions and the article-to-code map are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Ready-made Trino queries live in
[sql/trino_queries.sql](sql/trino_queries.sql).

## Repository layout

```
docker-compose.yml          every service, health-checked
docker/                     images: spark, airflow, hive-metastore, trino, producer
jobs/                       PySpark jobs (ingest, stream, silver, quality, inspect, demos)
dags/                       Airflow DAGs (pipeline + maintenance)
dbt/                        dbt project: Gold models, sources, tests, schema macro
sql/trino_queries.sql       queries to run in Trino
scripts/lake.sh             portable CLI for every step (also: Makefile)
scripts/s3-tree.sh          inspect the bucket through the S3 API
docs/PLAYBOOK.md            run + observe the whole flow
docs/ARCHITECTURE.md        design notes and the article mapping
```

## Design decisions that matter

* **One URI scheme for two engines.** Spark writes `s3://` (Hadoop's S3A is bound to that
  scheme) so Trino's native S3 file system can read the same paths. `s3a://` breaks Trino.
* **Catalog is a pointer.** Jobs write by path, then register an *external* Delta table.
  A crashed writer never leaves a half-created catalog entry.
* **The metastore can resolve S3.** Hive's image launches through Hadoop `RunJar`, which
  hides `hadoop-aws`; the custom entrypoint starts it with an explicit classpath.
* **dbt in its own venv.** dbt-core needs `protobuf>=6`, Airflow 2.10 pins 4.25.6. dbt is
  invoked from `/opt/dbt-venv/bin/dbt` and writes its target/logs to `/tmp`.
* **dbt writes `gold`.** A custom `generate_schema_name` macro avoids the default
  `gold_gold` concatenation.
* **Quality is separate from ACID.** Delta guarantees the commit, not correctness; the
  gate asserts uniqueness, domains, freshness and Gold↔Silver reconciliation.

## Known limitations

* **LocalStack is ephemeral.** This image ignores `PERSISTENCE=1`; recreating the
  container empties the bucket while the Postgres metastore survives. The pipeline is
  written for that: `register_catalog` drops entries whose Delta log is gone and dbt
  rebuilds Gold, so a re-run restores a consistent lake. Swapping in real S3 or MinIO
  needs only endpoint/credentials changes.
* Single-node Spark with per-application `spark.cores.max` caps (Thrift, streaming and
  batch share one worker).
* No Python UDFs, because driver and worker Python differ (3.11 vs 3.8).
* Debezium, Flink, Unity Catalog and Great Expectations are documented extension points,
  not shipped dependencies.

## Troubleshooting (most common)

| Symptom | Fix |
|---|---|
| `pull access denied for shopazone/...` | local image missing — `./scripts/lake.sh build` (`up` does this first) |
| `permission denied for table orders` | grant once: `GRANT SELECT ON ALL TABLES IN SCHEMA public TO orders;` |
| Spark app stuck in `WAITING` | another app holds the cores; submissions pass `spark.cores.max` |
| `No factory for location: s3://…` | keep `fs.native-s3.enabled=true` in the Trino catalog |
| `DELTA_PATH_DOES_NOT_EXIST` after `down`/`up` | ephemeral S3: re-run the DAG, the catalog self-heals |
| leftover `gold_gold` schema | from an older dbt config; `./scripts/lake.sh nuke` for a pristine catalog |

## Stop

```bash
./scripts/lake.sh down     # stop containers, keep volumes
./scripts/lake.sh nuke     # delete containers and volumes (fresh install)
```
