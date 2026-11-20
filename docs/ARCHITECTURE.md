# Architecture and design notes

## 1. The shape of the system

```
                       ┌───────────────────────────────┐
  Postgres "orders" ──►│ Spark batch: JDBC → Delta     │──┐
  (operational DB)     └───────────────────────────────┘  │
                                                          ├──► s3://lakehouse/bronze/*
  Kafka orders.events ─►┌──────────────────────────────┐  │        │
   (1 event/second)     │ Spark Structured Streaming   │──┘        │  Parquet + _delta_log
                        └──────────────────────────────┘           │
                                                                   ▼
                        ┌───────────────────────────────┐   ┌───────────────┐
                        │ Spark: dedupe, quarantine,    │◄──│ Delta tables  │
                        │ MERGE → silver.orders (+CDF)  │   └───────────────┘
                        └───────────────────────────────┘           │
                                    │                               │
                    ┌───────────────┴──────────────┐                │
                    ▼                              ▼                │
        ┌────────────────────┐          ┌──────────────────┐        │
        │ Hive Metastore     │          │ Trino 476        │────────┘
        │ (table → location) │─────────►│ delta_lake       │
        └────────────────────┘          └──────────────────┘
                    ▲
                    │ register / read
        ┌───────────┴──────────┐        ┌──────────────────┐
        │ dbt-spark (Thrift)   │───────►│ gold_* tables    │
        └──────────────────────┘        └──────────────────┘
                    ▲
             Apache Airflow orchestrates every step
```

## 2. Layers, and what each one owns

| Layer | Component | Owns |
|---|---|---|
| Ingestion | Postgres → Spark JDBC, Kafka → Structured Streaming | moving bytes in, no cleaning |
| Storage | LocalStack S3 (`s3://lakehouse`) | objects; the local emulator is **not** durable across container recreation |
| Table format | Delta Lake 3.2 on Spark 3.5.4 | ACID, schema, snapshots, CDF |
| Processing | Spark standalone cluster | batch + streaming + MERGE |
| Catalog | Hive Metastore 4.0 | table name → S3 location |
| Transformation | dbt-spark over HiveServer2 | Gold SQL models + tests |
| Serving | Trino 476 `delta_lake` | SQL on the same files |
| Orchestration | Airflow 2.10 (LocalExecutor) | order, retries, visibility |
| Quality | dbt tests + Spark DQ gate | correctness, not just commit safety |

## 3. Bucket layout

```
s3://lakehouse/
├── bronze/orders/                 append-only snapshots of the source table
├── bronze/order_events/           Kafka micro-batches, exactly-once
├── silver/orders/                 current state per order (MERGE, CDF on)
├── silver/orders_rejected/        quarantine with _reject_reason
├── gold/gold_customer_orders/     dbt model
├── gold/gold_daily_revenue/       dbt model
├── gold/dq_results/               quality gate history
└── checkpoints/bronze_order_events_v2/   Spark streaming offsets
```

Each table directory is a normal Delta table: Parquet data files plus
`_delta_log/` with JSON commits, periodic `*.checkpoint.parquet` files and the
`_last_checkpoint` pointer.

## 4. Decisions that make it work (and why)

### 4.1 Spark writes `s3://`, not `s3a://`

Hadoop's S3A filesystem is registered for the `s3` scheme
(`spark.hadoop.fs.s3.impl`), so Spark keeps using all its `fs.s3a.*` settings
while the URI in the Delta log and the metastore is `s3://`.

Reason: Trino 476 uses the native file system (`fs.native-s3.enabled=true`) and
only accepts `s3://`. With `s3a://` the metastore location is unusable by Trino
(`No factory for location: s3a://...`). One scheme for both engines removes a
whole class of "works in Spark, fails in Trino" problems.

### 4.2 Path-based writes, catalog as a pointer

Jobs write to a path and then register the table:

1. if there is no Delta log at the location, create the table via
   `DeltaTable.createIfNotExists(...).location(...)`;
2. register it as an **external** table
   (`CREATE EXTERNAL TABLE ... USING DELTA LOCATION ...`);
3. align a drifted location with `ALTER TABLE ... SET LOCATION` (metadata only).

Consequences: a crashed writer never leaves a half-created catalog entry, `DROP
TABLE` cannot delete data, and the catalog can be rebuilt from the bucket at any
time.

### 4.3 The metastore needs the S3A client

Hive 4's image launches the metastore through Hadoop's `RunJar`, which only uses
the manifest class-path of `hive-metastore-4.0.0.jar`; adding jars to
`/opt/hive/lib` or `HADOOP_CLASSPATH` has no effect. `ALTER TABLE ... SET
LOCATION` validates the new location, so without S3A the metastore fails with
`No FileSystem for scheme "s3"` and leaves placeholder locations behind.

`docker/hive/metastore-entrypoint.sh` starts the same main class with an explicit
classpath (`/opt/hive/lib/*` + `hadoop classpath`), which makes `hadoop-aws`
visible. This is the only custom launcher in the project.

### 4.4 Resource caps on a shared standalone cluster

One worker serves the streaming job, the Thrift server and every batch job.
Without caps the first application grabs all cores and the rest wait forever
(`app state WAITING`, no executor). Therefore:

* worker: `--cores 8 --memory 8g`
* streaming: `spark.cores.max=1`, executor 1 core / 1 GB
* Thrift: `spark.cores.max=2`
* Airflow batch tasks: `spark.cores.max=2`

### 4.5 Python versions: driver 3.11, workers 3.8

The Airflow/Spark driver image is Python 3.11, the Spark worker image ships 3.8.
PySpark refuses to run Python workers across minor versions, so every job uses
the DataFrame/SQL API only — no Python UDFs, no `mapInPandas`. The quality gate
inserts its results with SQL for the same reason.

### 4.6 dbt lives in its own virtualenv

dbt-core 1.11 requires `protobuf>=6`; the Airflow 2.10 constraints pin
`protobuf 4.25.6`. Installing both in one environment is unresolvable. dbt is
installed into `/opt/dbt-venv` and invoked by absolute path from the DAG, and it
writes `target/` and `logs/` to `/tmp` because the project is mounted read-only.

### 4.7 Quality is a separate gate

Delta guarantees that a transaction is atomic, not that its contents are correct.
The Silver job quarantines business-invalid rows instead of silently dropping
them, and `quality_checks.py` asserts: uniqueness, non-null keys, positive
amounts, status domain, freshness, quarantine visibility and Gold↔Silver
reconciliation. Results are appended to `gold.dq_results` so quality is a
time series, not a log line.

### 4.8 dbt writes into `gold`, not `gold_gold`

dbt's default schema macro prefixes a model's custom schema with the target
schema. With `schema: gold` in the profile **and** `+schema: gold` on the models
that produces `gold_gold`, which the quality gate and Trino do not look at.
`dbt/macros/generate_schema_name.sql` returns the custom schema verbatim, so the
models land exactly in `gold`.

## 5. Data contracts

`bronze.orders`

| column | type | note |
|---|---|---|
| order_id | bigint | source key, not unique in the source |
| customer_id | bigint | nullable in the source |
| order_amount | decimal(12,2) | may be negative (refund defect) |
| order_status | string | raw, unnormalised |
| created_at / updated_at | timestamp | event time |
| `_ingest_ts` / `_source_system` | timestamp / string | lineage |

`silver.orders` adds `order_date`, `_event_ts`, `_source`, `_ingested_at` and is
the only table downstream models read. `silver.orders_rejected` keeps
`_reject_reason` (`missing_customer_id`, `non_positive_amount`,
`unknown_order_status`, `missing_order_id`).

`gold_customer_orders`: one row per customer — orders, lifetime value, realised
revenue, cancellations, first/last order date.
`gold_daily_revenue`: one row per `order_date` — orders, active customers, gross
revenue, realised revenue, cancelled value.

## 6. Article → implementation map

| Article section | Where it lives |
|---|---|
| Why traditional data lakes fail (partial writes, no ACID) | `jobs/demo_atomicity.py`, `make atomicity` |
| Anatomy of a table (Parquet + JSON log + checkpoints) | `jobs/delta_inspect.py log`, `make inspect` |
| Transaction log actions (`add`, `remove`, `metaData`, `commitInfo`) | `delta_inspect.py log --actions` |
| Snapshot reconstruction | `delta_inspect.py snapshot` |
| Checkpoints and `_last_checkpoint` | `delta.checkpointInterval=5`; `make maintain` forces interval 1 |
| MVCC / readers keep their snapshot | `delta_inspect.py travel` |
| Atomicity | `make atomicity` |
| Isolation / optimistic concurrency | `make concurrency` |
| Durability | the commit file is the switch; the emulator is ephemeral (see 7) |
| Schema enforcement vs evolution | `jobs/demo_schema_enforcement.py` |
| MERGE / UPDATE / DELETE | `jobs/build_silver.py` |
| Time travel | `jobs/demo_time_travel.py` |
| Change Data Feed | `delta.enableChangeDataFeed=true` on Silver, `make changes` |
| VACUUM / OPTIMIZE | `jobs/maintenance.py`, `make maintain` |
| Ingestion layer (Kafka, Spark) | `jobs/producer.py`, `jobs/stream_order_events.py` |
| Storage layer (S3) | LocalStack S3 |
| Processing layer (Spark) | `spark-master`, `spark-worker`, all `jobs/*` |
| Catalog (Hive Metastore) | `hive-metastore` + `jobs/register_tables.py` |
| Consumption (Trino) | `trino` + `sql/trino_queries.sql` |
| Orchestration (Airflow) | `dags/shopazone_lakehouse.py` |
| dbt transformations + tests | `dbt/` |
| Production checklist (quality, maintenance, observability) | DQ gate, maintenance DAG, `_delta_log` inspector |

## 7. Known limitation: ephemeral object storage

`localstack/localstack:3.8` does not honour `PERSISTENCE=1` here: the state directory stays empty and a container recreation wipes the bucket. The metastore lives in Postgres and does survive, so catalog entries can outlive their data.

The pipeline is written for that: `register_tables.py` drops an entry when its Delta log is gone and re-registers when a log exists; `ensure_table()` does the same before every write; dbt rebuilds Gold from Silver. A re-run rebuilds the lake.

Swapping in real S3 or MinIO needs no code change, only the endpoint/credentials in `docker-compose.yml` and the two catalog files.

## 8. Deliberately out of scope

These are named in the article but would each deserve their own project:

* **Debezium CDC** — the batch source is a plain JDBC snapshot. A Debezium
  connector writing to Kafka would replace `ingest_batch_orders.py` without
  changing anything downstream.
* **Apache Flink** — the streaming path uses Spark Structured Streaming, which the
  article also presents as the default. A Flink Delta sink is an alternative
  writer, not a replacement for the transactional core.
* **Unity Catalog** — the catalog is Hive Metastore, the article's other example.
* **Great Expectations / Soda / Deequ** — the quality gate is intentionally
  dependency-light; swapping in GX would mean replacing `quality_checks.py` with
  a suite and keeping `gold.dq_results` as the sink.
* **Multi-node Hadoop/Spark** — one worker is enough to observe the architecture;
  nothing in the code depends on the cluster size.
