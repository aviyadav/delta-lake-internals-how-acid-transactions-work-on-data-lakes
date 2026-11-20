# Playbook — run and observe the whole data flow

This is the guided tour. Every command below was executed against this stack;
the sample output is real (row counts drift because the Kafka producer keeps
running).

The goal is not "the DAG turned green". It is to *see* the transaction log, the
checkpoint, the MERGE, the quarantine, the time travel and a second engine
reading the same files.

Total time: ~20 minutes, most of it waiting for the pipeline.

---

## 0. What you are about to observe

```
Postgres.orders ──batch──┐
                         ├──> Spark ──> S3 ──> Delta (_delta_log)
Kafka order events ──────┘        │
                                  ├──> Silver MERGE + quarantine + CDF
                                  ├──> Hive Metastore (table pointer)
                                  ├──> dbt ──> Gold aggregates + tests
                                  └──> Trino (second engine, same files)

Airflow: validate → bronze → silver → catalog → dbt gold → dbt tests
         → quality gate → publish/verify through Trino
```

---

## 1. Start the platform

```bash
./scripts/lake.sh build     # first run only, ~10 minutes
./scripts/lake.sh up
./scripts/lake.sh ps
```

Wait until every row says `healthy` (the worker container has no healthcheck).

> **WSL / Windows:** run the commands from the Linux shell inside your distro
> (Docker Desktop → Settings → Resources → WSL integration). Keep the project on
> the Linux filesystem (`~/codebase/...`); `/mnt/c` mounts are slow. After moving
> the folder, run `./scripts/lake.sh build` once so the images match this compose file.

`./scripts/lake.sh <cmd>` is the portable CLI. `make <cmd>` runs the same
commands if `make` is installed (Windows/Git Bash usually has no `make`).
Expected:

```
lake-airflow-scheduler   Up (healthy)
lake-airflow-webserver   Up (healthy)
lake-hive-metastore      Up (healthy)
lake-kafka               Up (healthy)
lake-postgres            Up (healthy)
lake-producer            Up
lake-s3                  Up (healthy)
lake-spark-master        Up (healthy)
lake-spark-thrift        Up (healthy)
lake-spark-worker        Up
lake-stream-bronze       Up
lake-trino               Up (healthy)
```

---

## 2. Prove the infrastructure before touching data

### 2.1 Object storage

```bash
docker exec lake-s3 awslocal s3 ls s3://lakehouse/
```

The bucket exists (empty on a fresh stack). The metastore, Spark and Trino all
point at `s3://lakehouse` through the S3 API.

### 2.2 Catalog

```bash
docker compose logs hive-metastore --tail 3
```

### 2.3 Streaming source

```bash
docker compose logs order-event-producer --tail 3
# [producer] sent=30 last_order=2005 status=PAID
docker exec lake-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic shopazone.orders.events
```

Every 17th event is re-sent unchanged, on purpose: the pipeline has real
duplicates to collapse.

### 2.4 Query engine

```bash
docker exec lake-trino trino --execute "show catalogs"
# delta, system, tpch
```

If Trino reports `No factory for location`, the `fs.native-s3.enabled=true`
flag in `docker/trino/catalog/delta.properties` is missing.

---

## 3. Run the pipeline

```bash
./scripts/lake.sh dag
```

Airflow UI: <http://localhost:8080> (admin / admin) → `shopazone_lakehouse` →
Graph. Or from the CLI:

```bash
./scripts/lake.sh status
```

Observed (first complete run):

```
validate_source       success   7s
ingest_bronze         success  15s
build_silver          success  22s
register_catalog      success   9s
dbt_run_gold          success  14s
dbt_test              success   5s
quality_gate          success  16s
publish_verify_trino  success   1s
```

### Why each task exists

| Task | Article stage | What it does |
|---|---|---|
| `validate_source` | Validate Source Data | blocks on null keys / no rows, reports business defects |
| `ingest_bronze` | Write Bronze Delta Table | JDBC read → append commit |
| `build_silver` | Transform Silver | MERGE + quarantine + CDF |
| `register_catalog` | Table management and catalog | external tables in Hive Metastore |
| `dbt_run_gold` | Build Gold Aggregates | Spark SQL models |
| `dbt_test` | Data quality | unique / not_null / accepted_values / reconciliation |
| `quality_gate` | Run Data Quality Checks | 10 rules, results persisted |
| `publish_verify_trino` | Consumption layer | reads the same Delta tables through Trino |

---

## 4. Follow the data, stage by stage

### 4.1 Source validation (Spark log)

Open the task log in the UI (`validate_source` → Logs) or:

```bash
./scripts/lake.sh tasklog validate_source | grep -A12 "source_has_rows"
```

```
check                 severity  result  observed
--------------------------------------------------------------
source_has_rows       blocking  PASS    33
order_id_not_null     blocking  PASS    0
timestamps_parse      blocking  PASS    0
duplicate_order_ids   warning   WARN    1
negative_amounts      warning   WARN    1
unknown_statuses      warning   WARN    1
```

**What this proves:** Delta makes a transaction atomic, not *correct*. The
duplicate, the negative amount and the unknown status are allowed into Bronze on
purpose — Silver quarantines them and the quality gate proves it.

### 4.2 Bronze, batch

```bash
./scripts/lake.sh tasklog ingest_bronze | grep "\[ingest\]"
# [ingest] rows read from Postgres : 33
# [ingest] Delta version           : 3 -> 4
```

Spark UI: <http://localhost:8090> → the finished application → SQL tab shows the
JDBC scan and the Delta write.

### 4.3 Bronze, streaming

```bash
docker compose logs stream-bronze --tail 6
# [stream] batch=12 input_rows=15 rate=2.4 rows/s table=bronze.order_events
```

Two things to look at:

1. The checkpoint in object storage — Spark records Kafka offsets there:
   ```bash
   docker exec lake-s3 awslocal s3 ls s3://lakehouse/checkpoints/bronze_order_events_v2/commits/ | tail -3
   ```
2. The Delta commits in the table:
   ```bash
   docker exec lake-s3 awslocal s3 ls s3://lakehouse/bronze/order_events/_delta_log/ | tail -3
   ```

**What this proves:** the checkpoint (offsets) and the Delta log (data) are
separate concerns; together they give exactly-once writes, and a restarted
stream resumes where it stopped.

### 4.4 Silver

```bash
./scripts/lake.sh tasklog build_silver | grep "\[silver\]"
# [silver] quarantined rows      : 3 (new: 3) -> s3://lakehouse/silver/orders_rejected
# [silver] silver version        : 2 -> 3
# [silver] current orders        : 89
# [silver] distinct order ids    : 89
# [silver] last commit           : v3 MERGE
# [silver] merge metrics         : {'numTargetRowsInserted': '60', 'numTargetRowsUpdated': '0', ...}
```

The quarantine table shows *why* each row was rejected:

```bash
docker compose exec -T trino trino --catalog delta --schema silver --execute \
  "select distinct order_id, order_status, order_amount, _reject_reason from orders_rejected order by order_id"
```

```
1021  REFUNDED  -75.00  non_positive_amount
1022  UNKNOWN   310.00  unknown_order_status
1024  PAID      520.00  missing_customer_id
```

**What this proves:** the Silver MERGE keeps one current row per order; the bad
rows are captured with a reason instead of being silently dropped.

### 4.5 Catalog

```bash
./scripts/lake.sh tasklog register_catalog | grep "\[catalog\]"
# [catalog] exists  bronze.orders -> s3://lakehouse/bronze/orders
# [catalog] exists  silver.orders -> s3://lakehouse/silver/orders
```

**What this proves:** the catalog stores a *pointer*. The truth is the Delta log
in object storage.

### 4.6 dbt Gold

Airflow task log `dbt_run_gold`:

```
1 of 2 OK created table gold.gold_customer_orders
2 of 2 OK created table gold.gold_daily_revenue
```

`dbt_test` then runs the schema tests plus
`tests/assert_gold_revenue_reconciles.sql`.

**What this proves:** dbt manages SQL transformations and tests; Spark still does
the writing, and `ref()`/`source()` give dependency order.

### 4.7 Quality gate

```
silver_order_id_unique          blocking  PASS    0
silver_customer_present         blocking  PASS    0
silver_amount_positive          blocking  PASS    0
silver_status_in_domain         blocking  PASS    0
silver_has_rows                 blocking  PASS    89
silver_no_future_orders         warning   PASS    0
gold_customer_reconciliation    blocking  PASS    0
gold_revenue_reconciliation     blocking  PASS    0
bronze_ingest_fresh             warning   PASS    1.47
quarantine_visible              info      PASS    9
```

Results are persisted:

```bash
docker exec lake-trino trino --catalog delta --schema gold --execute \
  "select check_name, passed, observed, checked_at from dq_results order by checked_at desc limit 5"
```

**What this proves:** a pipeline can be transactionally perfect and still wrong;
the gate is what stops publication.

### 4.8 The same table, a different engine

```bash
./scripts/lake.sh trino
```

```
 order_date  | total_orders | active_customers | realized_revenue
-------------+--------------+------------------+------------------
 2026-11-19  |           32 |                8 |          3720.00
 2026-11-18  |           27 |                8 |          2990.00
 2026-11-17  |           25 |                8 |          2515.00
```

Trino read the Parquet files listed in `_delta_log` that Spark wrote. No copy,
no export, no warehouse load.

---

## 5. The internals tour

### 5.1 Read the transaction log

```bash
./scripts/lake.sh inspect
```

```
table      : bronze.orders
location   : s3://lakehouse/bronze/orders
_delta_log : 9 entries

00000000000000000000.json  CREATE TABLE       commitInfo=1, metaData=1, protocol=1
00000000000000000001.json  SET TBLPROPERTIES  commitInfo=1, metaData=1
00000000000000000002.json  SET TBLPROPERTIES  commitInfo=1, metaData=1
00000000000000000003.json  WRITE              add=1, commitInfo=1
00000000000000000004.json  WRITE              add=1, commitInfo=1
00000000000000000005.checkpoint.parquet  <- reconciled table state
00000000000000000005.json  WRITE              add=1, commitInfo=1
_last_checkpoint  <- pointer to the newest checkpoint

_last_checkpoint: {"version":5,"size":5,"numOfAddFiles":3,...}
```

Each JSON file is one transaction. A version is visible only when its file
lands; that is the atomic switch.

### 5.2 Reconstruct a snapshot

```bash
./scripts/lake.sh snapshot
```

```
table                  : bronze.orders
requested version      : 5
active parquet files   : 3
active bytes           : 9,861
reconstruction         : replay add/remove actions from version 0
```

**What this proves:** the table is not a directory of files people read. It is
the set of files named by replaying the log (from the checkpoint forward).

### 5.3 Time travel

```bash
./scripts/lake.sh travel
```

```
== bronze.orders (latest v5)
version  rows  statuses
0        0
3        33    PAID=7, UNKNOWN=1, completed =1, CANCELLED=3, COMPLETED=14, ...
4        66    ...
5        99    ...

== silver.orders (latest v4)
version  rows
0        0
1        29
2        89
3        89
v0 -> v4: 0 ids removed, 89 ids added
```

Version 0 is empty; version 1 is batch only; version 2 adds streamed orders.
Bronze still shows the dirty statuses (`UNKNOWN`, `completed `) — Silver cleaned
them.

### 5.4 Change Data Feed

```bash
./scripts/lake.sh changes
```

```
order_id  order_status  _change_type  _commit_version  _commit_timestamp
1001      COMPLETED     insert        1                2026-11-19 22:43:...
1012      CREATED       update_postimage 3             ...
```

**What this proves:** row-level change history comes from the Delta log, not from
re-scanning the table.

### 5.5 Atomicity on purpose

```bash
./scripts/lake.sh atomicity
```

```
[atomicity] simulated: data files written, commit JSON never produced
  parquet files in table root : 3 -> 5
  Delta version               : 5 -> 5
  rows visible to readers     : 99 -> 99
[atomicity] PASS - uncommitted files are invisible to Delta
```

**What this proves:** a crashed writer can leave bytes behind; readers never see
them because no commit points at them.

### 5.6 Schema enforcement

```bash
./scripts/lake.sh schema
```

```
[schema] writing a row with an unknown column into silver.orders ...
[schema] REJECTED as expected: AnalysisException
[schema] version unchanged: 3 -> 3
```

### 5.7 Optimistic concurrency

```bash
./scripts/lake.sh concurrency
```

```
[occ] launching 2 concurrent writers against silver.orders
[occ] writer 0: COMMITTED
[occ] writer 1: COMMITTED
[occ] rows for customer 501 after both commits (last writer wins per row)
```

Both transactions commit as *separate* versions; Delta serialised them instead
of letting one overwrite the other's work. If the read windows overlap enough to
touch the same files, the loser gets a `Concurrent*Exception` printed instead of
`COMMITTED` — that is the OCC retry path.

### 5.8 Compaction and VACUUM

```bash
./scripts/lake.sh maintain
```

```
== bronze.orders (s3://lakehouse/bronze/orders) v5
  checkpointInterval=1 set
  OPTIMIZE -> files added=1, removed=4, total after=1
  version after compaction: 6
Deleted 5 files and directories in a total of 1 directories.
  VACUUM removed files older than 0 hours
```

The files deleted by VACUUM are the ones the atomicity demo staged, plus
superseded Parquet files. `_delta_log` stays; only unreferenced files go.

Re-run `./scripts/lake.sh inspect` and notice the new checkpoint — the log never shrinks, the
reconstruction just starts later.

---

## 6. Where to look while it runs

| Question | Where |
|---|---|
| Is the cluster healthy? | <http://localhost:8090> (master), <http://localhost:8091> (worker) |
| What did the job do? | Spark UI → SQL / Stages, driver UI on 4040 during a run |
| Which task failed and why? | Airflow → Grid → task → Logs |
| Which files are in the table? | `docker exec lake-s3 awslocal s3 ls s3://lakehouse/bronze/orders/ --recursive` |
| What does the catalog say? | `SHOW TABLES IN silver` in Spark, or `show tables` in Trino |
| Are the offsets committed? | `s3://lakehouse/checkpoints/bronze_order_events_v2/commits/` |
| What changed row by row? | `./scripts/lake.sh changes` (CDF) |

---

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Gold fails with `DELTA_PATH_DOES_NOT_EXIST` after `down`/`up` | the S3 emulator is not persistent, but the metastore kept the old entry | re-run the DAG: `register_catalog` drops stale entries and dbt recreates Gold |
| `gold_gold` schema in Trino | an older dbt config that prefixed the custom schema | current code writes to `gold`; `./scripts/lake.sh nuke` removes the leftover |
| `permission denied for table orders` | the seed ran as `postgres`, so the `orders` role had no SELECT | fixed in `docker/postgres/init/01-databases.sh`; on an existing volume run `GRANT SELECT ON ALL TABLES IN SCHEMA public TO orders;` |
| `pull access denied for shopazone/spark` | the project's images are built locally, never published | `./scripts/lake.sh build` — `up` now builds before starting |
| `No factory for location: s3://…` | Trino's native S3 file system not enabled | keep `fs.native-s3.enabled=true` in `docker/trino/catalog/delta.properties` |
| Spark job hangs in `WAITING`, master shows no free cores | each app took all cluster cores | every submission passes `spark.cores.max` (the DAG and Makefile do) |
| `Class org.apache.hadoop.fs.s3a.S3AFileSystem not found` in the metastore | metastore started through `RunJar` without hadoop-aws | the custom entrypoint launches the metastore with an explicit classpath |
| `Table … is not a Delta table` | catalog entry without a Delta log | `ensure_table` drops stale **external** entries and recreates the log first |
| dbt: `Database Error … could not connect` | Thrift server not up yet | `docker compose restart spark-thrift`, then re-run `dbt_run_gold` |
| dbt: `Airflow` module errors | dbt lives in `/opt/dbt-venv` | always call `/opt/dbt-venv/bin/dbt` |
| `PYTHON_VERSION_MISMATCH` | driver 3.11, workers 3.8 | jobs avoid Python UDFs; the DQ gate inserts results with SQL |
| Producer has no events | broker not ready when it started | `docker compose restart order-event-producer` |

---

## 8. Map back to the article

| Article section | Where to see it |
|---|---|
| Anatomy of a Delta table | `./scripts/lake.sh inspect`, S3 listing of `_delta_log/` and `*.parquet` |
| Delta Log Transaction Actions | `delta_inspect.py --table bronze.orders log --actions` |
| Snapshot reconstruction | `./scripts/lake.sh snapshot` |
| Checkpoints / `_last_checkpoint` | `./scripts/lake.sh inspect`, `numOfAddFiles` in `_last_checkpoint` |
| Transaction log vs checkpoint | both printed side by side by `./scripts/lake.sh inspect` |
| Atomicity | `./scripts/lake.sh atomicity` |
| Isolation / OCC | `./scripts/lake.sh concurrency` |
| Durability | the commit JSON is the switch; the local S3 emulator is ephemeral, so a restart can empty the lake — the next run repairs the catalog and rebuilds |
| INSERT / UPDATE / DELETE / MERGE | `ingest_batch_orders.py`, `build_silver.py` |
| Schema enforcement vs evolution | `./scripts/lake.sh schema` |
| Time travel | `./scripts/lake.sh travel` |
| Change Data Feed | `./scripts/lake.sh changes` |
| CDC vs CDF | Kafka producer (CDC-like events) vs `silver.orders` CDF |
| Ingestion layer | Postgres + Kafka |
| Storage layer | LocalStack S3 |
| Processing layer | Spark master/worker |
| Table management and catalog | Hive Metastore |
| Consumption layer | Trino |
| Orchestration | Airflow DAG |
| dbt | `dbt/models/gold` |
| Data quality | dbt tests + `quality_checks.py` |
| OPTIMIZE / VACUUM | `./scripts/lake.sh maintain` |
| Streaming | `stream_order_events.py` |
| Production considerations | `docs/ARCHITECTURE.md` |

---

## 9. Stop

```bash
./scripts/lake.sh down     # keeps data volumes
./scripts/lake.sh nuke     # deletes containers and volumes (fresh start)
```
