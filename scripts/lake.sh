#!/usr/bin/env bash
# Portable CLI for the lakehouse. Works anywhere Docker + bash exist (including
# Git Bash on Windows), where `make` may not be installed.
#
#   ./scripts/lake.sh up
#   ./scripts/lake.sh dag
#   ./scripts/lake.sh inspect
#
# The Makefile exposes the same commands.
set -euo pipefail

COMPOSE="docker compose"
SPARK_MASTER="spark://spark-master:7077"
JOBS="/opt/jobs"

usage() {
  cat <<'EOF'
Shop-A-Zone Delta lakehouse

  build          build the Spark, Airflow and metastore images
  up             start every container
  ps             container health
  logs           follow scheduler + streaming logs
  dag            trigger the pipeline
  status         per-task result of the latest run
  tasklog TASK   tail the newest task log (e.g. build_silver)
  spark-job JOB  run any job:  spark-job jobs/delta_inspect.py --table bronze.orders log

  inspect        _delta_log commit history (bronze.orders)
  snapshot       replay the log into an active-file snapshot
  history        Delta history (silver.orders)
  travel         time-travel row counts per version
  changes        Change Data Feed
  atomicity      staged-but-uncommitted write demo
  schema         schema enforcement demo
  concurrency    two concurrent writers (OCC)
  maintain       OPTIMIZE + checkpoint + VACUUM
  trino          query Gold through Trino
  events         follow the Kafka producer
  down           stop containers (data kept)
  nuke           stop and delete containers + volumes
EOF
}

spark_run() {
  $COMPOSE run --rm -T spark-client \
    "spark-submit --master ${SPARK_MASTER} --conf spark.cores.max=2 $*"
}

cmd="${1:-help}"
[ $# -gt 0 ] && shift

case "$cmd" in
  help|-h|--help) usage ;;
  up) $COMPOSE build && $COMPOSE up -d ;;
  build|down|nuke|ps|logs) $COMPOSE "$cmd" "$@" ;;
  dag)
    $COMPOSE exec -T airflow-scheduler airflow dags unpause shopazone_lakehouse
    $COMPOSE exec -T airflow-scheduler airflow dags trigger shopazone_lakehouse
    echo "open http://localhost:8080 -> shopazone_lakehouse"
    ;;
  trigger)
    $COMPOSE exec -T airflow-scheduler airflow dags trigger shopazone_lakehouse
    ;;
  status)
    RUN=$($COMPOSE exec -T airflow-scheduler airflow dags list-runs \
      -d shopazone_lakehouse -o plain | sed -n '2p' | awk '{print $2}')
    echo "run_id: $RUN"
    $COMPOSE exec -T airflow-scheduler airflow tasks states-for-dag-run \
      shopazone_lakehouse "$RUN"
    ;;
  tasklog)
    TASK="${1:?usage: lake.sh tasklog <task_id>}"
    $COMPOSE exec -T airflow-scheduler bash -c       "f=\$(ls -t /opt/airflow/logs/dag_id=shopazone_lakehouse/run_id=*/task_id=${TASK}/attempt=*.log 2>/dev/null | head -1);        [ -n \"\$f\" ] && tail -n 80 \"\$f\" || echo 'no log for task ${TASK} yet'"
    ;;
  spark-job)
    JOB="${1:?usage: lake.sh spark-job jobs/delta_inspect.py --table bronze.orders log}"
    shift
    spark_run "/opt/${JOB} $*"
    ;;
  inspect) spark_run "${JOBS}/delta_inspect.py --table bronze.orders log" ;;
  snapshot) spark_run "${JOBS}/delta_inspect.py --table bronze.orders snapshot" ;;
  history) spark_run "${JOBS}/delta_inspect.py --table silver.orders history" ;;
  travel) spark_run "${JOBS}/demo_time_travel.py" ;;
  changes) spark_run "${JOBS}/delta_inspect.py --table silver.orders changes" ;;
  atomicity) spark_run "${JOBS}/demo_atomicity.py" ;;
  schema) spark_run "${JOBS}/demo_schema_enforcement.py" ;;
  concurrency)
    $COMPOSE run --rm -T spark-client "spark-submit --master local[2] ${JOBS}/concurrency_demo.py"
    ;;
  maintain)
    spark_run "${JOBS}/maintenance.py --optimize --checkpoint --vacuum-hours 0 \
      --tables bronze.orders silver.orders"
    ;;
  trino)
    $COMPOSE exec -T trino trino --catalog delta --schema gold --execute \
      "select order_date, total_orders, active_customers, realized_revenue \
      from gold_daily_revenue order by order_date desc limit 10"
    ;;
  events) $COMPOSE logs -f --tail=20 order-event-producer ;;
  demo) "$0" atomicity && "$0" schema && "$0" travel && "$0" concurrency ;;
  *) echo "unknown command: $cmd" >&2; usage; exit 1 ;;
esac
