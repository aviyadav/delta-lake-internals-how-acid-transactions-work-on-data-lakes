SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help build up down nuke ps logs dag status inspect snapshot history travel changes \
        atomicity schema concurrency maintain trino trigger events demo spark-job

help:
	@echo "Shop-A-Zone Delta lakehouse"
	@echo ""
	@echo "  make build       build the Spark, Airflow and metastore images"
	@echo "  make up          start the whole platform (detached)"
	@echo "  make ps          show container health"
	@echo "  make dag         trigger the pipeline (watch it in the Airflow UI)"
	@echo "  make status      per-task result of the latest run"
	@echo "  make logs        follow the scheduler and streaming logs"
	@echo ""
	@echo "  make inspect     print the _delta_log commit history"
	@echo "  make snapshot    replay the log into an active-file snapshot"
	@echo "  make history     Delta table history"
	@echo "  make travel      time-travel row counts per version"
	@echo "  make changes     Change Data Feed for recent commits"
	@echo "  make atomicity   staged-but-uncommitted write demo"
	@echo "  make schema      schema enforcement demo"
	@echo "  make concurrency two concurrent writers (optimistic concurrency)"
	@echo "  make maintain    OPTIMIZE + checkpoint + VACUUM"
	@echo "  make demo        run the three ACID demos in sequence"
	@echo ""
	@echo "  make trino       query Gold through Trino"
	@echo "  make events      follow the Kafka order-event producer"
	@echo "  make down        stop containers (keep data)"
	@echo "  make nuke        stop and delete containers + volumes (all data lost)"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) build
	$(COMPOSE) up -d
	@echo "airflow: http://localhost:8080 (admin/admin)   trino: http://localhost:8085"

down:
	$(COMPOSE) down

nuke:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}"

logs:
	$(COMPOSE) logs -f --tail=50 airflow-scheduler stream-bronze

dag:
	$(COMPOSE) exec -T airflow-scheduler airflow dags unpause shopazone_lakehouse
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger shopazone_lakehouse
	@echo "open http://localhost:8080 -> shopazone_lakehouse, or run: make status"

status:
	@RUN=$$($(COMPOSE) exec -T airflow-scheduler airflow dags list-runs \
		-d shopazone_lakehouse -o plain | sed -n '2p' | awk '{print $$2}'); \
	echo "run_id: $$RUN"; \
	$(COMPOSE) exec -T airflow-scheduler airflow tasks states-for-dag-run \
		shopazone_lakehouse "$$RUN"

trigger:
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger shopazone_lakehouse

spark-job:
	@test -n "$(JOB)" || (echo "usage: make spark-job JOB=jobs/delta_inspect.py ARGS='--table bronze.orders log'"; exit 1)
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 /opt/$(JOB) $(ARGS)"

inspect:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/delta_inspect.py --table bronze.orders log"

snapshot:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/delta_inspect.py --table bronze.orders snapshot"

history:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/delta_inspect.py --table silver.orders history"

travel:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/demo_time_travel.py"

changes:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/delta_inspect.py --table silver.orders changes"

atomicity:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/demo_atomicity.py"

schema:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/demo_schema_enforcement.py"

concurrency:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master local[2] /opt/jobs/concurrency_demo.py"

demo: atomicity schema travel concurrency

maintain:
	$(COMPOSE) run --rm -T spark-client \
		"spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 \
		/opt/jobs/maintenance.py --optimize --checkpoint --vacuum-hours 0 \
		--tables bronze.orders silver.orders"

trino:
	$(COMPOSE) exec -T trino trino --catalog delta --schema gold --execute \
		"select order_date, total_orders, active_customers, realized_revenue \
		from gold_daily_revenue order by order_date desc limit 10"

events:
	$(COMPOSE) logs -f --tail=20 order-event-producer
