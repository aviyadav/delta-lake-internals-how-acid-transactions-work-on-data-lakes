#!/bin/sh
# Runs once, on first Postgres boot (empty data volume).
set -e

for db in airflow metastore orders; do
  echo "creating database $db"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -c "CREATE DATABASE $db"
done

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD 'airflow';
CREATE ROLE hive LOGIN PASSWORD 'hive';
CREATE ROLE orders LOGIN PASSWORD 'orders';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
GRANT ALL PRIVILEGES ON DATABASE orders TO orders;
SQL

# Postgres 15+ revokes CREATE on schema public from non-owners; Hive/dbt/Airflow
# all create their own tables, so hand them the schema explicitly.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname metastore \
  -c "ALTER SCHEMA public OWNER TO hive; GRANT ALL ON SCHEMA public TO hive;"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow \
  -c "ALTER SCHEMA public OWNER TO airflow; GRANT ALL ON SCHEMA public TO airflow;"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname orders \
  -c "ALTER SCHEMA public OWNER TO orders; GRANT ALL ON SCHEMA public TO orders;"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname orders -f /docker-entrypoint-initdb.d/02-orders.sql

# The seed script runs as the superuser, so the tables belong to postgres.
# The pipeline connects as the "orders" role and must be able to read them.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname orders <<'SQL'
GRANT SELECT ON ALL TABLES IN SCHEMA public TO orders;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO orders;
SQL
