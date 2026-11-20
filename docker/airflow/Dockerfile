# Spark is shipped inside the Airflow image because Airflow is the spark-submit
# driver host (cluster mode against spark://spark-master:7077).
FROM apache/spark:3.5.4-python3 AS sparkdist

FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=sparkdist /opt/spark /opt/spark
COPY docker/spark/jars.sh /tmp/jars.sh
RUN bash /tmp/jars.sh && rm -f /tmp/jars.sh
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/opt/spark/bin:$PATH

# dbt runs from its own venv: dbt-core 1.11 needs protobuf>=6 while the Airflow
# 2.10 constraints pin 4.25.6. Keeping dbt isolated keeps both happy.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-spark[pyhive]==1.11.1" \
    && chown -R airflow:root /opt/dbt-venv

USER airflow

RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      "apache-airflow-providers-apache-spark" \
      "apache-airflow-providers-trino" \
      "pyspark" \
      "delta-spark==3.2.0"
