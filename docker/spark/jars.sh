#!/usr/bin/env bash
# Bake the exact connector set into $SPARK_HOME/jars so driver and executors share
# one classpath without downloading from Maven at runtime.
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
JARS="$SPARK_HOME/jars"
mkdir -p "$JARS"

fetch() {
  local url="$1" target="$JARS/$(basename "$1")"
  echo "==> $target"
  curl -fsSL --retry 5 --retry-delay 3 -o "$target" "$url"
}

# Delta Lake 3.2.0 -> Spark 3.5.x
fetch https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
fetch https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# S3A file system (Spark 3.5.3 ships Hadoop 3.3.4)
fetch https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fetch https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Spark Structured Streaming <-> Kafka
fetch https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar
fetch https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar
fetch https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
fetch https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# JDBC source for the batch ingestion job
fetch https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

ls -1 "$JARS" | grep -E 'delta|s3a|hadoop-aws|kafka|postgresql|aws-java-sdk' || true
