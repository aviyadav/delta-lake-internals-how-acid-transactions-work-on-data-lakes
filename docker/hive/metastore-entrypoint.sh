#!/bin/sh
# The stock image starts the metastore through Hadoop's RunJar, which only honours
# the manifest class-path of hive-metastore.jar. That hides hadoop-aws, so the
# metastore cannot resolve s3:// table locations. Launching the metastore with an
# explicit classpath (Hive + Hadoop) makes the S3A filesystem visible to it.
set -e

if ! /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "[metastore] initialising schema"
  /opt/hive/bin/schematool -dbType postgres -initSchema
fi

CP="/opt/hive/lib/*:/opt/hive/conf:/opt/hadoop/etc/hadoop"
for dir in /opt/hadoop/share/hadoop/*/ /opt/hadoop/share/hadoop/*/lib/; do
  CP="${CP}:${dir}*"
done

exec java -Xmx2G -cp "$CP" \
  -Dhive.log.dir=/tmp/hive-logs \
  -Dlog4j2.formatMsgNoLookups=true \
  org.apache.hadoop.hive.metastore.HiveMetaStore
