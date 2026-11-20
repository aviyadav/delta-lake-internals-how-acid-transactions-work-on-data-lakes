FROM apache/hive:4.0.0

USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hive/lib/postgresql-42.7.3.jar
# The metastore validates table locations, so it needs the S3A filesystem too.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.3.jar /opt/hive/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar
COPY docker/hive/metastore-entrypoint.sh /opt/hive/metastore-entrypoint.sh
RUN chmod +x /opt/hive/metastore-entrypoint.sh

USER hive
