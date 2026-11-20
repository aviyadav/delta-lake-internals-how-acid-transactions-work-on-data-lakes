FROM apache/spark:3.5.4-python3

USER root

COPY docker/spark/jars.sh /tmp/jars.sh
RUN bash /tmp/jars.sh && rm -f /tmp/jars.sh

# delta-spark installs the `delta` Python module used by the jobs.
RUN pip install --no-cache-dir --no-deps "delta-spark==3.2.0"

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /opt/jobs /opt/data && chown -R spark:spark /opt/jobs /opt/data

ENV PATH=/opt/spark/bin:$PATH

USER spark
