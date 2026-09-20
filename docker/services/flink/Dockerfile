FROM flink:1.20.3-java11

ARG HADOOP_VERSION=3.3.6

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
        python3 \
        python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm /tmp/hadoop.tar.gz

RUN pip3 install --no-cache-dir \
    apache-flink==1.20.3

COPY configs/flink/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY configs/flink/log4j2.xml /opt/flink/conf/log4j2.xml

COPY configs/hadoop/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY configs/hadoop/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml

COPY src /app/src

RUN mkdir -p \
    /app/data/raw \
    /app/data/processed \
    /opt/flink/checkpoints \
    /opt/flink/savepoints \
    && chown -R flink:flink \
        /app/data \
        /opt/flink/checkpoints \
        /opt/flink/savepoints

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/hdfs/lib/* \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

WORKDIR /app

USER flink

EXPOSE 8081 6123 6124

CMD ["bash", "-c", "start-cluster.sh && tail -f /dev/null"]