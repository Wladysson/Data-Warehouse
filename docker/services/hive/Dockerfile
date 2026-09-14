FROM eclipse-temurin:11-jre

ARG HIVE_VERSION=4.0.1
ARG HADOOP_VERSION=3.3.6

ENV HIVE_HOME=/opt/hive \
    HIVE_CONF_DIR=/opt/hive/conf \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hive/bin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
        python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
    && rm /tmp/hadoop.tar.gz

COPY configs/hadoop/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY configs/hadoop/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml

RUN curl -fsSL \
    "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
    -o /tmp/hive.tar.gz \
    && tar -xzf /tmp/hive.tar.gz -C /opt \
    && mv "/opt/apache-hive-${HIVE_VERSION}-bin" "${HIVE_HOME}" \
    && rm /tmp/hive.tar.gz

COPY configs/hive/hive-site.xml ${HIVE_CONF_DIR}/hive-site.xml
COPY configs/hive/hive-env.sh ${HIVE_CONF_DIR}/hive-env.sh

RUN chmod +x ${HIVE_CONF_DIR}/hive-env.sh \
    && mkdir -p \
        /opt/hive/data \
        /opt/hive/logs \
        /opt/hive/run \
        /opt/hive/tmp \
        /opt/hive/warehouse

WORKDIR /opt/hive

EXPOSE 10000 10002

CMD ["hiveserver2"]