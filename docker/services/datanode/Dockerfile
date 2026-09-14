FROM eclipse-temurin:11-jre

ARG HADOOP_VERSION=3.3.6

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
    && rm /tmp/hadoop.tar.gz

COPY configs/hadoop/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY configs/hadoop/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY configs/hadoop/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
COPY configs/hadoop/yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml

RUN mkdir -p /hadoop/dfs/data

EXPOSE 9864

CMD ["hdfs", "datanode"]