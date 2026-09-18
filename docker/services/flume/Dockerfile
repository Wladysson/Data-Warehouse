FROM eclipse-temurin:11-jre

ARG FLUME_VERSION=1.11.0
ARG HADOOP_VERSION=3.3.6

ENV FLUME_HOME=/opt/flume \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/flume/bin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH \
    FLUME_CLASSPATH=/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/hdfs/lib/*

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
    "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" \
    -o /tmp/flume.tar.gz \
    && tar -xzf /tmp/flume.tar.gz -C /opt \
    && mv "/opt/apache-flume-${FLUME_VERSION}-bin" "${FLUME_HOME}" \
    && rm /tmp/flume.tar.gz

RUN sed -i 's/^JAVA_OPTS="-Xmx20m"/JAVA_OPTS="-Xms256m -Xmx512m"/' \
    ${FLUME_HOME}/bin/flume-ng

RUN mkdir -p \
    /opt/flume/conf \
    /opt/flume/checkpoint \
    /opt/flume/data \
    /app/data/raw

COPY configs/flume/flume-conf.properties \
    /opt/flume/conf/flume-conf.properties

COPY configs/flume/flume-env.sh \
    /opt/flume/conf/flume-env.sh

RUN chmod +x /opt/flume/conf/flume-env.sh

WORKDIR /app

CMD ["/opt/flume/bin/flume-ng", "agent", "-n", "agent", "-c", "/opt/flume/conf", "-f", "/opt/flume/conf/flume-conf.properties", "-Dflume.root.logger=INFO,console"]