FROM eclipse-temurin:11-jre

ARG HIVE_VERSION=4.0.1

ENV HIVE_HOME=/opt/hive \
    HIVE_CONF_DIR=/opt/hive/conf \
    HADOOP_HOME=/opt/hadoop \
    PATH=/opt/hive/bin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
        python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
    -o /tmp/hive.tar.gz \
    && tar -xzf /tmp/hive.tar.gz -C /opt \
    && mv "/opt/apache-hive-${HIVE_VERSION}-bin" "${HIVE_HOME}" \
    && rm /tmp/hive.tar.gz

COPY configs/hive/hive-site.xml ${HIVE_CONF_DIR}/hive-site.xml
COPY configs/hive/hive-env.sh ${HIVE_CONF_DIR}/hive-env.sh

RUN chmod +x ${HIVE_CONF_DIR}/hive-env.sh \
    && mkdir -p /opt/hive/data \
    && mkdir -p /opt/hive/logs

EXPOSE 10000 10002

CMD ["hive", "--service", "hiveserver2"]