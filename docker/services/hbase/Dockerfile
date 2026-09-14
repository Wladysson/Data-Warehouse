FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.5.10

ENV HBASE_HOME=/opt/hbase \
    HBASE_CONF_DIR=/opt/hbase/conf \
    PATH=/opt/hbase/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" \
    -o /tmp/hbase.tar.gz \
    && tar -xzf /tmp/hbase.tar.gz -C /opt \
    && mv "/opt/hbase-${HBASE_VERSION}" "${HBASE_HOME}" \
    && rm /tmp/hbase.tar.gz

COPY configs/hbase/hbase-site.xml ${HBASE_CONF_DIR}/hbase-site.xml
COPY configs/hbase/hbase-env.sh ${HBASE_CONF_DIR}/hbase-env.sh

RUN chmod +x ${HBASE_CONF_DIR}/hbase-env.sh \
    && mkdir -p /opt/hbase/data

EXPOSE 16000 16010 16020 9090

CMD ["hbase", "master", "start"]