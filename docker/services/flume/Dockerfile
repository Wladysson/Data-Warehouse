FROM eclipse-temurin:11-jre

ARG FLUME_VERSION=1.11.0

ENV FLUME_HOME=/opt/flume \
    PATH=/opt/flume/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
        python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" \
    -o /tmp/flume.tar.gz \
    && tar -xzf /tmp/flume.tar.gz -C /opt \
    && mv "/opt/apache-flume-${FLUME_VERSION}-bin" "${FLUME_HOME}" \
    && rm /tmp/flume.tar.gz

RUN mkdir -p \
    /opt/flume/conf \
    /app/data/raw

COPY configs/flume/flume-conf.properties /opt/flume/conf/flume-conf.properties
COPY configs/flume/flume-env.sh /opt/flume/conf/flume-env.sh

WORKDIR /app

EXPOSE 41414

CMD ["flume-ng", "agent", "--conf", "/opt/flume/conf", "--conf-file", "/opt/flume/conf/flume-conf.properties", "--name", "agent", "-Dflume.root.logger=INFO,console"]