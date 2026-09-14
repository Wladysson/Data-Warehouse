FROM flink:1.20.3-java11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
        python3 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    apache-flink==1.20.3

COPY configs/flink/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY configs/flink/log4j2.xml /opt/flink/conf/log4j2.xml

COPY src /app/src

RUN mkdir -p \
    /app/data/raw \
    /app/data/processed

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

WORKDIR /app

USER flink

EXPOSE 8081 6123 6124

CMD ["bash", "-c", "start-cluster.sh && tail -f /dev/null"]