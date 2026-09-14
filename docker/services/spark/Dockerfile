FROM apache/spark:3.5.6-python3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY configs/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY configs/spark/hive-site.xml /opt/spark/conf/hive-site.xml
COPY configs/spark/log4j.properties /opt/spark/conf/log4j.properties

COPY src /app/src

RUN mkdir -p \
    /app/data/raw \
    /app/data/processed

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

WORKDIR /app

USER 185

EXPOSE 4040 7077 8080

CMD ["bash", "-c", "sleep infinity"]