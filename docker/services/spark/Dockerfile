FROM apache/spark:3.5.6-python3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
        software-properties-common \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        liblzma-dev \
        libncursesw5-dev \
        tk-dev \
        uuid-dev \
        wget \
    && rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.12.10

RUN curl -fsSL \
    https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
    -o /tmp/python.tgz \
    && tar -xzf /tmp/python.tgz -C /tmp \
    && cd /tmp/Python-${PYTHON_VERSION} \
    && ./configure --enable-optimizations --with-ensurepip=install \
    && make -j2 \
    && make altinstall \
    && rm -rf /tmp/python.tgz /tmp/Python-${PYTHON_VERSION}

RUN ln -sf /usr/local/bin/python3.12 /usr/local/bin/python3 \
    && ln -sf /usr/local/bin/python3.12 /usr/local/bin/python \
    && ln -sf /usr/local/bin/pip3.12 /usr/local/bin/pip3 \
    && python3 --version \
    && pip3 --version

RUN pip3 install --no-cache-dir \
    PyHive \
    thrift

COPY configs/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY configs/spark/hive-site.xml /opt/spark/conf/hive-site.xml
COPY configs/spark/log4j.properties /opt/spark/conf/log4j.properties

COPY src /app/src

RUN mkdir -p \
    /app/data/raw \
    /app/data/processed

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=/usr/local/bin/python3.12 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

WORKDIR /app

USER 185

EXPOSE 4040 7077 8080

CMD ["bash", "-c", "sleep infinity"]