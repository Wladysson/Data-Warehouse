FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY configs /app/configs
COPY sql /app/sql

RUN mkdir -p \
    /app/data/raw/clicks \
    /app/data/raw/carts \
    /app/data/raw/orders \
    /app/data/raw/deliveries \
    /app/data/processed/clean \
    /app/data/processed/curated

CMD ["python3", "-m", "src.data_generator.gerador"]