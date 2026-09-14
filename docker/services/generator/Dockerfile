FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "pydantic>=2.10,<3.0" \
    "python-dotenv>=1.0,<2.0" \
    "PyYAML>=6.0,<7.0"

COPY src /app/src

RUN mkdir -p \
    /app/data/raw/clicks \
    /app/data/raw/carts \
    /app/data/raw/orders \
    /app/data/raw/deliveries

CMD ["python3", "-m", "src.data_generator.gerador"]