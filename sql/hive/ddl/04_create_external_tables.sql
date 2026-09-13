USE ecommerce_staging;

CREATE EXTERNAL TABLE IF NOT EXISTS external_raw_events (
    event_id              STRING,
    event_type            STRING,
    event_timestamp       TIMESTAMP,
    ingestion_timestamp   TIMESTAMP,
    customer_id           STRING,
    session_id            STRING,
    metadata              MAP<STRING, STRING>,
    product_id            STRING,
    page                  STRING,
    action                STRING,
    cart_id               STRING,
    quantity              INT,
    order_id              STRING,
    unit_price            DECIMAL(18,2),
    total_amount          DECIMAL(18,2),
    status                STRING,
    delivery_id           STRING,
    carrier               STRING,
    estimated_delivery    TIMESTAMP,
    actual_delivery       TIMESTAMP
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Tabela externa para eventos brutos armazenados no HDFS'
STORED AS PARQUET
LOCATION '/data/raw/events';

CREATE EXTERNAL TABLE IF NOT EXISTS external_clean_events (
    event_id              STRING,
    event_type            STRING,
    event_timestamp       TIMESTAMP,
    ingestion_timestamp   TIMESTAMP,
    customer_id           STRING,
    session_id            STRING,
    metadata              MAP<STRING, STRING>,
    product_id            STRING,
    page                  STRING,
    action                STRING,
    cart_id               STRING,
    quantity              INT,
    order_id              STRING,
    unit_price            DECIMAL(18,2),
    total_amount          DECIMAL(18,2),
    status                STRING,
    delivery_id           STRING,
    carrier               STRING,
    estimated_delivery    TIMESTAMP,
    actual_delivery       TIMESTAMP
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Tabela externa para eventos normalizados da camada Clean'
STORED AS PARQUET
LOCATION '/data/processed/clean';

CREATE EXTERNAL TABLE IF NOT EXISTS external_curated_events (
    event_id                 STRING,
    event_type               STRING,
    event_timestamp          TIMESTAMP,
    ingestion_timestamp      TIMESTAMP,
    customer_id              STRING,
    session_id               STRING,
    product_id               STRING,
    page                     STRING,
    action                   STRING,
    cart_id                  STRING,
    quantity                 INT,
    order_id                 STRING,
    unit_price               DECIMAL(18,2),
    total_amount              DECIMAL(18,2),
    status                   STRING,
    delivery_id              STRING,
    carrier                  STRING,
    estimated_delivery       TIMESTAMP,
    actual_delivery          TIMESTAMP,
    event_year               INT,
    event_month              INT,
    event_day                INT,
    event_hour               INT,
    event_delay_seconds      DOUBLE,
    is_late_event            BOOLEAN
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Tabela externa para dados curados destinados à análise'
STORED AS PARQUET
LOCATION '/data/processed/curated';

CREATE EXTERNAL TABLE IF NOT EXISTS external_streaming_metrics (
    event_type             STRING,
    aggregation_key        STRING,
    window_start            TIMESTAMP,
    window_end              TIMESTAMP,
    event_count             BIGINT,
    metric_value            DOUBLE,
    processing_timestamp    TIMESTAMP
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Resultados de processamento streaming disponibilizados no HDFS'
STORED AS PARQUET
LOCATION '/data/streaming/metrics';

CREATE EXTERNAL TABLE IF NOT EXISTS external_streaming_alerts (
    alert_id                STRING,
    alert_type              STRING,
    severity                STRING,
    message                 STRING,
    event_id                STRING,
    event_type              STRING,
    alert_timestamp         TIMESTAMP,
    metadata                MAP<STRING, STRING>
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Alertas produzidos pelo processamento de streaming'
STORED AS PARQUET
LOCATION '/data/streaming/alerts';