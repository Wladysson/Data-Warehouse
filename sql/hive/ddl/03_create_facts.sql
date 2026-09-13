USE ecommerce;

CREATE TABLE IF NOT EXISTS fact_sales (
    event_id             STRING,
    order_id             STRING,
    customer_id          STRING,
    product_id           STRING,
    quantity             INT,
    unit_price           DECIMAL(18,2),
    total_amount         DECIMAL(18,2),
    status               STRING,
    event_timestamp      TIMESTAMP,
    ingestion_timestamp  TIMESTAMP,
    session_id           STRING
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Fato de vendas e pedidos da plataforma de e-commerce'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/fact_sales';

CREATE TABLE IF NOT EXISTS fact_clicks (
    event_id             STRING,
    customer_id          STRING,
    product_id           STRING,
    session_id           STRING,
    page                 STRING,
    action               STRING,
    event_timestamp      TIMESTAMP,
    ingestion_timestamp  TIMESTAMP
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Fato de interações e cliques dos clientes'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/fact_clicks';

CREATE TABLE IF NOT EXISTS fact_carts (
    event_id             STRING,
    cart_id              STRING,
    customer_id          STRING,
    product_id           STRING,
    session_id           STRING,
    quantity             INT,
    action               STRING,
    event_timestamp      TIMESTAMP,
    ingestion_timestamp  TIMESTAMP
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Fato de eventos e alterações dos carrinhos de compras'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/fact_carts';

CREATE TABLE IF NOT EXISTS fact_deliveries (
    event_id              STRING,
    order_id              STRING,
    delivery_id           STRING,
    customer_id           STRING,
    status                STRING,
    carrier                STRING,
    estimated_delivery    TIMESTAMP,
    actual_delivery      TIMESTAMP,
    event_timestamp       TIMESTAMP,
    ingestion_timestamp   TIMESTAMP
)
PARTITIONED BY (
    event_date DATE
)
COMMENT 'Fato de eventos e indicadores logísticos das entregas'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/fact_deliveries';