USE ecommerce;

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk        BIGINT,
    customer_id        STRING,
    name               STRING,
    email              STRING,
    registration_date  TIMESTAMP,
    segment            STRING,
    metadata           MAP<STRING, STRING>
)
COMMENT 'Dimensão de clientes da plataforma de e-commerce'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/dim_customer';

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk       BIGINT,
    product_id       STRING,
    name             STRING,
    category_id      STRING,
    brand            STRING,
    price            DECIMAL(18,2),
    stock_quantity   BIGINT,
    metadata         MAP<STRING, STRING>
)
COMMENT 'Dimensão de produtos da plataforma de e-commerce'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/dim_product';

CREATE TABLE IF NOT EXISTS dim_category (
    category_sk     BIGINT,
    category_id     STRING,
    name            STRING,
    description     STRING,
    parent_category_id STRING,
    metadata        MAP<STRING, STRING>
)
COMMENT 'Dimensão hierárquica de categorias de produtos'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/dim_category';

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk          INT,
    calendar_date    DATE,
    year             INT,
    quarter          INT,
    month            INT,
    month_name       STRING,
    week_of_year     INT,
    day_of_month     INT,
    day_of_week      INT,
    day_name         STRING,
    is_weekend       BOOLEAN,
    is_month_start   BOOLEAN,
    is_month_end     BOOLEAN
)
COMMENT 'Dimensão calendário para análises temporais'
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce/dim_date';