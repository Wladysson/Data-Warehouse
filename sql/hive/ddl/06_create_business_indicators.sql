CREATE DATABASE IF NOT EXISTS ecommerce_analytics
COMMENT 'Indicadores de negócio consolidados do e-commerce'
LOCATION '/data/warehouse/ecommerce_analytics';

USE ecommerce_analytics;

CREATE TABLE IF NOT EXISTS top_clicked_products (
    product_id        STRING,
    click_count        BIGINT,
    unique_customers  BIGINT,
    unique_sessions    BIGINT
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/top_clicked_products';

CREATE TABLE IF NOT EXISTS top_cart_products (
    product_id        STRING,
    cart_event_count  BIGINT,
    unique_carts      BIGINT,
    unique_customers  BIGINT,
    total_quantity    BIGINT
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/top_cart_products';

CREATE TABLE IF NOT EXISTS orders_by_status (
    status                STRING,
    order_count           BIGINT,
    total_revenue         DOUBLE,
    average_order_value  DOUBLE
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/orders_by_status';

CREATE TABLE IF NOT EXISTS deliveries_by_status (
    status                STRING,
    delivery_event_count  BIGINT,
    unique_deliveries    BIGINT,
    unique_orders        BIGINT,
    average_delay_hours  DOUBLE
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/deliveries_by_status';

CREATE TABLE IF NOT EXISTS daily_sales_summary (
    event_date            DATE,
    total_orders          BIGINT,
    total_revenue         DOUBLE,
    average_order_value  DOUBLE,
    minimum_order_value  DOUBLE,
    maximum_order_value  DOUBLE,
    total_items_sold      BIGINT
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/daily_sales_summary';

CREATE TABLE IF NOT EXISTS sales_by_delivery_status (
    delivery_status      STRING,
    sales_count          BIGINT,
    total_sales_amount  DOUBLE,
    average_sale_amount  DOUBLE
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/sales_by_delivery_status';

CREATE TABLE IF NOT EXISTS conversion_funnel (
    product_id                STRING,
    clicks                    BIGINT,
    cart_adds                BIGINT,
    orders                    BIGINT,
    revenue                    DOUBLE,
    click_to_cart_rate        DOUBLE,
    cart_to_order_rate        DOUBLE,
    overall_conversion_rate  DOUBLE
)
STORED AS PARQUET
LOCATION '/data/warehouse/ecommerce_analytics/conversion_funnel';