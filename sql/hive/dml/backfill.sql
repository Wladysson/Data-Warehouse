USE ecommerce;

INSERT OVERWRITE TABLE fact_sales PARTITION (event_date)
SELECT
    event_id,
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    status,
    event_timestamp,
    ingestion_timestamp,
    session_id,
    CAST(event_timestamp AS DATE) AS event_date
FROM ecommerce_staging.external_curated_events
WHERE event_type = 'ORDER'
  AND order_id IS NOT NULL
  AND event_timestamp IS NOT NULL;

INSERT OVERWRITE TABLE fact_clicks PARTITION (event_date)
SELECT
    event_id,
    customer_id,
    product_id,
    session_id,
    page,
    action,
    event_timestamp,
    ingestion_timestamp,
    CAST(event_timestamp AS DATE) AS event_date
FROM ecommerce_staging.external_curated_events
WHERE event_type = 'CLICK'
  AND event_timestamp IS NOT NULL;

INSERT OVERWRITE TABLE fact_carts PARTITION (event_date)
SELECT
    event_id,
    cart_id,
    customer_id,
    product_id,
    session_id,
    quantity,
    action,
    event_timestamp,
    ingestion_timestamp,
    CAST(event_timestamp AS DATE) AS event_date
FROM ecommerce_staging.external_curated_events
WHERE event_type = 'CART'
  AND event_timestamp IS NOT NULL;

INSERT OVERWRITE TABLE fact_deliveries PARTITION (event_date)
SELECT
    event_id,
    order_id,
    delivery_id,
    customer_id,
    status,
    carrier,
    estimated_delivery,
    actual_delivery,
    event_timestamp,
    ingestion_timestamp,
    CAST(event_timestamp AS DATE) AS event_date
FROM ecommerce_staging.external_curated_events
WHERE event_type = 'DELIVERY'
  AND event_timestamp IS NOT NULL;