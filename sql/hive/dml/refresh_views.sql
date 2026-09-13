USE ecommerce;

ANALYZE TABLE fact_sales
COMPUTE STATISTICS;

ANALYZE TABLE fact_clicks
COMPUTE STATISTICS;

ANALYZE TABLE fact_carts
COMPUTE STATISTICS;

ANALYZE TABLE fact_deliveries
COMPUTE STATISTICS;

ANALYZE TABLE dim_customer
COMPUTE STATISTICS;

ANALYZE TABLE dim_product
COMPUTE STATISTICS;

ANALYZE TABLE dim_category
COMPUTE STATISTICS;

ANALYZE TABLE dim_date
COMPUTE STATISTICS;

USE ecommerce_analytics;

SELECT *
FROM vw_sales_daily
LIMIT 1;

SELECT *
FROM vw_sales_by_product
LIMIT 1;

SELECT *
FROM vw_customer_sales
LIMIT 1;

SELECT *
FROM vw_customer_behavior
LIMIT 1;

SELECT *
FROM vw_cart_behavior
LIMIT 1;

SELECT *
FROM vw_delivery_performance
LIMIT 1;

SELECT *
FROM vw_sales_delivery
LIMIT 1;