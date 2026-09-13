USE ecommerce_analytics;

CREATE OR REPLACE VIEW vw_sales_daily AS
SELECT
    event_date,
    COUNT(*) AS total_sales,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_items,
    SUM(total_amount) AS gross_revenue,
    AVG(total_amount) AS average_order_value
FROM ecommerce.fact_sales
WHERE status = 'COMPLETED'
GROUP BY event_date;

CREATE OR REPLACE VIEW vw_sales_by_product AS
SELECT
    s.event_date,
    s.product_id,
    p.name AS product_name,
    p.category_id,
    c.name AS category_name,
    SUM(s.quantity) AS total_quantity,
    SUM(s.total_amount) AS total_revenue,
    COUNT(DISTINCT s.order_id) AS total_orders
FROM ecommerce.fact_sales s
LEFT JOIN ecommerce.dim_product p
    ON s.product_id = p.product_id
LEFT JOIN ecommerce.dim_category c
    ON p.category_id = c.category_id
WHERE s.status = 'COMPLETED'
GROUP BY
    s.event_date,
    s.product_id,
    p.name,
    p.category_id,
    c.name;

CREATE OR REPLACE VIEW vw_customer_sales AS
SELECT
    s.customer_id,
    c.name AS customer_name,
    c.segment,
    COUNT(DISTINCT s.order_id) AS total_orders,
    SUM(s.quantity) AS total_items,
    SUM(s.total_amount) AS total_spent,
    AVG(s.total_amount) AS average_order_value,
    MIN(s.event_date) AS first_purchase_date,
    MAX(s.event_date) AS last_purchase_date
FROM ecommerce.fact_sales s
LEFT JOIN ecommerce.dim_customer c
    ON s.customer_id = c.customer_id
WHERE s.status = 'COMPLETED'
GROUP BY
    s.customer_id,
    c.name,
    c.segment;

CREATE OR REPLACE VIEW vw_customer_behavior AS
SELECT
    customer_id,
    COUNT(*) AS total_clicks,
    COUNT(DISTINCT product_id) AS unique_products_clicked,
    COUNT(DISTINCT session_id) AS total_sessions
FROM ecommerce.fact_clicks
GROUP BY customer_id;

CREATE OR REPLACE VIEW vw_cart_behavior AS
SELECT
    customer_id,
    COUNT(DISTINCT cart_id) AS total_carts,
    SUM(quantity) AS total_items_added,
    COUNT(*) AS total_cart_events
FROM ecommerce.fact_carts
GROUP BY customer_id;

CREATE OR REPLACE VIEW vw_delivery_performance AS
SELECT
    event_date,
    carrier,
    COUNT(*) AS total_deliveries,
    SUM(
        CASE
            WHEN actual_delivery IS NOT NULL
                 AND estimated_delivery IS NOT NULL
                 AND actual_delivery <= estimated_delivery
            THEN 1
            ELSE 0
        END
    ) AS deliveries_on_time,
    SUM(
        CASE
            WHEN actual_delivery IS NOT NULL
                 AND estimated_delivery IS NOT NULL
                 AND actual_delivery > estimated_delivery
            THEN 1
            ELSE 0
        END
    ) AS deliveries_delayed
FROM ecommerce.fact_deliveries
GROUP BY
    event_date,
    carrier;

CREATE OR REPLACE VIEW vw_sales_delivery AS
SELECT
    s.order_id,
    s.customer_id,
    s.product_id,
    s.event_date AS sale_date,
    s.total_amount,
    d.delivery_id,
    d.status AS delivery_status,
    d.carrier,
    d.estimated_delivery,
    d.actual_delivery
FROM ecommerce.fact_sales s
LEFT JOIN ecommerce.fact_deliveries d
    ON s.order_id = d.order_id;