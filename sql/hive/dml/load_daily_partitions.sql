USE ecommerce_staging;

MSCK REPAIR TABLE external_raw_events;

MSCK REPAIR TABLE external_clean_events;

MSCK REPAIR TABLE external_curated_events;

MSCK REPAIR TABLE external_streaming_metrics;

MSCK REPAIR TABLE external_streaming_alerts;

USE ecommerce;

MSCK REPAIR TABLE fact_sales;

MSCK REPAIR TABLE fact_clicks;

MSCK REPAIR TABLE fact_carts;

MSCK REPAIR TABLE fact_deliveries;