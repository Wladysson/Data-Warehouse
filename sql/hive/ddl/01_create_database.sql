CREATE DATABASE IF NOT EXISTS ecommerce
COMMENT 'Data Warehouse principal da pipeline de e-commerce'
LOCATION '/data/warehouse/ecommerce';

CREATE DATABASE IF NOT EXISTS ecommerce_staging
COMMENT 'Camada de staging para dados intermediários do processamento'
LOCATION '/data/warehouse/ecommerce_staging';

CREATE DATABASE IF NOT EXISTS ecommerce_analytics
COMMENT 'Camada analítica para consultas e indicadores consolidados'
LOCATION '/data/warehouse/ecommerce_analytics';

-- Define o Data Warehouse principal como banco de trabalho.
USE ecommerce;