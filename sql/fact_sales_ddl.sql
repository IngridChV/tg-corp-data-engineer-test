-- =====================================================================
-- TG CORP - MÓDULO 3.2: SINTAXIS DDL PARA FACT_SALES CON OPTIMIZACIONES
-- =====================================================================
--
-- Descripción:
-- DDL para crear la tabla de hechos `fact_sales` (500M+ registros)
-- aplicando las mejores prácticas de GCP BigQuery para optimizar costos
-- y slot-time:
--
-- 1. Particionamiento:
--    Particionamos por `sale_date` (tipo DATE). Esto segmenta los datos 
--    en bloques diarios. Al filtrar por rangos de fecha, BigQuery realiza
--    "Partition Pruning" (ignora particiones fuera del rango), reduciendo 
--    drásticamente la cantidad de bytes leídos y el costo de la consulta.
--
-- 2. Clustering:
--    Agrupamos (Clustering) por `region_id` y `customer_segment`.
--    Esto pre-ordena los datos físicamente dentro de cada partición diaria.
--    Al agrupar o filtrar por estas columnas, BigQuery solo lee los bloques 
--    específicos que contienen estos valores, optimizando el rendimiento.
-- =====================================================================

CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.fact_sales`
(
    sale_id STRING NOT NULL,
    sale_date DATE NOT NULL,
    customer_id STRING NOT NULL,
    region_id INT64 NOT NULL,
    customer_segment STRING NOT NULL,
    product_sku STRING NOT NULL,
    quantity INT64,
    unit_price FLOAT64,
    total_amount FLOAT64,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY sale_date
CLUSTER BY region_id, customer_segment;
