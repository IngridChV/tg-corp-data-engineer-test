-- =====================================================================
-- TG CORP - MÓDULO 3.1: SENTENCIA MERGE SQL EN BIGQUERY (SCD TYPE 2)
-- =====================================================================
--
-- Descripción:
-- Este script realiza una carga SCD Tipo 2 (Slowly Changing Dimension Tipo 2)
-- en la tabla `dim_customers`. Invalida los registros anteriores cuyos
-- atributos hayan cambiado (is_active = FALSE, end_date = CURRENT_TIMESTAMP())
-- e inserta la nueva versión activa (is_active = TRUE, start_date = CURRENT_TIMESTAMP()).
--
-- Para lograr esto en una sola sentencia MERGE de BigQuery, la subconsulta
-- de origen (USING) duplica las filas modificadas utilizando un UNION ALL:
--   1. Una fila con 'merge_key = customer_id' para hacer MATCH con el registro activo actual y desactivarlo.
--   2. Una fila con 'merge_key = NULL' para fallar el MATCH e insertar la nueva versión del cliente.
-- =====================================================================

MERGE INTO `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.dim_customers` T
USING (
  -- -----------------------------------------------------------------
  -- FILA 1: Registros de clientes que cambiaron de estado.
  -- Se usarán para MATCHEAR y DESACTIVAR el registro activo actual.
  -- -----------------------------------------------------------------
  SELECT 
    stg.customer_id AS merge_key,
    stg.customer_id,
    stg.segment,
    stg.country,
    stg.timestamp
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.stg_customers` stg
  INNER JOIN `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.dim_customers` dim
    ON stg.customer_id = dim.customer_id
    AND dim.is_active = TRUE
    -- Detectar cambios en atributos supervisados
    AND (stg.segment != dim.segment OR stg.country != dim.country)

  UNION ALL

  -- -----------------------------------------------------------------
  -- FILA 2: Nuevos clientes o clientes existentes con cambios.
  -- Al llevar 'merge_key = NULL', nunca hará MATCH con la tabla destino,
  -- forzando un INSERT del nuevo registro activo.
  -- -----------------------------------------------------------------
  SELECT 
    CAST(NULL AS STRING) AS merge_key,
    stg.customer_id,
    stg.segment,
    stg.country,
    stg.timestamp
  FROM `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.stg_customers` stg
  LEFT JOIN `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.dim_customers` dim
    ON stg.customer_id = dim.customer_id
    AND dim.is_active = TRUE
  WHERE dim.customer_id IS NULL -- Es un cliente nuevo
     OR (stg.segment != dim.segment OR stg.country != dim.country) -- Es un cliente existente con cambios
) S
-- La condición de cruce busca coincidir únicamente con el registro ACTIVO actual
ON T.customer_id = S.merge_key AND T.is_active = TRUE

-- 1. Si coincide, desactivamos el registro histórico anterior
WHEN MATCHED THEN
  UPDATE SET 
    T.is_active = FALSE,
    T.end_date = CURRENT_TIMESTAMP()

-- 2. Si no coincide (clientes nuevos o la fila de inserción del cambio), insertamos la versión activa
WHEN NOT MATCHED THEN
  INSERT (
    customer_sk,
    customer_id,
    segment,
    country,
    is_active,
    start_date,
    end_date
  )
  VALUES (
    -- Generar clave subrogada única (Surrogate Key) para SCD Tipo 2
    GENERATE_UUID(),
    S.customer_id,
    S.segment,
    S.country,
    TRUE,
    CURRENT_TIMESTAMP(),
    CAST(NULL AS TIMESTAMP)
  );





