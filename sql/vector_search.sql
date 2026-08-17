-- =====================================================================
-- TG CORP - MÓDULO 4.2: INTEGRACIÓN CON BIGQUERY VECTOR SEARCH
-- =====================================================================
--
-- Descripción:
-- 1. Creación de la tabla optimizada para almacenar fragmentos (chunks) 
--    y sus correspondientes embeddings vectoriales (ARRAY<FLOAT64>).
-- 2. Búsqueda de similitud de Coseno utilizando la función nativa VECTOR_SEARCH
--    y generación de embeddings al vuelo con ML.GENERATE_EMBEDDING.
-- =====================================================================

-- 1. Tabla para persistencia de Embeddings
CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.rag_doc_embeddings`
(
    chunk_id STRING NOT NULL,
    doc_id STRING NOT NULL,
    source_gcs_uri STRING,
    chunk_index INT64,
    content STRING,
    embedding ARRAY<FLOAT64>
);

-- 2. Búsqueda por similitud semántica (Top 5 vecinos más cercanos con distancia Coseno)
SELECT 
    base.doc_id, 
    base.chunk_id, 
    base.content, 
    distance
FROM VECTOR_SEARCH(
    TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.rag_doc_embeddings`,
    'embedding',
    (
      SELECT ml_generate_embedding_result AS embedding
      FROM ML.GENERATE_EMBEDDING(
        MODEL `${GCP_PROJECT_ID}.${BQ_DATASET_NAME}.embedding_model`,
        (SELECT '¿Cómo resolver error de timeout en la API?' AS content),
        STRUCT('RETRIEVAL_QUERY' AS task_type)
      )
    ),
    top_k => 5,
    distance_type => 'COSINE'
);


