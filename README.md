# TG CORP — Solución de Ingeniería de Datos & Data Lakehouse en GCP
##  Estructura del Repositorio

```text
PRUEBATECNICA/
├── .gitignore                      # Exclusiones de control de versiones y entornos virtuales
├── requirements.txt                # Dependencias fijadas para ejecución determinista
├── README.md                       # Especificación técnica y arquitectura de la solución
├── ci_cd/
│   └── cloudbuild.yaml             # Pipeline declarativo CI/CD multi-entorno con canary deployment
├── diagrama/
│   └── arquitectura.mermaid        # Especificación visual en código del Lakehouse
├── sql/
│   ├── fact_sales_ddl.sql          # DDL optimizado con Partitioning y Clustering
│   ├── scd2_customers.sql          # MERGE atómico para Slowly Changing Dimension Tipo 2
│   └── vector_search.sql           # DDL vectorial y consulta semántica con BigQuery Vector Search
└── src/
    ├── __init__.py                 # Marcador de paquete modular Python
    ├── ingest_api.py               # Pipeline resiliente: API REST -> Parquet GCS -> BigQuery
    └── rag_embeddings.py           # Pipeline RAG: Chunking recursivo + Vertex AI Embeddings






5.2 Seguridad e Identidades (IAM): 

┌─────────────────────────────────┬────────────────────────────────┬───────────────────────────────┐
│ Recurso                         │ Rol IAM                        │ Justificación                 │
├─────────────────────────────────┼────────────────────────────────┼───────────────────────────────┤
│ GCS: bucket origen              │ roles/storage.objectViewer     │ Solo lectura de archivos      │
│ GCS: bucket data-lake (Parquet) │ roles/storage.objectCreator    │ Solo crear, no borrar/listar  │
│ GCS: bucket DLQ                 │ roles/storage.objectCreator    │ Solo crear registros DLQ      │
│ BigQuery: dataset transactions  │ roles/bigquery.dataEditor      │ INSERT en tablas destino      │
│ BigQuery: jobs                  │ roles/bigquery.jobUser         │ Ejecutar queries de dedup     │
│ Vertex AI: predicciones         │ roles/aiplatform.user          │ Generar embeddings            │
│ Secret Manager: api-key         │ roles/secretmanager.secretAcc..│ Leer credencial de la API     │
└─────────────────────────────────┴────────────────────────────────┴───────────────────────────────┘

 Roles explícitamente NO asignados:
   - roles/editor / roles/owner         → demasiado permisivo
   - roles/storage.admin                → permite borrar buckets
   - roles/bigquery.admin               → permite borrar datasets/tablas
   - roles/bigquery.dataOwner           → permite borrar tablas
   - roles/iam.serviceAccountTokenCreator → permite impersonar otras SAs