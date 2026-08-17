import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Generator
import pandas as pd
import requests
from google.cloud import storage, bigquery
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configurar Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class GCPDataIngester:
    def __init__(self, 
                 project_id: str | None = None, 
                 gcs_bucket: str | None = None, 
                 bq_dataset: str | None = None, 
                 bq_table: str | None = None):
        """
        Inicializa el ingestor de datos con las configuraciones de GCP.
        Si no se especifican, se leen de las variables de entorno.
        """
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET_NAME")
        self.bq_dataset = bq_dataset or os.getenv("BQ_DATASET_NAME")
        self.bq_table = bq_table or os.getenv("BQ_TABLE_NAME")
        
        self._gcs_client = None
        self._bq_client = None

    @property
    def gcs_client(self):
        if self._gcs_client is None:
            self._gcs_client = storage.Client(project=self.project_id)
        return self._gcs_client

    @property
    def bq_client(self):
        if self._bq_client is None:
            self._bq_client = bigquery.Client(project=self.project_id)
        return self._bq_client

    @retry(
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.HTTPError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True
    )
    def _fetch_page(self, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Consume una página de la API con reintentos exponenciales y manejo de rate limits.
        """
        logger.info(f"Consumiendo URL: {url} con parámetros: {params}")
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            logger.warning(f"Rate limit excedido (429). Esperando {retry_after} segundos...")
            raise requests.exceptions.RequestException(f"Rate limit hit. Retry-After: {retry_after}")
            
        response.raise_for_status()
        return response.json()

    def fetch_all_data(self, base_url: str, headers: Dict[str, str] | None = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Paginación robusta que consume todas las páginas disponibles desde la API mediante un generador.
        """
        headers = headers or {}
        page = 1
        has_more = True

        while has_more:
            params = {"page": page}
            try:
                result = self._fetch_page(base_url, params, headers)
                
                if result.get("status") != "success":
                    logger.error(f"Error de estado en la API en la página {page}: {result.get('status')}")
                    break
                
                data = result.get("data", [])
                if not data:
                    logger.info("No se encontraron más datos. Paginación completada.")
                    has_more = False
                    break
                
                logger.info(f"Página {page} consumida con éxito. Registros obtenidos: {len(data)}")
                yield data
                
                page += 1
            except Exception as e:
                logger.error(f"Error irrecuperable al consumir página {page}: {e}")
                raise e

    def transform_data(self, raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transformación y Unnesting:
        - Aplana estructuras anidadas (customer, metadata).
        - Explota la lista de ítems para tener una estructura tabular.
        - Añade marcas de tiempo e identifica la fecha para particionado.
        """
        if not raw_records:
            return pd.DataFrame()

        processed_rows = []
        for record in raw_records:
            try:
                tx_id = record.get("transaction_id")
                timestamp_str = record.get("timestamp")
                
                # Aplanar Cliente
                customer = record.get("customer", {})
                cust_id = customer.get("id")
                cust_segment = customer.get("segment")
                cust_country = customer.get("country")
                
                # Aplanar Metadatos
                metadata = record.get("metadata", {})
                source_app = metadata.get("source_app")
                ip_address = metadata.get("ip_address")
                
                # Explodear ítems en filas individuales
                items = record.get("items", [])
                if not items:
                    items = [{"sku": None, "qty": None, "unit_price": None}]
                
                for item in items:
                    row = {
                        "transaction_id": tx_id,
                        "timestamp": pd.to_datetime(timestamp_str),
                        "customer_id": cust_id,
                        "customer_segment": cust_segment,
                        "customer_country": cust_country,
                        "item_sku": item.get("sku"),
                        "item_qty": int(item.get("qty")) if item.get("qty") is not None else None,
                        "item_unit_price": float(item.get("unit_price")) if item.get("unit_price") is not None else None,
                        "meta_source_app": source_app,
                        "meta_ip_address": ip_address,
                        "ingested_at": datetime.now(timezone.utc)
                    }
                    processed_rows.append(row)
            except Exception as e:
                logger.error(f"Error procesando registro {record.get('transaction_id', 'Desconocido')}: {e}")
                continue

        return pd.DataFrame(processed_rows)

    def save_to_gcs_parquet(self, df: pd.DataFrame) -> List[str]:
        """
        Guarda el DataFrame en GCS en formato Parquet particionando por fecha (yyyy/mm/dd).
        """
        if df.empty:
            logger.warning("DataFrame vacío. No hay nada que guardar en GCS.")
            return []

        if not self.gcs_bucket:
            raise ValueError("El nombre del bucket de GCS no está configurado.")

        bucket = self.gcs_client.bucket(self.gcs_bucket)
        df['temp_date'] = df['timestamp'].dt.date
        unique_dates = df['temp_date'].unique()
        
        uploaded_files = []

        for date in unique_dates:
            df_partition = df[df['temp_date'] == date].copy()
            df_partition.drop(columns=['temp_date'], inplace=True)
            
            partition_path = f"raw_transactions/{date.strftime('%Y/%m/%d')}"
            file_name = f"data_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.parquet"
            blob_path = f"{partition_path}/{file_name}"
            
            local_parquet_path = f"/tmp/{file_name}"
            os.makedirs(os.path.dirname(local_parquet_path), exist_ok=True)
            
            try:
                df_partition.to_parquet(local_parquet_path, engine='pyarrow', index=False)
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_parquet_path)
                logger.info(f"Archivo subido con éxito a GCS: gs://{self.gcs_bucket}/{blob_path}")
                uploaded_files.append(f"gs://{self.gcs_bucket}/{blob_path}")
            finally:
                if os.path.exists(local_parquet_path):
                    os.remove(local_parquet_path)
                    
        return uploaded_files

    def load_incremental_to_bigquery(self, df: pd.DataFrame):
        """
        Carga incremental idempotente a BigQuery mediante Staging Table + MERGE.
        """
        if df.empty:
            logger.warning("DataFrame vacío. No hay nada que cargar en BigQuery.")
            return

        if not self.bq_dataset or not self.bq_table:
            raise ValueError("El dataset o la tabla de BigQuery no están configurados.")

        target_table_id = f"{self.project_id}.{self.bq_dataset}.{self.bq_table}"
        staging_table_id = f"{target_table_id}_staging_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

        logger.info(f"Iniciando carga incremental a la tabla destino: {target_table_id}")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("transaction_id", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("customer_segment", "STRING"),
                bigquery.SchemaField("customer_country", "STRING"),
                bigquery.SchemaField("item_sku", "STRING"),
                bigquery.SchemaField("item_qty", "INTEGER"),
                bigquery.SchemaField("item_unit_price", "FLOAT"),
                bigquery.SchemaField("meta_source_app", "STRING"),
                bigquery.SchemaField("meta_ip_address", "STRING"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP")
            ]
        )

        try:
            load_job = self.bq_client.load_table_from_dataframe(
                df, staging_table_id, job_config=job_config
            )
            load_job.result()
            logger.info(f"Datos cargados con éxito a la tabla temporal: {staging_table_id}")

            try:
                self.bq_client.get_table(target_table_id)
            except Exception:
                logger.info(f"La tabla destino no existe. Creando tabla: {target_table_id}")
                init_job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                    schema=job_config.schema
                )
                empty_df = df.iloc[0:0]
                self.bq_client.load_table_from_dataframe(empty_df, target_table_id, job_config=init_job_config).result()

            merge_query = f"""
            MERGE INTO `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.transaction_id = S.transaction_id AND T.item_sku = S.item_sku
            WHEN MATCHED THEN
                UPDATE SET 
                    timestamp = S.timestamp,
                    customer_id = S.customer_id,
                    customer_segment = S.customer_segment,
                    customer_country = S.customer_country,
                    item_qty = S.item_qty,
                    item_unit_price = S.item_unit_price,
                    meta_source_app = S.meta_source_app,
                    meta_ip_address = S.meta_ip_address,
                    ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN
                INSERT (
                    transaction_id, timestamp, customer_id, customer_segment, customer_country,
                    item_sku, item_qty, item_unit_price, meta_source_app, meta_ip_address, ingested_at
                )
                VALUES (
                    S.transaction_id, S.timestamp, S.customer_id, S.customer_segment, S.customer_country,
                    S.item_sku, S.item_qty, S.item_unit_price, S.meta_source_app, S.meta_ip_address, S.ingested_at
                );
            """
            query_job = self.bq_client.query(merge_query)
            query_job.result()
            logger.info("Operación MERGE ejecutada con éxito. Carga incremental completada.")
            
        finally:
            self.bq_client.delete_table(staging_table_id, not_found_ok=True)
            logger.info(f"Tabla temporal de staging eliminada: {staging_table_id}")

    def run_pipeline(self, api_url: str, headers: Dict[str, str] | None = None):
        """Ejecuta el pipeline completo de ingesta."""
        logger.info("Iniciando Pipeline de Ingestión...")
        for raw_data_batch in self.fetch_all_data(api_url, headers):
            df_processed = self.transform_data(raw_data_batch)
            self.save_to_gcs_parquet(df_processed)
            self.load_incremental_to_bigquery(df_processed)
        logger.info("Pipeline de Ingestión finalizado exitosamente.")


if __name__ == "__main__":
    # Prueba local de Unnesting y Transformación con el payload oficial de TG Corp
    mock_payload = [
        {
            "transaction_id": "TX98412",
            "timestamp": "2026-08-10T14:32:10Z",
            "customer": {"id": "CUST_501", "segment": "ENTERPRISE", "country": "CL"},
            "items": [{"sku": "PROD_A", "qty": 2, "unit_price": 150.00}],
            "metadata": {"source_app": "mobile_v2", "ip_address": "190.160.10.5"}
        }
    ]

    print("\n" + "=" * 60)
    print("EJECUTANDO PRUEBA LOCAL DE TRANSFORMACIÓN Y UNNESTING")
    print("=" * 60)

    ingester = GCPDataIngester(project_id="tg-corp-prod")
    df_transformed = ingester.transform_data(mock_payload)
    
    print("\nDataFrame Resultante:")
    print(df_transformed.to_string())
    print("\n" + "=" * 60)
    
    