import os
import json
import logging
from typing import List, Dict, Any, Generator
from google.cloud import storage
from google.cloud import aiplatform
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel

# Configurar Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class VertexAIEmbeddingsPipeline:
    def __init__(self, 
                 project_id: str | None = None, 
                 location: str = "us-central1",
                 gcs_bucket: str | None = None,
                 bq_dataset: str | None = None,
                 bq_table: str | None = None):
        """
        Inicializa el pipeline de embeddings de Vertex AI.
        """
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.location = location
        self.gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET_NAME")
        self.bq_dataset = bq_dataset or os.getenv("BQ_DATASET_NAME")
        self.bq_table = bq_table or os.getenv("BQ_TABLE_NAME")
        
        # Inicializar los clientes de GCP de manera diferida
        self._storage_client = None
        self._bq_client = None
        self._aiplatform_initialized = False

    @property
    def storage_client(self):
        if self._storage_client is None:
            self._storage_client = storage.Client(project=self.project_id)
        return self._storage_client

    @property
    def bq_client(self):
        if self._bq_client is None:
            self._bq_client = bigquery.Client(project=self.project_id)
        return self._bq_client

    def _init_vertex_ai(self):
        """Inicializa el SDK de Vertex AI."""
        if not self._aiplatform_initialized:
            aiplatform.init(project=self.project_id, location=self.location)
            self._aiplatform_initialized = True

    def recursive_chunk_text(self, text: str, max_chars: int = 800, overlap: int = 100) -> List[str]:
        """
        Estrategia de Chunking:
        Implementa un divisor de texto recursivo personalizado (Custom Recursive Text Splitter).
        Intenta dividir por párrafos, luego por líneas, luego por palabras hasta cumplir el max_chars,
        manteniendo un solapamiento (overlap) para no perder contexto semántico entre bloques.
        """
        if not text:
            return []

        separators = ["\n\n", "\n", " ", ""]
        chunks = []
        
        def split_recursive(text_to_split: str, current_sep_idx: int) -> List[str]:
            if len(text_to_split) <= max_chars:
                return [text_to_split]
            
            if current_sep_idx >= len(separators):
                # Caso base: no quedan separadores, cortar directamente por longitud
                return [text_to_split[i:i + max_chars] for i in range(0, len(text_to_split), max_chars)]
            
            separator = separators[current_sep_idx]
            splits = text_to_split.split(separator) if separator != "" else list(text_to_split)
            
            final_chunks = []
            current_chunk = ""
            
            for split in splits:
                # Reconstruir con el separador original
                test_chunk = current_chunk + separator + split if current_chunk else split
                
                if len(test_chunk) <= max_chars:
                    current_chunk = test_chunk
                else:
                    if current_chunk:
                        final_chunks.append(current_chunk)
                    
                    # Si la sección sola es más grande que max_chars, dividirla recursivamente
                    if len(split) > max_chars:
                        final_chunks.extend(split_recursive(split, current_sep_idx + 1))
                        current_chunk = ""
                    else:
                        current_chunk = split
            
            if current_chunk:
                final_chunks.append(current_chunk)
                
            return final_chunks

        # Obtener los bloques iniciales
        raw_chunks = split_recursive(text, 0)
        
        # Aplicar el solapamiento (Overlap)
        # Recombinamos chunks para garantizar que haya un overlap entre bloques contiguos
        overlapped_chunks = []
        for i, chunk in enumerate(raw_chunks):
            if i == 0:
                overlapped_chunks.append(chunk)
            else:
                # Obtener los últimos 'overlap' caracteres del chunk anterior
                prev_chunk = raw_chunks[i-1]
                overlap_text = prev_chunk[-overlap:] if len(prev_chunk) > overlap else prev_chunk
                overlapped_chunks.append(overlap_text + chunk)
                
        return overlapped_chunks

    def generate_embeddings_batch(self, texts: List[str], model_name: str = "text-embedding-004") -> List[List[float]]:
        """
        Llama a la API de Vertex AI para generar embeddings de un lote de textos.
        Soporta procesamiento por lotes para cumplir con los límites de la API de Vertex AI (máx 250 textos por llamada).
        """
        if not texts:
            return []

        self._init_vertex_ai()
        model = TextEmbeddingModel.from_pretrained(model_name)
        
        all_embeddings = []
        batch_size = 250  # Límite de Vertex AI por API Call
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            logger.info(f"Generando embeddings para lote de tamaño: {len(batch)} ({i + 1} a {i + len(batch)})")
            
            # Obtener embeddings de Vertex AI
            embeddings = model.get_embeddings(batch)
            for emb in embeddings:
                all_embeddings.append(emb.values)
                
        return all_embeddings

    def read_text_from_gcs(self, blob_uri: str) -> str:
        """
        Lee el contenido de un archivo de texto largo almacenado en GCS.
        Format: gs://bucket-name/path/to/file.txt
        """
        if not blob_uri.startswith("gs://"):
            raise ValueError("El URI debe comenzar con 'gs://'")
            
        path_parts = blob_uri[5:].split("/", 1)
        bucket_name = path_parts[0]
        blob_path = path_parts[1]
        
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        logger.info(f"Descargando contenido desde: {blob_uri}")
        return blob.download_as_text(encoding="utf-8")

    def persist_embeddings_to_gcs(self, 
                                  records: List[Dict[str, Any]], 
                                  output_blob_path: str):
        """
        Guarda los vectores y metadatos resultantes en GCS en formato JSON Lines (JSONL).
        """
        if not self.gcs_bucket:
            raise ValueError("El bucket de GCS no está configurado.")
            
        bucket = self.storage_client.bucket(self.gcs_bucket)
        blob = bucket.blob(output_blob_path)
        
        # Convertir a formato JSON Lines
        jsonl_data = "\n".join(json.dumps(record) for record in records)
        
        logger.info(f"Guardando embeddings persistidos en GCS: gs://{self.gcs_bucket}/{output_blob_path}")
        blob.upload_from_string(jsonl_data, content_type="application/json")

    def load_embeddings_to_bigquery(self, records: List[Dict[str, Any]]):
        """
        Carga los vectores embedding y sus metadatos directamente a BigQuery
        para habilitar búsquedas de similitud coseno con BigQuery Vector Search.
        """
        if not self.bq_dataset or not self.bq_table:
            raise ValueError("El dataset o la tabla de BigQuery no están configurados.")
            
        table_id = f"{self.project_id}.{self.bq_dataset}.{self.bq_table}"
        
        # Estructurar registros para BigQuery (BigQuery admite ARRAY de FLOAT64 para embeddings)
        bq_rows = []
        for r in records:
            bq_rows.append({
                "id": r["id"],
                "text": r["text"],
                "embedding": r["embedding"],
                "metadata": json.dumps(r["metadata"]) # Guardado como string JSON
            })

        logger.info(f"Cargando {len(bq_rows)} vectores embedding a BigQuery: {table_id}")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"), # ARRAY de FLOAT64
                bigquery.SchemaField("metadata", "STRING", mode="NULLABLE") # Metadatos en formato string JSON
            ]
        )

        # Crear tabla si no existe
        try:
            self.bq_client.get_table(table_id)
        except Exception:
            logger.info(f"La tabla de embeddings destino no existe. Creándola: {table_id}")
            table = bigquery.Table(table_id, schema=job_config.schema)
            self.bq_client.create_table(table)

        # Cargar los datos
        load_job = self.bq_client.load_table_from_json(bq_rows, table_id, job_config=job_config)
        load_job.result()
        logger.info("Vectores cargados con éxito en BigQuery.")

    def run_pipeline(self, 
                     input_gcs_uri: str, 
                     output_gcs_filename: str | None = None,
                     save_to_bq: bool = True):
        """
        Orquesta el flujo completo de chunking y generación de embeddings para un documento de GCS.
        """
        # 1. Leer documento de GCS
        text_content = self.read_text_from_gcs(input_gcs_uri)
        
        # 2. Aplicar estrategia de Chunking
        chunks = self.recursive_chunk_text(text_content, max_chars=800, overlap=100)
        logger.info(f"Documento fragmentado en {len(chunks)} bloques (chunks).")
        
        # 3. Generar embeddings mediante la API de Vertex AI
        embeddings = self.generate_embeddings_batch(chunks)
        
        # 4. Estructurar registros con metadatos
        records = []
        base_name = os.path.basename(input_gcs_uri)
        for idx, (chunk_text, embedding_vector) in enumerate(zip(chunks, embeddings)):
            records.append({
                "id": f"{base_name}_chunk_{idx}",
                "text": chunk_text,
                "embedding": embedding_vector,
                "metadata": {
                    "source_uri": input_gcs_uri,
                    "chunk_index": idx,
                    "character_count": len(chunk_text)
                }
            })
            
        # 5. Persistir vectores
        # 5.1 En GCS (Formato JSONL)
        if not output_gcs_filename:
            output_gcs_filename = f"embeddings/{base_name.replace('.', '_')}_embeddings.jsonl"
        self.persist_embeddings_to_gcs(records, output_gcs_filename)
        
        # 5.2 En BigQuery (para BQ Vector Search)
        if save_to_bq:
            self.load_embeddings_to_bigquery(records)
            
        logger.info("Pipeline de Embeddings de Vertex AI finalizado exitosamente.")

if __name__ == "__main__":
    # Prueba del algoritmo de Chunking Recursivo con texto de soporte técnico
    sample_text = (
        "TG CORP - MANUAL OPERACIONAL DE SOPORTE TÉCNICO\n\n"
        "1. ERROR DE TIMEOUT EN API:\n"
        "Cuando el pipeline de ingesta reciba un código HTTP 429 o 504 Gateway Timeout, "
        "se debe aplicar una estrategia de reintentos exponenciales con jitter y validar "
        "el encabezado Retry-After.\n\n"
        "2. EVENTOS EN TIEMPO REAL:\n"
        "Los eventos transmitidos desde la plataforma web se publican en Cloud Pub/Sub "
        "y son procesados por Cloud Dataflow en modo streaming antes de persistir en BigQuery."
    )

    print("\n" + "=" * 60)
    print("EJECUTANDO PRUEBA DE CHUNKING Y PARSEO LOCAL")
    print("=" * 60)

    pipeline = VertexAIEmbeddingsPipeline(project_id="tg-corp-prod")
    chunks = pipeline.recursive_chunk_text(sample_text, max_chars=200, overlap=30)

    logger.info(f"Texto segmentado exitosamente en {len(chunks)} fragmentos (chunks):")
    for idx, chunk in enumerate(chunks):
        print(f"\n[Chunk {idx}] ({len(chunk)} caracteres):\n{chunk}")
    print("\n" + "=" * 60)
