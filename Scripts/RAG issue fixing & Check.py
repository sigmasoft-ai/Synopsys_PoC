# Databricks notebook source


df = spark.sql("SELECT COUNT(*) as total_rows FROM workspace.ss_demo.gold_unstructured_embeddings")
display(df)

# Also check embedding dimension
df2 = spark.sql("SELECT chunk_id, size(embedding) as emb_dim FROM workspace.ss_demo.gold_unstructured_embeddings LIMIT 5")
display(df2)


# COMMAND ----------

import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Delete the mismatched index
delete_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}"
resp = requests.delete(delete_url, headers=headers)
print(resp.status_code, resp.json())


# COMMAND ----------

create_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes"

index_spec = {
    "name": "workspace.ss_demo.idx_unstructured_sales_docs",
    "endpoint_name": "ss_vector_search_demo",
    "primary_key": "chunk_id",
    "index_type": "DELTA_SYNC",
    "delta_sync_index_spec": {
        "source_table": "workspace.ss_demo.gold_unstructured_embeddings",
        "pipeline_type": "TRIGGERED",
        "embedding_vector_columns": [
            {
                "name": "embedding",
                "embedding_dimension": 1024
            }
        ]
    }
}

resp = requests.post(create_url, headers=headers, json=index_spec)
print(resp.status_code, json.dumps(resp.json(), indent=2))


# COMMAND ----------

import time

# Trigger sync
sync_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/sync"
resp = requests.post(sync_url, headers=headers)
print("Sync triggered:", resp.status_code)

# Poll until rows are indexed
status_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}"

for i in range(20):
    resp = requests.get(status_url, headers=headers)
    data = resp.json()
    state = data.get("status", {}).get("detailed_state", "UNKNOWN")
    rows  = data.get("status", {}).get("indexed_row_count", 0)
    print(f"[{i+1}] State: {state} | Rows indexed: {rows}")
    if rows and int(rows) > 0:
        print("Index is ready!")
        break
    time.sleep(15)


# COMMAND ----------



df = spark.sql("SELECT COUNT(*) as total_rows FROM workspace.ss_demo.gold_unstructured_embeddings")
display(df)

# Also check embedding dimension
df2 = spark.sql("SELECT chunk_id, size(embedding) as emb_dim FROM workspace.ss_demo.gold_unstructured_embeddings LIMIT 5")
display(df2)


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED workspace.ss_demo.rag_get_product_policy;
# MAGIC DESCRIBE FUNCTION EXTENDED workspace.ss_demo.rag_search_product_docs;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Garbled PDF text ("L ifetim e", "Em ail")  FIXING

# COMMAND ----------

from pyspark.sql import functions as F
import re

# Fix mid-word spaces in chunk_text
spark.sql("""
  UPDATE workspace.ss_demo.gold_unstructured_embeddings
  SET chunk_text = regexp_replace(
                    regexp_replace(chunk_text, '([A-Za-z]) ([a-z])', '$1$2'),
                    '\\\\u0000', ''
                  )
  WHERE chunk_text RLIKE '[A-Za-z] [a-z]'
""")


# COMMAND ----------

import requests
headers = {"Authorization": "Bearer dapi92c060756e0340881cacb93ad22b2f45"}
requests.post(
    "https://2950269589585042.cloud.databricks.com/api/2.0/vector-search/indexes/workspace.ss_demo.idx_unstructured_sales_docs/sync",
    headers=headers
)


# COMMAND ----------

def clean_line(text):
    import re
    # Fix OCR mid-word spaces: "war ranty" -> "warranty" only when splitting a single word
    # Pattern: letter-space-lowercase but NOT after punctuation or numbers
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    # Normalize multiple spaces
    text = re.sub(r'\s{2,}', ' ', text)
    # Remove null bytes and stray bullet unicode
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text
