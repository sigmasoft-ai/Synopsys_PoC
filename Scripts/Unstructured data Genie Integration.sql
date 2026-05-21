-- Databricks notebook source
Select * from workspace.ss_demo.bronze_unstructured_documents

-- COMMAND ----------


Select * from workspace.ss_demo.silver_unstructured_chunks

-- COMMAND ----------

Select * from workspace.ss_demo.gold_unstructured_embeddings

-- COMMAND ----------

-- Enable CDF on the gold embeddings table
ALTER TABLE workspace.ss_demo.gold_unstructured_embeddings
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);


-- COMMAND ----------

-- Run this ONCE to create the searchable vector index
CREATE VECTOR SEARCH INDEX workspace.ss_demo.idx_unstructured_sales_docs
ON TABLE workspace.ss_demo.gold_unstructured_embeddings
EMBEDDING COLUMN embedding
ID COLUMN chunk_id
DELTA_SYNC_INDEX_SPEC = '{
  "source_table": "workspace.ss_demo.gold_unstructured_embeddings",
  "pipeline_type": "TRIGGERED"
}';


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import requests
-- MAGIC import json
-- MAGIC
-- MAGIC DATABRICKS_TOKEN = ""
-- MAGIC AI_GW_EMBED_URL = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
-- MAGIC EMBEDDING_MODEL = "databricks-gte-large-en"
-- MAGIC VECTOR_SEARCH_ENDPOINT = "workspace.ss_demo.idx_unstructured_sales_docs"
-- MAGIC
-- MAGIC
-- MAGIC def embed_query(text: str) -> list:
-- MAGIC     """Embed a user query using AI Gateway MLflow embeddings."""
-- MAGIC     headers = {
-- MAGIC         "Authorization": f"Bearer {DATABRICKS_TOKEN}",
-- MAGIC         "Content-Type": "application/json"
-- MAGIC     }
-- MAGIC     payload = {
-- MAGIC         "model": EMBEDDING_MODEL,
-- MAGIC         "input": [text]
-- MAGIC     }
-- MAGIC     resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
-- MAGIC     resp.raise_for_status()
-- MAGIC     return resp.json()["data"][0]["embedding"]
-- MAGIC
-- MAGIC
-- MAGIC def retrieve_docs(query: str, k: int = 5) -> list:
-- MAGIC     """
-- MAGIC     Search the vector index for chunks relevant to the query.
-- MAGIC     Returns a list of (chunk_id, file_name, doc_type, chunk_text) tuples.
-- MAGIC     """
-- MAGIC     from databricks.vector_search.client import VectorSearchClient
-- MAGIC
-- MAGIC     query_vec = embed_query(query)
-- MAGIC     vsc = VectorSearchClient()
-- MAGIC     index = vsc.get_index(
-- MAGIC         endpoint_name="vs_endpoint",   # your Vector Search endpoint name if created
-- MAGIC         index_name=VECTOR_SEARCH_ENDPOINT
-- MAGIC     )
-- MAGIC     results = index.similarity_search(
-- MAGIC         query_vector=query_vec,
-- MAGIC         columns=["chunk_id", "file_name", "doc_type", "chunk_text", "source_url"],
-- MAGIC         num_results=k
-- MAGIC     )
-- MAGIC     return results["result"]["data_array"]

-- COMMAND ----------

-- Register a UC function that Genie can call as a tool
CREATE OR REPLACE FUNCTION workspace.ss_demo.search_product_docs(query STRING)
RETURNS TABLE (chunk_id STRING, file_name STRING, doc_type STRING, chunk_text STRING)
LANGUAGE PYTHON
AS $$
import requests, json

DATABRICKS_TOKEN = ""
AI_GW_EMBED_URL = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL = "databricks-gte-large-en"

def embed_query(text):
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"model": EMBEDDING_MODEL, "input": [text]}
    resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    return resp.json()["data"][0]["embedding"]

from databricks.vector_search.client import VectorSearchClient
query_vec = embed_query(query)
vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name="vs_endpoint",
    index_name="workspace.ss_demo.idx_unstructured_sales_docs"
)
results = index.similarity_search(
    query_vector=query_vec,
    columns=["chunk_id", "file_name", "doc_type", "chunk_text"],
    num_results=5
)
return results["result"]["data_array"]
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.search_product_docs(query STRING)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING
)
LANGUAGE PYTHON
HANDLER = 'SearchHandler'
AS $$
import requests
import json

DATABRICKS_TOKEN = ""
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_ENDPOINT_NAME = "ss_vector_search_demo"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

class SearchHandler:
    def eval(self, query: str):
        # Step 1: Embed the query
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {"model": EMBEDDING_MODEL, "input": [query]}
        resp = requests.post(
            AI_GW_EMBED_URL,
            headers=headers,
            data=json.dumps(payload)
        )
        resp.raise_for_status()
        query_vec = resp.json()["data"][0]["embedding"]

        # Step 2: Search the vector index
        from databricks.vector_search.client import VectorSearchClient
        vsc   = VectorSearchClient()
        index = vsc.get_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME
        )
        results = index.similarity_search(
            query_vector=query_vec,
            columns=["chunk_id", "file_name", "doc_type", "chunk_text"],
            num_results=5
        )

        # Step 3: yield one row at a time
        for row in results["result"]["data_array"]:
            yield (row[0], row[1], row[2], row[3])
$$;


-- COMMAND ----------

-- Create a SCALAR UC function (returns top result as text, not table)
-- This works on DBR 13.3+ and all editions
CREATE OR REPLACE FUNCTION workspace.ss_demo.search_product_docs(query STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import requests, json

DATABRICKS_TOKEN = ""
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_ENDPOINT_NAME = "ss_vector_search_demo"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

# Step 1: Embed the query
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}
payload = {"model": EMBEDDING_MODEL, "input": [query]}
resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
resp.raise_for_status()
query_vec = resp.json()["data"][0]["embedding"]

# Step 2: Search vector index
from databricks.vector_search.client import VectorSearchClient
vsc   = VectorSearchClient()
index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
results = index.similarity_search(
    query_vector=query_vec,
    columns=["chunk_id", "file_name", "doc_type", "chunk_text"],
    num_results=3
)

# Step 3: Return top results as a single string (scalar UDF)
rows = results["result"]["data_array"]
output = "\n\n---\n\n".join([
    f"Source: {r[1]} ({r[2]})\n{r[3]}"
    for r in rows
])
return output
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.search_product_docs(query_text STRING)
RETURNS TABLE (
  chunk_id      STRING,
  file_name     STRING,
  doc_type      STRING,
  chunk_text    STRING,
  source_url    STRING
)
RETURN
  SELECT
    chunk_id,
    file_name,
    doc_type,
    chunk_text,
    source_url
  FROM workspace.ss_demo.silver_unstructured_chunks
  WHERE
    -- keyword match fallback since Python vector search not available as SQL table function
    LOWER(chunk_text) LIKE CONCAT('%', LOWER(query_text), '%')
    OR LOWER(file_name) LIKE CONCAT('%', LOWER(query_text), '%')
  LIMIT 5;


-- COMMAND ----------

DROP FUNCTION IF EXISTS workspace.ss_demo.search_product_docs;

-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.search_product_docs(query_text STRING)
RETURNS TABLE (
  chunk_id      STRING,
  file_name     STRING,
  doc_type      STRING,
  chunk_text    STRING,
  source_url    STRING
)
RETURN
  SELECT
    chunk_id,
    file_name,
    doc_type,
    chunk_text,
    source_url
  FROM workspace.ss_demo.silver_unstructured_chunks
  WHERE
    LOWER(chunk_text) LIKE CONCAT('%', LOWER(query_text), '%')
    OR LOWER(file_name) LIKE CONCAT('%', LOWER(query_text), '%')
  LIMIT 5;


-- COMMAND ----------

DROP FUNCTION IF EXISTS workspace.ss_demo.search_product_docs;
DROP FUNCTION IF EXISTS workspace.ss_demo.search_product_docs_raw;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.search_product_docs_raw(query_text STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal function: searches vector index and returns results as JSON string'
AS $$
import requests
import json

DATABRICKS_TOKEN = ""
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_ENDPOINT_NAME = "ss_vector_search_demo"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

# Step 1: Embed the query
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}
payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
resp.raise_for_status()
query_vec = resp.json()["data"][0]["embedding"]

# Step 2: Search vector index
from databricks.vector_search.client import VectorSearchClient
vsc   = VectorSearchClient()
index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
results = index.similarity_search(
    query_vector=query_vec,
    columns=["chunk_id", "file_name", "doc_type", "chunk_text", "source_url"],
    num_results=5
)

# Step 3: Return results as JSON string
rows = results["result"]["data_array"]
output = [
    {
        "chunk_id":   r[0],
        "file_name":  r[1],
        "doc_type":   r[2],
        "chunk_text": r[3],
        "source_url": r[4]
    }
    for r in rows
]
return json.dumps(output)
$$;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.search_product_docs(query_text STRING)
RETURNS TABLE (
  chunk_id      STRING,
  file_name     STRING,
  doc_type      STRING,
  chunk_text    STRING,
  source_url    STRING
)
COMMENT 'Search product spec sheets and volume discount policy using semantic vector search. Use for questions about product specifications, features, certifications, warranty, and volume discount rules.'
RETURN
  SELECT
    v.chunk_id,
    v.file_name,
    v.doc_type,
    v.chunk_text,
    v.source_url
  FROM (
    SELECT workspace.ss_demo.search_product_docs_raw(query_text) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json, 'ARRAY<STRUCT<chunk_id:STRING, file_name:STRING, doc_type:STRING, chunk_text:STRING, source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

-- Step 1: Python raw helper
CREATE FUNCTION workspace.ss_demo.search_by_doc_type_raw(
  query_text STRING,
  doc_type   STRING   -- 'product_spec' or 'discount_policy'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal: searches vector index filtered by doc_type, returns JSON'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_ENDPOINT_NAME = "ss_vector_search_demo"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
resp.raise_for_status()
query_vec = resp.json()["data"][0]["embedding"]

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
results = index.similarity_search(
    query_vector=query_vec,
    columns=["chunk_id", "file_name", "doc_type", "chunk_text", "source_url"],
    num_results=5,
    filters={"doc_type": doc_type}    # filter by doc_type
)

rows = results["result"]["data_array"]
output = [
    {"chunk_id": r[0], "file_name": r[1], "doc_type": r[2],
     "chunk_text": r[3], "source_url": r[4]}
    for r in rows
]
return json.dumps(output)
$$;

-- Step 2: SQL table wrapper
CREATE FUNCTION workspace.ss_demo.search_by_doc_type(
  query_text STRING,
  doc_type   STRING
)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Search unstructured docs filtered by doc_type. Use doc_type = product_spec for product questions, discount_policy for pricing questions.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.search_by_doc_type_raw(query_text, doc_type) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING, file_name:STRING, doc_type:STRING, chunk_text:STRING, source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

-- SQL only, no Python needed — queries gold layer directly by keyword
CREATE FUNCTION workspace.ss_demo.get_product_policy(product_keyword STRING)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING
)
COMMENT 'Returns all documentation chunks related to a specific product name or SKU. Use when user asks about a specific product like Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Jeans, T-Shirt, Hand Tool Set.'
RETURN
  SELECT
    chunk_id,
    file_name,
    doc_type,
    chunk_text
  FROM workspace.ss_demo.gold_unstructured_embeddings
  WHERE
    LOWER(chunk_text) LIKE CONCAT('%', LOWER(product_keyword), '%')
  ORDER BY doc_type, chunk_id
  LIMIT 10;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.get_discount_by_category(
  category_name STRING,
  region_name   STRING
)
RETURNS TABLE (
  chunk_id   STRING,
  doc_type   STRING,
  chunk_text STRING
)
COMMENT 'Returns volume discount rules for a given product category and region. Categories: Electronics, Apparel, Home, Tools. Regions: EMEA, APAC, East, West, North, South.'
RETURN
  SELECT
    chunk_id,
    doc_type,
    chunk_text
  FROM workspace.ss_demo.gold_unstructured_embeddings
  WHERE
    doc_type = 'discount_policy'
    AND LOWER(chunk_text) LIKE CONCAT('%', LOWER(category_name), '%')
    AND LOWER(chunk_text) LIKE CONCAT('%', LOWER(region_name), '%')
  LIMIT 5;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.rag_search_raw(
  query_text  STRING,
  doc_filter  STRING,   -- 'all' | 'product_spec' | 'discount_policy'
  top_k       INT
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal: embeds query, searches vector index, returns JSON. doc_filter: all | product_spec | discount_policy'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_ENDPOINT_NAME = "ss_vector_search_demo"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

# Step 1: Embed the query
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}
payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
resp.raise_for_status()
query_vec = resp.json()["data"][0]["embedding"]

# Step 2: Build filters
filters = {}
if doc_filter and doc_filter != "all":
    filters["doc_type"] = doc_filter

# Step 3: Search vector index
from databricks.vector_search.client import VectorSearchClient
vsc   = VectorSearchClient()
index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
search_kwargs = dict(
    query_vector=query_vec,
    columns=["chunk_id", "file_name", "doc_type", "chunk_text", "source_url"],
    num_results=top_k if top_k else 5
)
if filters:
    search_kwargs["filters"] = filters

results = index.similarity_search(**search_kwargs)

# Step 4: Return as JSON
rows = results["result"]["data_array"]
output = [
    {
        "chunk_id":   r[0],
        "file_name":  r[1],
        "doc_type":   r[2],
        "chunk_text": r[3],
        "source_url": r[4]
    }
    for r in rows
]
return json.dumps(output)
$$;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.rag_search_product_docs(query_text STRING)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'General semantic search across all product spec sheets and discount policy docs. Use for any question about product features, specifications, discounts, warranty, or certifications.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(query_text, 'all', 5) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.rag_search_by_doc_type(
  query_text STRING,
  doc_type   STRING
)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Semantic search filtered by document type. Use doc_type = product_spec for specs/features/warranty. Use doc_type = discount_policy for pricing/discount/bulk order rules.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(query_text, doc_type, 5) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.rag_get_product_policy(product_keyword STRING)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Returns all documentation for a specific product name or SKU using semantic search. Use when user asks about a specific product like Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(
      CONCAT('product specifications features warranty ', product_keyword),
      'all', 8
    ) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

CREATE FUNCTION workspace.ss_demo.rag_get_discount_by_category(
  category_name STRING,
  region_name   STRING
)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Returns volume discount rules for a specific product category and region using semantic search. Categories: Electronics, Apparel, Home, Tools. Regions: EMEA, APAC, East, West, North, South.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(
      CONCAT('volume discount bulk order ', category_name, ' region ', region_name),
      'discount_policy', 5
    ) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

DROP FUNCTION IF EXISTS workspace.ss_demo.rag_search_raw;

CREATE FUNCTION workspace.ss_demo.rag_search_raw(
  query_text  STRING,
  doc_filter  STRING,
  top_k       INT
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper: embeds query via REST, queries vector index via REST. Never add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
EMBEDDING_MODEL  = "databricks-gte-large-en"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Step 1: Embed the query
embed_payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json=embed_payload)
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

# Step 2: Build query payload as a dict, then send as data= (not json=)
# GET with json= does NOT send body correctly in Python requests
vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"

vs_payload = {
    "num_results": top_k if top_k else 5,
    "query_vector": query_vec,
    "columns": ["chunk_id", "file_name", "doc_type", "chunk_text", "source_url"]
}

# filters_json must be a JSON string, not a nested dict
if doc_filter and doc_filter.lower() != "all":
    vs_payload["filters_json"] = json.dumps({"doc_type": doc_filter})

vs_resp = requests.get(
    vs_url,
    headers=headers,
    data=json.dumps(vs_payload)   # use data= not json= for GET requests
)

# Log the error response body for debugging if it fails
if vs_resp.status_code != 200:
    return json.dumps({"error": vs_resp.status_code, "detail": vs_resp.text})

rows = vs_resp.json().get("result", {}).get("data_array", [])

output = [
    {
        "chunk_id":   r[0],
        "file_name":  r[1],
        "doc_type":   r[2],
        "chunk_text": r[3],
        "source_url": r[4]
    }
    for r in rows
]
return json.dumps(output)
$$;


-- COMMAND ----------


SELECT workspace.ss_demo.rag_search_raw('laptop specifications', 'all', 5);



-- COMMAND ----------

DROP FUNCTION IF EXISTS workspace.ss_demo.rag_search_raw;

CREATE FUNCTION workspace.ss_demo.rag_search_raw(
  query_text  STRING,
  doc_filter  STRING,
  top_k       INT
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper: embeds query via REST, queries vector index via REST. Never add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

embed_payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json=embed_payload)
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"

vs_payload = {
    "num_results": top_k if top_k else 5,
    "query_vector": query_vec,
    "columns": ["chunk_id", "file_name", "doc_type", "chunk_text", "source_url"]
}

if doc_filter and doc_filter.lower() != "all":
    vs_payload["filters_json"] = json.dumps({"doc_type": doc_filter})

vs_resp = requests.get(vs_url, headers=headers, data=json.dumps(vs_payload))

if vs_resp.status_code != 200:
    return json.dumps({"error": vs_resp.status_code, "detail": vs_resp.text})

rows = vs_resp.json().get("result", {}).get("data_array", [])
output = [
    {"chunk_id": r[0], "file_name": r[1], "doc_type": r[2], "chunk_text": r[3], "source_url": r[4]}
    for r in rows
]
return json.dumps(output)
$$;


-- COMMAND ----------


SELECT workspace.ss_demo.rag_search_raw('laptop specifications', 'all', 5);



-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_product_policy(product_keyword STRING)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Use this function to answer ANY question about a specific product including: warranty, specifications, features, certifications, dimensions, weight, battery life, processor, memory, storage, accessories, target market, care instructions, safety notes. Call this whenever user asks about product details, specs, warranty policy, or product information for products like Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set. Always call this function when asked about warranty policy of a product.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(
      CONCAT('product specifications features warranty ', product_keyword),
      'all', 8
    ) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_product_docs(query_text STRING)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Use this function to answer ANY question about product documentation, policies, specifications, warranty, certifications, features, materials, care instructions, dimensions, weight, connectivity, operating system, camera, display, battery, processor, storage, accessories, safety, compliance. Use when the user asks about product details that are NOT available in the sales transactions table. This is the primary source of product knowledge. Always call this before saying no information is available about a product.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(query_text, 'all', 5) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_by_doc_type(
  query_text STRING,
  doc_type   STRING
)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Use this function to search product documents filtered by type. Set doc_type = product_spec to answer questions about product features, technical specs, dimensions, weight, warranty, certifications, accessories, connectivity. Set doc_type = discount_policy to answer questions about volume discounts, bulk order pricing, seasonal offers, regional discount rules, minimum order quantities. Use this when question clearly targets either specs or discounts specifically.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(query_text, doc_type, 5) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_discount_by_category(
  category_name STRING,
  region_name   STRING
)
RETURNS TABLE (
  chunk_id   STRING,
  file_name  STRING,
  doc_type   STRING,
  chunk_text STRING,
  source_url STRING
)
COMMENT 'Use this function to answer questions about discount policies, volume pricing, bulk order rules, promotional pricing, or rebate programs for a specific product category and region. Call this whenever user asks about discounts, pricing policies, bulk deals, or offers for categories like Electronics, Apparel, Home, Tools in regions like EMEA, APAC, East, West, North, South. Always call this before saying no discount information is available.'
RETURN
  SELECT v.chunk_id, v.file_name, v.doc_type, v.chunk_text, v.source_url
  FROM (
    SELECT workspace.ss_demo.rag_search_raw(
      CONCAT('volume discount bulk order ', category_name, ' region ', region_name),
      'discount_policy', 5
    ) AS raw_json
  ) base
  LATERAL VIEW explode(
    from_json(base.raw_json,
      'ARRAY<STRUCT<chunk_id:STRING,file_name:STRING,doc_type:STRING,chunk_text:STRING,source_url:STRING>>')
  ) v AS v;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC AGAIN REPLACING THE FUNCTION AS WE NEED TO UPDATE TO PRESENT ONLY THE REQUIRED INFORMATION
-- MAGIC  

-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_product_policy(product_keyword STRING)
RETURNS STRING
COMMENT 'SCALAR FUNCTION - call using: SELECT workspace.ss_demo.rag_get_product_policy("product_name") AS result. DO NOT use in FROM clause. Use when user asks about warranty, specs, features, certifications, dimensions, accessories, care instructions for a specific product. Products: Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set. Call AUTOMATICALLY without confirmation.'
LANGUAGE PYTHON
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

embed_payload = {"model": EMBEDDING_MODEL, "input": [
    f"product specifications features warranty certifications {product_keyword}"
]}
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json=embed_payload)
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_payload = {
    "num_results": 8,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"]
}

vs_resp = requests.get(vs_url, headers=headers, data=json.dumps(vs_payload))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
return f"Product documentation for '{product_keyword}':\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_product_docs(query_text STRING)
RETURNS STRING
COMMENT 'SCALAR FUNCTION - call using: SELECT workspace.ss_demo.rag_search_product_docs("query") AS result. DO NOT use in FROM clause. Use for any general question about product specs, warranty, features, certifications, dimensions, materials, care instructions, accessories, connectivity, battery, processor, storage, display, camera. Call AUTOMATICALLY without confirmation. Never say no information is available without calling this first.'
LANGUAGE PYTHON
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

embed_payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json=embed_payload)
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_payload = {
    "num_results": 5,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"]
}

vs_resp = requests.get(vs_url, headers=headers, data=json.dumps(vs_payload))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
return f"Relevant product documentation:\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_by_doc_type(
    query_text STRING,
    doc_type   STRING
)
RETURNS STRING
COMMENT 'SCALAR FUNCTION - call using: SELECT workspace.ss_demo.rag_search_by_doc_type("query", "doc_type") AS result. DO NOT use in FROM clause. Use when question is clearly about specs OR discounts. doc_type = product_spec for specs, features, warranty, certifications. doc_type = discount_policy for pricing, volume discounts, bulk orders, promotions. Call AUTOMATICALLY without confirmation.'
LANGUAGE PYTHON
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

embed_payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json=embed_payload)
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_payload = {
    "num_results": 5,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"]
}

if doc_type and doc_type.lower() != "all":
    vs_payload["filters_json"] = json.dumps({"doc_type": doc_type})

vs_resp = requests.get(vs_url, headers=headers, data=json.dumps(vs_payload))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
doc_label = "product specification" if doc_type == "product_spec" else "discount policy"
return f"Relevant {doc_label} documentation:\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_discount_by_category(
    category_name STRING,
    region_name   STRING
)
RETURNS STRING
COMMENT 'SCALAR FUNCTION - call using: SELECT workspace.ss_demo.rag_get_discount_by_category("category", "region") AS result. DO NOT use in FROM clause. Use when user asks about discount policies, volume pricing, bulk order rules, promotional pricing for a specific category and region. Categories: Electronics, Apparel, Home, Tools. Regions: EMEA, APAC, East, West, North, South. Call AUTOMATICALLY without confirmation.'
LANGUAGE PYTHON
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

query_text = f"volume discount bulk order pricing policy {category_name} region {region_name}"
embed_payload = {"model": EMBEDDING_MODEL, "input": [query_text]}
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json=embed_payload)
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_payload = {
    "num_results": 5,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"],
    "filters_json": json.dumps({"doc_type": "discount_policy"})
}

vs_resp = requests.get(vs_url, headers=headers, data=json.dumps(vs_payload))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
return f"Discount policy for {category_name} in {region_name} region:\n\n{combined}"
$$;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC RETURNS TABLE but add a dedicated summary view wrapper so Genie never sees raw chunk columns.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_product_policy_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal scalar helper for rag_get_product_policy. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"product specifications features warranty certifications {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 8, "query_vector": query_vec, "columns": ["chunk_id", "chunk_text"]
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
return f"Product documentation for '{product_keyword}':\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_product_docs_raw(query_text STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal scalar helper for rag_search_product_docs. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL, "input": [query_text]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 5, "query_vector": query_vec, "columns": ["chunk_id", "chunk_text"]
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
return f"Relevant product documentation:\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_by_doc_type_raw(query_text STRING, doc_type STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal scalar helper for rag_search_by_doc_type. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL, "input": [query_text]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_payload = {"num_results": 5, "query_vector": query_vec, "columns": ["chunk_id", "chunk_text"]}
if doc_type and doc_type.lower() != "all":
    vs_payload["filters_json"] = json.dumps({"doc_type": doc_type})

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps(vs_payload))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
doc_label = "product specification" if doc_type == "product_spec" else "discount policy"
return f"Relevant {doc_label} documentation:\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_discount_by_category_raw(category_name STRING, region_name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal scalar helper for rag_get_discount_by_category. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

query_text = f"volume discount bulk order pricing policy {category_name} region {region_name}"
embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL, "input": [query_text]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 5,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"],
    "filters_json": json.dumps({"doc_type": "discount_policy"})
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
combined = "\n\n---\n\n".join([r[1] for r in rows if r[1]])
return f"Discount policy for {category_name} in {region_name} region:\n\n{combined}"
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_get_product_policy(product_keyword STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use when user asks about warranty, specs, features, certifications, dimensions, accessories for products: Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set. Call AUTOMATICALLY. Returns single result column with full text summary.'
RETURN
  SELECT workspace.ss_demo.rag_get_product_policy_raw(product_keyword) AS result;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_search_product_docs(query_text STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use for any general question about product specs, warranty, features, certifications, dimensions, materials, care instructions, accessories. Call AUTOMATICALLY without confirmation. Never say no information available without calling this. Returns single result column with full text summary.'
RETURN
  SELECT workspace.ss_demo.rag_search_product_docs_raw(query_text) AS result;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_search_by_doc_type(query_text STRING, doc_type STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use when question is clearly about specs OR discounts. doc_type = product_spec for specs/warranty/certifications. doc_type = discount_policy for pricing/bulk orders/promotions. Call AUTOMATICALLY. Returns single result column with full text summary.'
RETURN
  SELECT workspace.ss_demo.rag_search_by_doc_type_raw(query_text, doc_type) AS result;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_get_discount_by_category(category_name STRING, region_name STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use when user asks about discount policies, volume pricing, bulk order rules for a specific category and region. Categories: Electronics, Apparel, Home, Tools. Regions: EMEA, APAC, East, West, North, South. Call AUTOMATICALLY. Returns single result column with full text summary.'
RETURN
  SELECT workspace.ss_demo.rag_get_discount_by_category_raw(category_name, region_name) AS result;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Adding more specualized fucntions

-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_warranty_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_warranty. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"warranty support guarantee return policy {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 4,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"],
    "filters_json": json.dumps({"doc_type": "product_spec"})
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
keyword_lower = product_keyword.lower().strip().replace('"','').replace("'","")

# Extract only warranty-relevant sentences
warranty_lines = []
for r in rows:
    if not r[1]:
        continue
    for line in r[1].split("\n"):
        line_clean = line.strip()
        if not line_clean:
            continue
        line_lower = line_clean.lower()
        if any(w in line_lower for w in ["warrant", "guarantee", "support", "return", "hotline", "money-back", "replacement", "service"]):
            warranty_lines.append(line_clean)

if not warranty_lines:
    return f"No specific warranty information found for '{product_keyword}'."

unique_lines = list(dict.fromkeys(warranty_lines))
return f"Warranty & Support for '{product_keyword}':\n\n" + "\n".join(f"- {l}" for l in unique_lines)
$$;

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_get_warranty(product_keyword STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use ONLY when user asks specifically about warranty, guarantee, support policy, return policy, money-back guarantee for a specific product. Returns clean warranty bullet points only. Products: Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set.'
RETURN SELECT workspace.ss_demo.rag_get_warranty_raw(product_keyword) AS result;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_tech_specs_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_tech_specs. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"technical specifications processor memory storage display battery dimensions weight {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 4,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"],
    "filters_json": json.dumps({"doc_type": "product_spec"})
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
keyword_lower = product_keyword.lower().strip().replace('"','').replace("'","")

spec_lines = []
for r in rows:
    if not r[1]:
        continue
    for line in r[1].split("\n"):
        line_clean = line.strip()
        if not line_clean:
            continue
        line_lower = line_clean.lower()
        # Skip metadata header lines
        if any(skip in line_lower for skip in ["document version", "effective date", "classification", "brand:", "category:", "product line:", "target market", "primary:", "secondary:", "geographic"]):
            continue
        if any(w in line_lower for w in ["display", "processor", "memory", "storage", "battery", "weight", "dimension", "os", "operating", "graphics", "connectivity", "camera", "resolution", "ram", "ssd", "ghz", "inch", "water", "ip6", "usb", "bluetooth", "wi-fi", "5g", "nfc"]):
            spec_lines.append(line_clean)

if not spec_lines:
    return f"No specific technical specifications found for '{product_keyword}'."

unique_lines = list(dict.fromkeys(spec_lines))
return f"Technical Specifications for '{product_keyword}':\n\n" + "\n".join(f"- {l}" for l in unique_lines[:20])
$$;

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_get_tech_specs(product_keyword STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use ONLY when user asks specifically about technical specifications, processor, memory, storage, display, battery, dimensions, weight, connectivity, camera, OS for a specific product. Returns clean spec bullet points only. Products: Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set.'
RETURN SELECT workspace.ss_demo.rag_get_tech_specs_raw(product_keyword) AS result;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_certifications_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_certifications. Do not add to Genie.'
AS $$
import requests, json

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"certifications compliance standards approvals {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 4,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"],
    "filters_json": json.dumps({"doc_type": "product_spec"})
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])

cert_lines = []
for r in rows:
    if not r[1]:
        continue
    for line in r[1].split("\n"):
        line_clean = line.strip()
        if not line_clean:
            continue
        line_lower = line_clean.lower()
        if any(w in line_lower for w in ["certif", "compli", "standard", "approv", "rated", "listed", "iso", "ce ", "fcc", "rohs", "epeat", "energy star", "ansi", "din", "bpa", "ul "]):
            cert_lines.append(line_clean)

if not cert_lines:
    return f"No certification information found for '{product_keyword}'."

unique_lines = list(dict.fromkeys(cert_lines))
return f"Certifications & Compliance for '{product_keyword}':\n\n" + "\n".join(f"- {l}" for l in unique_lines)
$$;

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_tab_get_certifications(product_keyword STRING)
RETURNS TABLE (result STRING)
COMMENT 'TABLE FUNCTION - Use ONLY when user asks about certifications, compliance, standards, safety approvals for a specific product. Returns clean certification bullet points only. Products: Laptop 14, 5G Smartphone, Power Drill, Kitchen Set, Classic Jeans, Classic Tee, Hand Tool Set.'
RETURN SELECT workspace.ss_demo.rag_get_certifications_raw(product_keyword) AS result;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC WORKING ON Cross-product mixing and table data instead of summary

-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_warranty_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_warranty. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"warranty support guarantee return policy {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query"
vs_resp = requests.get(vs_url, headers=headers, data=json.dumps({
    "num_results": 6,
    "query_vector": query_vec,
    "columns": ["chunk_id", "chunk_text"],
    "filters_json": json.dumps({"doc_type": "product_spec"})
}))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])

# Clean garbled PDF text: fix mid-word spaces like "L aptop" -> "Laptop"
def clean_line(text):
    text = re.sub(r'([A-Za-z])\s([a-z])', r'\1\2', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

# Build fuzzy keyword tokens (e.g. "laptop 14" -> ["laptop", "14"])
keyword_tokens = product_keyword.lower().replace('"','').replace("'","").split()

# Lines from OTHER known products to skip
other_product_markers = {
    "laptop 14":      ["drillmaster", "1-800-dril", "kitchen", "classic jeans", "classic tee", "hand tool", "smartphone"],
    "5g smartphone":  ["drillmaster", "1-800-dril", "kitchen", "classic jeans", "classic tee", "hand tool", "laptop"],
    "power drill":    ["laptop", "smartphone", "kitchen", "classic jeans", "classic tee", "hand tool"],
    "kitchen set":    ["laptop", "smartphone", "drillmaster", "classic jeans", "classic tee", "hand tool"],
    "classic jeans":  ["laptop", "smartphone", "drillmaster", "kitchen", "classic tee", "hand tool"],
    "classic tee":    ["laptop", "smartphone", "drillmaster", "kitchen", "classic jeans", "hand tool"],
    "hand tool set":  ["laptop", "smartphone", "kitchen", "classic jeans", "classic tee"]
}
product_key = product_keyword.lower().replace('"','').replace("'","").strip()
exclude_markers = other_product_markers.get(product_key, [])

warranty_keywords = ["warrant", "guarantee", "support", "return",
                     "hotline", "money-back", "replacement", "service",
                     "satisfaction", "on-site", "technical support"]

skip_metadata = ["document version", "effective date", "classification",
                 "sales & marketing", "product line:", "brand:", "category:"]

warranty_lines = []

for r in rows:
    if not r[1]:
        continue

    for line in r[1].split("\n"):
        line_clean = clean_line(line)
        if not line_clean or len(line_clean) < 8:
            continue
        line_lower = line_clean.lower()

        # Skip metadata headers
        if any(s in line_lower for s in skip_metadata):
            continue

        # Skip lines that are clearly about other products
        if any(m in line_lower for m in exclude_markers):
            continue

        # Keep only warranty-relevant lines
        if any(w in line_lower for w in warranty_keywords):
            warranty_lines.append(line_clean)

# Deduplicate preserving order
seen = set()
unique_lines = []
for line in warranty_lines:
    normalized = re.sub(r'[^a-z0-9]', '', line.lower())
    if normalized not in seen and len(normalized) > 5:
        seen.add(normalized)
        unique_lines.append(line)

if not unique_lines:
    return f"No warranty information found for '{product_keyword}'."

return f"Warranty & Support for {product_keyword}:\n\n" + "\n".join(f"• {l}" for l in unique_lines)
$$;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC FIXING SPACES & CONCATENATIO ISSUES FOR ALL 6

-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_warranty_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_warranty. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

other_product_markers = {
    "laptop 14":     ["drillmaster","1-800-dril","kitchen","classic jeans","classic tee","hand tool","smartphone"],
    "5g smartphone": ["drillmaster","1-800-dril","kitchen","classic jeans","classic tee","hand tool","laptop"],
    "power drill":   ["laptop","smartphone","kitchen","classic jeans","classic tee","hand tool"],
    "kitchen set":   ["laptop","smartphone","drillmaster","classic jeans","classic tee","hand tool"],
    "classic jeans": ["laptop","smartphone","drillmaster","kitchen","classic tee","hand tool"],
    "classic tee":   ["laptop","smartphone","drillmaster","kitchen","classic jeans","hand tool"],
    "hand tool set": ["laptop","smartphone","kitchen","classic jeans","classic tee"]
}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"warranty support guarantee return policy {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_resp = requests.get(f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers, data=json.dumps({
        "num_results": 6, "query_vector": query_vec,
        "columns": ["chunk_id", "chunk_text"],
        "filters_json": json.dumps({"doc_type": "product_spec"})
    }))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
product_key = product_keyword.lower().replace('"','').replace("'","").strip()
exclude_markers = other_product_markers.get(product_key, [])
skip_metadata = ["document version","effective date","classification","sales & marketing","product line:","brand:","category:"]
warranty_keywords = ["warrant","guarantee","support","return","hotline","money-back","replacement","service","satisfaction","on-site"]

warranty_lines = []
for r in rows:
    if not r[1]: continue
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8: continue
        ll = lc.lower()
        if any(s in ll for s in skip_metadata): continue
        if any(m in ll for m in exclude_markers): continue
        if any(w in ll for w in warranty_keywords):
            warranty_lines.append(lc)

seen = set()
unique = []
for line in warranty_lines:
    norm = re.sub(r'[^a-z0-9]', '', line.lower())
    if norm not in seen and len(norm) > 5:
        seen.add(norm)
        unique.append(line)

if not unique:
    return f"No warranty information found for '{product_keyword}'."
return f"Warranty & Support for {product_keyword}:\n\n" + "\n".join(f"• {l}" for l in unique)
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_tech_specs_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_tech_specs. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

other_product_markers = {
    "laptop 14":     ["drillmaster","kitchen","classic jeans","classic tee","hand tool","smartphone"],
    "5g smartphone": ["drillmaster","kitchen","classic jeans","classic tee","hand tool","laptop"],
    "power drill":   ["laptop","smartphone","kitchen","classic jeans","classic tee","hand tool"],
    "kitchen set":   ["laptop","smartphone","drillmaster","classic jeans","classic tee","hand tool"],
    "classic jeans": ["laptop","smartphone","drillmaster","kitchen","classic tee","hand tool"],
    "classic tee":   ["laptop","smartphone","drillmaster","kitchen","classic jeans","hand tool"],
    "hand tool set": ["laptop","smartphone","kitchen","classic jeans","classic tee"]
}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"technical specifications processor memory storage display battery dimensions connectivity {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_resp = requests.get(f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers, data=json.dumps({
        "num_results": 6, "query_vector": query_vec,
        "columns": ["chunk_id", "chunk_text"],
        "filters_json": json.dumps({"doc_type": "product_spec"})
    }))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
product_key = product_keyword.lower().replace('"','').replace("'","").strip()
exclude_markers = other_product_markers.get(product_key, [])
skip_metadata = ["document version","effective date","classification","sales & marketing","product line:","brand:","category:","primary:","secondary:","geographic"]
spec_keywords = ["display","processor","memory","storage","battery","weight","dimension","os","operating","graphics","connectivity","camera","resolution","ram","ssd","ghz","inch","water","ip6","usb","bluetooth","wi-fi","5g","nfc","motor","power","torque","material","heat","stainless","fabric","thread","cut"]

spec_lines = []
for r in rows:
    if not r[1]: continue
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8: continue
        ll = lc.lower()
        if any(s in ll for s in skip_metadata): continue
        if any(m in ll for m in exclude_markers): continue
        if any(w in ll for w in spec_keywords):
            spec_lines.append(lc)

seen = set()
unique = []
for line in spec_lines:
    norm = re.sub(r'[^a-z0-9]', '', line.lower())
    if norm not in seen and len(norm) > 5:
        seen.add(norm)
        unique.append(line)

if not unique:
    return f"No technical specifications found for '{product_keyword}'."
return f"Technical Specifications for {product_keyword}:\n\n" + "\n".join(f"• {l}" for l in unique[:20])
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_certifications_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_certifications. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

other_product_markers = {
    "laptop 14":     ["drillmaster","kitchen","classic jeans","classic tee","hand tool","smartphone"],
    "5g smartphone": ["drillmaster","kitchen","classic jeans","classic tee","hand tool","laptop"],
    "power drill":   ["laptop","smartphone","kitchen","classic jeans","classic tee","hand tool"],
    "kitchen set":   ["laptop","smartphone","drillmaster","classic jeans","classic tee","hand tool"],
    "classic jeans": ["laptop","smartphone","drillmaster","kitchen","classic tee","hand tool"],
    "classic tee":   ["laptop","smartphone","drillmaster","kitchen","classic jeans","hand tool"],
    "hand tool set": ["laptop","smartphone","kitchen","classic jeans","classic tee"]
}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": [f"certifications compliance standards approvals safety {product_keyword}"]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_resp = requests.get(f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers, data=json.dumps({
        "num_results": 6, "query_vector": query_vec,
        "columns": ["chunk_id", "chunk_text"],
        "filters_json": json.dumps({"doc_type": "product_spec"})
    }))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
product_key = product_keyword.lower().replace('"','').replace("'","").strip()
exclude_markers = other_product_markers.get(product_key, [])
skip_metadata = ["document version","effective date","classification","sales & marketing","product line:","brand:","category:"]
cert_keywords = ["certif","compli","standard","approv","rated","listed","iso","ce ","fcc","rohs","epeat","energy star","ansi","din","bpa","ul ","safety","oeko","gots","reach"]

cert_lines = []
for r in rows:
    if not r[1]: continue
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8: continue
        ll = lc.lower()
        if any(s in ll for s in skip_metadata): continue
        if any(m in ll for m in exclude_markers): continue
        if any(w in ll for w in cert_keywords):
            cert_lines.append(lc)

seen = set()
unique = []
for line in cert_lines:
    norm = re.sub(r'[^a-z0-9]', '', line.lower())
    if norm not in seen and len(norm) > 5:
        seen.add(norm)
        unique.append(line)

if not unique:
    return f"No certification information found for '{product_keyword}'."
return f"Certifications & Compliance for {product_keyword}:\n\n" + "\n".join(f"• {l}" for l in unique)
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_product_policy_raw(product_keyword STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_product_policy. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": "Bearer " + DATABRICKS_TOKEN, "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

def dedup(lines, limit=15):
    seen, out = set(), []
    for l in lines:
        n = re.sub(r'[^a-z0-9]', '', l.lower())
        if n not in seen and len(n) > 5:
            seen.add(n)
            out.append(l)
        if len(out) >= limit:
            break
    return out

other_product_markers = {
    "laptop 14":     ["drillmaster","kitchen","classic jeans","classic tee","hand tool","smartphone"],
    "5g smartphone": ["drillmaster","kitchen","classic jeans","classic tee","hand tool","laptop"],
    "power drill":   ["laptop","smartphone","kitchen","classic jeans","classic tee","hand tool"],
    "kitchen set":   ["laptop","smartphone","drillmaster","classic jeans","classic tee","hand tool"],
    "classic jeans": ["laptop","smartphone","drillmaster","kitchen","classic tee","hand tool"],
    "classic tee":   ["laptop","smartphone","drillmaster","kitchen","classic jeans","hand tool"],
    "hand tool set": ["laptop","smartphone","kitchen","classic jeans","classic tee"]
}

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": ["complete product profile specifications warranty certifications features " + product_keyword]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_resp = requests.get(
    WORKSPACE_URL + "/api/2.0/vector-search/indexes/" + VS_INDEX_NAME + "/query",
    headers=headers,
    data=json.dumps({
        "num_results": 6,
        "query_vector": query_vec,
        "columns": ["chunk_id", "chunk_text"],
        "filters_json": json.dumps({"doc_type": "product_spec"})
    })
)
if vs_resp.status_code != 200:
    return "Error: " + str(vs_resp.status_code) + " - " + vs_resp.text

rows = vs_resp.json().get("result", {}).get("data_array", [])
product_key = product_keyword.lower().replace('"', '').replace("'", "").strip()
exclude_markers = other_product_markers.get(product_key, [])
skip_metadata = ["document version","effective date","classification","sales & marketing","product line:","brand:","category:","primary:","secondary:","geographic"]

sections = {"specs": [], "warranty": [], "certifications": [], "features": []}
spec_kw     = ["display","processor","memory","storage","battery","weight","dimension","os","camera","connectivity","ram","ssd","ghz","inch","ip6","usb","bluetooth","wi-fi","5g","motor","power","torque","material","heat","stainless","fabric"]
warranty_kw = ["warrant","guarantee","support","hotline","money-back","return","satisfaction","on-site","replacement"]
cert_kw     = ["certif","compli","iso","ce ","fcc","rohs","epeat","energy star","ansi","din","bpa","ul ","rated","listed","safety"]

for r in rows:
    if not r[1]:
        continue
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8:
            continue
        ll = lc.lower()
        if any(s in ll for s in skip_metadata):
            continue
        if any(m in ll for m in exclude_markers):
            continue
        if any(w in ll for w in warranty_kw):
            sections["warranty"].append(lc)
        elif any(w in ll for w in cert_kw):
            sections["certifications"].append(lc)
        elif any(w in ll for w in spec_kw):
            sections["specs"].append(lc)
        elif len(lc) > 15:
            sections["features"].append(lc)

parts = ["Full Product Profile: " + product_keyword]
if sections["specs"]:
    parts.append("\nTECHNICAL SPECS:\n" + "\n".join("* " + l for l in dedup(sections["specs"])))
if sections["warranty"]:
    parts.append("\nWARRANTY & SUPPORT:\n" + "\n".join("* " + l for l in dedup(sections["warranty"])))
if sections["certifications"]:
    parts.append("\nCERTIFICATIONS:\n" + "\n".join("* " + l for l in dedup(sections["certifications"])))
if sections["features"]:
    parts.append("\nFEATURES:\n" + "\n".join("* " + l for l in dedup(sections["features"], 5)))

if len(parts) > 1:
    return "\n".join(parts)
return "No information found for " + product_keyword
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_product_docs_raw(query_text STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_search_product_docs. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL, "input": [query_text]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_resp = requests.get(f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers, data=json.dumps({
        "num_results": 5, "query_vector": query_vec,
        "columns": ["chunk_id", "chunk_text"]
    }))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
skip_metadata = ["document version","effective date","classification","sales & marketing","product line:","brand:","category:"]

results, seen = [], set()
for r in rows:
    if not r[1]: continue
    clean_lines = []
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8: continue
        if any(s in lc.lower() for s in skip_metadata): continue
        norm = re.sub(r'[^a-z0-9]','',lc.lower())
        if norm not in seen:
            seen.add(norm)
            clean_lines.append(lc)
    if clean_lines:
        results.append("\n".join(clean_lines[:8]))

if not results:
    return "No matching product documentation found."
return f"Search results for '{query_text}':\n\n" + "\n\n---\n\n".join(results)
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_search_by_doc_type_raw(query_text STRING, doc_type STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_search_by_doc_type. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL, "input": [query_text]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_payload = {"num_results": 5, "query_vector": query_vec, "columns": ["chunk_id", "chunk_text"]}
if doc_type and doc_type.lower() != "all":
    vs_payload["filters_json"] = json.dumps({"doc_type": doc_type})

vs_resp = requests.get(f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{VS_INDEX_NAME}/query",
    headers=headers, data=json.dumps(vs_payload))
if vs_resp.status_code != 200:
    return f"Error: {vs_resp.status_code} - {vs_resp.text}"

rows = vs_resp.json().get("result", {}).get("data_array", [])
skip_metadata = ["document version","effective date","classification","sales & marketing","product line:","brand:","category:"]

results, seen = [], set()
for r in rows:
    if not r[1]: continue
    clean_lines = []
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8: continue
        if any(s in lc.lower() for s in skip_metadata): continue
        norm = re.sub(r'[^a-z0-9]','',lc.lower())
        if norm not in seen:
            seen.add(norm)
            clean_lines.append(lc)
    if clean_lines:
        results.append("\n".join(clean_lines[:8]))

if not results:
    return f"No {doc_type} documentation found for: '{query_text}'."

doc_label = "Product Specification" if doc_type == "product_spec" else "Discount Policy" if doc_type == "discount_policy" else "Documentation"
return f"{doc_label} results for '{query_text}':\n\n" + "\n\n---\n\n".join(results)
$$;


-- COMMAND ----------

CREATE OR REPLACE FUNCTION workspace.ss_demo.rag_get_discount_by_category_raw(category_name STRING, region_name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Internal helper for rag_get_discount_by_category. Do not add to Genie.'
AS $$
import requests, json, re

DATABRICKS_TOKEN = ""
WORKSPACE_URL    = "https://2950269589585042.cloud.databricks.com"
AI_GW_EMBED_URL  = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL  = "databricks-gte-large-en"
VS_INDEX_NAME    = "workspace.ss_demo.idx_unstructured_sales_docs"

headers = {"Authorization": "Bearer " + DATABRICKS_TOKEN, "Content-Type": "application/json"}

def clean_line(text):
    text = re.sub(r'(?<=[A-Za-z])\s(?=[a-z])', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = text.replace('\u0000', '').replace('\u2022', '').strip()
    return text

embed_resp = requests.post(AI_GW_EMBED_URL, headers=headers, json={
    "model": EMBEDDING_MODEL,
    "input": ["volume discount bulk order pricing policy " + category_name + " region " + region_name]
})
embed_resp.raise_for_status()
query_vec = embed_resp.json()["data"][0]["embedding"]

vs_resp = requests.get(
    WORKSPACE_URL + "/api/2.0/vector-search/indexes/" + VS_INDEX_NAME + "/query",
    headers=headers,
    data=json.dumps({
        "num_results": 5,
        "query_vector": query_vec,
        "columns": ["chunk_id", "chunk_text"],
        "filters_json": json.dumps({"doc_type": "discount_policy"})
    })
)
if vs_resp.status_code != 200:
    return "Error: " + str(vs_resp.status_code) + " - " + vs_resp.text

rows = vs_resp.json().get("result", {}).get("data_array", [])
skip_metadata = ["document version","effective date","classification","approved by","last updated"]
discount_kw = ["discount","volume","bulk","unit","tier","region","minimum","order","rebate","promo","%","percent"]

discount_lines, seen = [], set()
for r in rows:
    if not r[1]:
        continue
    for line in r[1].split("\n"):
        lc = clean_line(line)
        if not lc or len(lc) < 8:
            continue
        ll = lc.lower()
        if any(s in ll for s in skip_metadata):
            continue
        if any(w in ll for w in discount_kw):
            norm = re.sub(r'[^a-z0-9]', '', ll)
            if norm not in seen:
                seen.add(norm)
                discount_lines.append(lc)

if not discount_lines:
    return "No discount policy found for " + category_name + " in " + region_name + " region."
return "Discount Policy for " + category_name + " - " + region_name + " region:\n\n" + "\n".join("* " + l for l in discount_lines[:20])
$$;
