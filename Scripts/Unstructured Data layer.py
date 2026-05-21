# Databricks notebook source
from pyspark.sql import functions as F
import requests
from io import BytesIO

# Raw GitHub URLs for the PDFs
pdf_files = [
    (
        "Product_Specification_Sheets.pdf",
        "https://github.com/sigmasoft-ai/Synopsys_PoC/raw/main/UnS_Data_Sources/Product_Specification_Sheets.pdf",
        "product_spec"
    ),
    (
        "Volume_Discount_Policy.pdf",
        "https://github.com/sigmasoft-ai/Synopsys_PoC/raw/main/UnS_Data_Sources/Volume_Discount_Policy.pdf",
        "discount_policy"
    )
]

rows = []
for file_name, url, doc_type in pdf_files:
    resp = requests.get(url)
    resp.raise_for_status()
    pdf_bytes = resp.content

    # Simple PDF text extraction using PyPDF2 (or pdfplumber if available)
    from PyPDF2 import PdfReader
    reader = PdfReader(BytesIO(pdf_bytes))
    pages_text = []
    for page in reader.pages:
        try:
            pages_text.append(page.extract_text() or "")
        except Exception:
            pages_text.append("")
    full_text = "\n\n".join(pages_text)

    rows.append((file_name, url, doc_type, full_text))

bronze_df = spark.createDataFrame(
    rows,
    ["file_name", "source_url", "doc_type", "raw_content"]
).withColumn("ingested_at", F.current_timestamp())

bronze_df.write.mode("overwrite").saveAsTable(
    "workspace.ss_demo.bronze_unstructured_documents"
)


# COMMAND ----------

import requests
from io import BytesIO
from PyPDF2 import PdfReader
from pyspark.sql import functions as F

# Step 1: Reuse your PAT
token = ""  # consider moving to a secret in production

# Step 2: GitHub API URLs for the PDFs (same structure as your CSV example)
pdf_files = [
    (
        "Product_Specification_Sheets.pdf",
        "https://api.github.com/repos/sigmasoft-ai/Synopsys_PoC/contents/UnS_Data_Sources/Product_Specification_Sheets.pdf",
        "product_spec"
    ),
    (
        "Volume_Discount_Policy.pdf",
        "https://api.github.com/repos/sigmasoft-ai/Synopsys_PoC/contents/UnS_Data_Sources/Volume_Discount_Policy.pdf",
        "discount_policy"
    )
]

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3.raw"  # return raw file bytes
}

rows = []
for file_name, url, doc_type in pdf_files:
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    pdf_bytes = resp.content

    # Parse PDF bytes into text (simple extractor)
    reader = PdfReader(BytesIO(pdf_bytes))
    pages_text = []
    for page in reader.pages:
        try:
            pages_text.append(page.extract_text() or "")
        except Exception:
            pages_text.append("")
    full_text = "\n\n".join(pages_text)

    rows.append((file_name, url, doc_type, full_text))

# Create bronze table: one row per PDF
bronze_pdf_df = spark.createDataFrame(
    rows,
    ["file_name", "source_url", "doc_type", "raw_content"]
).withColumn("ingested_at", F.current_timestamp())

(
  bronze_pdf_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ss_demo.bronze_unstructured_documents")
)


# COMMAND ----------

pip install pypdf2

# COMMAND ----------

pip install pypdf

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests
from io import BytesIO
from pypdf import PdfReader   # << use pypdf instead of PyPDF2
from pyspark.sql import functions as F

token = ""  # move to secret in real use

pdf_files = [
    (
        "Product_Specification_Sheets.pdf",
        "https://api.github.com/repos/sigmasoft-ai/Synopsys_PoC/contents/UnS_Data_Sources/Product_Specification_Sheets.pdf",
        "product_spec"
    ),
    (
        "Volume_Discount_Policy.pdf",
        "https://api.github.com/repos/sigmasoft-ai/Synopsys_PoC/contents/UnS_Data_Sources/Volume_Discount_Policy.pdf",
        "discount_policy"
    )
]

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3.raw"
}

rows = []
for file_name, url, doc_type in pdf_files:
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    pdf_bytes = resp.content

    reader = PdfReader(BytesIO(pdf_bytes))
    pages_text = []
    for page in reader.pages:
        try:
            pages_text.append(page.extract_text() or "")
        except Exception:
            pages_text.append("")
    full_text = "\n\n".join(pages_text)

    rows.append((file_name, url, doc_type, full_text))

bronze_pdf_df = spark.createDataFrame(
    rows,
    ["file_name", "source_url", "doc_type", "raw_content"]
).withColumn("ingested_at", F.current_timestamp())

(
  bronze_pdf_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ss_demo.bronze_unstructured_documents")
)


# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("workspace.ss_demo.bronze_unstructured_documents")

# Split on blank lines to get paragraph-like chunks
chunks = (bronze
  .withColumn("paragraph",
              F.explode(
                  F.split(F.col("raw_content"), r"\n\s*\n")  # blank lines
              ))
  .withColumn("chunk_text", F.trim("paragraph"))
  .filter(F.length("chunk_text") > 80)  # drop tiny/noisy chunks
  .withColumn("chunk_id", F.monotonically_increasing_id().cast("string"))
  .withColumn("section_title", F.lit(None).cast("string"))
  .select(
      "chunk_id",
      "file_name",
      "source_url",
      "doc_type",
      "section_title",
      "chunk_text",
      "ingested_at"
  )
)

chunks.write.mode("overwrite").saveAsTable(
    "workspace.ss_demo.silver_unstructured_chunks"
)


# COMMAND ----------

from pyspark.sql import functions as F

chunks = spark.table("workspace.ss_demo.silver_unstructured_chunks")

# Step 3.1: Call an embedding model using Databricks Model Serving
# Replace endpoint name with your configured embedding endpoint
EMBEDDING_ENDPOINT = "databricks-bge-large-en"  # example

@F.udf("array<float>")
def embed_text(text: str):
    import requests, json, os
    # Use Databricks workspace URL + personal access token if needed
    endpoint_url = f"{os.environ['DATABRICKS_WORKSPACE_URL']}/serving-endpoints/{EMBEDDING_ENDPOINT}/invocations"
    headers = {
        "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
        "Content-Type": "application/json"
    }
    payload = {"inputs": [text]}
    resp = requests.post(endpoint_url, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    # assume "embeddings" is returned as list of vectors
    return resp.json()["embeddings"][0]

embed_df = (chunks
  .withColumn("embedding", embed_text("chunk_text"))
  .select(
      "chunk_id",
      "file_name",
      "source_url",
      "doc_type",
      "section_title",
      "chunk_text",
      "embedding"
  )
)

embed_df.write.mode("overwrite").saveAsTable(
    "workspace.ss_demo.gold_unstructured_embeddings"
)


# COMMAND ----------

from pyspark.sql import functions as F
import requests
import json
from dbruntime.databricks_repl_context import get_context

chunks = spark.table("workspace.ss_demo.silver_unstructured_chunks")

EMBEDDING_ENDPOINT = "databricks-bge-large-en"  # your serving endpoint name


def _get_workspace_url():
    # Use Databricks context to get the host (no env var needed)
    ctx = get_context()
    return "https://" + ctx.browserHostName()


@F.udf("array<float>")
def embed_text(text: str):
    if text is None or text.strip() == "":
        return []

    workspace_url = _get_workspace_url()
    endpoint_url = f"{workspace_url}/serving-endpoints/{EMBEDDING_ENDPOINT}/invocations"

    # In notebooks, Databricks injects auth automatically; no token header needed
    headers = {"Content-Type": "application/json"}

    payload = {
        "input": text,            # adjust to your endpoint schema
        "truncate": "END"
    }

    resp = requests.post(endpoint_url, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()

    # Adjust this according to your endpoint’s actual response schema
    data = resp.json()
    # Example for DBR Foundation endpoints: data["data"][0]["embedding"]
    embedding = data["data"][0]["embedding"]
    return embedding

embed_df = (
    chunks
      .withColumn("embedding", embed_text("chunk_text"))
      .select(
          "chunk_id",
          "file_name",
          "source_url",
          "doc_type",
          "section_title",
          "chunk_text",
          "embedding"
      )
)

embed_df.write.mode("overwrite").saveAsTable(
    "workspace.ss_demo.gold_unstructured_embeddings"
)


# COMMAND ----------

pip install OpenAI

# COMMAND ----------

from pyspark.sql import functions as F
from openai import OpenAI

# 1) Configure AI Gateway OpenAI client
DATABRICKS_TOKEN = ""  # move to secret/env later

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://2950269589585042.ai-gateway.cloud.databricks.com/openai/v1"
)

EMBEDDING_MODEL = "databricks-gte-large-en"

# 2) Read your already-prepared silver chunks
chunks = spark.table("workspace.ss_demo.silver_unstructured_chunks")

# 3) UDF that calls AI Gateway embeddings
@F.udf("array<float>")
def embed_text(text: str):
    if text is None or text.strip() == "":
        return []
    resp = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    # OpenAI-compatible response: data[0].embedding
    return resp.data[0].embedding

# 4) Apply UDF and write gold embeddings table
embed_df = (
    chunks
      .withColumn("embedding", embed_text("chunk_text"))
      .select(
          "chunk_id",
          "file_name",
          "source_url",
          "doc_type",
          "section_title",
          "chunk_text",
          "embedding"
      )
)

embed_df.write.mode("overwrite").saveAsTable(
    "workspace.ss_demo.gold_unstructured_embeddings"
)


# COMMAND ----------

from pyspark.sql import functions as F
from openai import OpenAI

DATABRICKS_TOKEN = ""
AI_GW_BASE_URL = "https://2950269589585042.ai-gateway.cloud.databricks.com/openai/v1"
EMBEDDING_MODEL = "databricks-gpt-5-1-codex-max"

def _embed_once(text: str):
    if text is None or text.strip() == "":
        return []
    # Create a lightweight client per call (or per partition if you optimize later)
    client = OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url=AI_GW_BASE_URL
    )
    resp = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text
    )
    return resp.data[0].embedding

@F.udf("array<float>")
def embed_text(text: str):
    return _embed_once(text)

chunks = spark.table("workspace.ss_demo.silver_unstructured_chunks")

embed_df = (
    chunks
      .withColumn("embedding", embed_text("chunk_text"))
      .select(
          "chunk_id",
          "file_name",
          "source_url",
          "doc_type",
          "section_title",
          "chunk_text",
          "embedding"
      )
)

embed_df.write.mode("overwrite").saveAsTable(
    "workspace.ss_demo.gold_unstructured_embeddings"
)


# COMMAND ----------

# FULL BLOCK: create embeddings with AI Gateway MLflow API
# Assumes: workspace.ss_demo.silver_unstructured_chunks already exists

from pyspark.sql import functions as F
import requests
import json

# ---- CONFIG ----
DATABRICKS_TOKEN = ""   # move to secret/env in real use
AI_GW_EMBED_URL = "https://2950269589585042.ai-gateway.cloud.databricks.com/mlflow/v1/embeddings"
EMBEDDING_MODEL = "databricks-gte-large-en"

# ---- Helper that calls AI Gateway embeddings ----
def _embed_once(text: str):
    if text is None or text.strip() == "":
        return []

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": EMBEDDING_MODEL,
        "input": [text]          # list of strings as required by MLflow embeddings API
    }

    resp = requests.post(AI_GW_EMBED_URL, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    data = resp.json()
    return data["data"][0]["embedding"]

@F.udf("array<float>")
def embed_text(text: str):
    return _embed_once(text)

# ---- Read silver chunks, generate embeddings, write gold table ----
chunks = spark.table("workspace.ss_demo.silver_unstructured_chunks")

embed_df = (
    chunks
      .withColumn("embedding", embed_text("chunk_text"))
      .select(
          "chunk_id",
          "file_name",
          "source_url",
          "doc_type",
          "section_title",
          "chunk_text",
          "embedding"
      )
)

(
  embed_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ss_demo.gold_unstructured_embeddings")
)

display(spark.table("workspace.ss_demo.gold_unstructured_embeddings").limit(5))
