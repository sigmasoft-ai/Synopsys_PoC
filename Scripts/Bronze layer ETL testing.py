# Databricks notebook source
# DBTITLE 1,Untitled

import pandas as pd

url = "https://github.com/sigmasoft-ai/Synopsys_PoC/blob/main/S_Data_Sources/Sales_Transactions_5000.csv"
pdf = pd.read_csv(url)

df = spark.createDataFrame(pdf)

(
  df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ss_demo.bronze_sales_transactions")
)


# COMMAND ----------

import pandas as pd

# Change this URL format
url = "https://raw.githubusercontent.com/sigmasoft-ai/Synopsys_PoC/main/S_Data_Sources/Sales_Transactions_5000.csv"
pdf = pd.read_csv(url)
df = spark.createDataFrame(pdf)
(
  df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ss_demo.bronze_sales_transactions")
)


# COMMAND ----------

import requests
import pandas as pd
from io import StringIO

# Step 1: Create your PAT at GitHub Settings > Developer settings > Personal access tokens
# Grant 'repo' scope for private repository access
token = ''

# Step 2: Use GitHub API to fetch the file
url = "https://api.github.com/repos/sigmasoft-ai/Synopsys_PoC/contents/S_Data_Sources/Sales_Transactions_5000.csv"
headers = {
    'Authorization': f'token {token}',
    'Accept': 'application/vnd.github.v3.raw'
}

response = requests.get(url, headers=headers)
pdf = pd.read_csv(StringIO(response.text))

df = spark.createDataFrame(pdf)
(
  df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ss_demo.bronze_sales_transactions")
)
