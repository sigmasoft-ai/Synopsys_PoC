# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_get_warranty_raw('Laptop 14');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_get_warranty_raw('Laptop 14');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_get_tech_specs_raw('5G Smartphone');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_get_certifications_raw('Power Drill');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_get_product_policy_raw('Kitchen Set');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5. Test cross-product search function
# MAGIC SELECT workspace.ss_demo.rag_search_product_docs_raw('which products are water resistant');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_search_by_doc_type_raw('bulk order minimum quantity', 'discount_policy');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workspace.ss_demo.rag_get_discount_by_category_raw('Electronics', 'APAC');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Test warranty function
# MAGIC SELECT workspace.ss_demo.rag_get_warranty_raw('Laptop 14');
# MAGIC
# MAGIC -- 2. Test tech specs function
# MAGIC SELECT workspace.ss_demo.rag_get_tech_specs_raw('5G Smartphone');
# MAGIC
# MAGIC -- 3. Test certifications function
# MAGIC SELECT workspace.ss_demo.rag_get_certifications_raw('Power Drill');
# MAGIC
# MAGIC -- 4. Test full product profile function
# MAGIC SELECT workspace.ss_demo.rag_get_product_policy_raw('Kitchen Set');
# MAGIC
# MAGIC -- 5. Test cross-product search function
# MAGIC SELECT workspace.ss_demo.rag_search_product_docs_raw('which products are water resistant');
# MAGIC
# MAGIC -- 6. Test doc type filtered search function
# MAGIC SELECT workspace.ss_demo.rag_search_by_doc_type_raw('bulk order minimum quantity', 'discount_policy');
# MAGIC
# MAGIC -- 7. Test discount by category function
# MAGIC SELECT workspace.ss_demo.rag_get_discount_by_category_raw('Electronics', 'APAC');
# MAGIC