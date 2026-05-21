-- Databricks notebook source
Select count(1) from ss_demo.bronze_sales_transactions

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.silver_sales_fact AS
SELECT
  -- Primary key
  CAST(transaction_id AS BIGINT)                  AS transaction_id,
  
  -- Date attributes
  CAST(transaction_date AS DATE)                  AS transaction_date,
  CAST(date_id AS INT)                            AS date_id,
  CAST(month AS TINYINT)                          AS month,
  CAST(SUBSTRING(quarter, 2, 1) AS TINYINT)       AS quarter_num,
  quarter                                         AS quarter,
  YEAR(CAST(transaction_date AS DATE))            AS year,
  
  -- Customer attributes
  customer_type,
  sold_to_customer_name,
  end_user,
  CASE 
    WHEN customer_type = 'B2B' THEN 'Business'
    WHEN customer_type = 'B2C' THEN 'Consumer'
    ELSE 'Unknown'
  END AS customer_segment,
  
  -- Geography attributes
  country,
  state,
  city,
  region,
  
  -- Product attributes
  product_sku,
  product_name,
  category,
  brand,
  
  -- Measures
  CAST(quantity AS INT)                           AS quantity,
  CAST(unit_price AS DECIMAL(18,2))               AS unit_price,
  CAST(total_amount AS DECIMAL(18,2))             AS total_amount,
  currency,
  
  -- Derived metrics
  CAST(quantity * unit_price AS DECIMAL(18,2))    AS calculated_amount,
  CASE 
    WHEN total_amount > 1000 THEN 'High Value'
    WHEN total_amount > 100 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS transaction_value_band,
  
  -- Audit columns
  CURRENT_TIMESTAMP()                             AS processed_timestamp

FROM workspace.ss_demo.bronze_sales_transactions
WHERE total_amount IS NOT NULL 
  AND quantity > 0
  AND transaction_date IS NOT NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.gold_sales_transactions_wide AS
SELECT
  -- Transaction grain
  transaction_id,
  transaction_date,
  date_id,
  year,
  month,
  quarter,
  CAST(SUBSTRING(quarter, 2, 1) AS TINYINT)       AS quarter_num,

  -- Date features
  DAYOFMONTH(transaction_date)                    AS day_of_month,
  DAYOFWEEK(transaction_date)                     AS day_of_week_num,
  DATE_FORMAT(transaction_date, 'EEEE')           AS day_of_week_name,
  DATE_FORMAT(transaction_date, 'MMMM')           AS month_name,
  WEEKOFYEAR(transaction_date)                    AS week_of_year,
  CASE WHEN DAYOFWEEK(transaction_date) IN (1,7)
       THEN 'Weekend' ELSE 'Weekday' END          AS day_type,

  -- Customer
  sold_to_customer_name                           AS customer_name,
  customer_type,
  CASE WHEN customer_type = 'B2B'
       THEN 'Business' ELSE 'Consumer' END        AS customer_segment,
  end_user,

  -- Geography
  country,
  state,
  city,
  region,

  -- Product
  product_sku,
  product_name,
  category,
  brand,

  -- Measures
  quantity                                        AS units_sold,
  unit_price,
  total_amount                                    AS revenue,
  currency,

  -- Derived
  quantity * unit_price                           AS calculated_revenue,
  CASE
    WHEN total_amount > 1000 THEN 'High Value'
    WHEN total_amount > 100  THEN 'Medium Value'
    ELSE 'Low Value'
  END                                             AS transaction_value_band,

  CURRENT_TIMESTAMP()                             AS processed_timestamp
FROM workspace.ss_demo.silver_sales_fact;


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.gold_daily_sales_summary AS
SELECT
  transaction_date                                AS date,
  year,
  month,
  quarter,
  DATE_FORMAT(transaction_date, 'MMMM')           AS month_name,

  COUNT(DISTINCT transaction_id)                  AS total_orders,
  SUM(units_sold)                                 AS total_units_sold,
  SUM(revenue)                                    AS total_revenue,
  AVG(revenue)                                    AS avg_order_value,

  COUNT(DISTINCT customer_name)                   AS unique_customers,
  SUM(CASE WHEN customer_type = 'B2B' THEN revenue ELSE 0 END)
                                                  AS b2b_revenue,
  SUM(CASE WHEN customer_type = 'B2C' THEN revenue ELSE 0 END)
                                                  AS b2c_revenue,

  CURRENT_TIMESTAMP()                             AS last_updated
FROM workspace.ss_demo.gold_sales_transactions_wide
GROUP BY transaction_date, year, month, quarter, month_name;


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.gold_product_performance AS
SELECT
  product_sku,
  product_name,
  category,
  brand,

  SUM(units_sold)                                 AS total_units_sold,
  SUM(revenue)                                    AS total_revenue,
  AVG(unit_price)                                 AS avg_selling_price,
  COUNT(DISTINCT transaction_id)                  AS total_orders,
  COUNT(DISTINCT customer_name)                   AS unique_customers,
  COUNT(DISTINCT country)                         AS countries_sold,
  COUNT(DISTINCT region)                          AS regions_sold,

  CURRENT_TIMESTAMP()                             AS last_updated
FROM workspace.ss_demo.gold_sales_transactions_wide
GROUP BY product_sku, product_name, category, brand;


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.gold_geography_performance AS
SELECT
  country,
  region,
  state,
  city,

  SUM(revenue)                                    AS total_revenue,
  SUM(units_sold)                                 AS total_units_sold,
  COUNT(DISTINCT transaction_id)                  AS total_orders,
  COUNT(DISTINCT customer_name)                   AS unique_customers,
  AVG(revenue)                                    AS avg_order_value,

  COUNT(DISTINCT product_sku)                     AS unique_products_sold,
  COUNT(DISTINCT category)                        AS categories_sold,

  MIN(transaction_date)                           AS first_sale_date,
  MAX(transaction_date)                           AS last_sale_date,
  COUNT(DISTINCT transaction_date)                AS days_with_sales,

  SUM(CASE WHEN customer_type = 'B2B' THEN 1 ELSE 0 END)
                                                  AS b2b_customers,
  SUM(CASE WHEN customer_type = 'B2C' THEN 1 ELSE 0 END)
                                                  AS b2c_customers,

  CURRENT_TIMESTAMP()                             AS last_updated
FROM workspace.ss_demo.gold_sales_transactions_wide
GROUP BY country, region, state, city;


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.semantic_mart_sales_fact AS
SELECT
  -- PK and keys
  transaction_id,
  date_id                                         AS date_key,
  product_sku                                     AS product_key,
  customer_name                                   AS customer_key,
  CONCAT(country, '-', state, '-', city)          AS geography_key,

  -- Time
  transaction_date                                AS date,
  year,
  quarter,
  CAST(SUBSTRING(quarter, 2, 1) AS TINYINT)       AS quarter_num,
  month,
  DATE_FORMAT(transaction_date, 'MMMM')           AS month_name,
  DAYOFMONTH(transaction_date)                    AS day_of_month,
  DAYOFWEEK(transaction_date)                     AS day_of_week_num,
  DATE_FORMAT(transaction_date, 'EEEE')           AS day_of_week_name,
  WEEKOFYEAR(transaction_date)                    AS week_of_year,
  CASE WHEN DAYOFWEEK(transaction_date) IN (1,7)
       THEN 'Weekend' ELSE 'Weekday' END          AS day_type,

  -- Customer
  customer_name,
  customer_type                                   AS customer_segment_code,
  CASE WHEN customer_type='B2B' THEN 'Business'
       ELSE 'Consumer' END                        AS customer_segment_name,
  end_user,

  -- Geography
  country,
  state,
  city,
  region,

  -- Product
  product_sku,
  product_name,
  category,
  brand,

  -- Measures
  units_sold                                      AS quantity,
  unit_price,
  revenue                                         AS total_amount,
  revenue                                         AS revenue_usd,
  currency,

  -- Derived measures
  quantity * unit_price                           AS calculated_revenue_usd,
  CASE WHEN customer_type = 'B2B'
       THEN revenue * 0.85 ELSE revenue END       AS net_revenue_usd,
  transaction_value_band,

  processed_timestamp
FROM workspace.ss_demo.gold_sales_transactions_wide;

ALTER TABLE workspace.ss_demo.semantic_mart_sales_fact
ALTER COLUMN transaction_id SET NOT NULL;

ALTER TABLE workspace.ss_demo.semantic_mart_sales_fact
ADD CONSTRAINT pk_semantic_fact PRIMARY KEY (transaction_id);

COMMENT ON TABLE workspace.ss_demo.semantic_mart_sales_fact IS
'Unified semantic mart fact table with transactional sales data plus business-friendly dimensions. Primary table for Genie, BI dashboards, and AI copilots.';


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.semantic_mart_date_dim AS
SELECT DISTINCT
  date_id                                         AS date_key,
  transaction_date                                AS date,
  year,
  quarter,
  CAST(SUBSTRING(quarter, 2, 1) AS TINYINT)       AS quarter_num,
  month,
  DATE_FORMAT(transaction_date, 'MMMM')           AS month_name,
  DAYOFMONTH(transaction_date)                    AS day_of_month,
  DAYOFWEEK(transaction_date)                     AS day_of_week_num,
  DATE_FORMAT(transaction_date, 'EEEE')           AS day_of_week_name,
  WEEKOFYEAR(transaction_date)                    AS week_of_year,
  CASE WHEN DAYOFWEEK(transaction_date) IN (1,7)
       THEN TRUE ELSE FALSE END                   AS is_weekend
FROM workspace.ss_demo.gold_sales_transactions_wide;

ALTER TABLE workspace.ss_demo.semantic_mart_date_dim
ALTER COLUMN date_key SET NOT NULL;

ALTER TABLE workspace.ss_demo.semantic_mart_date_dim
ADD CONSTRAINT pk_semantic_date PRIMARY KEY (date_key);


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.semantic_mart_product_dim AS
SELECT
  ROW_NUMBER() OVER (ORDER BY p.product_sku)      AS product_key,
  p.product_sku                                   AS sku,
  p.product_name                                  AS product,
  p.category,
  p.brand,
  gp.total_revenue,
  gp.total_units_sold,
  gp.total_orders,
  gp.avg_selling_price,
  gp.unique_customers,
  gp.countries_sold,
  gp.regions_sold
FROM (
  SELECT DISTINCT product_sku, product_name, category, brand
  FROM workspace.ss_demo.gold_sales_transactions_wide
) p
LEFT JOIN workspace.ss_demo.gold_product_performance gp
  ON p.product_sku = gp.product_sku;

ALTER TABLE workspace.ss_demo.semantic_mart_product_dim
ALTER COLUMN product_key SET NOT NULL;

ALTER TABLE workspace.ss_demo.semantic_mart_product_dim
ADD CONSTRAINT pk_semantic_product PRIMARY KEY (product_key);


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.semantic_mart_customer_dim AS
SELECT
  ROW_NUMBER() OVER (ORDER BY customer_name)      AS customer_key,
  customer_name,
  customer_type                                   AS segment_code,
  CASE WHEN customer_type='B2B' THEN 'Business'
       ELSE 'Consumer' END                        AS segment_name,
  end_user,
  COUNT(DISTINCT transaction_id)                  AS total_transactions,
  SUM(revenue)                                    AS lifetime_revenue,
  AVG(revenue)                                    AS avg_transaction_value,
  SUM(units_sold)                                 AS total_units_purchased,
  COUNT(DISTINCT category)                        AS categories_purchased,
  COUNT(DISTINCT country)                         AS countries_active,
  MIN(transaction_date)                           AS first_purchase_date,
  MAX(transaction_date)                           AS last_purchase_date
FROM workspace.ss_demo.gold_sales_transactions_wide
GROUP BY customer_name, customer_type, end_user;

ALTER TABLE workspace.ss_demo.semantic_mart_customer_dim
ALTER COLUMN customer_key SET NOT NULL;

ALTER TABLE workspace.ss_demo.semantic_mart_customer_dim
ADD CONSTRAINT pk_semantic_customer PRIMARY KEY (customer_key);


-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.ss_demo.semantic_mart_geography_dim AS
SELECT
  ROW_NUMBER() OVER (ORDER BY country, state, city) AS geography_key,
  country,
  region,
  state,
  city,
  total_revenue,
  total_units_sold,
  total_orders,
  unique_customers,
  avg_order_value,
  b2b_customers,
  b2c_customers,
  days_with_sales
FROM workspace.ss_demo.gold_geography_performance;

ALTER TABLE workspace.ss_demo.semantic_mart_geography_dim
ALTER COLUMN geography_key SET NOT NULL;

ALTER TABLE workspace.ss_demo.semantic_mart_geography_dim
ADD CONSTRAINT pk_semantic_geography PRIMARY KEY (geography_key);


-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.ss_demo.semantic_revenue_kpi AS
SELECT
  date,
  year,
  quarter,
  month,
  month_name,
  total_revenue                                   AS revenue_usd,
  total_orders                                    AS orders,
  total_units_sold                                AS units_sold,
  avg_order_value                                 AS average_order_value_usd,
  unique_customers                                AS active_customers,
  b2b_revenue,
  b2c_revenue,
  ROUND(b2b_revenue / NULLIF(total_revenue,0) * 100,1)
                                                  AS b2b_revenue_percent,
  ROUND(b2c_revenue / NULLIF(total_revenue,0) * 100,1)
                                                  AS b2c_revenue_percent,
  LAG(total_revenue, 7) OVER(ORDER BY date)       AS revenue_7d_ago,
  ROUND(
    (total_revenue - LAG(total_revenue,7) OVER(ORDER BY date))
    / NULLIF(LAG(total_revenue,7) OVER(ORDER BY date),0) * 100,
    2
  )                                               AS week_over_week_growth_percent
FROM workspace.ss_demo.gold_daily_sales_summary;


-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.ss_demo.semantic_product_performance AS
SELECT
  sku,
  product,
  category,
  brand,
  total_revenue                                   AS revenue_usd,
  total_units_sold                                AS units_sold,
  total_orders                                    AS orders,
  avg_selling_price                               AS average_price_usd,
  unique_customers                                AS customer_count,
  countries_sold,
  regions_sold,
  DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
  DENSE_RANK() OVER (
    PARTITION BY category 
    ORDER BY total_revenue DESC
  )                                               AS category_revenue_rank
FROM workspace.ss_demo.semantic_mart_product_dim;


-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.ss_demo.semantic_customer_segments AS
SELECT
  segment_name                                    AS customer_segment,
  COUNT(*)                                        AS customer_count,
  SUM(lifetime_revenue)                           AS total_revenue_usd,
  ROUND(AVG(lifetime_revenue),2)                  AS avg_revenue_per_customer_usd,
  ROUND(AVG(total_transactions),2)               AS avg_transactions_per_customer
FROM workspace.ss_demo.semantic_mart_customer_dim
GROUP BY segment_name;


-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.ss_demo.semantic_geography_insights AS
SELECT
  country,
  region,
  state,
  city,
  total_revenue                                   AS revenue_usd,
  total_units_sold                                AS units_sold,
  total_orders                                    AS orders,
  avg_order_value                                 AS avg_order_value_usd,
  unique_customers                                AS customers,
  b2b_customers,
  b2c_customers,
  ROUND(b2b_customers / NULLIF(unique_customers,0) * 100,1)
                                                  AS b2b_customer_percent,
  days_with_sales
FROM workspace.ss_demo.semantic_mart_geography_dim;
