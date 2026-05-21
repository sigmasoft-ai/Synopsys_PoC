-- Databricks notebook source
CREATE OR REPLACE TABLE workspace.ss_demo.semantic_mart_sales_fact AS
SELECT
  -- Primary key
  transaction_id,
  
  -- Foreign keys (for joins by analysts)
  date_id                                         AS date_key,
  product_sku                                     AS product_key,
  customer_name                                   AS customer_key,
  CONCAT(country, '-', state, '-', city)          AS geography_key,
  
  -- Time dimensions (denormalized for Genie)
  transaction_date                                AS date,
  year,
  quarter,
  quarter_num,
  month,
  month_name,
  day_of_month,
  day_of_week_name,
  day_type,
  
  -- Customer dimensions (denormalized for Genie)
  customer_name,
  customer_type                                   AS customer_segment_code,
  customer_segment                                AS customer_segment_name,
  end_user,
  
  -- Geography dimensions (denormalized for Genie)
  country,
  region,
  state,
  city,
  
  -- Product dimensions (denormalized for Genie)
  product_sku,
  product_name,
  category,
  brand,
  
  -- Measures (for aggregation)
  units_sold                                      AS quantity,
  unit_price,
  revenue                                         AS total_amount,
  revenue                                         AS revenue_usd,
  currency,
  
  -- Derived metrics (pre-calculated)
  CASE 
    WHEN customer_type = 'B2B' THEN revenue * 0.85
    ELSE revenue
  END                                             AS net_revenue_usd,
  
  transaction_value_band,
  
  -- Metadata
  processed_timestamp

FROM workspace.ss_demo.gold_sales_transactions_wide;

-- Set primary key for integrity
ALTER TABLE workspace.ss_demo.semantic_mart_sales_fact 
ALTER COLUMN transaction_id SET NOT NULL;

ALTER TABLE workspace.ss_demo.semantic_mart_sales_fact 
ADD CONSTRAINT pk_sales_fact PRIMARY KEY (transaction_id);

-- Rich business metadata for Genie
COMMENT ON TABLE workspace.ss_demo.semantic_mart_sales_fact IS 
'Unified sales fact table combining transactional detail with dimensional context. Use for all sales analysis including revenue trends, product performance, customer behavior, and geographic analysis. Contains both denormalized attributes (for natural language queries) and dimensional keys (for SQL joins).';

COMMENT ON COLUMN workspace.ss_demo.semantic_mart_sales_fact.revenue_usd IS 'Total transaction revenue in USD';
COMMENT ON COLUMN workspace.ss_demo.semantic_mart_sales_fact.customer_segment_name IS 'Business for B2B, Consumer for B2C';
COMMENT ON COLUMN workspace.ss_demo.semantic_mart_sales_fact.region IS 'Geographic region: EMEA, East, West, South, North, APAC';
