-- Run this ONCE before first pipeline execution in each environment

-- NONPROD
CREATE TABLE IF NOT EXISTS dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt (
  sales_ord_id STRING,
  event_id STRING,
  sales_ord_tran_id STRING,
  sales_ord_tran_dt DATE,
  host_sys_cd STRING,
  host_vax_acct_num STRING,
  host_acct_create_dt DATE
  -- Add all other columns from your treasury schema
)
USING delta
COMMENT 'Pre-created treasury table for step 4 JOIN dependency';

-- PREPROD  
CREATE TABLE IF NOT EXISTS dataservices_preprod.treasury_teradata_base_preprod.pfocusdb_sales_ord_tran_dlt (
  sales_ord_id STRING,
  event_id STRING,
  sales_ord_tran_id STRING,
  sales_ord_tran_dt DATE,
  host_sys_cd STRING,
  host_vax_acct_num STRING,
  host_acct_create_dt DATE
  -- Add all other columns from your treasury schema
)
USING delta
COMMENT 'Pre-created treasury table for step 4 JOIN dependency';

-- PROD
CREATE TABLE IF NOT EXISTS dataservices_treasury.treasury_teradata_base.pfocusdb_sales_ord_tran_dlt (
  sales_ord_id STRING,
  event_id STRING,
  sales_ord_tran_id STRING,
  sales_ord_tran_dt DATE,
  host_sys_cd STRING,
  host_vax_acct_num STRING,
  host_acct_create_dt DATE
  -- Add all other columns from your treasury schema
)
USING delta
COMMENT 'Pre-created treasury table for step 4 JOIN dependency';
