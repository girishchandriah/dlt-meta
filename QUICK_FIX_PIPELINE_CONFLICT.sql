-- ============================================================================
-- QUICK FIX: Drop all DLT-managed tables to resolve pipeline conflict
-- ============================================================================
--
-- ERROR: Table already managed by pipeline 1d6fc313-9068-4d44-89e4-ff4b8398d939
--
-- SOLUTION: Run this script in a Databricks SQL notebook to drop all
--           conflicting tables, then re-deploy your pipeline.
--
-- ⚠️  WARNING: This will DELETE all data in these tables!
-- ============================================================================

USE CATALOG privacy_nonprod;

-- ============================================================================
-- LANDING LAYER TABLES (Bronze)
-- ============================================================================

-- Customers
DROP TABLE IF EXISTS dltmeta_landing.customers;
DROP TABLE IF EXISTS dltmeta_landing.customers_quarantine;

-- Transactions
DROP TABLE IF EXISTS dltmeta_landing.transactions;
DROP TABLE IF EXISTS dltmeta_landing.transactions_quarantine;

-- Products
DROP TABLE IF EXISTS dltmeta_landing.products;
DROP TABLE IF EXISTS dltmeta_landing.products_quarantine;

-- Stores
DROP TABLE IF EXISTS dltmeta_landing.stores;
DROP TABLE IF EXISTS dltmeta_landing.stores_quarantine;

-- ============================================================================
-- REFINERY LAYER TABLES (Silver)
-- ============================================================================

-- Customers
DROP TABLE IF EXISTS dltmeta_refinery.customers;

-- Transactions
DROP TABLE IF EXISTS dltmeta_refinery.transactions;

-- Products
DROP TABLE IF EXISTS dltmeta_refinery.products;

-- Stores
DROP TABLE IF EXISTS dltmeta_refinery.stores;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Check that tables are dropped
SHOW TABLES IN dltmeta_landing;
SHOW TABLES IN dltmeta_refinery;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
--
-- After running this script:
--
-- 1. Go to Lakehouse App (http://localhost:5000)
-- 2. Click "Deploy" tab
-- 3. Verify these settings:
--    - Onboard Landing Group Name: A1
--    - Onboard Refinery Group Name: A1
--    - DLT Target Schema: dltmeta_landing (or your landing schema)
-- 4. Click "Deploy"
-- 5. Go to Databricks UI → Workflows → Delta Live Tables
-- 6. Find your new pipeline and click "Start"
--
-- Your pipeline should now run successfully!
-- ============================================================================
