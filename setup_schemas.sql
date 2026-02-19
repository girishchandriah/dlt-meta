-- ============================================================================
-- SETUP: Create required schemas for DLT-META pipeline
-- ============================================================================
--
-- This script creates the necessary schemas (databases) that your
-- DLT-META pipeline expects to exist.
--
-- Run this in a Databricks SQL notebook BEFORE running your pipeline.
-- ============================================================================

USE CATALOG privacy_nonprod;

-- ============================================================================
-- CREATE SCHEMAS
-- ============================================================================

-- Landing layer schema (Bronze)
CREATE SCHEMA IF NOT EXISTS dltmeta_landing
COMMENT 'Landing/Bronze layer for DLT-META pipelines - raw data ingestion';

-- Refinery layer schema (Silver)
CREATE SCHEMA IF NOT EXISTS dltmeta_refinery
COMMENT 'Refinery/Silver layer for DLT-META pipelines - cleaned and transformed data';

-- Treasury layer schema (Gold) - if you plan to use it
CREATE SCHEMA IF NOT EXISTS dltmeta_treasury
COMMENT 'Treasury/Gold layer for DLT-META pipelines - aggregated business-level data';

-- DLT-META configuration schema (for dataflowspec tables)
CREATE SCHEMA IF NOT EXISTS dlt_meta_dataflowspecs
COMMENT 'Schema for storing DLT-META dataflowspec configuration tables';

-- ============================================================================
-- VERIFY SCHEMAS EXIST
-- ============================================================================

SHOW SCHEMAS IN privacy_nonprod LIKE 'dltmeta_%';
SHOW SCHEMAS IN privacy_nonprod LIKE 'dlt_meta_%';

-- ============================================================================
-- CHECK PERMISSIONS (Optional)
-- ============================================================================

-- Verify you have the right permissions
-- SHOW GRANTS ON SCHEMA privacy_nonprod.dltmeta_landing;
-- SHOW GRANTS ON SCHEMA privacy_nonprod.dltmeta_refinery;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
--
-- After running this script:
--
-- 1. Verify all schemas were created (check output above)
-- 2. Go back to your DLT pipeline and click "Start"
-- 3. The pipeline should now create tables in these schemas
--
-- If you still get errors:
-- - Check that your onboarding.json has the correct schema names
-- - Verify the pipeline configuration has the correct dataflowspecTable paths
-- ============================================================================
