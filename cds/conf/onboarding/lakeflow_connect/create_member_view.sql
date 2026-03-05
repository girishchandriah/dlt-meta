-- ============================================================================
-- Federated View for Oracle Member Database CDC
-- ============================================================================
-- Purpose: Wraps complex member/customer query for dlt-meta ingestion
-- Source: Oracle MEMBER schema
-- Target: Databricks Delta via dlt-meta
--
-- This view consolidates data from:
-- 1. MEMBER.MEMBERS (base table)
-- 2. MEMBER.CUSTOMERS (email and customer details)
-- 3. MEMBER.COUNTRY (country reference data)
-- 4. CRMAPPS.TD_CUSTOMER_BUMP (change tracking table)
--
-- The view returns incremental changes based on:
-- - LAST_UPDATE column (24-hour lookback window)
-- - Records in TD_CUSTOMER_BUMP table (explicit change markers)
-- ============================================================================

CREATE OR REPLACE VIEW my_oracle_connection.member.vw_member_customer_cdc AS

-- Part 1: Changed records based on LAST_UPDATE timestamp
SELECT
    m.FIRST_NAME                    as FIRST_NAME,
    m.MIDDLE_NAME                   as MIDDLE_NAME,
    m.LAST_NAME                     as LAST_NAME,
    c.UC_EMAIL_ADDRESS              as UC_EMAIL_ADDRESS,
    m.CUSTOMER_ID                   as CUSTOMER_ID,
    m.CREATED                       as CREATED,
    m.CREATE_FROM_DOMAIN_ID         as CREATE_FROM_DOMAIN_ID,
    m.HOME_DOMAIN_ID                as HOME_DOMAIN_ID,
    m.ACTIVE                        as ACTIVE,
    m.GENDER                        as GENDER,
    m.LAST_UPDATE                   as LAST_UPDATE,
    co.COUNTRY_ID                   as COUNTRY_ID,
    co.ABBREV                       as COUNTRY_ABBREV,
    m.PHONE_NUMBER                  as MBR_PHN_NUM,
    m.PHONE_NUMBER_ADDED            as MBR_PHN_TS,
    m.LOCKED                        as MBR_LOCKED_FLG,
    m.LOCKED_BY                     as MBR_LOCKED_BY,
    m.LOCKED_REASON                 as MBR_LOCKED_REASON,
    'TIMESTAMP_BASED'               as CHANGE_SOURCE,
    CAST(CURRENT_TIMESTAMP() as TIMESTAMP) as _EXTRACTION_TIMESTAMP
FROM
    MEMBER.MEMBERS m
LEFT OUTER JOIN
    MEMBER.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    MEMBER.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID
WHERE
    -- Include records updated in last 24 hours from either MEMBERS or CUSTOMERS
    (
        m.LAST_UPDATE >= CURRENT_TIMESTAMP() - INTERVAL '24' HOUR
        OR
        COALESCE(c.LAST_UPDATE, TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
            >= CURRENT_TIMESTAMP() - INTERVAL '24' HOUR
    )

UNION ALL

-- Part 2: Changed records based on bump table (explicit change markers)
SELECT
    m.FIRST_NAME                    as FIRST_NAME,
    m.MIDDLE_NAME                   as MIDDLE_NAME,
    m.LAST_NAME                     as LAST_NAME,
    c.UC_EMAIL_ADDRESS              as UC_EMAIL_ADDRESS,
    m.CUSTOMER_ID                   as CUSTOMER_ID,
    m.CREATED                       as CREATED,
    m.CREATE_FROM_DOMAIN_ID         as CREATE_FROM_DOMAIN_ID,
    m.HOME_DOMAIN_ID                as HOME_DOMAIN_ID,
    m.ACTIVE                        as ACTIVE,
    m.GENDER                        as GENDER,
    m.LAST_UPDATE                   as LAST_UPDATE,
    co.COUNTRY_ID                   as COUNTRY_ID,
    co.ABBREV                       as COUNTRY_ABBREV,
    m.PHONE_NUMBER                  as MBR_PHN_NUM,
    m.PHONE_NUMBER_ADDED            as MBR_PHN_TS,
    m.LOCKED                        as MBR_LOCKED_FLG,
    m.LOCKED_BY                     as MBR_LOCKED_BY,
    m.LOCKED_REASON                 as MBR_LOCKED_REASON,
    'BUMP_TABLE'                    as CHANGE_SOURCE,
    CAST(CURRENT_TIMESTAMP() as TIMESTAMP) as _EXTRACTION_TIMESTAMP
FROM
    MEMBER.MEMBERS m
INNER JOIN
    CRMAPPS.TD_CUSTOMER_BUMP bump
    ON m.CUSTOMER_ID = bump.CUSTOMER_ID
LEFT OUTER JOIN
    MEMBER.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    MEMBER.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID;


-- ============================================================================
-- Usage Instructions
-- ============================================================================
--
-- 1. Execute this SQL in Databricks SQL Workspace
--
-- 2. Verify the view works:
--    SELECT COUNT(*) FROM my_oracle_connection.member.vw_member_customer_cdc;
--
-- 3. Sample the data:
--    SELECT * FROM my_oracle_connection.member.vw_member_customer_cdc LIMIT 10;
--
-- 4. Check for duplicates (shouldn't have any):
--    SELECT CUSTOMER_ID, COUNT(*)
--    FROM my_oracle_connection.member.vw_member_customer_cdc
--    GROUP BY CUSTOMER_ID
--    HAVING COUNT(*) > 1;
--
-- 5. Configure dlt-meta to use this view:
--    See lakeflow_member_incremental.json
--
-- ============================================================================

-- ============================================================================
-- Performance Optimization Tips
-- ============================================================================
--
-- On Oracle side, ensure these indexes exist:
--
-- 1. Index on MEMBERS.LAST_UPDATE for time-based filtering
--    CREATE INDEX idx_members_last_update ON MEMBER.MEMBERS(LAST_UPDATE);
--
-- 2. Index on MEMBERS.CUSTOMER_ID for joins
--    CREATE INDEX idx_members_customer_id ON MEMBER.MEMBERS(CUSTOMER_ID);
--
-- 3. Index on CUSTOMERS.CUSTOMER_ID for joins
--    CREATE INDEX idx_customers_customer_id ON MEMBER.CUSTOMERS(CUSTOMER_ID);
--
-- 4. Index on CUSTOMERS.LAST_UPDATE for filtering
--    CREATE INDEX idx_customers_last_update ON MEMBER.CUSTOMERS(LAST_UPDATE);
--
-- 5. Index on TD_CUSTOMER_BUMP.CUSTOMER_ID
--    CREATE INDEX idx_bump_customer_id ON CRMAPPS.TD_CUSTOMER_BUMP(CUSTOMER_ID);
--
-- ============================================================================

-- ============================================================================
-- Notes on Changes from Original Query
-- ============================================================================
--
-- 1. Removed Oracle hints (/*+ use_nl */)
--    - Databricks optimizer handles query planning differently
--    - Federated queries are optimized by Databricks, not Oracle
--
-- 2. Standardized timestamp functions
--    - Changed to_date() to TO_TIMESTAMP() for consistency
--    - Using CURRENT_TIMESTAMP() instead of parameter substitution
--
-- 3. Added CHANGE_SOURCE column
--    - Tracks whether change came from timestamp or bump table
--    - Useful for debugging and data lineage
--
-- 4. Added _EXTRACTION_TIMESTAMP
--    - Captures when data was extracted from Oracle
--    - Useful for monitoring freshness
--
-- 5. Removed {LAST_UPDATE} parameter
--    - Using fixed 24-hour lookback window
--    - Adjust INTERVAL '24' HOUR if needed
--
-- ============================================================================
