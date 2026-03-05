-- ============================================================================
-- RECOMMENDED: Create View in Local Databricks Schema
-- ============================================================================
-- This creates the view in main.staging (local Databricks catalog)
-- View definition lives in Databricks, queries Oracle via federation
-- ============================================================================

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS main.staging
COMMENT 'Staging layer for external data sources';

-- Create the view
CREATE OR REPLACE VIEW main.staging.vw_member_customer_cdc AS

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
    my_oracle_connection.member.MEMBERS m
LEFT OUTER JOIN
    my_oracle_connection.member.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    my_oracle_connection.member.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID
WHERE
    (
        m.LAST_UPDATE >= CURRENT_TIMESTAMP() - INTERVAL '24' HOUR
        OR
        COALESCE(c.LAST_UPDATE, TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
            >= CURRENT_TIMESTAMP() - INTERVAL '24' HOUR
    )

UNION ALL

-- Part 2: Changed records based on bump table
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
    my_oracle_connection.member.MEMBERS m
INNER JOIN
    my_oracle_connection.crmapps.TD_CUSTOMER_BUMP bump
    ON m.CUSTOMER_ID = bump.CUSTOMER_ID
LEFT OUTER JOIN
    my_oracle_connection.member.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    my_oracle_connection.member.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID;

-- Grant permissions (adjust as needed for your organization)
GRANT SELECT ON main.staging.vw_member_customer_cdc TO `data_engineers`;
GRANT SELECT ON main.staging.vw_member_customer_cdc TO `data_analysts`;

-- ============================================================================
-- Test the View
-- ============================================================================

-- 1. Verify it works
SELECT COUNT(*) as total_records
FROM main.staging.vw_member_customer_cdc;

-- 2. Check sample data
SELECT *
FROM main.staging.vw_member_customer_cdc
LIMIT 10;

-- 3. Check for duplicates (should return 0 rows)
SELECT CUSTOMER_ID, COUNT(*) as duplicate_count
FROM main.staging.vw_member_customer_cdc
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;

-- 4. Check data freshness
SELECT
    MIN(LAST_UPDATE) as oldest_update,
    MAX(LAST_UPDATE) as newest_update,
    COUNT(*) as total_records,
    COUNT(DISTINCT CUSTOMER_ID) as unique_customers
FROM main.staging.vw_member_customer_cdc;

-- 5. Verify both parts of UNION ALL are working
SELECT
    CHANGE_SOURCE,
    COUNT(*) as record_count,
    COUNT(DISTINCT CUSTOMER_ID) as unique_customers
FROM main.staging.vw_member_customer_cdc
GROUP BY CHANGE_SOURCE;

-- ============================================================================
-- Note: Replace Connection Name
-- ============================================================================
-- Replace 'my_oracle_connection' with your actual federated connection name
--
-- To find your connection name, run:
-- SHOW CONNECTIONS;
--
-- Or check in Databricks SQL Workspace → Data Explorer → External Data
-- ============================================================================
