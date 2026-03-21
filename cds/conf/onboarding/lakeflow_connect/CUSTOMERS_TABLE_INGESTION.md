# CUSTOMERS Table Ingestion from Federated Oracle

## Problem Statement

- **Table:** `_member_db_catalog.member.CUSTOMERS`
- **Size:** 438 million rows
- **Challenge:** Full table scan is too slow for federated queries
- **Solution:** Incremental ingestion using `LAST_UPDATE` column with WHERE clause filter

---

## Configuration Overview

The [lakeflow_member_db.json](lakeflow_member_db.json) file has been configured to ingest CUSTOMERS table efficiently:

### Key Features

✅ **Filtered ingestion:** Only pulls last 2 days of data (`LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)`)
✅ **SCD Type 1 in Landing:** Keeps latest version only
✅ **SCD Type 2 in Refinery:** Maintains full history with `__START_AT` and `__END_AT`
✅ **Optimized for large tables:** 128MB target file size, auto-compaction enabled
✅ **Partitioned by load date:** Efficient pruning for recent queries

---

## Step-by-Step Implementation

### Step 1: Verify Oracle Connection (2 minutes)

```sql
-- Test connection to federated catalog
SHOW TABLES IN _member_db_catalog.member;

-- Verify CUSTOMERS table is accessible
DESCRIBE _member_db_catalog.member.CUSTOMERS;

-- Check row count (this may take time)
SELECT COUNT(*) FROM _member_db_catalog.member.CUSTOMERS;
```

### Step 2: Test Filtered Query (3 minutes)

**CRITICAL:** Always test with a filter before running the pipeline:

```sql
-- Test: How many rows in last 2 days?
SELECT COUNT(*)
FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 2);

-- Expected: Thousands to low millions (manageable)
-- If this returns > 10M rows, adjust filter to 1 day

-- Sample the data
SELECT
    CUSTOMER_ID,
    UC_EMAIL_ADDRESS,
    LAST_UPDATE,
    CREATED
FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)
LIMIT 10;

-- Verify CUSTOMER_ID is unique
SELECT CUSTOMER_ID, COUNT(*) as cnt
FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;
-- Should return 0 rows
```

### Step 3: Check Oracle Indexes (Ask DBA)

**Required indexes for performance:**

```sql
-- These indexes MUST exist on Oracle side
CREATE INDEX idx_customers_last_update ON CUSTOMERS(LAST_UPDATE);
CREATE INDEX idx_customers_customer_id ON CUSTOMERS(CUSTOMER_ID);
CREATE INDEX idx_customers_composite ON CUSTOMERS(CUSTOMER_ID, LAST_UPDATE);
```

Without these indexes, queries will be **extremely slow**.

### Step 4: Validate Configuration File

Review [lakeflow_member_db.json](lakeflow_member_db.json):

```json
{
  "data_flow_id": "6001",
  "data_flow_group": "federated_oracle_member_db",
  "source_system": "oracle_member_db",
  "source_format": "snapshot",

  "source_details": {
    "snapshot_format": "delta",
    "source_catalog": "_member_db_catalog",
    "source_database": "member",
    "source_table": "CUSTOMERS"
  },

  "landing_catalog_prod": "main",
  "landing_database_prod": "staging",
  "landing_table": "customers_raw",
  "landing_partition_columns": "_load_date",

  "landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)",

  "landing_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "1"
  },

  "refinery_catalog_prod": "main",
  "refinery_database_prod": "refinery",
  "refinery_table": "customers",

  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "2"
  }
}
```

**Key settings:**

- `landing_where_clause`: Filters at source (critical for performance!)
- `keys: ["CUSTOMER_ID"]`: Primary key for CDC
- `sequence_by: "LAST_UPDATE"`: Determines record order
- Landing = SCD Type 1 (latest only), Refinery = SCD Type 2 (full history)

### Step 5: Deploy Pipeline (3 minutes)

```bash
# Navigate to project root
cd /Users/Girish.Chandriah/ln_projects/dlt-meta

# Deploy using dlt-meta
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_db.json \
  --env prod \
  --layer landing_refinery
```

**Expected output:**
```
✓ Pipeline created: <pipeline-id>
✓ Landing table: main.staging.customers_raw
✓ Refinery table: main.refinery.customers
```

### Step 6: Run Pipeline (5-10 minutes)

```bash
# Start the pipeline
databricks pipelines start --pipeline-id <your-pipeline-id>

# Monitor progress
databricks pipelines get --pipeline-id <your-pipeline-id>
```

### Step 7: Verify Data (3 minutes)

```sql
-- Check landing layer (latest version only)
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT CUSTOMER_ID) as unique_customers,
    MIN(LAST_UPDATE) as oldest_update,
    MAX(LAST_UPDATE) as newest_update
FROM main.staging.customers_raw;

-- Check refinery layer (historical data)
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT CUSTOMER_ID) as unique_customers,
    SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) as current_records,
    SUM(CASE WHEN __END_AT IS NOT NULL THEN 1 ELSE 0 END) as historical_records
FROM main.refinery.customers;

-- View history for a specific customer
SELECT
    CUSTOMER_ID,
    UC_EMAIL_ADDRESS,
    LAST_UPDATE,
    __START_AT,
    __END_AT,
    CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as STATUS
FROM main.refinery.customers
WHERE CUSTOMER_ID = 123456  -- Replace with actual customer ID
ORDER BY __START_AT DESC;
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Oracle Database (438M rows)                                 │
│ • _member_db_catalog.member.CUSTOMERS                       │
│ • Filtered by: LAST_UPDATE >= CURRENT_DATE - 2 days         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ Lakehouse Federation                                        │
│ • Reads filtered data from Oracle                           │
│ • Pushes WHERE clause to Oracle                             │
│ • Transfers only recent changes (thousands of rows)         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ DLT-META Pipeline                                           │
│                                                             │
│ Landing: main.staging.customers_raw                         │
│ • SCD Type 1 (latest version)                               │
│ • Partitioned by _load_date                                 │
│ • Optimized writes: 128MB files                             │
│                                                             │
│ Refinery: main.refinery.customers                           │
│ • SCD Type 2 (full history)                                 │
│ • Tracks changes with __START_AT, __END_AT                  │
│ • Current records have __END_AT = NULL                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Performance Tuning

### Initial Load (Historical Data)

For the **first run only**, you may want to load more historical data:

```json
"landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 90)"
```

This will pull 90 days of history. **After the initial load**, change back to 2 days:

```json
"landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)"
```

### Adjust Filter Based on Volume

Monitor the row counts per day:

```sql
-- Check daily volume
SELECT
    DATE(LAST_UPDATE) as update_date,
    COUNT(*) as row_count
FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 7)
GROUP BY DATE(LAST_UPDATE)
ORDER BY update_date DESC;
```

**Guidelines:**
- If daily volume < 100K rows → Keep 2-day filter
- If daily volume > 1M rows → Reduce to 1-day filter
- If daily volume > 10M rows → Run pipeline multiple times per day

### Optimize Pipeline Schedule

**Recommended schedule:**

```json
{
  "schedule": {
    "quartz_cron_expression": "0 0 */4 * * ?",
    "timezone_id": "America/Los_Angeles"
  }
}
```

This runs every 4 hours, ensuring fresh data without overwhelming Oracle.

---

## Troubleshooting

### Issue: Pipeline times out

**Cause:** Too many rows being pulled

**Fix:** Reduce the filter window:
```json
"landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 1)"
```

### Issue: Missing recent records

**Cause:** Filter is too narrow

**Fix:** Increase the filter window:
```json
"landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 3)"
```

### Issue: Query is slow even with filter

**Cause:** Missing indexes on Oracle

**Fix:** Ask your DBA to create indexes (see Step 3)

### Issue: Duplicate records in refinery

**Cause:** Multiple updates to same CUSTOMER_ID within filter window

**Fix:** This is expected! SCD Type 2 tracks all changes:

```sql
-- View all changes for a customer
SELECT
    CUSTOMER_ID,
    UC_EMAIL_ADDRESS,
    __START_AT,
    __END_AT
FROM main.refinery.customers
WHERE CUSTOMER_ID = 123456
ORDER BY __START_AT;
```

### Issue: No data in landing table

**Cause:** No records match the filter

**Fix:** Verify there are recent updates:

```sql
SELECT
    COUNT(*),
    MIN(LAST_UPDATE),
    MAX(LAST_UPDATE)
FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 2);
```

---

## Next Steps: Add More Tables

Now that CUSTOMERS is working, add other tables from the member schema:

### 1. MEMBERS Table

Create `lakeflow_members.json`:

```json
{
  "data_flow_id": "6002",
  "source_details": {
    "source_catalog": "_member_db_catalog",
    "source_database": "member",
    "source_table": "MEMBERS"
  },
  "landing_table": "members_raw",
  "landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)",
  "landing_apply_changes_from_snapshot": {
    "keys": ["MEMBER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "1"
  }
}
```

### 2. COUNTRY Table (Small Reference Data)

Create `lakeflow_country.json`:

```json
{
  "data_flow_id": "6003",
  "source_details": {
    "source_catalog": "_member_db_catalog",
    "source_database": "member",
    "source_table": "COUNTRY"
  },
  "landing_table": "country_raw",
  "landing_apply_changes_from_snapshot": {
    "keys": ["COUNTRY_ID"],
    "scd_type": "1"
  }
}
```

**Note:** No WHERE clause for COUNTRY - it's a small dimension table, load it fully.

### 3. TD_CUSTOMER_BUMP Table

Create `lakeflow_customer_bump.json`:

```json
{
  "data_flow_id": "6004",
  "source_details": {
    "source_catalog": "_member_db_catalog",
    "source_database": "crmapps",
    "source_table": "TD_CUSTOMER_BUMP"
  },
  "landing_table": "customer_bump_raw",
  "landing_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  }
}
```

---

## Monitoring Queries

### Pipeline Health

```sql
-- Check landing layer freshness
SELECT
    MAX(_load_date) as latest_load_date,
    MAX(LAST_UPDATE) as latest_data_timestamp,
    DATEDIFF(day, MAX(LAST_UPDATE), CURRENT_DATE()) as data_age_days
FROM main.staging.customers_raw;

-- Check refinery layer completeness
SELECT
    COUNT(DISTINCT CUSTOMER_ID) as total_customers,
    SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) as active_records,
    MAX(__START_AT) as latest_change_timestamp
FROM main.refinery.customers;
```

### Data Quality Checks

```sql
-- Check for orphaned records (CUSTOMER_ID not in CUSTOMERS)
SELECT COUNT(*)
FROM main.refinery.customers c
WHERE NOT EXISTS (
    SELECT 1 FROM _member_db_catalog.member.CUSTOMERS o
    WHERE o.CUSTOMER_ID = c.CUSTOMER_ID
);

-- Check for late-arriving data
SELECT
    COUNT(*) as late_arrivals,
    MIN(LAST_UPDATE) as oldest_late_arrival
FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE < date_sub(CURRENT_DATE(), 2);
```

---

## Best Practices

1. ✅ **Always filter at source** with `landing_where_clause`
2. ✅ **Test queries in SQL Workspace** before deploying pipeline
3. ✅ **Monitor Oracle indexes** - ensure they exist and are maintained
4. ✅ **Schedule appropriately** - balance freshness vs. load on Oracle
5. ✅ **Use SCD Type 2 in refinery** - preserves audit trail
6. ✅ **Partition by date** - enables efficient pruning
7. ✅ **Monitor data volumes** - adjust filter window as needed
8. ✅ **Document dependencies** - track which tables depend on CUSTOMERS

---

## Resources

- **dlt-meta Documentation:** https://databrickslabs.github.io/dlt-meta/
- **Lakehouse Federation Guide:** https://docs.databricks.com/en/query-federation/oracle.html
- **Delta Live Tables CDC:** https://docs.databricks.com/en/delta-live-tables/cdc.html
- **Project Files:**
  - Configuration: [lakeflow_member_db.json](lakeflow_member_db.json)
  - Quick Start: [QUICK_START.md](QUICK_START.md)
  - Advanced Guide: [FEDERATED_ORACLE_CDC_GUIDE.md](FEDERATED_ORACLE_CDC_GUIDE.md)
