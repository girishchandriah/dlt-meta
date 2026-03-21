# Quick Start: Federated Oracle CDC with dlt-meta

## Your Situation

✅ You have a **complex SQL query** with JOINs and UNION ALL
✅ Query **filters by LAST_UPDATE** (incremental, not snapshot)
✅ Connecting to **Oracle database** via Lakehouse Federation
❌ **Cannot use** `applyChangesFromSnapshot` (that's for full table snapshots)

## What You Need

### Step 1: Create the Federated View (5 minutes)

Execute the SQL in [create_member_view.sql](create_member_view.sql) in **Databricks SQL Workspace**:

```sql
CREATE OR REPLACE VIEW my_oracle_connection.member.vw_member_customer_cdc AS
-- [Your complex SQL query - see create_member_view.sql]
```

Replace `my_oracle_connection` with your actual federated connection name.

### Step 2: Test the View (2 minutes)

```sql
-- Check it returns data
SELECT COUNT(*) FROM my_oracle_connection.member.vw_member_customer_cdc;

-- View sample records
SELECT * FROM my_oracle_connection.member.vw_member_customer_cdc LIMIT 10;

-- Verify no duplicates
SELECT CUSTOMER_ID, COUNT(*) as cnt
FROM my_oracle_connection.member.vw_member_customer_cdc
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;
```

### Step 3: Update dlt-meta Configuration (2 minutes)

Update [lakeflow_member_incremental.json](lakeflow_member_incremental.json):

```json
{
  "data_flow_id": "6002",
  "source_format": "delta",
  "source_details": {
    "source_catalog": "my_oracle_connection",
    "source_database": "member",
    "source_table": "vw_member_customer_cdc"
  },
  "refinery_cdc_apply_changes": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "2"
  }
}
```

**Key settings:**
- ✅ `source_format: "delta"` (not "snapshot")
- ✅ `refinery_cdc_apply_changes` (not `landing_apply_changes_from_snapshot`)
- ✅ `sequence_by: "LAST_UPDATE"` (tracks change order)

### Step 4: Deploy Pipeline (2 minutes)

```bash
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_incremental.json \
  --env prod \
  --layer landing_refinery
```

### Step 5: Run & Verify (5 minutes)

```bash
# Start the pipeline
databricks pipelines start --pipeline-id <your-pipeline-id>

# Check status
databricks pipelines get --pipeline-id <your-pipeline-id>
```

Then verify the data:

```sql
-- Landing: Latest data
SELECT COUNT(*) FROM main.landing.member_customer_data;

-- Refinery: Historical data with SCD Type 2
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    __START_AT,
    __END_AT,
    CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as STATUS
FROM main.refinery.member_customer_data
WHERE CUSTOMER_ID = 123456  -- Pick a test customer
ORDER BY __START_AT DESC;
```

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│ Oracle Database                                                  │
│ • MEMBER.MEMBERS                                                 │
│ • MEMBER.CUSTOMERS                                               │
│ • MEMBER.COUNTRY                                                 │
│ • CRMAPPS.TD_CUSTOMER_BUMP                                       │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ Federated View: vw_member_customer_cdc                           │
│ • Joins 4 tables                                                 │
│ • Filters by LAST_UPDATE >= now() - 24 hours                     │
│ • UNION ALL with bump table                                      │
│ • Returns incremental changes only                               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ Lakeflow Connect (Federated Read)                                │
│ • Reads from view as Delta format                                │
│ • Streams changes to DLT pipeline                                │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ DLT-META Pipeline                                                │
│ Landing Layer:                                                   │
│ • main.landing.member_customer_data                              │
│ • Stores raw changes                                             │
│                                                                  │
│ Refinery Layer:                                                  │
│ • main.refinery.member_customer_data                             │
│ • Applies CDC with cdcApplyChanges                               │
│ • SCD Type 2: maintains history                                  │
│ • Keys: [CUSTOMER_ID]                                            │
│ • Sequence: LAST_UPDATE                                          │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ Result: Historical Member Data                                   │
│                                                                  │
│ CUSTOMER_ID | FIRST_NAME | LAST_NAME | __START_AT  | __END_AT   │
│ ─────────────────────────────────────────────────────────────────│
│ 123456      | John       | Doe       | 2024-01-01  | 2024-02-01 │
│ 123456      | John       | Smith     | 2024-02-01  | NULL       │
│             │            │           │             │ ↑ CURRENT  │
└──────────────────────────────────────────────────────────────────┘
```

## Key Differences: Snapshot vs Incremental CDC

| What You Have | What You DON'T Have |
|---------------|---------------------|
| ✅ Incremental query (LAST_UPDATE filter) | ❌ Full table snapshot |
| ✅ Returns only changed records | ❌ Returns all records |
| ✅ Use `refinery_cdc_apply_changes` | ❌ Use `landing_apply_changes_from_snapshot` |
| ✅ Need `sequence_by` column | ❌ Only need `keys` |
| ✅ `source_format: "delta"` | ❌ `source_format: "snapshot"` |

## Configuration Comparison

### ❌ WRONG (Snapshot CDC)
```json
{
  "source_format": "snapshot",
  "source_details": {
    "snapshot_format": "delta",
    "source_table": "orders"
  },
  "landing_apply_changes_from_snapshot": {
    "keys": ["order_id"],
    "scd_type": "1"
  }
}
```
**Why wrong:** Your query is incremental, not a snapshot.

### ✅ CORRECT (Incremental CDC)
```json
{
  "source_format": "delta",
  "source_details": {
    "source_table": "vw_member_customer_cdc"
  },
  "refinery_cdc_apply_changes": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "2"
  }
}
```
**Why correct:** Matches your incremental query pattern.

## Files You Need

| File | Purpose | Action |
|------|---------|--------|
| [create_member_view.sql](create_member_view.sql) | SQL to create federated view | Execute in Databricks SQL |
| [lakeflow_member_incremental.json](lakeflow_member_incremental.json) | dlt-meta configuration | Deploy with dlt-meta CLI |
| [FEDERATED_ORACLE_CDC_GUIDE.md](FEDERATED_ORACLE_CDC_GUIDE.md) | Detailed guide with 3 options | Read for understanding |
| [QUICK_START.md](QUICK_START.md) | This file | Follow steps 1-5 |

## Troubleshooting

### Pipeline fails with "table not found"
**Fix:** Verify federated connection name and view exists:
```sql
SHOW TABLES IN my_oracle_connection.member;
```

### No data in refinery table
**Fix:** Check if view returns data and LAST_UPDATE is recent:
```sql
SELECT
    COUNT(*) as total_records,
    MIN(LAST_UPDATE) as oldest,
    MAX(LAST_UPDATE) as newest
FROM my_oracle_connection.member.vw_member_customer_cdc;
```

### Duplicate records in output
**Fix:** Verify CUSTOMER_ID is unique in the view:
```sql
SELECT CUSTOMER_ID, COUNT(*)
FROM my_oracle_connection.member.vw_member_customer_cdc
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;
```

### Slow performance
**Fix:** Add indexes to Oracle tables (requires DBA):
```sql
CREATE INDEX idx_members_last_update ON MEMBER.MEMBERS(LAST_UPDATE);
CREATE INDEX idx_members_customer_id ON MEMBER.MEMBERS(CUSTOMER_ID);
```

## Next Steps

1. ✅ Create the federated view
2. ✅ Test it returns data
3. ✅ Deploy dlt-meta pipeline
4. ✅ Schedule pipeline (hourly/daily based on SLA)
5. ✅ Set up monitoring and alerts
6. 📚 Read [FEDERATED_ORACLE_CDC_GUIDE.md](FEDERATED_ORACLE_CDC_GUIDE.md) for advanced topics

## Support

- **dlt-meta docs:** https://databrickslabs.github.io/dlt-meta/
- **Lakehouse Federation:** https://docs.databricks.com/en/query-federation/
- **DLT CDC:** https://docs.databricks.com/en/delta-live-tables/cdc.html
