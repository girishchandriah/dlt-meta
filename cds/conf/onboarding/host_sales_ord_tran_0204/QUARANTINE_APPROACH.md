# Quarantine Approach for Records Without sales_ord_id

## Overview

Records from landing that cannot be matched with a `sales_ord_id` are quarantined to a **Refinery (Silver) layer table** for investigation and remediation. This follows medallion architecture best practices where data quality concerns are handled in the cleansing layer, keeping Treasury (Gold) clean with only validated data.

## Architecture

```
Landing Table (Bronze)
    │
    ├─→ Has sales_ord_id? → YES → Treasury Table (Gold)
    │                              pfocusdb_sales_ord_tran_dlt_v2
    │
    └─→ Has sales_ord_id? → NO  → Refinery Quarantine Table (Silver)
                                   pfocusst_dsn_st_trans_quarantine_dlt_v2
```

## Why Refinery Layer for Quarantine?

**Quarantine is a data quality concern** and belongs in the **Refinery (Silver) layer**, not Treasury (Gold):

✅ **Benefits:**
- **Treasury stays clean** - Only validated, business-ready data
- **Architectural alignment** - Data quality checks happen in Silver layer
- **Clear separation** - Validation vs. business logic
- **Simpler queries** - Business analysts never see quarantine records
- **Better access control** - Data quality team owns refinery layer

See `QUARANTINE_ARCHITECTURE.md` for detailed architectural rationale.

## Two Dataflows in Same Pipeline

### Dataflow 204_combined (Main Treasury)
**Purpose:** Process records WITH sales_ord_id
- Reads from landing
- Joins with `pfocusdb_sales_ord` to get `sales_ord_id`
- Filters: `where sales_ord_id is not null`
- Writes to: `pfocusdb_sales_ord_tran_dlt_v2` (Treasury layer)

### Dataflow 204_quarantine (Refinery Quarantine)
**Purpose:** Capture records WITHOUT sales_ord_id
- Reads from landing (same source as 204_combined)
- Joins with `pfocusdb_sales_ord` to get `sales_ord_id`
- Filters: `where sales_ord_id is null`
- Writes to: `pfocusst_dsn_st_trans_quarantine_dlt_v2` (Refinery layer)

## Execution

Both dataflows run in the **same pipeline** with `data_flow_group: "sales_ord"`:

```json
{
  "layer": "landing_refinery_treasury",
  "landing.group": "sales_ord",
  "treasury.group": "sales_ord"
}
```

**Execution order:**
1. Landing layer creates: `pfocusst_ff_src_hst_dsn_trans_dlt_v2`
2. Refinery layer processes:
   - Dataflow 204_quarantine → `pfocusst_dsn_st_trans_quarantine_dlt_v2` (quarantine records)
3. Treasury layer processes:
   - Dataflow 204_combined → `pfocusdb_sales_ord_tran_dlt_v2` (valid records)

## Quarantine Table Schema

**Location:** `dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2`

```sql
CREATE TABLE refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2 (
  quarantine_id STRING,              -- Unique identifier (UUID)
  host_sys_cd STRING,                -- System code (AUS, UK1, etc.)
  host_vax_acct_num STRING,          -- Account number
  host_acct_create_dt DATE,          -- Account creation date
  event_id STRING,                   -- Event ID
  sales_ord_tran_id STRING,          -- Transaction ID
  sales_ord_tran_dt DATE,            -- Transaction date
  tran_opr_cd STRING,                -- Transaction operator code
  tran_void_flg STRING,              -- Void flag
  tran_amt DECIMAL(18,4),            -- Transaction amount
  tickets_purchased_qty INT,         -- Quantity
  trans_face_val_amt DECIMAL(18,4),  -- Face value
  trans_service_charge_amt DECIMAL(18,4), -- Service charge
  trans_set_tax_amt DECIMAL(18,4),   -- Tax
  trans_service_tax_amt DECIMAL(18,4), -- Service tax
  trans_service_tax_2_amt DECIMAL(18,4), -- Service tax 2
  trans_zone_charge_amt DECIMAL(18,4), -- Zone charge
  trans_fac_fee_amt DECIMAL(18,4),   -- Facility fee
  trans_exch_fee_amt DECIMAL(18,4),  -- Exchange fee
  price_level_id STRING,             -- Price level
  price_level_nm STRING,             -- Price level name
  tkt_type_cnt INT,                  -- Ticket type count
  acct_mode_cd STRING,               -- Account mode
  quarantine_reason STRING,          -- Reason: 'NO_SALES_ORD_ID'
  quarantine_ts TIMESTAMP            -- When quarantined
)
CLUSTER BY (quarantine_ts)
```

## Why Records End Up in Quarantine

### Reason: `NO_SALES_ORD_ID`

Records are quarantined when the join fails:

```sql
left join pfocusdb_sales_ord o
  on trim(upper(q.system)) = trim(upper(o.host_sys_cd))
  and q.acctdate = o.host_acct_create_dt
  and q._account = o.host_vax_acct_num
where o.sales_ord_id is null  -- No match found
```

**Common causes:**
1. **Sales order not yet loaded** - Transaction arrives before order
2. **Data mismatch** - System code, account, or date doesn't match
3. **Legacy/test data** - Not in `pfocusdb_sales_ord` reference table
4. **Data quality issues** - Null or invalid values in join keys

## Investigating Quarantined Records

### Query: Count by reason
```sql
SELECT
  quarantine_reason,
  COUNT(*) as record_count,
  DATE(quarantine_ts) as quarantine_date
FROM dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2
GROUP BY quarantine_reason, DATE(quarantine_ts)
ORDER BY quarantine_date DESC
```

### Query: Sample quarantined records
```sql
SELECT
  quarantine_id,
  host_sys_cd,
  host_vax_acct_num,
  host_acct_create_dt,
  event_id,
  sales_ord_tran_id,
  tran_amt,
  quarantine_reason,
  quarantine_ts
FROM dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2
ORDER BY quarantine_ts DESC
LIMIT 100
```

### Query: Check if sales_ord exists now
```sql
SELECT
  q.*,
  o.sales_ord_id,
  CASE WHEN o.sales_ord_id IS NOT NULL THEN 'FOUND_NOW' ELSE 'STILL_MISSING' END as status
FROM dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2 q
LEFT JOIN dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord o
  ON trim(upper(q.host_sys_cd)) = trim(upper(o.host_sys_cd))
  AND q.host_acct_create_dt = o.host_acct_create_dt
  AND q.host_vax_acct_num = o.host_vax_acct_num
ORDER BY q.quarantine_ts DESC
LIMIT 100
```

## Reprocessing Quarantined Records

### Option 1: Automatic Reprocessing

If the quarantined records now have matching sales_ord_id, they will be picked up in the next run automatically because:
1. Both dataflows read from landing (same source)
2. If sales_ord now exists, record goes to main treasury
3. Quarantine table keeps historical record

### Option 2: Manual Reprocessing

**Step 1:** Identify records that can now be processed:
```sql
CREATE OR REPLACE TEMP VIEW reprocessable_records AS
SELECT q.*
FROM pfocusst_dsn_st_trans_quarantine_dlt_v2 q
JOIN pfocusdb_sales_ord o
  ON trim(upper(q.host_sys_cd)) = trim(upper(o.host_sys_cd))
  AND q.host_acct_create_dt = o.host_acct_create_dt
  AND q.host_vax_acct_num = o.host_vax_acct_num
WHERE o.sales_ord_id IS NOT NULL
```

**Step 2:** Insert into main treasury table:
```sql
-- This would require custom SQL/Notebook to map quarantine schema to treasury schema
-- and insert with proper transformations
```

### Option 3: Alert on Quarantine Growth

**Monitor quarantine table size:**
```sql
SELECT
  COUNT(*) as total_quarantined,
  COUNT(DISTINCT DATE(quarantine_ts)) as days_with_quarantine,
  MIN(quarantine_ts) as oldest_record,
  MAX(quarantine_ts) as newest_record
FROM pfocusst_dsn_st_trans_quarantine_dlt_v2
```

**Set up alerts** when quarantine count exceeds threshold.

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────┐
│ CSV Files → Landing Table                               │
│   pfocusst_ff_src_hst_dsn_trans_dlt_v2                 │
└─────────────────────────────────────────────────────────┘
                │
                │ Both dataflows read from same landing
                ├─────────────────────┬─────────────────────┐
                │                     │                     │
                ▼                     ▼                     ▼
    ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐
    │ Join with         │ │ Join with         │ │                   │
    │ pfocusdb_sales_ord│ │ pfocusdb_sales_ord│ │                   │
    └───────────────────┘ └───────────────────┘ │                   │
                │                     │           │                   │
      ┌─────────┴─────────┐           │           │                   │
      │ sales_ord_id?     │           │           │                   │
      └─────────┬─────────┘           │           │                   │
            YES │     NO              │           │                   │
                │      │              │           │                   │
                ▼      ▼              │           │                   │
    ┌─────────────────────┐ ┌─────────────────────┐
    │ Main Treasury       │ │ Quarantine Treasury │
    │ (204_combined)      │ │ (204_quarantine)    │
    └─────────────────────┘ └─────────────────────┘
                │                     │
                ▼                     ▼
    ┌─────────────────────┐ ┌─────────────────────┐
    │ pfocusdb_sales_     │ │ pfocusdb_sales_     │
    │ ord_tran_dlt_v2     │ │ ord_tran_quarantine │
    │                     │ │ _dlt_v2             │
    │ (Main table)        │ │ (Quarantine table)  │
    └─────────────────────┘ └─────────────────────┘
```

## CDC Configuration Comparison

### Main Treasury Table
```json
"treasury_cdc_apply_changes": {
  "keys": [
    "sales_ord_id",          // ← Must have sales_ord_id
    "event_id",
    "sales_ord_tran_id",
    "sales_ord_tran_dt"
  ],
  "sequence_by": "host_acct_create_dt",
  "scd_type": "1"
}
```

### Quarantine Table
```json
"treasury_cdc_apply_changes": {
  "keys": [
    "host_sys_cd",           // ← No sales_ord_id (it's null!)
    "host_vax_acct_num",
    "host_acct_create_dt",
    "event_id",
    "sales_ord_tran_id",
    "sales_ord_tran_dt"
  ],
  "sequence_by": "quarantine_ts",
  "scd_type": "1"
}
```

**Key difference:** Quarantine uses `host_sys_cd` instead of `sales_ord_id` because the latter is null.

## Benefits

### 1. No Data Loss
- ✅ All records from landing are processed
- ✅ Records without sales_ord_id are preserved
- ✅ Can be reprocessed later

### 2. Data Quality Visibility
- ✅ Clear visibility into data quality issues
- ✅ Can track quarantine trends over time
- ✅ Easy to identify root causes

### 3. Separation of Concerns
- ✅ Main treasury only contains valid records
- ✅ Quarantine isolated from production data
- ✅ Can investigate without impacting main pipeline

### 4. Operational Flexibility
- ✅ Can reprocess quarantined records
- ✅ Can set up alerts on quarantine growth
- ✅ Can analyze patterns in bad data

## Monitoring & Alerting

### Daily Quarantine Report

```sql
-- Daily summary of quarantined records
SELECT
  DATE(quarantine_ts) as date,
  quarantine_reason,
  COUNT(*) as count,
  SUM(tran_amt) as total_amount,
  COUNT(DISTINCT host_sys_cd) as systems_affected
FROM pfocusst_dsn_st_trans_quarantine_dlt_v2
WHERE quarantine_ts >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY DATE(quarantine_ts), quarantine_reason
ORDER BY date DESC, count DESC
```

### Alert Thresholds

**Set up alerts when:**
- Quarantine count exceeds 100 records/day
- Quarantine percentage exceeds 5% of total records
- Total quarantined amount exceeds $10,000/day

## Summary

The quarantine approach:
- ✅ Captures records without sales_ord_id
- ✅ Runs in same pipeline as main transformation
- ✅ Provides data quality visibility
- ✅ Enables investigation and reprocessing
- ✅ Keeps main treasury table clean
- ✅ No data loss

**Two dataflows, one pipeline:**
1. **204_combined**: Processes valid records → Main treasury
2. **204_quarantine**: Captures invalid records → Quarantine table

Both read from the same landing table and run concurrently in the same pipeline execution.
