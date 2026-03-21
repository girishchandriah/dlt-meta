# Migration Guide: Sales Order Transaction Pipeline (0204)
## Platform Notebooks → DLT-META

**Version:** 1.0
**Date:** February 23, 2025
**Pipeline:** jb_edw_dsn_sales_ord_tran_0204
**Author:** Platform Data Engineering Team

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current Implementation (Platform Notebooks)](#current-implementation-platform-notebooks)
3. [DLT-META Approach 1: With Sales Ord ID Lookup](#dlt-meta-approach-1-with-sales-ord-id-lookup)
4. [DLT-META Approach 2: Without Sales Ord ID (Hash-Based)](#dlt-meta-approach-2-without-sales-ord-id-hash-based)
5. [Comparison Matrix](#comparison-matrix)
6. [Migration Strategy](#migration-strategy)
7. [Testing & Validation](#testing--validation)
8. [Rollback Plan](#rollback-plan)

---

## Executive Summary

This document provides a detailed migration guide for converting the **Sales Order Transaction pipeline (0204)** from Platform Notebooks to DLT-META.

### Current State

- **Implementation:** Platform Notebooks (Spark Streaming)
- **Complexity:** 6 sequential job steps
- **Total Code:** ~1,500 lines of YAML configuration
- **Architecture:** Landing → Refinery (multiple passes) → Treasury (multiple updates)

### Target State (Recommended: Approach 1)

- **Implementation:** DLT-META Framework
- **Complexity:** 2 DLT dataflows
- **Total Configuration:** ~200 lines (JSON + YAML)
- **Architecture:** Landing → Treasury (direct with combined transformation)

### Key Benefits

| Aspect | Platform Notebooks | DLT-META (Approach 1) |
|--------|-------------------|----------------------|
| **Lines of Config** | ~1,500 | ~200 (87% reduction) |
| **Number of Steps** | 6 sequential | 2 parallel dataflows |
| **Maintenance** | Complex, multi-step | Simple, declarative |
| **Self-Referencing** | ❌ Required | ✅ Avoided via workaround |
| **Void Handling** | Complex join logic | Simple CDC merge |
| **Data Quality** | Embedded in step-2 | Separate quarantine flow |

---

## Current Implementation (Platform Notebooks)

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                    6-Step Platform Notebooks Pipeline             │
└──────────────────────────────────────────────────────────────────┘

STEP 1: S3 → Landing (delta_from_s3_uc)
├─ Source: s3://...jb_edw_dsn_sales_ord_tran_0204/file-ingest
├─ Format: CSV (| delimiter)
└─ Target: landing.pfocusst_ff_src_hst_dsn_trans

STEP 2: Landing → Refinery (stream_delta_transformation)
├─ Applies transformations (date conversions, void handling)
├─ Joins with treasury.pfocusdb_sales_ord to get sales_ord_id
├─ Marks records as "processed" if sales_ord_id found
├─ Handles void transaction reversals (self-join with target)
└─ Target: refinery.pfocusst_dsn_st_trans_dl

STEP 3: Refinery → Treasury (stream_delta_transformation)
├─ Reads only processed records (sales_ord_id IS NOT NULL)
├─ Handles void updates via self-join with treasury target
└─ Target: treasury.pfocusdb_sales_ord_tran

STEP 4: Refinery → Refinery Copy (stream_delta_transformation)
├─ Creates copy of refinery data for further processing
└─ Target: refinery.pfocusst_dsn_st_trans_dl1

STEP 5: Refinery Copy → Treasury (stream_delta_transformation)
├─ Inserts new records, updates existing
└─ Target: treasury.pfocusdb_sales_ord_tran

STEP 6: Batch Update from MDB (batch_delta_transformation)
├─ Updates upsell_flg and dlvry_flg from MongoDB data
└─ Target: treasury.pfocusdb_sales_ord_tran
```

### File: `jb_edw_dsn_sales_ord_tran_0204.yml`

**Location:** `/Users/Girish.Chandriah/ln_projects/databricks-rundeck-jobs/job_configs_airflow/`

**Key Characteristics:**

1. **Step 1 (S3 Ingestion):**
   - Service: `delta_from_s3_uc`
   - Loads CSV files from S3
   - 85 columns defined in schema

2. **Step 2 (Complex Refinery Logic):**
   - ~770 lines of SQL
   - Self-references refinery table (reads existing records)
   - Joins with treasury.pfocusdb_sales_ord for sales_ord_id lookup
   - Handles void transaction reversals
   - Marks records as processed/unprocessed

3. **Step 3 (Treasury Void Updates):**
   - ~100 lines of SQL
   - Self-references treasury table (reads existing records)
   - Zeros out amounts for void transactions

4. **Step 4-5 (Additional Processing):**
   - Creates intermediate refinery copy
   - Further processing for edge cases

5. **Step 6 (MDB Updates):**
   - Batch updates from MongoDB data
   - Updates upsell_flg and dlvry_flg

### Critical Problem: Self-Referencing

**Platform Notebooks CAN do this, but DLT-META CANNOT:**

```sql
-- Step 2: Join with refinery table being written to
WITH unprocessed_records AS (
  SELECT up.*,
    if(tgt.sales_ord_id is not null, true, false) as processed
  FROM refinery.pfocusst_dsn_st_trans_dl up  -- ❌ Self-reference!
  LEFT JOIN tgt
    ON up.sales_ord_id = tgt.sales_ord_id
)

-- Step 3: Join with treasury table being written to
SELECT ...
CASE
  WHEN d.vdate = sales_ord_tran.sales_ord_tran_dt THEN 0
  ELSE sales_ord_tran.tran_amt
END as tran_amt
FROM a d
JOIN treasury.pfocusdb_sales_ord_tran sales_ord_tran  -- ❌ Self-reference!
  ON d.system = sales_ord_tran.host_sys_cd
  ...
```

**This is the main bottleneck preventing direct DLT-META migration.**

---

## DLT-META Approach 1: With Sales Ord ID Lookup

### Recommended Approach ✅

This approach **avoids self-referencing** by using DLT-META's CDC workaround pattern.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                 DLT-META: 2 Parallel Dataflows                   │
└─────────────────────────────────────────────────────────────────┘

DATAFLOW 204 (Main Pipeline):
┌──────────┐      ┌──────────────────┐      ┌───────────┐
│ Landing  │─────▶│ Transformation   │─────▶│ Treasury  │
│          │      │ (Combined SQL)   │      │           │
└──────────┘      └──────────────────┘      └───────────┘
- Auto Loader     - Apply refinery          - CDC Merge
- CSV from S3     - Join sales_ord          - SCD Type 1
- Schema DDL      - Zero void amounts       - No self-ref!

DATAFLOW 205 (Quarantine):
┌──────────┐      ┌──────────────────┐      ┌───────────┐
│ Landing  │─────▶│ Filter SQL       │─────▶│ Refinery  │
│          │      │ (No sales_ord_id)│      │(Quarantine)
└──────────┘      └──────────────────┘      └───────────┘
- Read landing    - WHERE sales_ord_id      - Track bad
- Same source       IS NULL                  - records
```

### Files Structure

```
dlt-meta/
└── cds/conf/onboarding/
    └── host_sales_ord_tran_0204/
        ├── onboarding_sales_ord_tran_landing_treasury.json  (Main config)
        └── transformations/
            ├── transformations_sales_ord_combined_treasury_nonprod.yaml
            ├── transformations_sales_ord_quarantine_nonprod.yaml
            └── ... (preprod, prod versions)
```

### Configuration File 1: Onboarding JSON

**File:** `onboarding_sales_ord_tran_landing_treasury.json`

```json
[
  {
    "data_flow_id": "204",
    "data_flow_group": "sales_ord",
    "source_system": "jb_edw",
    "source_format": "cloudFiles",

    // Source Configuration
    "source_details": {
      "source_database": "hostfile",
      "source_table": "dsn_sales_ord_tran",
      "source_path_nonprod": "s3a://.../jb_edw_dsn_sales_ord_tran_0204/file-ingest",
      "source_schema_path": "/Volumes/.../dsn_sales_ord_tran.ddl"
    },

    // Landing Configuration
    "landing_catalog_nonprod": "dataservices_nonprod",
    "landing_database_nonprod": "landing_nonprod",
    "landing_table": "pfocusst_ff_src_hst_dsn_trans_dlt_v2",
    "landing_reader_options": {
      "cloudFiles.format": "csv",
      "header": "false",
      "delimiter": "|",
      "cloudFiles.rescuedDataColumn": "_rescued_data"
    },

    // Skip Refinery (Direct to Treasury)
    "refinery_catalog_nonprod": null,
    "refinery_database_nonprod": null,
    "refinery_table": null,

    // Treasury Configuration
    "treasury_catalog_nonprod": "dataservices_nonprod",
    "treasury_database_nonprod": "treasury_teradata_base_nonprod",
    "treasury_table": "pfocusdb_sales_ord_tran_dlt_v2",
    "treasury_transformation_json_nonprod": "/Volumes/.../transformations_sales_ord_combined_treasury_nonprod.yaml",
    "treasury_cluster_by": ["sales_ord_tran_dt", "event_id"],

    // CDC Configuration (Handles void updates without self-reference!)
    "treasury_cdc_apply_changes": {
      "keys": ["sales_ord_id", "event_id", "sales_ord_tran_id", "sales_ord_tran_dt"],
      "sequence_by": "host_acct_create_dt",
      "scd_type": "1"
    },

    "version": "v3.0",
    "_comment": "Dataflow 204: Landing → Treasury direct. No refinery. CDC handles void updates."
  },
  {
    "data_flow_id": "205",
    "data_flow_group": "sales_ord",
    "source_system": "jb_edw",
    "source_format": "delta",

    // Source: Read from Landing
    "source_details": {
      "source_catalog_nonprod": "dataservices_nonprod",
      "source_database_nonprod": "landing_nonprod",
      "source_table_ref": "pfocusst_ff_src_hst_dsn_trans_dlt_v2"
    },

    // Skip Landing (Already in source)
    "landing_catalog_nonprod": null,

    // Quarantine in Refinery
    "refinery_catalog_nonprod": "dataservices_nonprod",
    "refinery_database_nonprod": "refinery_nonprod",
    "refinery_table": "pfocusst_dsn_st_trans_quarantine_dlt_v2",
    "refinery_transformation_json_nonprod": "/Volumes/.../transformations_sales_ord_quarantine_nonprod.yaml",
    "refinery_cluster_by": ["quarantine_ts"],

    // CDC for Quarantine
    "refinery_cdc_apply_changes": {
      "keys": ["host_sys_cd", "host_vax_acct_num", "host_acct_create_dt",
               "event_id", "sales_ord_tran_id", "sales_ord_tran_dt"],
      "sequence_by": "quarantine_ts",
      "scd_type": "1"
    },

    // No Treasury (Quarantine only)
    "treasury_catalog_nonprod": null,

    "version": "v3.0",
    "_comment": "Dataflow 205: Quarantine records without sales_ord_id"
  }
]
```

### Configuration File 2: Combined Treasury Transformation

**File:** `transformations_sales_ord_combined_treasury_nonprod.yaml`

```yaml
target_table: pfocusdb_sales_ord_tran_dlt_v2
transformation_name: Sales Order Transaction Combined Treasury Transformation
description: "Combined: Applies all refinery transformations + treasury logic in one pass"

sql_query: |
  -- CRITICAL: source_204 is DLT-managed view (incremental data from landing)
  -- DLT automatically tracks processed records, no self-reference needed!

  WITH q_extract AS (
    SELECT
      -- System normalization
      CASE
        WHEN f.system = 'AU1' THEN 'AUS'
        WHEN f.system = 'LON' THEN 'UK1'
        WHEN f.system = 'INT' THEN 'UK2'
        ELSE UPPER(f.system)
      END as system,

      -- Void handling: Zero out amounts (Workaround for self-reference!)
      CASE
        WHEN TRIM(UPPER(f.voidflag)) = 'T' THEN 0.0000
        ELSE f.facilityfee
      END as facility_fee,

      -- Date conversions with century handling
      IF(
        SUBSTRING(f.accountdate FROM -2) >
        (SELECT INT(value) FROM dataservices_nonprod.refinery_nonprod.jobs_configurations WHERE key='century_year'),
        ADD_MONTHS(TO_DATE(f.accountdate,'M/d/yy'), -1200),
        TO_DATE(f.accountdate, 'M/d/yy')
      ) as acctdate,

      -- All other field transformations...
      -- (Similar to Platform Notebooks step 2 logic)

      CASE WHEN TRIM(UPPER(f.voidflag)) = 'T' THEN 0.0000 ELSE f.totaldollars END as tsale,
      CASE WHEN TRIM(UPPER(f.voidflag)) = 'T' THEN 0 ELSE f.tickets END as tqty,

      f.voiddate as vdate,
      f.vaxacct as _account,
      f.vaxevid as vaxevid,
      f.transnum as trnum,
      f.transdate as tdate

    FROM source_204 f  -- DLT-managed incremental view
  ),

  query AS (
    SELECT *,
      -- Apply UDF transformations
      cdsudf_getoptype(vcode, system, acctdate) as void_optype,
      cdsudf_getoptype(tcode, system, acctdate) as tran_optype,
      cdsudf_getoptype(pcode, system, acctdate) as print_optype
    FROM q_extract
  ),

  -- Join with sales_ord dimension to get sales_ord_id
  enriched_data AS (
    SELECT
      q.*,
      o.sales_ord_id
    FROM query q
    LEFT JOIN dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord o
      ON TRIM(UPPER(q.system)) = TRIM(UPPER(o.host_sys_cd))
      AND q.acctdate = o.host_acct_create_dt
      AND q._account = o.host_vax_acct_num
    WHERE o.sales_ord_id IS NOT NULL  -- Only valid records
  ),

  -- Event tcode lookup (from Platform Notebooks step 2 update_1 logic)
  vw AS (
    SELECT
      event_id,
      tcode_slot_num,
      tcode,
      CASE
        WHEN tcode_slot_num >= 5 THEN 'NTL'
        ELSE TRIM(host_sys_cd)
      END as host_sys_cd
    FROM dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_event_tcode t
    WHERE NOT EXISTS (
      SELECT 1
      FROM dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_event_tcode t2
      WHERE t.event_id = t2.event_id
        AND t.event_id_src_sys_cd = t2.event_id_src_sys_cd
        AND t.tcode_slot_num = t2.tcode_slot_num
        AND UPPER(TRIM(t.cur_ind)) <> 'Y'
        AND UPPER(TRIM(t2.cur_ind)) = 'Y'
    )
  ),

  update_1 AS (
    SELECT
      s.*,
      -- Override operator types for specific Canadian events
      CASE
        WHEN SUBSTR(s.pcode, 1, 3) = e.tcode
          AND TRIM(UPPER(s.pcode)) NOT LIKE 'O%'
          AND TRIM(UPPER(s.print_optype)) = 'O' THEN 'B'
        ELSE s.print_optype
      END as print_optype,
      CASE
        WHEN SUBSTR(s.tcode, 1, 3) = e.tcode
          AND TRIM(UPPER(s.tcode)) NOT LIKE 'O%'
          AND TRIM(UPPER(s.tran_optype)) = 'O' THEN 'B'
        ELSE s.tran_optype
      END as tran_optype,
      CASE
        WHEN SUBSTR(s.vcode, 1, 3) = e.tcode
          AND TRIM(UPPER(s.vcode)) NOT LIKE 'O%'
          AND TRIM(UPPER(s.void_optype)) = 'O' THEN 'B'
        ELSE s.void_optype
      END as void_optype
    FROM enriched_data s
    JOIN vw e ON s.vaxevid = e.event_id
      AND e.tcode_slot_num IN (1, 2)
      AND UPPER(TRIM(e.host_sys_cd)) IN ('TOR', 'VAN', 'QUE')
  ),

  all_records AS (
    SELECT s.* FROM enriched_data s LEFT ANTI JOIN update_1 u
      ON s.system = u.system AND s.trnum = u.trnum
    UNION
    SELECT s.* FROM update_1 s
  )

  -- Final selection for treasury
  SELECT
    sales_ord_id,
    system as host_sys_cd,
    _account as host_vax_acct_num,
    acctdate as host_acct_create_dt,
    vaxevid as event_id,
    'EVENTDB' as event_id_src_sys_cd,
    NULL as src_event_id,
    trnum as sales_ord_tran_id,
    tdate as sales_ord_tran_dt,
    NULL as sales_ord_trans_lcl_dttm,
    tsale as tran_amt,
    acmode as acct_mode_cd,
    tqty as tickets_purchased_qty,
    pflag as print_flg,
    pdate as print_dt,
    vflag as tran_void_flg,
    vdate as tran_void_dt,
    pcode as print_opr_cd,
    print_optype as ovrrd_print_opr_type_cd,
    tcode as tran_opr_cd,
    tran_optype as ovrrd_tran_opr_type_cd,
    vcode as void_opr_cd,
    void_optype as ovrrd_void_opr_type_cd,
    ticket_value as trans_face_val_amt,
    serchg as trans_service_charge_amt,
    ser_chrg_tax as trans_service_tax_amt,
    ser_chrg_tax2 as trans_service_tax_2_amt,
    set_tax as trans_set_tax_amt,
    distance_charge as trans_zone_charge_amt,
    facility_fee as trans_fac_fee_amt,
    exchangee_fee as trans_exch_fee_amt,
    NULL as last_batch_id,
    NULL as init_run_id,
    NULL as last_run_id,
    tkt_transfer_sent_flg,
    tkt_transfer_rcvd_flg,
    tkt_transfer_surrender_flg,
    INITCAP(LOWER(resale_status_nm)) as resale_status_nm,
    NULL as upsell_flg,
    NULL as dlvry_flg,
    void_used_opr_cd,
    mimic_flg,
    price_level as price_level_id,
    price_level_name as price_level_nm,
    expmop as expected_pmt_method_type_id,
    expmopname as expected_pmt_method_type_cd,
    num_stypes as tkt_type_cnt
  FROM all_records
```

### Configuration File 3: Quarantine Transformation

**File:** `transformations_sales_ord_quarantine_nonprod.yaml`

```yaml
target_table: pfocusst_dsn_st_trans_quarantine_dlt_v2
transformation_name: Sales Order Transaction Quarantine
description: "Identifies and stores records without valid sales_ord_id for data quality tracking"

sql_query: |
  -- source_205 reads from landing table
  WITH q_extract AS (
    -- Apply same transformations as main flow
    SELECT
      CASE
        WHEN f.system = 'AU1' THEN 'AUS'
        WHEN f.system = 'LON' THEN 'UK1'
        WHEN f.system = 'INT' THEN 'UK2'
        ELSE UPPER(f.system)
      END as system,
      -- ... all transformation logic ...
      f.vaxacct as _account,
      f.vaxevid as vaxevid,
      f.transnum as trnum,
      f.transdate as tdate
    FROM source_205 f
  ),

  enriched_data AS (
    SELECT
      q.*,
      o.sales_ord_id
    FROM q_extract q
    LEFT JOIN dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord o
      ON TRIM(UPPER(q.system)) = TRIM(UPPER(o.host_sys_cd))
      AND q.acctdate = o.host_acct_create_dt
      AND q._account = o.host_vax_acct_num
  )

  -- Select ONLY quarantined records
  SELECT
    system as host_sys_cd,
    _account as host_vax_acct_num,
    acctdate as host_acct_create_dt,
    vaxevid as event_id,
    trnum as sales_ord_tran_id,
    tdate as sales_ord_tran_dt,
    CURRENT_TIMESTAMP() as quarantine_ts,
    'NO_SALES_ORD_ID' as quarantine_reason,
    -- ... all other columns ...
  FROM enriched_data
  WHERE sales_ord_id IS NULL  -- ONLY bad records
```

### Key DLT-META Features Used

#### 1. **No Self-Referencing** (Critical!)

**Platform Notebooks (Required self-reference):**
```sql
-- ❌ Cannot do in DLT
FROM refinery.pfocusst_dsn_st_trans_dl up
LEFT JOIN (SELECT DISTINCT sales_ord_id FROM treasury.pfocusdb_sales_ord_tran) tgt
```

**DLT-META (Workaround via CDC):**
```sql
-- ✅ DLT handles incremental processing automatically
-- source_204 is a DLT-managed view that tracks what's been processed
FROM source_204 f
```

How it works:
1. First transaction: `amount=100, void_flag='F'` → INSERT
2. Void transaction: `amount=0, void_flag='T'` → UPDATE (CDC automatically merges based on keys)
3. DLT's CDC `apply_changes` handles the merge without self-reference!

#### 2. **Automatic Incremental Processing**

DLT automatically:
- Tracks which records have been processed
- Provides incremental views (`source_204`)
- Handles duplicates via `sequence_by`

#### 3. **Parallel Dataflows**

```
DATAFLOW 204 (Main)      DATAFLOW 205 (Quarantine)
     ↓                            ↓
   Running                     Running
simultaneously!             simultaneously!
```

No sequential dependencies = faster processing!

---

## DLT-META Approach 2: Without Sales Ord ID (Hash-Based)

### Alternative Approach (When sales_ord Dimension Not Available)

If `pfocusdb_sales_ord` dimension table doesn't exist or you need to generate `sales_ord_id` yourself.

### Architecture Differences

```
APPROACH 1 (Recommended):              APPROACH 2 (Alternative):
┌──────────┐                          ┌──────────┐
│ Landing  │                          │ Landing  │
└─────┬────┘                          └─────┬────┘
      │                                     │
      ▼                                     ▼
┌──────────────────────┐           ┌──────────────────────┐
│ Join with sales_ord  │           │ Generate sales_ord_id│
│ dimension to get ID  │           │ using HASH function  │
└─────┬────────────────┘           └─────┬────────────────┘
      │                                   │
      ▼                                   ▼
┌──────────┐                          ┌──────────┐
│ Treasury │                          │ Treasury │
└──────────┘                          └──────────┘
```

### Configuration File: Onboarding JSON (Approach 2)

**File:** `onboarding_sales_ord_tran_landing_treasury_wo_sales_id.json`

```json
[
  {
    "data_flow_id": "210",  // Different data_flow_id
    "data_flow_group": "sales_ord_1",
    "source_system": "jb_edw",

    // Same source/landing configuration as Approach 1
    "landing_table": "pfocusst_ff_src_hst_dsn_trans_dlt_v3",  // Different table name

    // Treasury configuration
    "treasury_table": "pfocusdb_sales_ord_tran_dlt_v3",  // Different table name
    "treasury_transformation_json_nonprod": "/Volumes/.../transformations_sales_ord_combined_treasury_wo_sales_id_nonprod.yaml",

    // Same CDC configuration
    "treasury_cdc_apply_changes": {
      "keys": ["sales_ord_id", "event_id", "sales_ord_tran_id", "sales_ord_tran_dt"],
      "sequence_by": "host_acct_create_dt",
      "scd_type": "1"
    },

    "_comment": "Approach 2: Generates sales_ord_id via HASH instead of dimension lookup"
  }
]
```

### Transformation YAML (Approach 2)

**File:** `transformations_sales_ord_combined_treasury_wo_sales_id_nonprod.yaml`

```yaml
sql_query: |
  WITH q_extract AS (
    -- Same transformation logic as Approach 1
    SELECT ...
    FROM source_210 f
  ),

  query AS (
    SELECT *,
      cdsudf_getoptype(vcode, system, acctdate) as void_optype,
      cdsudf_getoptype(tcode, system, acctdate) as tran_optype,
      cdsudf_getoptype(pcode, system, acctdate) as print_optype
    FROM q_extract
  ),

  -- DIFFERENCE: Generate sales_ord_id via HASH instead of lookup
  enriched_data AS (
    SELECT
      q.*,
      -- Generate surrogate key using HASH
      ABS(HASH(
        CONCAT(
          COALESCE(system, ''),
          COALESCE(_account, ''),
          COALESCE(CAST(acctdate AS STRING), '')
        )
      )) * -1 AS sales_ord_id  -- Negative to distinguish from real IDs
    FROM query q
  )

  -- Rest of transformation same as Approach 1
  SELECT ... FROM enriched_data
```

### When to Use Approach 2

**✅ Use Approach 2 When:**
- `pfocusdb_sales_ord` dimension doesn't exist yet
- Building net-new pipeline without historical data
- Need to bootstrap the pipeline before dimension is available

**❌ Don't Use Approach 2 When:**
- Dimension table exists (use Approach 1 instead)
- Need to join with existing sales_ord_id values
- Downstream systems expect specific sales_ord_id format

---

## Comparison Matrix

### Configuration Complexity

| Metric | Platform Notebooks | DLT-META Approach 1 | DLT-META Approach 2 |
|--------|-------------------|---------------------|---------------------|
| **Total Lines** | ~1,500 | ~200 | ~200 |
| **Number of Files** | 1 YAML | 3 (1 JSON + 2 YAML) | 2 (1 JSON + 1 YAML) |
| **Job Steps** | 6 sequential | 2 parallel | 1 single |
| **Self-Reference** | ❌ Yes (requires Platform Notebooks) | ✅ No (avoided via CDC workaround) | ✅ No |
| **Maintenance Effort** | High | Low | Low |

### Feature Comparison

| Feature | Platform Notebooks | DLT-META Approach 1 | DLT-META Approach 2 |
|---------|-------------------|---------------------|---------------------|
| **Void Handling** | Complex self-join | Zero amounts + CDC | Zero amounts + CDC |
| **Quarantine** | Embedded in step-2 | Separate dataflow 205 | N/A (all records valid) |
| **Sales Ord Lookup** | Multiple joins | Single join in SQL | Hash-based generation |
| **Incremental Processing** | Manual checkpoints | Automatic (DLT) | Automatic (DLT) |
| **Data Quality** | Mixed with logic | Separate dataflow | Built-in (no bad records) |
| **Performance** | Good (Spark Streaming) | Better (DLT optimizations) | Better (DLT optimizations) |

### Operational Comparison

| Aspect | Platform Notebooks | DLT-META Approach 1 | DLT-META Approach 2 |
|--------|-------------------|---------------------|---------------------|
| **Deployment** | Control Panel | DLT Pipeline | DLT Pipeline |
| **Monitoring** | Custom dashboards | DLT Event Log | DLT Event Log |
| **Debugging** | Manual log analysis | DLT Error Tracking | DLT Error Tracking |
| **Rollback** | Redeploy notebook | Redeploy config | Redeploy config |
| **Testing** | Manual test data | DLT Expectations | DLT Expectations |

---

## Migration Strategy

### Phase 1: Preparation (Week 1)

**Tasks:**

1. **Backup Current Pipeline**
   ```bash
   # Backup Platform Notebooks config
   cp jb_edw_dsn_sales_ord_tran_0204.yml jb_edw_dsn_sales_ord_tran_0204.yml.backup

   # Document current state
   - Control Panel job ID
   - Rundeck schedules
   - Current run times
   - Data volumes
   ```

2. **Set Up DLT-META Environment**
   ```bash
   # Install DLT-META framework
   dltmeta install --env nonprod

   # Create onboarding directory
   mkdir -p dlt-meta/cds/conf/onboarding/host_sales_ord_tran_0204
   ```

3. **Create Transformation YAMLs**
   - Copy SQL from Platform Notebooks
   - Adapt to DLT-META patterns
   - Remove self-referencing logic
   - Add CDC workaround logic

4. **Validate Schema DDL**
   ```bash
   # Check schema file exists
   ls /Volumes/.../dsn_sales_ord_tran.ddl

   # Validate column mappings
   ```

### Phase 2: Development (Week 2)

**Tasks:**

1. **Create DLT-META Configurations**
   ```bash
   # Create onboarding JSON
   vi onboarding_sales_ord_tran_landing_treasury.json

   # Create transformation YAMLs
   vi transformations_sales_ord_combined_treasury_nonprod.yaml
   vi transformations_sales_ord_quarantine_nonprod.yaml
   ```

2. **Deploy to Nonprod**
   ```bash
   # Deploy DLT-META pipeline
   dltmeta deploy \
     --onboard_layer treasury \
     --onboard_file_path /path/to/onboarding_sales_ord_tran_landing_treasury.json \
     --env nonprod
   ```

3. **Test with Sample Data**
   ```sql
   -- Create test dataset (1 day of data)
   CREATE OR REPLACE TABLE test_data AS
   SELECT * FROM landing.pfocusst_ff_src_hst_dsn_trans
   WHERE insert_ts >= CURRENT_DATE() - 1
   LIMIT 10000;
   ```

4. **Validate Output**
   ```sql
   -- Compare Platform Notebooks vs DLT-META results
   SELECT
     COUNT(*) as record_count,
     SUM(tran_amt) as total_amount,
     COUNT(DISTINCT sales_ord_id) as unique_orders
   FROM treasury.pfocusdb_sales_ord_tran  -- Platform Notebooks
   WHERE sales_ord_tran_dt = CURRENT_DATE();

   SELECT
     COUNT(*) as record_count,
     SUM(tran_amt) as total_amount,
     COUNT(DISTINCT sales_ord_id) as unique_orders
   FROM treasury.pfocusdb_sales_ord_tran_dlt_v2  -- DLT-META
   WHERE sales_ord_tran_dt = CURRENT_DATE();
   ```

### Phase 3: Parallel Run (Week 3-4)

**Setup:**

```
┌────────────────────────┐      ┌────────────────────────┐
│ Platform Notebooks     │      │ DLT-META (Approach 1)  │
│ (Current Production)   │      │ (Shadow/Validation)    │
└────────┬───────────────┘      └───────┬────────────────┘
         │                               │
         ├─ landing.pfocusst_...         ├─ landing.pfocusst_..._dlt_v2
         ├─ refinery.pfocusst_...        └─ treasury.pfocusdb_..._dlt_v2
         └─ treasury.pfocusdb_...               (different table)

    Both pipelines run simultaneously!
```

**Tasks:**

1. **Schedule Both Pipelines**
   - Platform Notebooks: Keep existing schedule
   - DLT-META: Run 30 minutes after Platform Notebooks

2. **Daily Validation Checks**
   ```sql
   -- Reconciliation query
   WITH platform AS (
     SELECT sales_ord_id, event_id, sales_ord_tran_id, tran_amt
     FROM treasury.pfocusdb_sales_ord_tran
     WHERE sales_ord_tran_dt = CURRENT_DATE()
   ),
   dlt_meta AS (
     SELECT sales_ord_id, event_id, sales_ord_tran_id, tran_amt
     FROM treasury.pfocusdb_sales_ord_tran_dlt_v2
     WHERE sales_ord_tran_dt = CURRENT_DATE()
   )
   SELECT
     'Platform Notebooks' as source,
     COUNT(*) as records,
     SUM(tran_amt) as total_amount
   FROM platform
   UNION ALL
   SELECT
     'DLT-META' as source,
     COUNT(*) as records,
     SUM(tran_amt) as total_amount
   FROM dlt_meta;

   -- Should match within tolerance!
   ```

3. **Monitor for Discrepancies**
   - Record counts
   - Sum of amounts
   - Distinct sales_ord_id counts
   - Void transaction handling

### Phase 4: Cutover (Week 5)

**Pre-Cutover Checklist:**

- [ ] 2 weeks of parallel run with <0.1% discrepancy
- [ ] DLT-META performance meets SLA (< 2 hours for daily load)
- [ ] Quarantine dataflow captures expected bad records
- [ ] Downstream consumers tested with DLT-META data
- [ ] Rollback plan documented and tested
- [ ] Approval from data engineering leadership

**Cutover Steps:**

1. **Friday Evening (Low traffic time)**
   ```bash
   # Step 1: Disable Platform Notebooks pipeline
   # (Control Panel: Set job to "inactive")

   # Step 2: Final run of Platform Notebooks
   # (Process any remaining data)

   # Step 3: Rename DLT-META tables to production names
   ```

   ```sql
   -- Step 3: Swap table names
   ALTER TABLE treasury.pfocusdb_sales_ord_tran
     RENAME TO pfocusdb_sales_ord_tran_platform_backup;

   ALTER TABLE treasury.pfocusdb_sales_ord_tran_dlt_v2
     RENAME TO pfocusdb_sales_ord_tran;
   ```

   ```bash
   # Step 4: Update DLT-META config to use production table names

   # Step 5: Deploy updated DLT-META config

   # Step 6: First production run
   databricks jobs run-now --job-id <dlt-meta-job-id>
   ```

2. **Monday Morning: Validation**
   ```sql
   -- Verify weekend data processed correctly
   SELECT
     sales_ord_tran_dt,
     COUNT(*) as records,
     SUM(tran_amt) as total_amount
   FROM treasury.pfocusdb_sales_ord_tran
   WHERE sales_ord_tran_dt >= CURRENT_DATE() - 3
   GROUP BY sales_ord_tran_dt
   ORDER BY sales_ord_tran_dt;
   ```

3. **Week 1 Post-Cutover: Close Monitoring**
   - Daily reconciliation
   - Performance monitoring
   - User feedback
   - Incident response

### Phase 5: Cleanup (Week 6)

**Tasks:**

1. **Decommission Platform Notebooks**
   ```bash
   # Archive Platform Notebooks config
   mv jb_edw_dsn_sales_ord_tran_0204.yml archive/

   # Remove from Control Panel (set to "archived")
   ```

2. **Drop Backup Tables** (After 30-day retention)
   ```sql
   DROP TABLE IF EXISTS treasury.pfocusdb_sales_ord_tran_platform_backup;
   DROP TABLE IF EXISTS refinery.pfocusst_dsn_st_trans_dl;
   DROP TABLE IF EXISTS refinery.pfocusst_dsn_st_trans_dl1;
   ```

3. **Update Documentation**
   - Update Confluence pages
   - Update architecture diagrams
   - Update runbooks

---

## Testing & Validation

### Test Categories

#### 1. Unit Testing (Development)

**Test File Ingestion:**
```sql
-- Create test CSV file with known data
-- Upload to S3 test path
-- Verify landing table receives data

SELECT COUNT(*) FROM landing.pfocusst_ff_src_hst_dsn_trans_dlt_v2
WHERE insert_ts >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR;
```

**Test Transformations:**
```sql
-- Test void handling
SELECT system, vflag, facility_fee, tsale
FROM source_204
WHERE vflag = 'T';
-- Expected: All amounts should be 0

-- Test date conversions
SELECT accountdate, acctdate
FROM source_204
WHERE accountdate IS NOT NULL
LIMIT 10;
```

**Test CDC Merge:**
```sql
-- Insert initial record
-- Insert void record with same keys
-- Verify void record UPDATEs original

SELECT * FROM treasury.pfocusdb_sales_ord_tran_dlt_v2
WHERE sales_ord_id = <test_id>
  AND event_id = <test_event>
ORDER BY update_ts DESC;
```

#### 2. Integration Testing (Nonprod)

**Test End-to-End Flow:**
```bash
# 1. Place test file in S3
aws s3 cp test_file.csv s3://...nonprod/jb_edw_dsn_sales_ord_tran_0204/file-ingest/

# 2. Trigger DLT pipeline
databricks jobs run-now --job-id <dlt-meta-nonprod-job-id>

# 3. Wait for completion
databricks jobs get-run --run-id <run-id>

# 4. Validate results
```

```sql
-- Check landing
SELECT COUNT(*) FROM landing.pfocusst_ff_src_hst_dsn_trans_dlt_v2
WHERE file_name LIKE '%test_file%';

-- Check treasury
SELECT COUNT(*) FROM treasury.pfocusdb_sales_ord_tran_dlt_v2
WHERE host_acct_create_dt = CURRENT_DATE();

-- Check quarantine
SELECT COUNT(*), quarantine_reason
FROM refinery.pfocusst_dsn_st_trans_quarantine_dlt_v2
WHERE quarantine_ts >= CURRENT_DATE()
GROUP BY quarantine_reason;
```

#### 3. Performance Testing

**Load Testing:**
```sql
-- Process 1 month of historical data
-- Measure processing time
-- Compare to Platform Notebooks baseline

-- Platform Notebooks: ~6 hours for 1 month
-- DLT-META Target: < 4 hours for 1 month
```

**Stress Testing:**
```bash
# Test with large file (10M records)
# Measure:
# - Ingestion time
# - Transformation time
# - CDC merge time
# - Total end-to-end time
```

#### 4. Data Quality Testing

**Reconciliation Test:**
```sql
-- Compare Platform Notebooks vs DLT-META for same date range
WITH platform_summary AS (
  SELECT
    sales_ord_tran_dt,
    COUNT(*) as record_count,
    SUM(tran_amt) as total_amount,
    SUM(tickets_purchased_qty) as total_tickets,
    COUNT(DISTINCT sales_ord_id) as unique_orders
  FROM treasury.pfocusdb_sales_ord_tran
  WHERE sales_ord_tran_dt BETWEEN '2025-02-01' AND '2025-02-28'
  GROUP BY sales_ord_tran_dt
),
dlt_meta_summary AS (
  SELECT
    sales_ord_tran_dt,
    COUNT(*) as record_count,
    SUM(tran_amt) as total_amount,
    SUM(tickets_purchased_qty) as total_tickets,
    COUNT(DISTINCT sales_ord_id) as unique_orders
  FROM treasury.pfocusdb_sales_ord_tran_dlt_v2
  WHERE sales_ord_tran_dt BETWEEN '2025-02-01' AND '2025-02-28'
  GROUP BY sales_ord_tran_dt
)
SELECT
  p.sales_ord_tran_dt,
  p.record_count as platform_records,
  d.record_count as dlt_meta_records,
  (d.record_count - p.record_count) as record_diff,
  p.total_amount as platform_amount,
  d.total_amount as dlt_meta_amount,
  (d.total_amount - p.total_amount) as amount_diff,
  CASE
    WHEN ABS(d.record_count - p.record_count) / p.record_count > 0.001 THEN '❌ FAIL'
    WHEN ABS(d.total_amount - p.total_amount) / p.total_amount > 0.001 THEN '❌ FAIL'
    ELSE '✅ PASS'
  END as validation_status
FROM platform_summary p
JOIN dlt_meta_summary d ON p.sales_ord_tran_dt = d.sales_ord_tran_dt
ORDER BY p.sales_ord_tran_dt;
```

**Acceptance Criteria:**
- Record count difference: < 0.1%
- Amount difference: < 0.1%
- All dates in range covered
- No missing dates

---

## Rollback Plan

### Scenario 1: Issues During Development/Testing

**Action:** Simply continue using Platform Notebooks.

```bash
# No rollback needed - Platform Notebooks never stopped
# Fix DLT-META issues and retry
```

### Scenario 2: Issues During Parallel Run

**Action:** Continue Platform Notebooks, fix DLT-META in background.

```bash
# Platform Notebooks continues as production
# DLT-META runs in shadow mode
# No impact to production
```

### Scenario 3: Critical Issue After Cutover (Within 24 hours)

**Emergency Rollback:**

```sql
-- Step 1: Swap table names back
ALTER TABLE treasury.pfocusdb_sales_ord_tran
  RENAME TO pfocusdb_sales_ord_tran_dlt_v2_rollback;

ALTER TABLE treasury.pfocusdb_sales_ord_tran_platform_backup
  RENAME TO pfocusdb_sales_ord_tran;

-- Step 2: Verify Platform Notebooks data is complete
SELECT MAX(sales_ord_tran_dt) FROM treasury.pfocusdb_sales_ord_tran;
-- Should be within 1 day of current date
```

```bash
# Step 3: Re-enable Platform Notebooks pipeline
# (Control Panel: Set job to "active")

# Step 4: Trigger immediate run to catch up
# (Process any missing data since cutover)

# Step 5: Investigate DLT-META issue
# (Fix and plan for re-attempt)
```

**Rollback Time:** < 30 minutes

### Scenario 4: Issue After Cutover (> 24 hours)

**Careful Rollback with Data Merge:**

```sql
-- Step 1: Platform Notebooks may be missing data since cutover
-- Step 2: Need to merge DLT-META data back into Platform Notebooks table

CREATE OR REPLACE TABLE treasury.pfocusdb_sales_ord_tran_merged AS
SELECT * FROM treasury.pfocusdb_sales_ord_tran_platform_backup  -- Old data
UNION
SELECT * FROM treasury.pfocusdb_sales_ord_tran  -- New DLT-META data
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sales_ord_id, event_id, sales_ord_tran_id, sales_ord_tran_dt
  ORDER BY update_ts DESC
) = 1;

-- Step 3: Swap merged table to production
ALTER TABLE treasury.pfocusdb_sales_ord_tran
  RENAME TO pfocusdb_sales_ord_tran_dlt_v2_rollback;

ALTER TABLE treasury.pfocusdb_sales_ord_tran_merged
  RENAME TO pfocusdb_sales_ord_tran;

-- Step 4: Re-enable Platform Notebooks
```

**Rollback Time:** 1-2 hours (includes data merge)

---

## Appendix: Key Differences Summary

### Self-Referencing Workaround

| Aspect | Platform Notebooks | DLT-META |
|--------|-------------------|----------|
| **Refinery Self-Reference** | ✅ Allowed | ❌ Prohibited |
| **Treasury Self-Reference** | ✅ Allowed | ❌ Prohibited |
| **Void Handling** | Join with existing records | Zero amounts + CDC merge |
| **Incremental Tracking** | Manual SQL logic | Automatic (DLT framework) |

### Architecture Simplification

```
BEFORE (Platform Notebooks):          AFTER (DLT-META):
6 sequential steps                    2 parallel dataflows
~1,500 lines                          ~200 lines
Complex self-joins                    Simple CDC merge
Manual checkpoints                    Automatic (DLT)
Embedded data quality                 Separate quarantine
```

### Operational Benefits

1. **Reduced Complexity:** 87% reduction in configuration lines
2. **Improved Maintainability:** Declarative config vs. imperative code
3. **Better Monitoring:** DLT Event Log vs. custom dashboards
4. **Automatic Optimization:** DLT framework handles performance
5. **Standardization:** Follows organizational DLT-META standard

---

## Conclusion

**Recommended Approach: DLT-META Approach 1** ✅

- Simplifies 6-step pipeline to 2 parallel dataflows
- Reduces configuration by 87%
- Avoids self-referencing via CDC workaround
- Maintains data quality via separate quarantine
- Aligns with organizational standard

**Migration Timeline: 5-6 weeks**

- Week 1: Preparation
- Week 2: Development
- Week 3-4: Parallel run
- Week 5: Cutover
- Week 6: Cleanup

**Next Steps:**

1. Review this migration guide with team
2. Get approval from data engineering leadership
3. Schedule Week 1 preparation tasks
4. Begin DLT-META configuration development

---

**Document Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-02-23 | Initial migration guide created |
