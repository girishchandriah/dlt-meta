# Conversion Guide: jb_edw_dsn_sales_ord_tran_0204.yml to DLT-META Format

## Overview

Your original YAML file defines a **6-step sequential pipeline** for processing DSN sales order transactions through the medallion architecture (Landing → Refinery → Treasury). This guide explains how to convert it to DLT-META format.

## Original Pipeline Structure

### Step 1: Landing Layer (S3 → Delta)
- **Service**: `delta_from_s3_uc`
- **Source**: S3 CSV files (pipe-delimited, no header)
- **Target**: `landing.pfocusst_ff_src_hst_dsn_trans`
- **Purpose**: Raw ingestion of transaction files

### Step 2: Refinery Layer (Landing → Refinery)
- **Service**: `platinum_td_stream_delta_transformation_uc`
- **Source**: Landing table with CDC (readChangeFeed)
- **Target**: `refinery.pfocusst_dsn_st_trans_dl`
- **Purpose**: Complex transformations including:
  - System code normalization (AU1→AUS, LON→UK1, INT→UK2)
  - Date conversions with century_year logic
  - Void flag handling (zero out amounts when voided)
  - UDF calls: `cdsudf_getoptype()`
  - Join with treasury sales_ord table to mark processed records
  - Generate UUID for primary key

### Step 3: Treasury Layer (Refinery → Treasury - Void Updates)
- **Service**: `platinum_td_stream_delta_transformation_uc`
- **Source**: Refinery table (readChangeFeed, startingVersion: 2572)
- **Target**: `treasury.pfocusdb_sales_ord_tran`
- **Purpose**: Update treasury for voided transactions
  - Only processes records where `sales_ord_id is not null` and `vflag='T'`
  - Adjusts amounts based on void_date vs transaction_date

### Step 4: Intermediate Refinery (Refinery → Refinery)
- **Service**: `platinum_td_stream_delta_transformation_uc`
- **Source**: `refinery.pfocusst_dsn_st_trans_dl`
- **Target**: `refinery.pfocusst_dsn_st_trans_dl1`
- **Purpose**: Date adjustment for voided transactions
  - Changes `tdate` to `vdate` when transaction is voided and matched in treasury

### Step 5: Treasury Layer (Refinery → Treasury - Inserts/Updates)
- **Service**: `platinum_td_stream_delta_transformation_uc`
- **Source**: `refinery.pfocusst_dsn_st_trans_dl1` (readChangeFeed, startingVersion: 2586)
- **Target**: `treasury.pfocusdb_sales_ord_tran`
- **Purpose**: Main treasury inserts and updates
  - Updates existing treasury records (matched by composite key)
  - Inserts new records not yet in treasury
  - Filters out voided transactions (`vflag <> 'T'`)
  - Uses latest update_ts for deduplication

### Step 6: Treasury Batch Update
- **Service**: `platinum_batch_delta_transformation_td`
- **Source**: `refinery.pfocusst_mdb_st_order_events_0208_dl`
- **Target**: `treasury.pfocusdb_sales_ord_tran`
- **Purpose**: Batch update for upsell and delivery flags
  - Updates `upsell_flg` and `dlvry_flg` from order events table
  - Only updates records modified in last 7 days

## DLT-META Conversion Strategy

### Approach: Multi-Flow Pipeline

DLT-META uses a declarative approach where each data flow represents a single transformation path. For this complex pipeline, we need **3 separate data flows**:

1. **Main Flow** (data_flow_id: 204): Landing → Refinery → Treasury (Steps 1-3)
2. **Intermediate Flow** (data_flow_id: 204-intermediate): Refinery → Refinery → Treasury (Steps 4-5)
3. **Batch Update Flow** (data_flow_id: 204-batch-update): Refinery → Treasury (Step 6)

### Conversion Summary

| Original Step | DLT-META Equivalent | Notes |
|---------------|---------------------|-------|
| Step 1 | Main flow: `landing_*` fields | CSV ingestion with Auto Loader |
| Step 2 | Main flow: `refinery_*` fields + transformation JSON | Extract to external file |
| Step 3 | Main flow: `treasury_*` fields + transformation JSON | Void transaction updates |
| Step 4 | Intermediate flow: `refinery_*` fields | Date adjustments |
| Step 5 | Intermediate flow: `treasury_*` fields | Inserts/updates logic |
| Step 6 | Batch update flow: `treasury_*` fields | Batch update from order events |

## Files Created

### 1. Onboarding File
**Location**: `/Users/Girish.Chandriah/ln_projects/dlt-meta/onboarding_sales_ord_tran.json`

Contains 3 data flow definitions with proper environment suffixes and Unity Catalog references.

### 2. Transformation Files (Need to be Created)

You need to create these transformation JSON files with the SQL extracted from the YAML:

#### a. Step 2 Transformation (Refinery)
**File**: `/dbfs/dlt-meta/transformations/sales_ord/pfocusst_dsn_st_trans_dl_transformation_prod.json`
- Started in: `transformations_sales_ord_step2.json` (already created)
- Contains: The massive CTE-based SQL from Step 2 (lines 168-769 in YAML)
- **Action Required**: Complete the SQL (truncated due to length)

#### b. Step 3 Transformation (Treasury - Void Updates)
**File**: `/dbfs/dlt-meta/transformations/sales_ord/pfocusdb_sales_ord_tran_transformation_prod.json`
- Extract SQL from YAML lines 856-943
- Key logic: Join refinery with treasury, update amounts for voided transactions

#### c. Step 4 Transformation (Refinery Date Adjustments)
**File**: `/dbfs/dlt-meta/transformations/sales_ord/pfocusst_dsn_st_trans_dl1_transformation_prod.json`
- Extract SQL from YAML lines 1026-1084
- Key logic: Adjust tdate to vdate for matched voided transactions

#### d. Step 5 Transformation (Treasury Inserts/Updates)
**File**: `/dbfs/dlt-meta/transformations/sales_ord/pfocusdb_sales_ord_tran_step5_transformation_prod.json`
- Extract SQL from YAML lines 1171-1337
- Key logic: UNION of updates and inserts with qualify for deduplication

#### e. Step 6 Transformation (Treasury Batch Update)
**File**: `/dbfs/dlt-meta/transformations/sales_ord/pfocusdb_sales_ord_tran_batch_update_prod.json`
- Extract SQL from YAML lines 1418-1487
- Key logic: Update upsell_flg and dlvry_flg from order events (last 7 days)

**Note**: Create separate `_nonprod.json` and `_preprod.json` versions of each file with environment-specific references.

## Key Differences: Platform Notebooks vs DLT-META

### Platform Notebooks (Original YAML)
- **Execution Model**: Sequential steps orchestrated by service framework
- **SQL Location**: Inline in YAML (under `source_sql` key)
- **Services**: Explicit service names (`delta_from_s3_uc`, `platinum_td_stream_delta_transformation_uc`)
- **Environment Handling**: Nested `environments` object with trigger configs
- **Table References**: String interpolation with `{unity_catalog[layer]}.database{env_suffix}.table`
- **Streaming**: Explicit `trigger` configuration per environment
- **Change Data Feed**: Configured in `reader_options` per step

### DLT-META (Converted JSON)
- **Execution Model**: Declarative medallion architecture (DLT framework handles orchestration)
- **SQL Location**: External JSON files referenced by `*_transformation_json_*` fields
- **Services**: Implicit (determined by layer configuration)
- **Environment Handling**: Separate fields with `_prod`, `_nonprod`, `_preprod` suffixes
- **Table References**: Uses DLT-META variable substitution (handled by framework)
- **Streaming**: Defined at data flow level
- **Change Data Feed**: Configured in `refinery_reader_options` and `treasury_reader_options`

## Variable Substitution

### Original YAML Format
```yaml
target_db: "{unity_catalog[landing]}.landing{env_suffix}"
```

### DLT-META Equivalent
Use environment-specific field names:
```json
"landing_database_nonprod": "landing_nonprod",
"landing_database_preprod": "landing_preprod",
"landing_database_prod": "landing"
```

## Critical Configuration Notes

### 1. Unity Catalog References
Your YAML uses placeholders like `{unity_catalog[landing]}`. In DLT-META:
- Replace with actual catalog names in `*_catalog_prod` fields
- Example: `"landing_catalog_prod": "your_catalog_name"`

### 2. Environment Suffixes
Your YAML uses dynamic `{env_suffix}`:
- nonprod: `_nonprod`
- preprod: `_preprod`
- prod: `` (empty)

Apply these to database names in onboarding file.

### 3. CDC Configuration
Your YAML uses `readChangeFeed: true` and `startingVersion`. In DLT-META:
```json
"refinery_reader_options": {
  "readChangeFeed": "true"
},
"treasury_reader_options": {
  "readChangeFeed": "true",
  "startingVersion": "2572"
}
```

### 4. Complex Primary Keys
Step 3 and 5 use composite keys:
```yaml
primary_key: sales_ord_id,event_id,sales_ord_tran_id,sales_ord_tran_dt
```

In DLT-META, this becomes part of the transformation logic (MERGE statement).

### 5. UDFs
Your SQL calls `cdsudf_getoptype()`. Ensure these UDFs are registered in your Databricks environment before running the pipeline.

### 6. Configuration Table
SQL references `jobs_configurations` table for `century_year` value. Ensure this exists:
```sql
select int(value) from {unity_catalog[refinery]}.refinery{env_suffix}.jobs_configurations where key='century_year'
```

## Next Steps

### 1. Complete Transformation Files
Extract remaining SQL from YAML and create JSON files for steps 3-6. Use this template:

```json
{
  "transformation_id": "unique_id",
  "transformation_name": "Descriptive Name",
  "description": "Step X: Purpose of transformation",
  "sql_query": "
    -- Your SQL here from YAML source_sql
    -- Use placeholders that DLT-META will substitute
  "
}
```

### 2. Update Catalog/Database Names
Replace placeholder values in onboarding file:
- `unity_catalog_landing` → actual catalog name
- `unity_catalog_refinery` → actual catalog name
- `unity_catalog_treasury` → actual catalog name

### 3. Adjust File Paths
Update transformation file paths to match your DBFS/Unity Catalog volumes structure.

### 4. Create Environment-Specific Versions
Copy transformation JSONs for each environment (_nonprod, _preprod, _prod) if SQL differs by environment.

### 5. Test Incrementally
1. Test data_flow_id 204 (main flow) first
2. Then test 204-intermediate
3. Finally test 204-batch-update

### 6. Set Up Data Flow Groups
In your DLT pipeline configuration, use `data_flow_group: "sales_ord"` to group these related flows.

### 7. Configure Pipeline
When creating DLT pipeline, use:
```json
{
  "layer": "landing_refinery_treasury",
  "landing.group": "sales_ord",
  "refinery.group": "sales_ord",
  "treasury.group": "sales_ord",
  "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec",
  "refinery.dataflowspecTable": "catalog.schema.refinery_dataflowspec",
  "treasury.dataflowspecTable": "catalog.schema.treasury_dataflowspec"
}
```

## Validation Checklist

- [ ] All 3 onboarding flow definitions have correct catalog/database names
- [ ] All transformation JSON files created with complete SQL
- [ ] Environment-specific versions created (_nonprod, _preprod, _prod)
- [ ] File paths match your DBFS/Volume structure
- [ ] UDFs (`cdsudf_getoptype`) are registered
- [ ] Configuration table (`jobs_configurations`) exists with `century_year` key
- [ ] Source S3 paths are correct for each environment
- [ ] Primary keys match original YAML requirements
- [ ] CDC configuration includes correct `startingVersion` values
- [ ] Quarantine tables configured (if needed for data quality)

## Common Issues

### Issue 1: Missing UDFs
**Error**: `Function 'cdsudf_getoptype' not found`
**Solution**: Register UDFs in your Databricks workspace before running pipeline

### Issue 2: Placeholder Substitution
**Error**: Table `{unity_catalog[landing]}` not found
**Solution**: Use DLT-META environment-specific fields instead of string interpolation

### Issue 3: Sequential Execution
**Question**: How to ensure steps run in order?
**Answer**: DLT-META handles dependencies automatically. The three data flows can run in parallel or sequentially based on data availability. Use `data_flow_group` to organize related flows.

### Issue 4: Composite Keys
**Question**: How to handle multi-column primary keys?
**Answer**: In transformation SQL, use MERGE statement with composite key join condition. DLT will handle the merge operation.

## Example Transformation JSON Structure

```json
{
  "transformation_id": "pfocusdb_sales_ord_tran_step3",
  "transformation_name": "Treasury Void Transaction Updates",
  "description": "Step 3: Update treasury table for voided transactions",
  "sql_query": "
    with a as (
      select * from stream_source_table where sales_ord_id is not null
    ),
    tmp as (
      select
        d.system, d._account, d.acctdate, d.trnum, d.vaxevid, d.tdate,
        d.vflag, d.vdate, d.vcode, d.void_optype,
        case
          when d.vdate = sales_ord_tran.sales_ord_tran_dt then 0
          else sales_ord_tran.tran_amt
        end as tran_amt,
        -- ... rest of case statements
      from a d
      join treasury.pfocusdb_sales_ord_tran sales_ord_tran
        on trim(upper(sales_ord_tran.host_sys_cd)) = trim(upper(d.system))
        and sales_ord_tran.host_vax_acct_num = d._account
        -- ... rest of join conditions
        and trim(upper(sales_ord_tran.tran_void_flg)) = 'F'
        and trim(upper(d.vflag)) = 'T'
    )
    select
      tmp.void_optype as ovrrd_void_opr_type_cd,
      tmp.vdate as tran_void_dt,
      tmp.trans_face_val_amt,
      -- ... rest of columns
    from treasury.pfocusdb_sales_ord_tran sales_ord_tran
    join tmp
      on trim(upper(sales_ord_tran.host_sys_cd)) = trim(upper(tmp.system))
      and sales_ord_tran.host_vax_acct_num = tmp._account
      -- ... rest of join conditions
  "
}
```

## Additional Resources

- [DLT-META Quick Reference](ONBOARDING_QUICK_REFERENCE.md)
- [DLT-META Full Reference](ONBOARDING_FILE_REFERENCE.md)
- [Medallion Architecture](ONBOARDING_QUICK_REFERENCE.md#medallion-architecture-overview)
- [Organizing Large Scale ETLs](ORGANIZING_LARGE_SCALE_ETLS.md)

## Support

If you need help with:
- Complex SQL transformation conversion
- Environment-specific configuration
- Pipeline orchestration
- CDC configuration

Please refer to the DLT-META documentation or contact the platform team.
