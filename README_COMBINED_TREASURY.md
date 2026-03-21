# Combined Treasury Transformation - Landing → Treasury Direct

## Overview

This configuration implements a **direct Landing → Treasury pipeline** that skips the refinery layer entirely. All transformation logic (previously split between step 2 and step 3) is combined into ONE treasury SQL transformation.

## Architecture

### Old Architecture (3 layers)
```
Source (CSV) → Landing → Refinery → Treasury
                ↓         ↓           ↓
              Bronze    Silver      Gold
```

**Problems:**
- Cannot have refinery read from treasury (circular dependency)
- Self-referential checks in refinery cause issues
- Two separate dataflows trying to write same refinery table

### New Architecture (2 layers)
```
Source (CSV) → Landing → Treasury
                ↓         ↓
              Bronze    Gold
```

**Benefits:**
- ✅ No refinery table to manage
- ✅ All logic in one treasury transformation
- ✅ No circular dependencies
- ✅ Simpler architecture
- ✅ CDC handles updates and voids automatically

## Files

### 1. Transformation SQL
**File:** `transformations_sales_ord_combined_treasury_nonprod.yaml`

**What it does:**
1. **Reads from landing** table: `pfocusst_ff_src_hst_dsn_trans_dlt_v2`
2. **Applies all refinery transformations:**
   - System code mapping (AU1→AUS, LON→UK1, etc.)
   - Date conversions (handles century year cutoff)
   - Zero-out amounts for void transactions
   - Get operator types using UDF
3. **Enriches with sales_ord_id** from `pfocusdb_sales_ord` table
4. **Filters out already-processed records** (checks existing treasury)
5. **Handles two types of transactions:**
   - **Regular transactions:** New non-void records
   - **Void transactions:** Updates existing treasury records, zeroing out amounts
6. **Writes to treasury** table: `pfocusdb_sales_ord_tran_dlt_v2`

### 2. Onboarding Configuration
**File:** `onboarding_sales_ord_tran_landing_treasury.json`

**Key settings:**
```json
{
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "pfocusst_ff_src_hst_dsn_trans_dlt_v2",

  "refinery_database_nonprod": null,  // ← Skip refinery
  "refinery_table": null,

  "treasury_database_nonprod": "treasury_teradata_base_nonprod",
  "treasury_table": "pfocusdb_sales_ord_tran_dlt_v2"
}
```

## How It Works

### Step-by-Step Execution

```sql
-- Step 1: Extract and clean landing data
with q_extract as (
  -- Apply system code mapping, zero out void amounts, etc.
  select ... from LIVE.pfocusst_ff_src_hst_dsn_trans_dlt_v2
),

-- Step 2: Convert dates and get operator types
query as (
  -- Handle century year cutoff (19xx vs 20xx)
  -- Call UDF to get operator types
  select ... from q_extract
),

-- Step 3: Get sales_ord_id lookup
enriched_data as (
  -- Join with pfocusdb_sales_ord to get sales_ord_id
  select ...
  from query q
  left join pfocusdb_sales_ord o on ...
),

-- Step 4: Filter out already-processed
new_records as (
  -- Skip records already in treasury
  select ...
  where not exists in treasury
),

-- Step 5: Regular transactions (non-void)
regular_transactions as (
  -- Map to treasury schema
  -- Only non-void records (vflag <> 'T')
  select ... from new_records
  where vflag <> 'T'
),

-- Step 6: Void transactions (update existing)
void_transactions as (
  -- Join with existing treasury records
  -- Zero out amounts when void date = transaction date
  -- Update void flags and void operator codes
  select ...
  from enriched_data d
  join treasury t on ... (match on keys)
  where d.vflag = 'T' and t.tran_void_flg = 'F'
)

-- Step 7: Combine results
select * from regular_transactions
union all
select * from void_transactions
```

### CDC Keys

The treasury CDC configuration uses these keys:
```json
"keys": [
  "sales_ord_id",
  "event_id",
  "sales_ord_tran_id",
  "sales_ord_tran_dt"
]
```

**How CDC works:**
1. **New record arrives:** CDC inserts it
2. **Same keys arrive again (void):** CDC updates the existing record
3. **Result:** Void transactions update original transactions automatically

## Pipeline Configuration

### Option 1: Full Pipeline (Landing + Treasury)

**Layer:** `landing_refinery_treasury`

```json
{
  "layer": "landing_refinery_treasury",
  "landing.group": "sales_ord",
  "refinery.group": "sales_ord",
  "treasury.group": "sales_ord",
  "landing.dataflowspecTable": "onboarding_table",
  "refinery.dataflowspecTable": "onboarding_table",
  "treasury.dataflowspecTable": "onboarding_table"
}
```

**Result:**
- Landing layer runs (creates landing table)
- Refinery layer **SKIPPED** (because `refinery_database_nonprod` is null)
- Treasury layer runs (reads from landing, writes to treasury)

### Option 2: Treasury Only

**Layer:** `treasury`

```json
{
  "layer": "treasury",
  "treasury.group": "sales_ord",
  "treasury.dataflowspecTable": "onboarding_table"
}
```

**Use case:** If landing table already exists, run only treasury transformations

## Data Flow Example

### Scenario: Initial Load + Void Transaction

**Day 1: Regular sale transaction arrives**
```
Landing: {system: 'LON', vflag: 'F', tsale: 100.00, ...}
   ↓ Combined transformation
Treasury: {host_sys_cd: 'UK1', tran_void_flg: 'F', tran_amt: 100.00, ...}
```

**Day 2: Void transaction arrives for same sale**
```
Landing: {system: 'LON', vflag: 'T', vdate: '2025-01-15', ...}
   ↓ Combined transformation (void_transactions CTE)
Treasury: {host_sys_cd: 'UK1', tran_void_flg: 'T', tran_amt: 0.00, ...}
         ↑ CDC updates existing record (same keys)
```

## Void Transaction Logic

### When Void Date = Transaction Date
```sql
case when d.vdate = t.sales_ord_tran_dt then 0 else t.tran_amt end
```

**If void date matches original transaction date:**
- Zero out all amounts (tran_amt, trans_face_val_amt, etc.)
- Update void flag to 'T'
- Update void operator codes
- Keep original insert_ts, update update_ts

**If void date differs from transaction date:**
- Keep original amounts
- Still update void flag and codes

## Deduplication Strategy

The transformation includes built-in deduplication:

```sql
existing_treasury as (
  select host_sys_cd, host_vax_acct_num, ...
  from LIVE.pfocusdb_sales_ord_tran_dlt_v2
),
new_records as (
  select e.*
  from enriched_data e
  left join existing_treasury t on ...
  where t.host_sys_cd is null  -- Not in treasury yet
)
```

**This prevents:**
- Duplicate regular transactions
- Reprocessing same records multiple times

## Comparison: Old vs New

| Aspect | Old (3-layer) | New (2-layer) |
|--------|---------------|---------------|
| **Layers** | Landing → Refinery → Treasury | Landing → Treasury |
| **Transformation files** | 2 files (step2.yaml + step3.yaml) | 1 file (combined.yaml) |
| **Dataflows** | 2 dataflows (204 + 205) | 1 dataflow (204_combined) |
| **Refinery table** | Required | Not needed |
| **Processed flag** | Tracked in refinery | Checked dynamically |
| **Void logic** | In step 3 treasury | In combined treasury |
| **Circular dependency** | Yes (refinery checks treasury) | No |
| **Complexity** | High | Medium |

## Migration Steps

### From 3-layer to 2-layer

1. **Onboard the new configuration:**
   ```bash
   # Use onboarding_sales_ord_tran_landing_treasury.json
   ```

2. **Run initial pipeline:**
   ```json
   {"layer": "landing_refinery_treasury", "landing.group": "sales_ord"}
   ```

3. **Verify:**
   - Landing table created
   - Treasury table populated
   - No refinery table created

4. **Schedule regular runs:**
   - Use same configuration
   - Auto Loader ensures only new files processed
   - CDC ensures no duplicates in treasury

## Troubleshooting

### Issue: "Cannot redefine dataset"
**Solution:** Ensure only ONE dataflow writes to treasury table

### Issue: Records not appearing in treasury
**Check:**
1. `sales_ord_id is not null` in enriched_data
2. Record not already in `existing_treasury`
3. Landing table has data

### Issue: Void transactions not updating
**Check:**
1. Void record has `vflag = 'T'`
2. Matching treasury record has `tran_void_flg = 'F'`
3. Keys match exactly (system, account, date, event, tran_id)

## Performance Considerations

### Optimizations
- ✅ Treasury table has cluster keys: `sales_ord_tran_dt`, `event_id`
- ✅ CDC keys enable efficient merge operations
- ✅ Deduplication happens at query time (no extra table scans)
- ✅ Change Data Feed (`readChangeFeed: true`) processes only new records

### Potential Bottlenecks
- Large treasury self-join for void processing
- UDF calls for operator type lookup (called 3x per record)

**Mitigation:**
- Consider caching UDF results
- Use Delta table statistics for join optimization
- Monitor query execution plans

## Summary

The combined treasury transformation:
- ✅ Eliminates refinery layer complexity
- ✅ Consolidates all logic in one place
- ✅ Handles regular and void transactions
- ✅ Prevents duplicates automatically
- ✅ Uses CDC for efficient updates
- ✅ Maintains all business logic from original 3-layer design

**Result:** Simpler, more maintainable architecture that solves the circular dependency problem.
