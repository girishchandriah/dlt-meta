# Summary of Changes: Quarantine in Refinery Layer

## What Changed

Moved quarantine table from **Treasury (Gold)** layer to **Refinery (Silver)** layer.

## Before vs After

### Before (Incorrect)
```
Landing → Treasury (Valid + Quarantine)
```

**Configuration:**
```json
{
  "data_flow_id": "204_quarantine",
  "refinery_database_nonprod": null,
  "treasury_database_nonprod": "treasury_teradata_base_nonprod",
  "treasury_table": "pfocusdb_sales_ord_tran_quarantine_dlt_v2"
}
```

**Table location:**
- `treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_quarantine_dlt_v2`

---

### After (Correct)
```
Landing → Refinery (Quarantine) + Treasury (Valid)
```

**Configuration:**
```json
{
  "data_flow_id": "204_quarantine",
  "refinery_database_nonprod": "refinery_nonprod",
  "refinery_table": "pfocusst_dsn_st_trans_quarantine_dlt_v2",
  "treasury_database_nonprod": null
}
```

**Table location:**
- `refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2`

---

## Files Changed

### 1. Configuration
**File:** `onboarding_sales_ord_tran_landing_treasury.json`

**Changes:**
- Dataflow 204_quarantine now writes to refinery instead of treasury
- Table name changed: `pfocusdb_sales_ord_tran_quarantine_dlt_v2` → `pfocusst_dsn_st_trans_quarantine_dlt_v2`
- Database changed: `treasury_teradata_base_nonprod` → `refinery_nonprod`

### 2. Transformation SQL
**File:** `transformations_sales_ord_quarantine_nonprod.yaml`

**Changes:**
- `target_table` updated to new name
- Description clarified: "refinery layer for data quality review"

### 3. Documentation
**New file:** `QUARANTINE_ARCHITECTURE.md`
- Explains why refinery is correct layer
- Architectural best practices
- Industry standards alignment

**Updated files:**
- `QUARANTINE_APPROACH.md` - Updated table names and locations
- `REPROCESS_QUARANTINE_GUIDE.md` - Updated table references

---

## Complete Data Flow

```
┌─────────────────────────────────────────────────────────┐
│ CSV Files                                               │
└─────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────┐
│ Landing (Bronze)                                        │
│ landing_nonprod.pfocusst_ff_src_hst_dsn_trans_dlt_v2  │
└─────────────────────────────────────────────────────────┘
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
┌──────────────┐  ┌──────────────────────────────────────┐
│ Valid        │  │ Invalid (No sales_ord_id)            │
│ Records      │  │                                      │
└──────────────┘  └──────────────────────────────────────┘
        │                │
        │                ▼
        │         ┌──────────────────────────────────────┐
        │         │ Refinery (Silver) - Data Quality     │
        │         │ refinery_nonprod.                    │
        │         │ pfocusst_dsn_st_trans_               │
        │         │ quarantine_dlt_v2                    │
        │         │                                      │
        │         │ ✅ Data quality layer                │
        │         │ ✅ Keeps treasury clean              │
        │         │ ✅ Can be reprocessed                │
        │         └──────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│ Treasury (Gold) - Business Data Only                    │
│ treasury_teradata_base_nonprod.                         │
│ pfocusdb_sales_ord_tran_dlt_v2                         │
│                                                          │
│ ✅ Only validated records                               │
│ ✅ Business-ready data                                  │
│ ✅ No data quality issues                               │
└─────────────────────────────────────────────────────────┘
```

---

## Execution Order

### Layer Pipeline: `landing_refinery_treasury`

1. **Landing Layer:**
   - Dataflow 204_combined (source → landing)
   - Creates: `landing_nonprod.pfocusst_ff_src_hst_dsn_trans_dlt_v2`

2. **Refinery Layer:**
   - Dataflow 204_quarantine (landing → refinery quarantine)
   - Creates: `refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2`
   - **Only invalid records** (no sales_ord_id)

3. **Treasury Layer:**
   - Dataflow 204_combined (landing → treasury)
   - Creates: `treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2`
   - **Only valid records** (with sales_ord_id)

---

## Query Changes

### Before (Had to filter quarantine in Treasury)

```sql
-- Business query - had to exclude quarantine
SELECT
  DATE(sales_ord_tran_dt) as date,
  SUM(tran_amt) as revenue
FROM treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2
WHERE table_name != 'quarantine'  -- ❌ Shouldn't be needed in gold layer
GROUP BY DATE(sales_ord_tran_dt)
```

### After (Clean Treasury queries)

```sql
-- Business query - no filtering needed!
SELECT
  DATE(sales_ord_tran_dt) as date,
  SUM(tran_amt) as revenue
FROM treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2
GROUP BY DATE(sales_ord_tran_dt)
-- ✅ Treasury only has valid data - no quarantine filtering needed
```

### Data Quality Query (Refinery)

```sql
-- Separate data quality monitoring
SELECT
  quarantine_reason,
  COUNT(*) as count,
  DATE(quarantine_ts) as date
FROM dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2
GROUP BY quarantine_reason, DATE(quarantine_ts)
```

---

## Benefits of This Change

### ✅ Architectural Correctness

| Aspect | Before | After |
|--------|--------|-------|
| **Layer purpose** | Mixed (business + DQ) | Clear separation |
| **Treasury data quality** | Contains invalid records | 100% valid |
| **Medallion alignment** | Incorrect | Correct ✅ |
| **Data quality location** | Gold (wrong) | Silver (correct) ✅ |

### ✅ Operational Benefits

1. **Simpler business queries** - No quarantine filtering needed
2. **Clearer monitoring** - DQ metrics in refinery, business metrics in treasury
3. **Better access control** - DQ team owns refinery, analysts query treasury
4. **Natural reprocessing** - Quarantine → Refinery → Treasury progression

### ✅ Follows Best Practices

- **Databricks Medallion Architecture** ✅
- **Data Mesh Principles** ✅
- **Industry Standards** ✅

---

## Migration Impact

### No Breaking Changes

✅ **No impact on existing main treasury table:**
- `pfocusdb_sales_ord_tran_dlt_v2` remains unchanged
- Same schema, same location
- Business queries unchanged

✅ **New table in different location:**
- Quarantine is new table in refinery
- No conflict with existing tables

### What Users Need to Update

1. **If you query quarantine table:**
   ```sql
   -- Old location (don't use)
   FROM treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_quarantine_dlt_v2

   -- New location (use this)
   FROM refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2
   ```

2. **If you have dashboards/reports with quarantine:**
   - Update catalog: `treasury_teradata_base_nonprod` → `refinery_nonprod`
   - Update table name: `pfocusdb_sales_ord_tran_quarantine_dlt_v2` → `pfocusst_dsn_st_trans_quarantine_dlt_v2`

3. **If you reprocess quarantine records:**
   - Update notebook to read from new refinery location
   - See updated `REPROCESS_QUARANTINE_GUIDE.md`

---

## Rollout Plan

### Step 1: Deploy New Configuration
- Onboard updated `onboarding_sales_ord_tran_landing_treasury.json`
- Deploy new transformation YAML files

### Step 2: Run Initial Pipeline
```json
{
  "layer": "landing_refinery_treasury",
  "landing.group": "sales_ord",
  "refinery.group": "sales_ord",
  "treasury.group": "sales_ord"
}
```

### Step 3: Verify
- ✅ Landing table created
- ✅ Refinery quarantine table created (new location)
- ✅ Treasury table created (valid records only)

### Step 4: Update Downstream
- Update monitoring dashboards
- Update reprocessing notebooks
- Update documentation links

---

## Questions & Answers

### Q: Will this break existing queries?
**A:** No. Main treasury table unchanged. Only quarantine location changed.

### Q: Do we need to migrate old quarantine data?
**A:** No. Start fresh with new location. Old quarantine data (if any) can be archived.

### Q: Can we still reprocess quarantine records?
**A:** Yes. Process is same, just read from refinery instead of treasury. See updated guide.

### Q: Does this change pipeline execution time?
**A:** No. Same number of dataflows, same processing logic.

### Q: Why is this better?
**A:** See `QUARANTINE_ARCHITECTURE.md` for detailed explanation. Short answer: Keeps treasury clean with only business-ready data, following industry best practices.

---

## Summary

**Change:** Quarantine table moved from Treasury → Refinery

**Why:** Data quality concerns belong in Silver (Refinery) layer, not Gold (Treasury) layer

**Impact:** Minimal. Main tables unchanged. Only quarantine location changed.

**Benefit:** Cleaner architecture, simpler queries, better separation of concerns

**Action Required:** Update any references to quarantine table (location + name)

---

## References

- **Architecture rationale:** `QUARANTINE_ARCHITECTURE.md`
- **Quarantine approach:** `QUARANTINE_APPROACH.md`
- **Reprocessing guide:** `REPROCESS_QUARANTINE_GUIDE.md`
- **Main pipeline docs:** `README_COMBINED_TREASURY.md`
