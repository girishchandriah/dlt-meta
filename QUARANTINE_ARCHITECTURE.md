# Quarantine Architecture: Why Refinery Layer?

## Overview

Quarantine records are placed in the **Refinery (Silver) layer** instead of Treasury (Gold) layer. This aligns with medallion architecture best practices where data quality concerns are handled in the cleansing/transformation layer.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Landing (Bronze) - Raw Data                                 │
│ pfocusst_ff_src_hst_dsn_trans_dlt_v2                       │
└─────────────────────────────────────────────────────────────┘
                │
                │ Both dataflows read from landing
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
┌──────────────┐  ┌──────────────────────────────┐
│ Has          │  │ No                           │
│ sales_ord_id │  │ sales_ord_id                 │
└──────────────┘  └──────────────────────────────┘
        │                │
        │                ▼
        │         ┌─────────────────────────────────────┐
        │         │ Refinery (Silver) - Data Quality    │
        │         │ pfocusst_dsn_st_trans_              │
        │         │ quarantine_dlt_v2                   │
        │         │                                     │
        │         │ Purpose: Data Quality Review        │
        │         │ - Identify missing references       │
        │         │ - Enable reprocessing               │
        │         │ - Keep treasury clean               │
        │         └─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ Treasury (Gold) - Business-Ready Data                       │
│ pfocusdb_sales_ord_tran_dlt_v2                             │
│                                                              │
│ Purpose: Analytics & Reporting                              │
│ - Only validated, complete records                          │
│ - Ready for business consumption                            │
└─────────────────────────────────────────────────────────────┘
```

## Why Refinery Layer?

### 1. Medallion Architecture Alignment

**Bronze → Silver → Gold** follows a clear progression:

| Layer | Purpose | Data Quality |
|-------|---------|--------------|
| **Landing (Bronze)** | Raw ingestion | Unknown |
| **Refinery (Silver)** | Cleansing, validation, quality checks | ← **Quarantine belongs here** |
| **Treasury (Gold)** | Business-ready, aggregated | High quality |

**Quarantine is a data quality concern**, not a business metric, so it belongs in Silver (Refinery).

### 2. Separation of Concerns

#### ✅ Correct Architecture (Quarantine in Refinery)

```
Landing (Raw) → Refinery (Quality Check) → Treasury (Clean Business Data)
                     ↓
                Quarantine
                (Invalid records)
```

**Benefits:**
- Treasury only contains **validated, complete records**
- Data quality issues isolated in refinery layer
- Clear separation: validation vs. business logic

#### ❌ Incorrect Architecture (Quarantine in Treasury)

```
Landing → Treasury (Business Data + Quarantine)
              ↓
         Mixed purposes:
         - Business metrics
         - Data quality issues
```

**Problems:**
- Treasury polluted with quality issues
- Business users see invalid data
- Confusion about data readiness

### 3. Data Consumer Expectations

**Treasury (Gold) consumers expect:**
- ✅ Clean, validated data
- ✅ Ready for analytics
- ✅ No data quality issues
- ✅ Business-level aggregations

**Refinery (Silver) consumers expect:**
- ✅ Transformation in progress
- ✅ Quality checks happening
- ✅ Some records may be invalid
- ✅ Quarantine tables present

### 4. Operational Benefits

#### Monitoring & Alerting

**Refinery quarantine:**
```sql
-- Data quality metrics (Refinery)
SELECT
  'REFINERY_QUARANTINE' as layer,
  COUNT(*) as quarantine_count,
  COUNT(*) * 100.0 / (
    SELECT COUNT(*) FROM landing_table
  ) as quarantine_percentage
FROM refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2
```

**Treasury quality check:**
```sql
-- Business data quality (Treasury)
-- Should be 100% valid - no quarantine table needed
SELECT
  'TREASURY' as layer,
  COUNT(*) as record_count,
  COUNT(DISTINCT sales_ord_id) as unique_orders,
  SUM(tran_amt) as total_revenue
FROM treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2
```

**Clear separation:**
- Refinery metrics = Data quality / pipeline health
- Treasury metrics = Business KPIs

### 5. Reprocessing Flow

**With quarantine in Refinery:**
```
1. Record fails validation → Refinery quarantine
2. Issue resolved (sales_ord_id appears)
3. Reprocess from quarantine
4. Record passes validation → Moves to Treasury
```

**Clear progression:** Quarantine → Refinery → Treasury

**With quarantine in Treasury (incorrect):**
```
1. Record fails validation → Treasury quarantine (❌ wrong layer)
2. Treasury now has both:
   - Valid business records
   - Invalid quarantine records
3. Confusion: Is this treasury data usable?
```

### 6. Access Control & Governance

**Typical data access patterns:**

| Team | Landing | Refinery | Treasury |
|------|---------|----------|----------|
| **Data Engineering** | Read/Write | Read/Write | Write |
| **Data Quality Team** | Read | **Read/Write** ← Owns quarantine | Read |
| **Business Analysts** | No access | No access | **Read** |
| **BI Tools** | No access | No access | **Read** |

**Quarantine in Refinery:**
- ✅ Data Quality team has full access
- ✅ Business analysts never see it
- ✅ BI tools query only clean treasury data

**Quarantine in Treasury:**
- ❌ Business analysts see quarantine records
- ❌ BI dashboards may accidentally include invalid data
- ❌ Requires complex filtering in every query

### 7. Schema & Naming Consistency

**Refinery naming:**
- Main: `pfocusst_dsn_st_trans_dl_dlt_v2` (if it existed)
- Quarantine: `pfocusst_dsn_st_trans_quarantine_dlt_v2`
- Clear relationship: both start with `pfocusst_`

**Treasury naming:**
- Main: `pfocusdb_sales_ord_tran_dlt_v2`
- Quarantine: ~~`pfocusdb_sales_ord_tran_quarantine_dlt_v2`~~ (confusing - same prefix as business table)

### 8. Lineage & Data Flow

**Clean lineage with quarantine in Refinery:**

```
Source Files
    ↓
Landing: pfocusst_ff_src_hst_dsn_trans_dlt_v2
    ├─→ Valid records → Treasury: pfocusdb_sales_ord_tran_dlt_v2
    └─→ Invalid records → Refinery Quarantine: pfocusst_dsn_st_trans_quarantine_dlt_v2
                             (can be reprocessed later)
```

**Clear story:**
1. Raw data lands in landing
2. Quality check happens at refinery level
3. Only valid data reaches treasury

## Configuration Changes

### Old (Quarantine in Treasury)
```json
{
  "data_flow_id": "204_quarantine",
  "refinery_database_nonprod": null,
  "treasury_database_nonprod": "treasury_teradata_base_nonprod",
  "treasury_table": "pfocusdb_sales_ord_tran_quarantine_dlt_v2"
}
```

### New (Quarantine in Refinery)
```json
{
  "data_flow_id": "204_quarantine",
  "refinery_database_nonprod": "refinery_nonprod",
  "refinery_table": "pfocusst_dsn_st_trans_quarantine_dlt_v2",
  "treasury_database_nonprod": null
}
```

## Table Locations

### Main Tables
- **Landing:** `landing_nonprod.pfocusst_ff_src_hst_dsn_trans_dlt_v2`
- **Treasury:** `treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2`

### Quarantine Table
- **Refinery:** `refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2`

## Dataflow Execution

Both dataflows run in same pipeline with `layer=landing_refinery_treasury`:

1. **Landing layer:** Creates landing table
2. **Refinery layer:**
   - Dataflow 204_quarantine → Creates quarantine table
3. **Treasury layer:**
   - Dataflow 204_combined → Creates main treasury table

**Note:** Refinery layer only runs quarantine (no main refinery table created).

## Querying Pattern

### Check Data Quality (Refinery)

```sql
-- Quarantine analysis
SELECT
  quarantine_reason,
  COUNT(*) as count,
  DATE(quarantine_ts) as date
FROM dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2
GROUP BY quarantine_reason, DATE(quarantine_ts)
ORDER BY date DESC
```

### Business Analytics (Treasury)

```sql
-- Business metrics - no quarantine filtering needed!
SELECT
  DATE(sales_ord_tran_dt) as transaction_date,
  COUNT(*) as transaction_count,
  SUM(tran_amt) as total_revenue
FROM dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2
GROUP BY DATE(sales_ord_tran_dt)
ORDER BY transaction_date DESC
```

**Key benefit:** No need to filter out quarantine records in business queries!

## Reprocessing

When reprocessing quarantined records:

```python
# Read from refinery quarantine
quarantine_df = spark.read.table(
  "dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_quarantine_dlt_v2"
)

# Map back to landing schema
landing_df = quarantine_df.select(...)

# Insert into landing
landing_df.write.mode("append").saveAsTable(
  "dataservices_nonprod.landing_nonprod.pfocusst_ff_src_hst_dsn_trans_dlt_v2"
)

# Next pipeline run:
# - Landing → checks for sales_ord_id
# - If found → goes to Treasury ✅
# - If not found → stays in Refinery quarantine
```

## Benefits Summary

| Aspect | Refinery Quarantine | Treasury Quarantine |
|--------|---------------------|---------------------|
| **Architectural alignment** | ✅ Correct (data quality layer) | ❌ Wrong (business layer) |
| **Data consumer clarity** | ✅ Clear separation | ❌ Confused purpose |
| **Business query complexity** | ✅ No filtering needed | ❌ Must exclude quarantine |
| **Access control** | ✅ DQ team owns it | ❌ Mixed access patterns |
| **Monitoring** | ✅ Clear DQ metrics | ❌ Mixed with business metrics |
| **Reprocessing flow** | ✅ Natural progression | ❌ Confusing flow |
| **Schema naming** | ✅ Consistent with layer | ❌ Inconsistent |

## Industry Best Practices

### Databricks Medallion Architecture

From Databricks documentation:

> **Silver (Refinery) Layer:**
> - Validates and cleanses Bronze data
> - **Performs data quality checks**
> - Applies business rules
> - May include quarantine/rejected records tables

> **Gold (Treasury) Layer:**
> - Business-level aggregations
> - **Only contains validated, high-quality data**
> - Ready for production consumption

### Data Mesh Principles

> **Data Quality should be assessed at the transformation layer (Silver), not at the business consumption layer (Gold).**

## Conclusion

Placing quarantine in the **Refinery (Silver) layer** is the correct architectural choice because:

1. ✅ **Aligns with medallion architecture** (quality checks in silver)
2. ✅ **Keeps treasury clean** (only validated data)
3. ✅ **Clear separation of concerns** (validation vs. business logic)
4. ✅ **Simpler business queries** (no quarantine filtering needed)
5. ✅ **Better access control** (DQ team owns refinery)
6. ✅ **Natural reprocessing flow** (quarantine → refinery → treasury)
7. ✅ **Industry best practices** (Databricks, Data Mesh)

**Treasury should contain only clean, validated, business-ready data. Data quality issues belong in Refinery.**
