# Guide to Reprocessing Quarantined Records

## Overview

This guide explains how to identify and reprocess records from the quarantine table once the underlying issues are resolved.

## Three Reprocessing Approaches

### Approach 1: Automatic Reprocessing (Recommended)

**When to use:** If the source system will re-send the same files or if sales_ord_id appears later

**How it works:**
1. Source re-sends the CSV file (or new file with same transactions)
2. Landing table gets the record again
3. If `sales_ord_id` now exists in `pfocusdb_sales_ord`, the record goes to main treasury
4. Quarantine table keeps the historical failed record

**Pros:**
- ✅ No manual intervention needed
- ✅ Automatic and reliable
- ✅ Full audit trail

**Cons:**
- ❌ Requires source system to re-send data
- ❌ May have delay before reprocessing

**Implementation:**
```sql
-- Just wait for next landing ingestion
-- No action needed - pipeline handles it automatically
```

---

### Approach 2: Manual SQL Insert (Quick Fix)

**When to use:** Small number of records (< 1000) that can now be processed

**Steps:**

#### Step 1: Identify reprocessable records

```sql
-- Find quarantined records where sales_ord_id now exists
CREATE OR REPLACE TEMP VIEW reprocessable_records AS
SELECT
  q.*,
  o.sales_ord_id
FROM dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_quarantine_dlt_v2 q
INNER JOIN dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord o
  ON trim(upper(q.host_sys_cd)) = trim(upper(o.host_sys_cd))
  AND q.host_acct_create_dt = o.host_acct_create_dt
  AND q.host_vax_acct_num = o.host_vax_acct_num
WHERE o.sales_ord_id IS NOT NULL
  AND q.quarantine_ts >= '2025-01-01'  -- Adjust date range
;

-- Check count
SELECT COUNT(*) as reprocessable_count FROM reprocessable_records;
```

#### Step 2: Insert into main treasury table

```sql
-- Insert quarantined records into main treasury
-- IMPORTANT: Adjust column mappings as needed

INSERT INTO dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2
SELECT
  r.sales_ord_id,
  r.host_sys_cd,
  r.host_vax_acct_num,
  r.host_acct_create_dt,
  r.event_id,
  cast(null as string) as event_id_src_sys_cd,
  cast(null as string) as src_event_id,
  r.sales_ord_tran_id,
  r.sales_ord_tran_dt,
  cast(null as string) as sales_ord_trans_lcl_dttm,
  r.tran_amt,
  r.acct_mode_cd,
  r.tickets_purchased_qty,
  cast(null as string) as print_flg,
  cast(null as date) as print_dt,
  r.tran_void_flg,
  cast(null as date) as tran_void_dt,
  cast(null as string) as print_opr_id,
  cast(null as string) as print_opr_cd,
  cast(null as string) as ovrrd_print_opr_type_cd,
  cast(null as string) as tran_opr_id,
  r.tran_opr_cd,
  cast(null as string) as ovrrd_tran_opr_type_cd,
  cast(null as string) as void_opr_id,
  cast(null as string) as void_opr_cd,
  cast(null as string) as ovrrd_void_opr_type_cd,
  r.trans_face_val_amt,
  r.trans_service_charge_amt,
  r.trans_service_tax_amt,
  r.trans_service_tax_2_amt,
  r.trans_set_tax_amt,
  r.trans_zone_charge_amt,
  r.trans_fac_fee_amt,
  r.trans_exch_fee_amt,
  cast(null as string) as last_batch_id,
  cast(null as string) as init_run_id,
  cast(null as string) as last_run_id,
  current_timestamp() as insert_ts,
  current_timestamp() as update_ts,
  cast(null as string) as tkt_transfer_sent_flg,
  cast(null as string) as tkt_transfer_rcvd_flg,
  cast(null as string) as tkt_transfer_surrender_flg,
  cast(null as string) as resale_status_nm,
  cast(null as string) as upsell_flg,
  cast(null as string) as dlvry_flg,
  cast(null as string) as void_used_opr_cd,
  cast(null as string) as mimic_flg,
  r.price_level_id,
  r.price_level_nm,
  cast(null as string) as expected_pmt_method_type_id,
  cast(null as string) as expected_pmt_method_type_cd,
  r.tkt_type_cnt
FROM reprocessable_records r
;

-- Verify insertion
SELECT COUNT(*) as records_reprocessed FROM reprocessable_records;
```

#### Step 3: Mark quarantined records as reprocessed

```sql
-- Update quarantine table to mark as reprocessed
-- Note: This requires CDC update support or separate tracking table

-- Option A: If you have a separate tracking table
INSERT INTO quarantine_reprocessing_log
SELECT
  quarantine_id,
  'REPROCESSED' as status,
  current_timestamp() as reprocessed_ts
FROM reprocessable_records;

-- Option B: Just keep original quarantine record (recommended)
-- No action needed - quarantine table keeps historical record
```

**Pros:**
- ✅ Quick for small datasets
- ✅ Full control over mappings
- ✅ Can handle immediately

**Cons:**
- ❌ Manual SQL required
- ❌ Risk of mapping errors
- ❌ Missing some columns (have to cast as null)
- ❌ Not scalable for large datasets

---

### Approach 3: Resubmit to Landing via Notebook (Production-Grade)

**When to use:** Large number of records (> 1000) or regular reprocessing needed

**Create a reprocessing notebook:**

#### Notebook: `reprocess_quarantine.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Reprocess Quarantined Sales Order Transactions
# MAGIC
# MAGIC This notebook identifies quarantined records that can now be processed
# MAGIC and resubmits them to landing for standard pipeline processing.

# COMMAND ----------
# Configuration
quarantine_table = "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_quarantine_dlt_v2"
landing_table = "dataservices_nonprod.landing_nonprod.pfocusst_ff_src_hst_dsn_trans_dlt_v2"
sales_ord_table = "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord"

# COMMAND ----------
# Step 1: Find reprocessable records
reprocessable_df = spark.sql(f"""
  SELECT
    q.*,
    o.sales_ord_id
  FROM {quarantine_table} q
  INNER JOIN {sales_ord_table} o
    ON trim(upper(q.host_sys_cd)) = trim(upper(o.host_sys_cd))
    AND q.host_acct_create_dt = o.host_acct_create_dt
    AND q.host_vax_acct_num = o.host_vax_acct_num
  WHERE o.sales_ord_id IS NOT NULL
    AND q.quarantine_ts >= current_date - INTERVAL 30 DAYS
""")

record_count = reprocessable_df.count()
print(f"Found {record_count} reprocessable records")

# COMMAND ----------
# Step 2: Display sample for verification
display(reprocessable_df.limit(10))

# COMMAND ----------
# Step 3: Map back to landing schema
# This recreates the original landing record format

landing_schema_df = reprocessable_df.select(
  col("host_sys_cd").alias("system"),
  col("host_vax_acct_num").alias("vaxacct"),
  col("host_acct_create_dt").alias("accountdate"),
  col("event_id").alias("vaxevid"),
  col("sales_ord_tran_id").alias("transnum"),
  col("sales_ord_tran_dt").alias("transdate"),
  col("tran_opr_cd").alias("transopcode"),
  col("tran_void_flg").alias("voidflag"),
  col("tran_amt").alias("totaldollars"),
  col("tickets_purchased_qty").alias("tickets"),
  col("trans_face_val_amt").alias("ticketvalue"),
  col("trans_service_charge_amt").alias("servicedollars"),
  col("trans_set_tax_amt").alias("tax"),
  col("trans_service_tax_amt").alias("servicetax"),
  col("trans_service_tax_2_amt").alias("servicetax2"),
  col("trans_zone_charge_amt").alias("zonecharge"),
  col("trans_fac_fee_amt").alias("facilityfee"),
  col("trans_exch_fee_amt").alias("exchangefee"),
  col("price_level_id").alias("pricelevelid"),
  col("price_level_nm").alias("pricelevelname"),
  col("tkt_type_cnt").alias("seattypes"),
  col("acct_mode_cd").alias("acmode"),
  lit(None).cast("string").alias("printflag"),
  lit(None).cast("string").alias("printdate"),
  lit(None).cast("string").alias("printopcode"),
  lit(None).cast("string").alias("voiddate"),
  lit(None).cast("string").alias("voidopcode"),
  lit(None).cast("string").alias("exmopid"),
  lit(None).cast("string").alias("exmopname"),
  lit(None).cast("string").alias("firstqualifier"),
  lit(None).cast("string").alias("vcode_used"),
  lit(None).cast("string").alias("mimic_flag"),
  lit(None).cast("int").alias("tkt_transfer_sent_flg"),
  lit(None).cast("int").alias("tkt_transfer_rcvd_flg"),
  lit(None).cast("int").alias("tkt_transfer_surrender_flg"),
  lit(None).cast("int").alias("resale_status_nm"),
  current_timestamp().alias("landing_insert_ts")
)

# COMMAND ----------
# Step 4: Insert into landing table
# This triggers the pipeline to reprocess these records

if record_count > 0:
  landing_schema_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(landing_table)

  print(f"Successfully resubmitted {record_count} records to landing")
else:
  print("No records to reprocess")

# COMMAND ----------
# Step 5: Log reprocessing activity
reprocessing_log_df = reprocessable_df.select(
  col("quarantine_id"),
  col("sales_ord_id"),
  lit("RESUBMITTED_TO_LANDING").alias("status"),
  current_timestamp().alias("reprocessed_ts")
)

# Save to tracking table (create if doesn't exist)
reprocessing_log_df.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable("dataservices_nonprod.treasury_teradata_base_nonprod.quarantine_reprocessing_log")

print("Reprocessing logged")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 1. Wait for next pipeline run (or trigger manually)
# MAGIC 2. Verify records appear in main treasury table
# MAGIC 3. Check that counts match
```

#### Schedule the notebook

**Workflow YAML:**
```yaml
# reprocess_quarantine_workflow.yaml
name: Reprocess Quarantine Records
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
  timezone_id: "America/Los_Angeles"

tasks:
  - task_key: reprocess_quarantine
    notebook_task:
      notebook_path: "/Workspace/Shared/reprocess_quarantine"
    job_cluster_key: standard_cluster

job_clusters:
  - job_cluster_key: standard_cluster
    new_cluster:
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2
```

**Pros:**
- ✅ Handles large datasets
- ✅ Leverages existing pipeline logic
- ✅ No manual SQL mapping
- ✅ Full audit trail
- ✅ Can be scheduled/automated
- ✅ Records go through full transformation

**Cons:**
- ❌ Requires notebook development
- ❌ Need to map back to landing schema
- ❌ Slight delay (wait for next pipeline run)

---

## Monitoring Reprocessing

### Create a reprocessing dashboard

```sql
-- Track reprocessing success rate
SELECT
  r.reprocessed_ts,
  r.status,
  COUNT(*) as count,
  COUNT(t.sales_ord_id) as found_in_treasury
FROM dataservices_nonprod.treasury_teradata_base_nonprod.quarantine_reprocessing_log r
LEFT JOIN dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2 t
  ON r.sales_ord_id = t.sales_ord_id
WHERE r.reprocessed_ts >= current_date - INTERVAL 7 DAYS
GROUP BY r.reprocessed_ts, r.status
ORDER BY r.reprocessed_ts DESC
```

### Check for still-missing records

```sql
-- Records resubmitted but still not in treasury
SELECT
  r.quarantine_id,
  r.sales_ord_id,
  r.reprocessed_ts,
  DATEDIFF(day, r.reprocessed_ts, current_timestamp()) as days_since_reprocess
FROM dataservices_nonprod.treasury_teradata_base_nonprod.quarantine_reprocessing_log r
LEFT JOIN dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt_v2 t
  ON r.sales_ord_id = t.sales_ord_id
WHERE t.sales_ord_id IS NULL
  AND r.status = 'RESUBMITTED_TO_LANDING'
  AND r.reprocessed_ts < current_timestamp() - INTERVAL 1 DAY
ORDER BY r.reprocessed_ts
```

---

## Preventing Future Quarantine Issues

### Root Cause Analysis

```sql
-- Analyze patterns in quarantined records
SELECT
  host_sys_cd,
  COUNT(*) as quarantine_count,
  MIN(quarantine_ts) as first_occurrence,
  MAX(quarantine_ts) as last_occurrence,
  COUNT(DISTINCT DATE(quarantine_ts)) as days_affected
FROM dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_quarantine_dlt_v2
WHERE quarantine_ts >= current_date - INTERVAL 30 DAYS
GROUP BY host_sys_cd
ORDER BY quarantine_count DESC
```

### Preventive Actions

1. **Improve source data quality**
   - Work with source system team to ensure `sales_ord_id` is populated
   - Add validation at source

2. **Add buffer time**
   - If transactions arrive before orders, add delay in processing
   - Load orders first, then transactions

3. **Create reference data alerts**
   - Alert when `pfocusdb_sales_ord` hasn't been updated recently
   - Monitor for missing reference data

4. **Enhanced quarantine reasons**
   - Add more specific reasons: 'TIMING_ISSUE', 'DATA_MISMATCH', 'INVALID_SYSTEM'
   - Helps prioritize remediation

---

## Comparison of Approaches

| Approach | Effort | Scalability | Reliability | Audit Trail | Speed |
|----------|--------|-------------|-------------|-------------|-------|
| **Automatic** | Low | High | High | Excellent | Slow |
| **Manual SQL** | Medium | Low | Medium | Manual | Fast |
| **Notebook** | High (once) | High | High | Excellent | Medium |

---

## Recommended Workflow

### For Regular Operations (Recommended)

1. **Daily monitoring:**
   - Check quarantine count each morning
   - Investigate if count > threshold (e.g., 50 records)

2. **Weekly reprocessing:**
   - Run notebook-based reprocessing every Sunday
   - Automatically resubmits records that can now be processed

3. **Monthly review:**
   - Analyze quarantine trends
   - Work with data providers on root causes
   - Update validation rules if needed

### For Emergency Reprocessing

1. Identify records to reprocess (Step 1)
2. Use **Approach 2 (Manual SQL)** for immediate needs (< 1000 records)
3. Use **Approach 3 (Notebook)** for large batches (> 1000 records)
4. Verify records appear in main treasury within 24 hours

---

## Summary

**Three ways to reprocess:**
1. ✅ **Automatic** - Wait for source to re-send (best for most cases)
2. ✅ **Manual SQL** - Quick fix for small batches (use sparingly)
3. ✅ **Notebook** - Production-grade solution for regular reprocessing (recommended)

**Best practice:**
- Use notebook-based reprocessing (Approach 3)
- Schedule it to run weekly
- Monitor for still-missing records
- Focus on preventing issues at source

The notebook approach gives you:
- ✅ Full transformation pipeline
- ✅ Scalability
- ✅ Automation
- ✅ Complete audit trail
- ✅ No manual SQL mapping errors
