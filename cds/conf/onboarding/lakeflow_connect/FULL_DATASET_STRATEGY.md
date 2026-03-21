# Full Dataset Loading Strategy for 438M Row Table

## Your Requirement

✅ **Need:** Full historical dataset (all 438M CUSTOMERS rows)
✅ **Challenge:** Too slow to scan 438M rows on every pipeline run
✅ **Solution:** Initial bulk load + incremental updates

---

## How It Works

### The Delta Table Will Contain Full Dataset

```
┌─────────────────────────────────────────────────────────┐
│ Delta Table: main.staging.customers_full                │
│ • Contains ALL 438M customers (full history)            │
│ • Updated incrementally (only changed records)          │
│ • No full table scan after initial load                 │
└─────────────────────────────────────────────────────────┘
```

### Pipeline Behavior

| Run Type | Oracle Scan | Rows Read | Rows in Delta Table |
|----------|-------------|-----------|---------------------|
| **Initial Load** | Full table (438M) | 438M rows | 438M rows |
| **Run 2** | Last 2 days only | ~50K rows | 438M rows (updated) |
| **Run 3** | Last 2 days only | ~50K rows | 438M rows (updated) |
| **Run N** | Last 2 days only | ~50K rows | 438M rows (updated) |

**Key Point:** After initial load, Oracle only scans recent data (WHERE clause), but Delta table contains full dataset.

---

## Two Approaches

### **Option 1: Batch Historical Load (Recommended)**

Load historical data in date ranges to avoid timeout.

#### Step 1: Create Configuration (No WHERE Clause)

Use [lakeflow_member_db_full.json](lakeflow_member_db_full.json) - no `landing_where_clause`.

#### Step 2: Load Historical Data in Batches

```python
# Databricks notebook for historical load
from datetime import datetime, timedelta

# Define date ranges (adjust based on your data distribution)
start_date = datetime(2020, 1, 1)  # Adjust to your oldest data
end_date = datetime.now()

# Process in monthly batches
current_date = start_date
batch_num = 1

while current_date < end_date:
    next_date = current_date + timedelta(days=30)

    print(f"Batch {batch_num}: Loading {current_date.date()} to {next_date.date()}")

    # Read from Oracle with date filter
    df = spark.sql(f"""
        SELECT *
        FROM _member_db_catalog.member.CUSTOMERS
        WHERE LAST_UPDATE >= '{current_date.date()}'
          AND LAST_UPDATE < '{next_date.date()}'
    """)

    # Write to Delta (append mode for first batch, merge for subsequent)
    if batch_num == 1:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("LAST_UPDATE_DATE") \
            .saveAsTable("main.staging.customers_full")
    else:
        from delta.tables import DeltaTable

        target = DeltaTable.forName(spark, "main.staging.customers_full")

        target.alias("target").merge(
            df.alias("source"),
            "target.CUSTOMER_ID = source.CUSTOMER_ID"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    current_date = next_date
    batch_num += 1

    print(f"✓ Batch {batch_num - 1} complete: {df.count()} rows")

print(f"✓ Historical load complete: Total batches = {batch_num - 1}")
```

#### Step 3: Switch to Incremental Updates

After historical load completes, update the config to use WHERE clause:

**Edit [lakeflow_member_db.json](lakeflow_member_db.json):**

```json
{
  "landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)"
}
```

This ensures subsequent runs only scan last 2 days.

---

### **Option 2: Single Initial Load (Simpler, But Risky)**

Load all 438M rows in one shot. **Warning:** May timeout!

#### Step 1: Initial Load Without Filter

```bash
# Deploy WITHOUT landing_where_clause
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_db_full.json \
  --env prod \
  --layer landing_refinery
```

#### Step 2: Run Initial Load

```bash
# This will take HOURS (possibly 4-8 hours for 438M rows)
databricks pipelines start --pipeline-id <pipeline-id>

# Monitor progress
databricks pipelines get --pipeline-id <pipeline-id>
```

**Risks:**
- May timeout (cluster max runtime)
- High Oracle load
- High network transfer
- Expensive (compute costs)

#### Step 3: Add Incremental Filter

After successful initial load, edit the config:

```json
{
  "landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)"
}
```

Redeploy the pipeline with the updated config.

---

## How Incremental Updates Work

After initial load, here's what happens on each run:

### Pipeline Execution

```sql
-- Step 1: Read only recent changes from Oracle (WHERE clause pushed down)
SELECT * FROM _member_db_catalog.member.CUSTOMERS
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 2);
-- Returns ~50K rows (not 438M!)

-- Step 2: MERGE into existing Delta table
MERGE INTO main.staging.customers_full AS target
USING recent_changes AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
-- Updates ~50K rows in 438M row table
```

### Result

- ✅ Oracle only scans last 2 days (fast!)
- ✅ Only changed records transferred over network
- ✅ Delta table maintains full 438M row dataset
- ✅ Queries against Delta table see all historical data

---

## Recommended Approach: Parallel Historical Load

For 438M rows, process in parallel by date ranges:

### Step 1: Create Batch Loading Notebook

```python
# Notebook: historical_customers_load.py

from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor
import datetime

def load_date_range(start_date, end_date):
    """Load customers for a specific date range"""
    print(f"Loading {start_date} to {end_date}")

    df = spark.sql(f"""
        SELECT
            *,
            DATE(LAST_UPDATE) as LAST_UPDATE_DATE
        FROM _member_db_catalog.member.CUSTOMERS
        WHERE LAST_UPDATE >= '{start_date}'
          AND LAST_UPDATE < '{end_date}'
    """)

    row_count = df.count()

    if row_count == 0:
        print(f"  No data for {start_date}")
        return

    # Append to Delta table
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("LAST_UPDATE_DATE") \
        .saveAsTable("main.staging.customers_full")

    print(f"  ✓ Loaded {row_count:,} rows")
    return row_count

# Configure date ranges
START_YEAR = 2020  # Adjust to your data
END_YEAR = 2026

# Generate monthly date ranges
date_ranges = []
for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        start = datetime.date(year, month, 1)
        if month == 12:
            end = datetime.date(year + 1, 1, 1)
        else:
            end = datetime.date(year, month + 1, 1)

        if end <= datetime.date.today():
            date_ranges.append((start, end))

# Initial table creation
print("Creating initial table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS main.staging.customers_full (
        CUSTOMER_ID BIGINT,
        UC_EMAIL_ADDRESS STRING,
        LAST_UPDATE TIMESTAMP,
        LAST_UPDATE_DATE DATE,
        -- Add other columns here
    )
    USING DELTA
    PARTITIONED BY (LAST_UPDATE_DATE)
""")

# Load in batches
print(f"Loading {len(date_ranges)} monthly batches...")
total_rows = 0

for start, end in date_ranges:
    row_count = load_date_range(start, end)
    if row_count:
        total_rows += row_count

print(f"\n✓ Historical load complete: {total_rows:,} total rows")

# Optimize table
print("Optimizing Delta table...")
spark.sql("OPTIMIZE main.staging.customers_full")
print("✓ Optimization complete")
```

### Step 2: Run the Notebook

```bash
# Submit as a job
databricks jobs create --json '{
  "name": "CUSTOMERS_Historical_Load",
  "tasks": [{
    "task_key": "load_history",
    "notebook_task": {
      "notebook_path": "/Path/To/historical_customers_load"
    },
    "new_cluster": {
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "i3.2xlarge",
      "num_workers": 8,
      "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true"
      }
    },
    "timeout_seconds": 28800
  }]
}'
```

### Step 3: Monitor Progress

```sql
-- Check loading progress
SELECT
    LAST_UPDATE_DATE,
    COUNT(*) as row_count,
    MIN(LAST_UPDATE) as min_timestamp,
    MAX(LAST_UPDATE) as max_timestamp
FROM main.staging.customers_full
GROUP BY LAST_UPDATE_DATE
ORDER BY LAST_UPDATE_DATE DESC;

-- Check total count
SELECT COUNT(*) as total_customers
FROM main.staging.customers_full;
-- Target: ~438M rows
```

---

## After Historical Load: Incremental Updates

### Update Configuration

Edit [lakeflow_member_db.json](lakeflow_member_db.json):

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
  "landing_table": "customers_full",
  "landing_partition_columns": "LAST_UPDATE_DATE",

  "landing_where_clause": "LAST_UPDATE >= date_sub(CURRENT_DATE(), 2)",

  "landing_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "1"
  },

  "refinery_catalog_prod": "main",
  "refinery_database_prod": "refinery",
  "refinery_table": "customers_full",

  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "2"
  }
}
```

### Deploy Incremental Pipeline

```bash
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_db.json \
  --env prod \
  --layer landing_refinery
```

### Schedule for Continuous Updates

```json
{
  "schedule": {
    "quartz_cron_expression": "0 0 */4 * * ?",
    "timezone_id": "America/Los_Angeles"
  }
}
```

Runs every 4 hours, only scanning last 2 days of Oracle data.

---

## Verification Queries

### Check Full Dataset is Present

```sql
-- Count all customers
SELECT COUNT(*) as total_customers
FROM main.staging.customers_full;
-- Expected: ~438M

-- Check data distribution by year
SELECT
    YEAR(LAST_UPDATE) as year,
    COUNT(*) as customer_count
FROM main.staging.customers_full
GROUP BY YEAR(LAST_UPDATE)
ORDER BY year;
```

### Verify Incremental Updates Working

```sql
-- Check recent updates (from last pipeline run)
SELECT COUNT(*)
FROM main.staging.customers_full
WHERE LAST_UPDATE >= date_sub(CURRENT_DATE(), 2);
-- Should match incremental load row count

-- View history for a test customer (refinery only)
SELECT
    CUSTOMER_ID,
    UC_EMAIL_ADDRESS,
    LAST_UPDATE,
    __START_AT,
    __END_AT,
    CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as status
FROM main.refinery.customers_full
WHERE CUSTOMER_ID = 123456
ORDER BY __START_AT DESC;
```

---

## Performance Comparison

| Approach | Initial Load Time | Incremental Load Time | Oracle Load | Risk |
|----------|-------------------|----------------------|-------------|------|
| **Full scan every run** | 6-8 hours | 6-8 hours | Very High | ❌ Unsustainable |
| **Incremental with WHERE** | 6-8 hours (once) | 5-10 minutes | Low | ✅ Recommended |
| **Batch historical load** | 4-6 hours (parallel) | 5-10 minutes | Medium | ✅ Best |

---

## Key Takeaways

1. ✅ **Initial load:** All 438M rows loaded ONCE (historical)
2. ✅ **Incremental updates:** Only recent changes (last 2 days)
3. ✅ **Delta table:** Always contains full dataset
4. ✅ **Oracle scans:** Only recent data after initial load
5. ✅ **Network transfer:** Minimal after initial load
6. ✅ **Query performance:** Fast (Delta table is local)

The Delta table acts as a **cache** of the full Oracle dataset, updated incrementally.

---

## Next Steps

1. Choose your approach:
   - ✅ **Batch parallel load** (recommended for 438M rows)
   - ⚠️ Single full load (risky, may timeout)

2. Run historical load (one-time)

3. Configure incremental pipeline with WHERE clause

4. Schedule for continuous updates

5. Monitor and optimize

**Need help implementing?** Let me know which approach you prefer!
