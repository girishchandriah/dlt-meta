# CDC Apply Changes From Snapshot - Complete Guide

## What is `applyChangesFromSnapshot`?

**`applyChangesFromSnapshot`** is a **declarative CDC (Change Data Capture)** feature in DLT-META that automatically detects changes between **full table snapshots** without needing explicit change indicators (like operation columns).

## The Problem It Solves

### Traditional CDC Approach (cdcApplyChanges)

Traditional CDC requires **explicit change markers** in your data:

```csv
customer_id,name,email,operation,timestamp
1001,John Doe,john@email.com,INSERT,2024-01-01
1001,John Doe,john.doe@email.com,UPDATE,2024-01-02
1002,Jane Smith,jane@email.com,DELETE,2024-01-03
```

**Requirements:**
- ✅ Needs `operation` column (INSERT/UPDATE/DELETE)
- ✅ Needs `timestamp` or `sequence_by` column
- ✅ Receives **incremental changes only**

### Snapshot-Based CDC Approach (applyChangesFromSnapshot)

With snapshot-based CDC, you receive **complete table snapshots** periodically:

**Snapshot 1 (Day 1):**
```csv
customer_id,name,email,status
1001,John Doe,john@email.com,ACTIVE
1002,Jane Smith,jane@email.com,ACTIVE
```

**Snapshot 2 (Day 2):**
```csv
customer_id,name,email,status
1001,John Doe,john.doe@email.com,ACTIVE  ← Email changed
1003,Bob Johnson,bob@email.com,ACTIVE    ← New record
# 1002 missing → Deleted
```

**What DLT-META Does:**
- 🔍 **Compares** current snapshot with previous snapshot
- 🔄 **Detects** inserts, updates, and deletes automatically
- 📝 **Applies** changes to target table
- 📊 **Maintains** SCD Type 1 or Type 2 history

**Requirements:**
- ✅ Just needs **primary key** columns
- ✅ No operation column needed
- ✅ No timestamp column needed (optional for SCD2)
- ✅ Receives **full snapshots**

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Full Table Snapshots (Daily/Hourly)                       │
│  - Snapshot 1: 100 rows                                     │
│  - Snapshot 2: 102 rows (2 changed, 1 new, 1 deleted)      │
│  - Snapshot 3: 105 rows                                     │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  DLT create_auto_cdc_from_snapshot_flow                     │
│  1. Load latest snapshot                                    │
│  2. Compare with target table using keys                    │
│  3. Detect changes:                                         │
│     - New keys → INSERT                                     │
│     - Existing keys with different values → UPDATE          │
│     - Missing keys → DELETE (soft or hard)                  │
│  4. Apply changes to target table                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Target Table (SCD Type 1 or Type 2)                       │
│  - Maintains current state (Type 1)                         │
│  - OR maintains history with start/end dates (Type 2)       │
└─────────────────────────────────────────────────────────────┘
```

### Change Detection Algorithm

DLT-META uses the **primary key(s)** to detect changes:

```python
# Pseudo-code of what DLT does internally
current_snapshot = read_latest_snapshot()
target_table = read_target_table()

# 1. INSERTS: Keys in snapshot but not in target
inserts = current_snapshot.anti_join(target_table, on=keys)

# 2. UPDATES: Keys in both, but values different
updates = current_snapshot.join(target_table, on=keys)
                          .where(any_column_changed())

# 3. DELETES: Keys in target but not in snapshot
deletes = target_table.anti_join(current_snapshot, on=keys)

# Apply changes
target_table.merge(inserts, updates, deletes)
```

## Configuration

### Basic Configuration

#### In Onboarding File

```json
{
  "data_flow_id": "201",
  "data_flow_group": "snapshot_cdc",
  "source_format": "snapshot",
  "source_details": {
    "snapshot_format": "delta",  // or "csv", "parquet", "json"
    "source_catalog": "my_catalog",
    "source_database": "source_db",
    "source_table": "products_snapshot"
  },

  // Landing Layer
  "landing_database_prod": "landing",
  "landing_table": "products",
  "landing_apply_changes_from_snapshot": {
    "keys": ["product_id"],
    "scd_type": "1"  // or "2" for Type 2
  },

  // Refinery Layer (optional)
  "refinery_database_prod": "refinery",
  "refinery_table": "products",
  "refinery_apply_changes_from_snapshot": {
    "keys": ["product_id"],
    "scd_type": "2",
    "track_history_column_list": ["price", "status"],  // Columns to track
    "track_history_except_column_list": ["last_updated"]  // Columns NOT to track
  }
}
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `keys` | Yes | Primary key column(s) for identifying records |
| `scd_type` | Yes | `"1"` (overwrite) or `"2"` (history) |
| `track_history_column_list` | No | SCD2: Columns that trigger history tracking |
| `track_history_except_column_list` | No | SCD2: Columns to exclude from history |

### SCD Type 1 vs Type 2

#### SCD Type 1 (Overwrite)

**Behavior:** Updates overwrite existing records. No history kept.

```json
"landing_apply_changes_from_snapshot": {
  "keys": ["customer_id"],
  "scd_type": "1"
}
```

**Example:**

| Before | | | After |
|--------|---------|------|--------|
| customer_id | name | email | → Action |
| 1001 | John Doe | john@old.com | → **Updated** |

| After | | |
|--------|---------|------|
| customer_id | name | email |
| 1001 | John Doe | john@new.com |

**Use when:**
- Don't need history
- Storage is a concern
- Only current state matters

#### SCD Type 2 (History)

**Behavior:** Updates create new records. Old records are marked as inactive with end dates.

```json
"refinery_apply_changes_from_snapshot": {
  "keys": ["customer_id"],
  "scd_type": "2",
  "track_history_column_list": ["email", "status"]
}
```

**Example:**

| Before | | | | |
|--------|---------|------|------------|----------|
| customer_id | email | status | __START_AT | __END_AT |
| 1001 | john@old.com | ACTIVE | 2024-01-01 | NULL |

**After Update:**

| After | | | | |
|--------|---------|------|------------|----------|
| customer_id | email | status | __START_AT | __END_AT |
| 1001 | john@old.com | ACTIVE | 2024-01-01 | **2024-01-02** |
| 1001 | john@new.com | ACTIVE | **2024-01-02** | NULL |

**Automatically Added Columns:**
- `__START_AT` - When this version became active
- `__END_AT` - When this version became inactive (NULL = current)

**Use when:**
- Need to track history
- Audit requirements
- Time-based analytics

### Track History Options (SCD Type 2 Only)

#### `track_history_column_list` (Inclusive)

**Specifies which columns trigger a new history record:**

```json
"refinery_apply_changes_from_snapshot": {
  "keys": ["customer_id"],
  "scd_type": "2",
  "track_history_column_list": ["email", "status", "tier"]
}
```

**Behavior:**
- Changes to `email`, `status`, or `tier` → Create new history record
- Changes to other columns → Just update current record (no new history)

**Use when:** Only specific columns are important for history.

#### `track_history_except_column_list` (Exclusive)

**Specifies which columns do NOT trigger history:**

```json
"refinery_apply_changes_from_snapshot": {
  "keys": ["customer_id"],
  "scd_type": "2",
  "track_history_except_column_list": ["last_login", "click_count", "metadata"]
}
```

**Behavior:**
- Changes to `last_login`, `click_count`, `metadata` → Just update (no new history)
- Changes to any other column → Create new history record

**Use when:** Most columns need history, but a few don't.

## Complete Examples

### Example 1: Simple SCD Type 1 (CSV Snapshots)

```json
{
  "data_flow_id": "100",
  "data_flow_group": "products_snapshot",
  "source_format": "snapshot",
  "source_details": {
    "source_path_prod": "/snapshots/products/LOAD_",
    "snapshot_format": "csv"
  },
  "landing_reader_options": {
    "header": "true"
  },
  "landing_database_prod": "landing",
  "landing_table": "products",
  "landing_apply_changes_from_snapshot": {
    "keys": ["product_id"],
    "scd_type": "1"
  }
}
```

**Snapshot Files:**
```
/snapshots/products/
  ├── LOAD_20240101.csv
  ├── LOAD_20240102.csv
  └── LOAD_20240103.csv
```

**How it works:**
1. DLT reads latest snapshot (LOAD_20240103.csv)
2. Compares with landing.products table
3. Updates, inserts, deletes to match snapshot
4. No history kept (Type 1)

### Example 2: SCD Type 2 with History Tracking (Delta Snapshots)

```json
{
  "data_flow_id": "201",
  "data_flow_group": "customers_snapshot",
  "source_format": "snapshot",
  "source_details": {
    "snapshot_format": "delta",
    "source_catalog": "source_catalog",
    "source_database": "snapshots",
    "source_table": "customers_daily_snapshot"
  },

  "landing_database_prod": "landing",
  "landing_table": "customers",
  "landing_apply_changes_from_snapshot": {
    "keys": ["customer_id"],
    "scd_type": "2"
  },

  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_apply_changes_from_snapshot": {
    "keys": ["customer_id"],
    "scd_type": "2",
    "track_history_except_column_list": [
      "last_login_date",
      "session_count",
      "metadata"
    ]
  },
  "refinery_transformation_json_prod": "/transforms/customer_cleanup.json"
}
```

**Behavior:**
- **Landing:** Maintains full SCD2 history of all columns
- **Refinery:** Only tracks history for important columns (not metadata/counters)
- Applies transformation before storing in refinery

### Example 3: Multi-Column Primary Key

```json
{
  "data_flow_id": "301",
  "data_flow_group": "order_items_snapshot",
  "source_format": "snapshot",
  "source_details": {
    "source_path_prod": "/snapshots/order_items/",
    "snapshot_format": "parquet"
  },

  "landing_database_prod": "landing",
  "landing_table": "order_items",
  "landing_apply_changes_from_snapshot": {
    "keys": ["order_id", "line_item_id"],  // Composite key
    "scd_type": "1"
  }
}
```

**Primary Key:** Combination of `order_id` + `line_item_id`

## Comparison: cdcApplyChanges vs applyChangesFromSnapshot

| Aspect | cdcApplyChanges | applyChangesFromSnapshot |
|--------|-----------------|-------------------------|
| **Input Data** | Incremental changes with operation markers | Full table snapshots |
| **Required Columns** | operation, sequence_by | keys only |
| **Change Detection** | Explicit (from operation column) | Automatic (comparison-based) |
| **Deletes** | Must be marked with operation='D' | Detected automatically |
| **Storage** | Efficient (only changes) | Higher (full snapshots) |
| **Complexity** | Source system must track changes | Source just exports full table |
| **Use Case** | Real-time CDC feeds | Batch snapshot exports |
| **Databricks DLT API** | `create_auto_cdc_flow` | `create_auto_cdc_from_snapshot_flow` |

## When to Use Each

### Use `applyChangesFromSnapshot` When:

✅ Source system provides **full table exports** (daily/hourly dumps)
✅ Source system **cannot track changes** (no CDC support)
✅ Data comes from **third-party vendors** (CSV/Excel exports)
✅ Dealing with **legacy systems** without change tracking
✅ Need to **detect deletes automatically** (missing records)
✅ Source data is **small to medium size** (can afford full snapshots)

**Examples:**
- Daily CSV exports from vendor
- Database full table dumps
- Excel files from partners
- SaaS product snapshots

### Use `cdcApplyChanges` When:

✅ Source system has **native CDC** (database triggers, log mining)
✅ Receiving **streaming CDC events** (Kafka, Event Hub)
✅ Need **real-time processing** (sub-second latency)
✅ Source data is **very large** (snapshots impractical)
✅ Have explicit **operation indicators** (INSERT/UPDATE/DELETE)
✅ Need **exact sequence** of operations

**Examples:**
- Database CDC via Debezium
- Kafka CDC streams
- Event Hub change feeds
- DMS CDC replication

## Best Practices

### 1. Snapshot Naming Convention

Use consistent naming for automatic detection:

```
Good:
/snapshots/products/LOAD_20240101.csv
/snapshots/products/LOAD_20240102.csv
/snapshots/products/LOAD_20240103.csv

Bad:
/snapshots/products/export.csv  ← Always same name
```

### 2. Choose Right SCD Type

```
SCD Type 1 (Overwrite):
- Reference data (product catalogs)
- Master data with no history needs
- Small lookup tables

SCD Type 2 (History):
- Customer data (track status changes)
- Pricing data (track price history)
- Compliance/audit requirements
```

### 3. Selective History Tracking

Don't track history for everything in SCD2:

```json
"track_history_except_column_list": [
  "last_modified_timestamp",  // Just metadata
  "row_count",                // Derived value
  "etl_processed_date",       // ETL metadata
  "hash_value"                // Technical column
]
```

**Benefits:**
- Reduces storage
- Fewer history records
- Better query performance

### 4. Composite Keys

For tables without single primary key:

```json
"keys": ["customer_id", "product_id", "order_date"]
```

**Ensure:** Keys uniquely identify a record in the snapshot.

### 5. Monitor Snapshot Freshness

Set up alerts for:
- Missing snapshots
- Stale snapshots (no updates for X days)
- Snapshot size anomalies

### 6. Test Delete Detection

Verify deletes are detected:

```sql
-- Check for records marked as deleted (SCD2)
SELECT * FROM refinery.customers
WHERE __END_AT IS NOT NULL
ORDER BY __END_AT DESC
LIMIT 10;
```

## Troubleshooting

### Issue 1: No Changes Detected

**Symptom:** Pipeline runs but no updates appear

**Causes:**
1. Keys don't match between snapshots
2. All records are identical
3. Snapshot is empty

**Solution:**
```sql
-- Verify keys exist and are unique in snapshot
SELECT keys, COUNT(*)
FROM snapshot_source
GROUP BY keys
HAVING COUNT(*) > 1;  -- Should return 0 rows

-- Check for actual changes
SELECT COUNT(*) FROM current_snapshot
EXCEPT
SELECT COUNT(*) FROM previous_snapshot;
```

### Issue 2: Too Many History Records

**Symptom:** SCD2 table grows too fast

**Solution:**
```json
// Add track_history_except_column_list
"track_history_except_column_list": [
  "last_updated",
  "row_hash",
  "metadata"
]
```

### Issue 3: Deletes Not Working

**Symptom:** Deleted records still appear

**For SCD Type 1:** Records should be physically deleted
**For SCD Type 2:** Check `__END_AT` is not NULL

**Verify:**
```sql
-- SCD Type 2: Should see ended records
SELECT * FROM table WHERE __END_AT IS NOT NULL;

-- SCD Type 1: Should not exist
SELECT * FROM current_snapshot
WHERE key NOT IN (SELECT key FROM target_table);
```

## Performance Considerations

### Snapshot Size

| Snapshot Size | Frequency | Recommendation |
|---------------|-----------|----------------|
| < 1M rows | Hourly | ✅ Good fit |
| 1M - 10M rows | Daily | ✅ Good fit |
| 10M - 100M rows | Weekly | ⚠️ Consider partitioning |
| > 100M rows | Monthly | ❌ Use cdcApplyChanges instead |

### Optimization Tips

1. **Partition snapshots** if large:
   ```json
   "source_details": {
     "source_path_prod": "/snapshots/products/{year}/{month}/",
     "snapshot_format": "parquet"
   }
   ```

2. **Use efficient file formats**:
   - ✅ Parquet (best)
   - ✅ Delta (good)
   - ⚠️ CSV (acceptable for small)
   - ❌ JSON (avoid for large)

3. **Cluster target tables**:
   ```json
   "landing_cluster_by": ["customer_id"],
   "landing_table_properties": {
     "pipelines.autoOptimize.managed": "true"
   }
   ```

## Advanced: Custom Snapshot Readers

For complex snapshot scenarios, you can provide a custom reader function:

```python
def next_snapshot_and_version(latest_version, dataflow_spec):
    """
    Custom function to determine next snapshot to process.

    Args:
        latest_version: Last processed snapshot version
        dataflow_spec: Current dataflow configuration

    Returns:
        DataFrame of next snapshot
    """
    # Custom logic to find and read next snapshot
    next_file = find_next_snapshot(latest_version)
    return spark.read.parquet(next_file)

# Pass to pipeline
DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer="landing",
    landing_next_snapshot_and_version=next_snapshot_and_version
)
```

## Summary

### Key Takeaways

1. **`applyChangesFromSnapshot`** is for declarative CDC from full snapshots
2. **No operation columns needed** - changes detected automatically
3. **Supports SCD Type 1 and Type 2** - choose based on history needs
4. **Best for batch snapshot exports** - not real-time CDC
5. **Automatic delete detection** - missing records = deletes
6. **Efficient with right configuration** - use selective history tracking

### Decision Tree

```
Do you have full table snapshots?
├─ Yes → Use applyChangesFromSnapshot
│  └─ Need history?
│     ├─ Yes → SCD Type 2
│     └─ No  → SCD Type 1
│
└─ No (have incremental CDC)
   └─ Use cdcApplyChanges
```

## See Also

- [Onboarding File Reference](ONBOARDING_FILE_REFERENCE.md)
- [CDC Configuration Guide](CDC_CONFIGURATION.md)
- [Examples](../demo/conf/snapshot-onboarding.template)
