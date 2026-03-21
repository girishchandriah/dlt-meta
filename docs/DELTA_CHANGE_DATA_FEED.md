# Delta Change Data Feed (CDF) with DLT-META

## What is Delta Change Data Feed?

**Delta Change Data Feed (CDF)** is a native Delta Lake feature that automatically tracks and stores **row-level changes** (inserts, updates, deletes) made to a Delta table. When enabled, Delta maintains a change log that you can query to get incremental changes.

## How It Differs from Other CDC Approaches

### Comparison of All CDC Methods

| Method | Source Type | Change Tracking | Use Case |
|--------|------------|-----------------|----------|
| **cdcApplyChanges** | Any source with operation column | Explicit markers (INSERT/UPDATE/DELETE) | Real-time CDC streams, database logs |
| **applyChangesFromSnapshot** | Full table snapshots | Compare current vs previous | Batch exports, legacy systems |
| **Delta CDF** | Delta table with CDF enabled | Delta Lake automatic tracking | Delta-to-Delta pipelines, downstream consumers |

## Delta Change Data Feed Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Source Delta Table (CDF Enabled)                           │
│  - Changes automatically tracked by Delta Lake              │
│  - _change_type: insert, update_preimage, update_postimage, │
│                  delete                                      │
│  - _commit_version: Delta version number                    │
│  - _commit_timestamp: When change occurred                  │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  DLT-META reads Delta CDF                                   │
│  - Uses readStream on Delta table                           │
│  - Delta automatically provides change records              │
│  - Processes changes incrementally                          │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Target Table (Landing or Refinery)                         │
│  - Apply changes using cdcApplyChanges                      │
│  - OR just append all changes to audit table                │
└─────────────────────────────────────────────────────────────┘
```

## How Delta CDF Works

### When CDF is Enabled

```sql
-- Enable CDF on a Delta table
ALTER TABLE my_catalog.source_db.customers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**What Delta Tracks Automatically:**

| _change_type | Description | When It Occurs |
|-------------|-------------|----------------|
| `insert` | New row added | INSERT operation |
| `update_preimage` | Old values before update | UPDATE operation (before) |
| `update_postimage` | New values after update | UPDATE operation (after) |
| `delete` | Row deleted | DELETE operation |

**Additional Metadata Columns:**

- `_commit_version` - Delta table version when change occurred
- `_commit_timestamp` - Timestamp of the change
- All original table columns

### Example: What CDF Looks Like

**Original Table:**
```sql
SELECT * FROM customers;
```

| customer_id | name | email | status |
|------------|------|-------|--------|
| 1001 | John Doe | john@email.com | ACTIVE |

**After Updates:**
```sql
-- Update
UPDATE customers SET email = 'john.doe@email.com' WHERE customer_id = 1001;

-- Insert
INSERT INTO customers VALUES (1002, 'Jane Smith', 'jane@email.com', 'ACTIVE');

-- Delete
DELETE FROM customers WHERE customer_id = 1003;
```

**CDF Output (What DLT-META Sees):**

| customer_id | name | email | status | _change_type | _commit_version | _commit_timestamp |
|------------|------|-------|--------|--------------|-----------------|-------------------|
| 1001 | John Doe | john@email.com | ACTIVE | update_preimage | 5 | 2024-01-15 10:30:00 |
| 1001 | John Doe | john.doe@email.com | ACTIVE | update_postimage | 5 | 2024-01-15 10:30:00 |
| 1002 | Jane Smith | jane@email.com | ACTIVE | insert | 6 | 2024-01-15 10:31:00 |
| 1003 | Bob Johnson | bob@email.com | INACTIVE | delete | 7 | 2024-01-15 10:32:00 |

## Using Delta CDF with DLT-META

### Approach 1: Read CDF and Apply Changes (Recommended)

**Use Delta CDF as source + cdcApplyChanges to apply to target**

#### Configuration

```json
{
  "data_flow_id": "300",
  "data_flow_group": "delta_cdf",
  "source_system": "delta_cdf",
  "source_format": "delta",  // Delta table as source

  "source_details": {
    "catalog": "source_catalog",
    "database": "source_db",
    "table": "customers_with_cdf"  // Delta table with CDF enabled
  },

  "landing_database_prod": "landing",
  "landing_table": "customers",

  // Map CDF change types to CDC operations
  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "_commit_version",  // Use Delta version as sequence
    "scd_type": "2",
    "apply_as_deletes": "_change_type = 'delete'",  // Map delete operation
    "except_column_list": [
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    ]
  }
}
```

#### How It Works

1. **Source:** DLT-META reads from Delta table with CDF enabled
2. **Delta Provides:** Change records with `_change_type` metadata
3. **Apply Changes:** Uses `cdcApplyChanges` to merge into target
4. **Sequence:** Uses `_commit_version` for ordering changes

### Approach 2: Streaming Reads from Delta CDF

**Direct streaming from Delta CDF to landing**

#### Configuration

```json
{
  "data_flow_id": "301",
  "data_flow_group": "delta_cdf_stream",
  "source_format": "delta",

  "source_details": {
    "catalog": "source_catalog",
    "database": "source_db",
    "table": "orders_with_cdf"
  },

  "landing_database_prod": "landing",
  "landing_table": "orders_changes",

  // Store all changes (including preimages/postimages)
  "landing_reader_options": {
    "readChangeFeed": "true",  // Enable CDF reading
    "startingVersion": "0"     // Start from version 0, or "latest"
  }
}
```

#### Use Cases

- **Audit trail:** Keep complete history of all changes
- **Change data capture:** Track what changed and when
- **Downstream CDC:** Feed changes to downstream systems
- **Debugging:** Investigate data quality issues

### Approach 3: Incremental Processing with CDF

**Process only new changes since last run**

```json
{
  "data_flow_id": "302",
  "data_flow_group": "incremental_cdf",
  "source_format": "delta",

  "source_details": {
    "catalog": "source_catalog",
    "database": "source_db",
    "table": "products_with_cdf"
  },

  "landing_reader_options": {
    "readChangeFeed": "true",
    "startingVersion": "latest"  // Only new changes
  },

  "landing_database_prod": "landing",
  "landing_table": "products",

  "refinery_database_prod": "refinery",
  "refinery_table": "products",
  "refinery_cdc_apply_changes": {
    "keys": ["product_id"],
    "sequence_by": "_commit_timestamp",
    "scd_type": "1",
    "apply_as_deletes": "_change_type = 'delete'"
  }
}
```

## Enabling CDF on Source Tables

### At Table Creation

```sql
CREATE TABLE source_catalog.source_db.customers (
  customer_id BIGINT,
  name STRING,
  email STRING,
  status STRING
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
);
```

### On Existing Table

```sql
ALTER TABLE source_catalog.source_db.customers
SET TBLPROPERTIES (
  delta.enableChangeDataFeed = true
);
```

**Important:** CDF only tracks changes **after** it's enabled. Historical changes before enabling CDF are not available.

### Check if CDF is Enabled

```sql
DESCRIBE DETAIL source_catalog.source_db.customers;
-- Look for: delta.enableChangeDataFeed = true in properties
```

Or:

```sql
SHOW TBLPROPERTIES source_catalog.source_db.customers;
-- Check: delta.enableChangeDataFeed
```

## Reading CDF Manually (For Testing)

### Read All Changes Since Version

```python
# Read changes from version 10 onwards
changes_df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .table("source_catalog.source_db.customers")

changes_df.show()
```

### Read Changes Between Versions

```python
# Read changes between version 5 and 10
changes_df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 5) \
    .option("endingVersion", 10) \
    .table("source_catalog.source_db.customers")
```

### Read Changes Between Timestamps

```python
# Read changes in last 24 hours
changes_df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2024-01-14") \
    .option("endingTimestamp", "2024-01-15") \
    .table("source_catalog.source_db.customers")
```

### Stream CDF Changes

```python
# Stream changes in real-time
changes_stream = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", "latest") \
    .table("source_catalog.source_db.customers")

# Process changes
changes_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/customers_cdf") \
    .table("landing.customers_changes")
```

## Complete Example: Delta CDF Pipeline

### Scenario

**Source:** Delta table with CDF enabled (updated by upstream process)
**Goal:** Maintain SCD Type 2 in refinery layer

### Step 1: Source Table with CDF

```sql
-- Upstream system maintains this table
CREATE TABLE source.raw.customer_master (
  customer_id BIGINT,
  name STRING,
  email STRING,
  phone STRING,
  tier STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
);
```

### Step 2: Onboarding Configuration

```json
{
  "data_flow_id": "400",
  "data_flow_group": "customer_cdf",
  "source_system": "upstream_delta",
  "source_format": "delta",

  "source_details": {
    "catalog": "source",
    "database": "raw",
    "table": "customer_master"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing",
  "landing_table": "customers",
  "landing_table_comment": "Customer changes from upstream Delta CDF",

  "landing_reader_options": {
    "readChangeFeed": "true",
    "startingVersion": "latest"
  },

  "landing_table_properties": {
    "delta.enableChangeDataFeed": "true",  // Enable CDF on landing too
    "pipelines.autoOptimize.managed": "true"
  },

  "refinery_catalog_prod": "my_catalog",
  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_table_comment": "Customer SCD Type 2 from CDF",

  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "_commit_timestamp",
    "scd_type": "2",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": [
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    ],
    "track_history_except_column_list": [
      "updated_date"
    ]
  },

  "refinery_cluster_by": ["customer_id"],

  "refinery_table_properties": {
    "pipelines.autoOptimize.zOrderCols": "customer_id"
  }
}
```

### Step 3: Pipeline Configuration

```json
{
  "name": "customer_cdf_pipeline",
  "configuration": {
    "layer": "landing_refinery",
    "landing.group": "customer_cdf",
    "refinery.group": "customer_cdf",
    "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec",
    "refinery.dataflowspecTable": "catalog.schema.refinery_dataflowspec",
    "dlt_meta_whl": "/path/to/wheel.whl"
  },
  "catalog": "my_catalog",
  "serverless": true,
  "continuous": true  // Run continuously to pick up changes
}
```

### Step 4: What Happens

1. **Upstream Updates Source:**
   ```sql
   UPDATE source.raw.customer_master
   SET tier = 'GOLD'
   WHERE customer_id = 1001;
   ```

2. **Delta CDF Records:**
   - `update_preimage`: Old record (tier='SILVER')
   - `update_postimage`: New record (tier='GOLD')

3. **DLT-META Processes:**
   - Reads CDF from source
   - Writes to landing with all CDF metadata
   - Applies changes to refinery using CDC
   - Creates SCD Type 2 history record

4. **Refinery Result:**
   ```
   customer_id | tier   | __START_AT          | __END_AT
   1001       | SILVER | 2024-01-01 00:00:00 | 2024-01-15 10:30:00
   1001       | GOLD   | 2024-01-15 10:30:00 | NULL
   ```

## Advanced: Handling CDF Change Types

### Understanding Change Types

When reading CDF, you get multiple records per update:

```python
# Example: Processing CDF changes
cdf_df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .table("source.customers")

# See change types
cdf_df.groupBy("_change_type").count().show()

# Output:
# +-------------------+-----+
# | _change_type      |count|
# +-------------------+-----+
# | insert            | 150 |
# | update_preimage   | 45  |
# | update_postimage  | 45  |
# | delete            | 5   |
# +-------------------+-----+
```

### Filter for Specific Changes

```python
# Only current state (inserts + update postimages)
current_state = cdf_df.filter(
    col("_change_type").isin("insert", "update_postimage")
)

# Only deletes
deletes = cdf_df.filter(col("_change_type") == "delete")
```

### Custom Transformation

```json
// In refinery_transformation_json
{
  "transformation_id": "cdf_current_state",
  "sql_query": "
    SELECT *
    FROM source_customers
    WHERE _change_type IN ('insert', 'update_postimage')
  "
}
```

## Best Practices

### 1. Enable CDF on Source Tables

✅ **Enable CDF on:**
- Source Delta tables that will be consumed by DLT-META
- Landing tables (to enable downstream CDF consumption)
- Tables with frequent updates

❌ **Don't enable CDF on:**
- Append-only tables (no updates/deletes)
- Very large tables with millions of updates/hour (storage cost)
- Temporary staging tables

### 2. Choose Right Sequence Column

```json
// Use commit_timestamp for time-based ordering
"sequence_by": "_commit_timestamp"

// Use commit_version for strict version ordering
"sequence_by": "_commit_version"
```

**Prefer `_commit_timestamp`** for most use cases (handles clock skew better).

### 3. Filter Out Preimages

If you only want current state, exclude preimages:

```json
"refinery_where_clause": "_change_type != 'update_preimage'"
```

Or in cdcApplyChanges, they'll be automatically handled.

### 4. Monitor CDF Storage

CDF data is stored with Delta table, increasing storage:

```sql
-- Check table size including CDF
DESCRIBE DETAIL source.customers;

-- Vacuum to clean up old CDF data (after retention period)
VACUUM source.customers RETAIN 168 HOURS;  -- 7 days
```

### 5. Set Appropriate Retention

```sql
-- Set CDF retention (default: same as table retention)
ALTER TABLE source.customers
SET TBLPROPERTIES (
  delta.deletedFileRetentionDuration = 'interval 7 days',
  delta.logRetentionDuration = 'interval 30 days'
);
```

**Balance:**
- Longer retention = More storage cost
- Shorter retention = Less time to recover from failures

## Troubleshooting

### Issue 1: CDF Not Available

**Symptom:** Error reading CDF, or no `_change_type` column

**Solution:**
```sql
-- Check if CDF is enabled
SHOW TBLPROPERTIES source.customers;

-- Enable if not
ALTER TABLE source.customers
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**Remember:** CDF only tracks changes **after** enabling!

### Issue 2: Missing Historical Changes

**Symptom:** Need changes before CDF was enabled

**Solution:** Use `applyChangesFromSnapshot` instead for initial load, then switch to CDF.

### Issue 3: Too Much CDF Data

**Symptom:** Storage costs high, slow queries

**Solution:**
```sql
-- Vacuum old CDF data
VACUUM source.customers RETAIN 168 HOURS;

-- Reduce retention period
ALTER TABLE source.customers
SET TBLPROPERTIES (
  delta.logRetentionDuration = 'interval 7 days'
);
```

### Issue 4: Duplicate Records

**Symptom:** Seeing both preimage and postimage

**Solution:** Filter in transformation:
```json
"refinery_where_clause": "_change_type IN ('insert', 'update_postimage', 'delete')"
```

Or rely on `cdcApplyChanges` to handle it automatically.

## Performance Considerations

### CDF Overhead

| Aspect | Impact | Mitigation |
|--------|--------|-----------|
| **Storage** | +20-30% | Set shorter retention, vacuum regularly |
| **Write Performance** | Minimal (1-5%) | Delta handles efficiently |
| **Read Performance** | Faster than full scans | CDF is incremental |

### Optimization Tips

1. **Use Streaming Reads:**
   ```json
   "landing_reader_options": {
     "readChangeFeed": "true",
     "startingVersion": "latest"
   }
   ```

2. **Cluster Target Tables:**
   ```json
   "refinery_cluster_by": ["customer_id"]
   ```

3. **Z-Order for Queries:**
   ```json
   "refinery_table_properties": {
     "pipelines.autoOptimize.zOrderCols": "customer_id,date"
   }
   ```

## When to Use Delta CDF

### ✅ Use Delta CDF When:

- Source is **Delta table** (or can be converted to Delta)
- Source table has **frequent updates/deletes**
- Need **automatic change tracking** (no manual CDC setup)
- Building **Delta-to-Delta pipelines**
- Want **incremental processing** efficiency
- Source system controls the Delta table

### ❌ Don't Use Delta CDF When:

- Source is **not Delta** (CSV, JSON, Parquet files)
- Source is **external database** (use database CDC instead)
- Source is **streaming** (Kafka, Event Hub)
- Table is **append-only** (no updates/deletes)

## Decision Matrix

| Scenario | Best Approach |
|----------|---------------|
| Source: Delta table with CDF | **Use Delta CDF** ✓ |
| Source: Database with native CDC | **cdcApplyChanges** |
| Source: Daily CSV snapshots | **applyChangesFromSnapshot** |
| Source: Real-time Kafka CDC | **cdcApplyChanges** |
| Source: Delta table without CDF | **Enable CDF**, or use snapshot approach |
| Source: External API dumps | **applyChangesFromSnapshot** |

## Summary

### Key Takeaways

1. **Delta CDF** is automatic change tracking built into Delta Lake
2. **No extra CDC setup** required - Delta handles it natively
3. **Best for Delta-to-Delta** pipelines within Databricks
4. **Provides metadata** like `_change_type`, `_commit_version`, `_commit_timestamp`
5. **Incremental and efficient** - only processes changes
6. **Works with cdcApplyChanges** - combine CDF with SCD Type 1/2
7. **Storage overhead** - plan for 20-30% more storage

### Quick Comparison

```
Delta CDF:
  Source: Delta table ✓
  Setup: Enable CDF property
  Changes: Automatic tracking
  Best for: Delta-to-Delta pipelines

cdcApplyChanges:
  Source: Any with operation column
  Setup: Operation + sequence columns
  Changes: Explicit markers
  Best for: Database CDC, Kafka

applyChangesFromSnapshot:
  Source: Full snapshots
  Setup: Just primary key
  Changes: Comparison-based
  Best for: Batch exports, legacy
```

## See Also

- [CDC Apply Changes](CDC_CONFIGURATION.md)
- [Apply Changes From Snapshot](CDC_APPLY_CHANGES_FROM_SNAPSHOT.md)
- [Onboarding Reference](ONBOARDING_FILE_REFERENCE.md)
- [Delta Lake CDF Documentation](https://docs.delta.io/latest/delta-change-data-feed.html)
