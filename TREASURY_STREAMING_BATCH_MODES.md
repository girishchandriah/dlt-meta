# Treasury Layer: Streaming vs Batch Mode

The DLT-META framework now supports both **streaming** and **batch** modes for the treasury layer within the same pipeline.

## Overview

The treasury layer can operate in two modes:
1. **Streaming Mode** - Reads Change Data Feed from refinery as a stream, uses CDC for incremental updates
2. **Batch Mode** - Reads refinery table as batch, suitable for full refreshes or snapshot processing

## Configuration

### Streaming Mode (Recommended for CDC)

To enable streaming mode for treasury, configure these settings:

```json
{
  "treasury_reader_options": {
    "readChangeFeed": "true"
  },
  "treasury_cdc_apply_changes": {
    "keys": ["primary_key_col1", "primary_key_col2"],
    "sequence_by": "timestamp_column",
    "scd_type": "1",
    "except_column_list": []
  }
}
```

**Requirements:**
- `readChangeFeed: true` in `treasury_reader_options`
- `treasury_cdc_apply_changes` must be configured
- Refinery table must have Change Data Feed enabled

**Benefits:**
- Incremental processing (only changed data)
- Streaming end-to-end pipeline
- Lower latency
- Automatic CDC merge logic (upserts/deletes)

### Batch Mode (Legacy)

To use batch mode for treasury:

```json
{
  "treasury_reader_options": {},
  "treasury_cdc_apply_changes": {
    "keys": ["primary_key_col1", "primary_key_col2"],
    "sequence_by": "timestamp_column",
    "scd_type": "1"
  }
}
```

**OR** set explicit snapshot mode:

```json
{
  "sourceFormat": "snapshot",
  "treasury_reader_options": {}
}
```

**Characteristics:**
- Reads entire refinery table as batch
- Writes to intermediate staging table (if CDC configured)
- Requires manual merge logic for final table
- Suitable for scheduled full refreshes

**Note:** Batch mode with CDC has limitations - it creates an intermediate table and requires additional merge logic.

## Detection Logic

The framework automatically detects which mode to use based on:

1. **Streaming Mode Enabled When:**
   - `treasury_reader_options.readChangeFeed` = `"true"` **AND**
   - `treasury_cdc_apply_changes` is configured

   **OR**

   - `sourceFormat` is set and is NOT `"snapshot"`

2. **Batch Mode Enabled When:**
   - `readChangeFeed` is not `"true"` or not present
   - `sourceFormat` = `"snapshot"`
   - No CDC configuration

## Implementation Details

### Streaming Mode Flow

```
Landing (Stream)
  ↓ (Auto Loader)
Refinery (Stream + CDC)
  ↓ (Change Data Feed)
Treasury (Stream + CDC) ← Uses create_auto_cdc_flow
```

**Code Path:**
1. `read_treasury()` → Uses `spark.readStream.table()` with `readChangeFeed`
2. `dlt.view()` → Creates streaming view with SQL transformation
3. `create_streaming_table()` → Creates empty streaming target table
4. `dlt.create_auto_cdc_flow()` → Applies CDC from view to table

### Batch Mode Flow

```
Landing (Stream)
  ↓ (Auto Loader)
Refinery (Stream + CDC)
  ↓ (Batch Read)
Treasury (Batch) ← Manual table write or staging table
```

**Code Path:**
1. `read_treasury()` → Uses `spark.read.table()` (batch)
2. `dlt.view()` → Creates batch view with SQL transformation
3. `_write_treasury_batch_cdc()` → Writes to intermediate staging table
4. *Manual step required* → Merge intermediate table(s) into final treasury table

## Examples

### Example 1: Streaming Treasury with CDC (Recommended)

**Configuration:**
```json
{
  "data_flow_id": "204",
  "treasury_catalog_nonprod": "dataservices_nonprod",
  "treasury_database_nonprod": "treasury_teradata_base_nonprod",
  "treasury_table": "pfocusdb_sales_ord_tran_dlt",
  "treasury_reader_options": {
    "readChangeFeed": "true"
  },
  "treasury_cdc_apply_changes": {
    "keys": ["sales_ord_id", "event_id", "sales_ord_tran_id", "sales_ord_tran_dt"],
    "sequence_by": "host_acct_create_dt",
    "scd_type": "1"
  },
  "treasury_cluster_by": ["sales_ord_tran_dt", "event_id"]
}
```

**Result:**
- End-to-end streaming pipeline
- Automatic upserts based on keys
- Treasury table stays in sync with refinery changes

### Example 2: Batch Treasury (Full Refresh)

**Configuration:**
```json
{
  "data_flow_id": "204",
  "treasury_catalog_nonprod": "dataservices_nonprod",
  "treasury_database_nonprod": "treasury_teradata_base_nonprod",
  "treasury_table": "pfocusdb_sales_ord_tran_dlt",
  "sourceFormat": "snapshot",
  "treasury_reader_options": {},
  "treasury_cluster_by": ["sales_ord_tran_dt", "event_id"]
}
```

**Result:**
- Batch processing of entire refinery table
- Overwrites or appends to treasury table
- No CDC merge logic

### Example 3: Mixed Mode Pipeline (Advanced)

You can have multiple dataflows in the same pipeline with different modes:

**Dataflow 1:** Streaming treasury with CDC
**Dataflow 2:** Batch treasury for historical snapshots

```json
[
  {
    "data_flow_id": "204_streaming",
    "treasury_reader_options": {
      "readChangeFeed": "true"
    },
    "treasury_cdc_apply_changes": { ... }
  },
  {
    "data_flow_id": "204_batch",
    "sourceFormat": "snapshot",
    "treasury_reader_options": {}
  }
]
```

## Helper Method: `_is_treasury_streaming()`

The framework provides a helper method to detect streaming mode:

```python
def _is_treasury_streaming(self):
    """Check if treasury layer is configured for streaming mode.

    Returns:
        bool: True if treasury should use streaming, False for batch mode

    Treasury uses streaming when:
    - readChangeFeed is set to 'true' in treasury_reader_options
    - CDC apply changes is configured
    - sourceFormat is not 'snapshot'
    """
```

## Troubleshooting

### Error: "View is not a streaming view"
**Cause:** Treasury is configured with CDC but not using streaming mode
**Solution:** Add `"readChangeFeed": "true"` to `treasury_reader_options`

### Error: "STREAMING_TARGET_NOT_DEFINED"
**Cause:** CDC configured but streaming table not created
**Solution:** Ensure `readChangeFeed: true` is set to trigger streaming mode detection

### Performance Issues with Batch Mode
**Cause:** Processing entire refinery table on each run
**Solution:** Switch to streaming mode with CDC for incremental processing

### Intermediate Tables Created
**Cause:** Batch mode with CDC creates staging tables
**Solution:** Either switch to streaming mode OR create manual merge logic for intermediate tables

## Migration Guide

### From Batch to Streaming Mode

1. Ensure refinery table has Change Data Feed enabled:
   ```sql
   ALTER TABLE dataservices_nonprod.refinery_nonprod.my_table
   SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
   ```

2. Update configuration:
   ```json
   "treasury_reader_options": {
     "readChangeFeed": "true"
   }
   ```

3. Drop existing treasury table (for clean start):
   ```sql
   DROP TABLE IF EXISTS dataservices_nonprod.treasury_nonprod.my_table;
   ```

4. Restart DLT pipeline

### From Streaming to Batch Mode

1. Update configuration (remove readChangeFeed):
   ```json
   "treasury_reader_options": {},
   "sourceFormat": "snapshot"
   ```

2. Remove CDC configuration if not needed:
   - Remove `treasury_cdc_apply_changes` section

3. Restart DLT pipeline

## Best Practices

1. **Use Streaming Mode for Production CDC Pipelines**
   - Lower latency, incremental processing
   - Automatic merge logic
   - Better resource utilization

2. **Use Batch Mode for:**
   - One-time historical loads
   - Full table refreshes
   - Testing/development without CDC requirements

3. **Enable Change Data Feed on All Upstream Tables**
   ```python
   spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
   ```

4. **Monitor Pipeline Performance**
   - Streaming: Check stream processing rate, lag
   - Batch: Check execution time, data volume processed

5. **Test Both Modes in Non-Production First**
   - Validate data correctness
   - Measure performance differences
   - Verify CDC merge behavior

## Related Documentation

- [Databricks Delta Live Tables CDC](https://docs.databricks.com/delta-live-tables/cdc.html)
- [Change Data Feed](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [DLT-META Framework](https://databrickslabs.github.io/dlt-meta/)

---

**Last Updated:** 2025-02-22
**Framework Version:** DLT-META v0.0.9+
