# CDC Methods Comparison - Complete Guide

## Overview

DLT-META supports **three different CDC (Change Data Capture) approaches**. This guide helps you choose the right one for your use case.

## The Three CDC Methods

### 1. cdcApplyChanges (Traditional CDC)
**Source has explicit change markers**

### 2. applyChangesFromSnapshot (Declarative CDC)
**Source provides full table snapshots**

### 3. Delta Change Data Feed (CDF)
**Source is Delta table with automatic change tracking**

---

## Quick Comparison Table

| Aspect | cdcApplyChanges | applyChangesFromSnapshot | Delta CDF |
|--------|----------------|-------------------------|-----------|
| **Source Type** | Any (CSV, Kafka, DB logs) | Full snapshots (CSV, Delta) | Delta table only |
| **Change Markers** | Explicit operation column | None (auto-detected) | Automatic (_change_type) |
| **Required Fields** | keys, sequence_by, operation | keys only | keys, sequence_by |
| **Streaming Support** | ✅ Yes | ❌ No (batch) | ✅ Yes |
| **Delete Detection** | Explicit marker | Missing records | Automatic |
| **Setup Complexity** | Medium | Low | Very Low |
| **Storage Efficiency** | High | Low (full snapshots) | Medium (+20-30%) |
| **Latency** | Real-time | Batch (hourly/daily) | Real-time |
| **Source Requirements** | CDC-capable | Can export full tables | Must be Delta table |
| **Best For** | Database CDC, Kafka | Legacy systems, vendor exports | Delta-to-Delta pipelines |

---

## Detailed Comparison

### Data Format Requirements

#### cdcApplyChanges
```csv
customer_id,name,email,operation,update_timestamp
1001,John,john@email.com,INSERT,2024-01-01 10:00:00
1001,John,john.doe@email.com,UPDATE,2024-01-02 11:00:00
1002,Jane,jane@email.com,DELETE,2024-01-03 12:00:00
```
**Requires:** operation + sequence_by columns

#### applyChangesFromSnapshot
```csv
# Snapshot 1 (Day 1)
customer_id,name,email
1001,John,john@email.com
1002,Jane,jane@email.com

# Snapshot 2 (Day 2)
customer_id,name,email
1001,John,john.doe@email.com
1003,Bob,bob@email.com
# 1002 missing = deleted
```
**Requires:** Just primary key

#### Delta CDF
```
Source Delta Table (CDF enabled)
↓ (automatic)
customer_id,name,email,_change_type,_commit_timestamp
1001,John,john@email.com,insert,2024-01-01 10:00:00
1001,John,john@email.com,update_preimage,2024-01-02 11:00:00
1001,John,john.doe@email.com,update_postimage,2024-01-02 11:00:00
1002,Jane,jane@email.com,delete,2024-01-03 12:00:00
```
**Requires:** Delta table with CDF property enabled

---

## Configuration Examples

### Example 1: cdcApplyChanges (Database CDC via Debezium)

```json
{
  "data_flow_id": "100",
  "data_flow_group": "mysql_cdc",
  "source_format": "kafka",  // Debezium CDC to Kafka

  "source_details": {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "mysql.customers"
  },

  "landing_database_prod": "landing",
  "landing_table": "customers",

  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "ts_ms",  // Debezium timestamp
    "scd_type": "2",
    "apply_as_deletes": "op = 'd'",  // Debezium delete marker
    "except_column_list": ["op", "ts_ms"]
  }
}
```

**Use Case:** Real-time CDC from MySQL/PostgreSQL/Oracle via Debezium

### Example 2: applyChangesFromSnapshot (Daily Vendor Exports)

```json
{
  "data_flow_id": "200",
  "data_flow_group": "vendor_snapshot",
  "source_format": "snapshot",

  "source_details": {
    "source_path_prod": "/vendor/exports/products/LOAD_",
    "snapshot_format": "csv"
  },

  "landing_reader_options": {
    "header": "true"
  },

  "landing_database_prod": "landing",
  "landing_table": "products",
  "landing_apply_changes_from_snapshot": {
    "keys": ["product_id"],
    "scd_type": "2"
  },

  "refinery_database_prod": "refinery",
  "refinery_table": "products",
  "refinery_apply_changes_from_snapshot": {
    "keys": ["product_id"],
    "scd_type": "2",
    "track_history_except_column_list": ["last_updated"]
  }
}
```

**Use Case:** Daily CSV exports from external vendor

### Example 3: Delta CDF (Delta-to-Delta Pipeline)

```json
{
  "data_flow_id": "300",
  "data_flow_group": "delta_cdf",
  "source_format": "delta",

  "source_details": {
    "catalog": "upstream",
    "database": "source",
    "table": "customers"  // Has CDF enabled
  },

  "landing_reader_options": {
    "readChangeFeed": "true",
    "startingVersion": "latest"
  },

  "landing_database_prod": "landing",
  "landing_table": "customers",

  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "_commit_timestamp",
    "scd_type": "2",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"]
  }
}
```

**Use Case:** Consuming changes from upstream Delta table within Databricks

---

## Decision Tree

```
What type of source do you have?

├─ Delta Table
│  ├─ Has CDF enabled?
│  │  ├─ Yes → Use Delta CDF ✓
│  │  └─ No
│  │     ├─ Can enable CDF? → Enable and use Delta CDF ✓
│  │     └─ Cannot enable → Use applyChangesFromSnapshot
│  │
│  └─ Is Delta but changes come separately (Kafka)?
│     └─ Use cdcApplyChanges
│
├─ Database (MySQL, PostgreSQL, Oracle)
│  ├─ Has native CDC (Debezium, DMS)?
│  │  └─ Yes → Use cdcApplyChanges ✓
│  │
│  └─ No CDC, just full exports?
│     └─ Use applyChangesFromSnapshot ✓
│
├─ Streaming (Kafka, Event Hub)
│  ├─ Has change markers (operation column)?
│  │  └─ Yes → Use cdcApplyChanges ✓
│  │
│  └─ No markers?
│     └─ Cannot detect deletes - use append only
│
├─ File Exports (CSV, Parquet, JSON)
│  ├─ Has operation column?
│  │  └─ Yes → Use cdcApplyChanges ✓
│  │
│  └─ Full table exports (snapshots)?
│     └─ Use applyChangesFromSnapshot ✓
│
└─ SaaS/API (Salesforce, SAP)
   ├─ Provides CDC feed?
   │  └─ Yes → Use cdcApplyChanges ✓
   │
   └─ Provides full exports?
      └─ Use applyChangesFromSnapshot ✓
```

---

## Performance Comparison

| Metric | cdcApplyChanges | applyChangesFromSnapshot | Delta CDF |
|--------|----------------|-------------------------|-----------|
| **Latency** | Seconds to minutes | Hours to days | Seconds to minutes |
| **Throughput** | High (incremental) | Medium (full table) | High (incremental) |
| **Storage** | Low (changes only) | High (full snapshots) | Medium (changes + log) |
| **Network** | Low | High | Medium |
| **Compute** | Low | Medium-High | Low-Medium |

### Cost Analysis

**For 1M row table with 10K daily changes:**

| Method | Daily Data Processed | Storage Cost | Compute Cost |
|--------|---------------------|--------------|--------------|
| cdcApplyChanges | 10K rows | $ | $ |
| applyChangesFromSnapshot | 1M rows | $$$ | $$$ |
| Delta CDF | 10K rows | $$ | $ |

**Recommendation:** Delta CDF or cdcApplyChanges for large tables with small change rate.

---

## Feature Comparison

### Change Detection

| Feature | cdcApplyChanges | applyChangesFromSnapshot | Delta CDF |
|---------|----------------|-------------------------|-----------|
| **Insert Detection** | Explicit marker | Automatic | Automatic |
| **Update Detection** | Explicit marker | Automatic | Automatic |
| **Delete Detection** | Explicit marker | Missing = delete | Automatic |
| **Column-level Changes** | Via sequence_by | All columns compared | All columns tracked |

### SCD Support

| Feature | cdcApplyChanges | applyChangesFromSnapshot | Delta CDF |
|---------|----------------|-------------------------|-----------|
| **SCD Type 1** | ✅ Yes | ✅ Yes | ✅ Yes |
| **SCD Type 2** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Track History List** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Selective Tracking** | ✅ Yes | ✅ Yes | ✅ Yes |

### Processing Modes

| Feature | cdcApplyChanges | applyChangesFromSnapshot | Delta CDF |
|---------|----------------|-------------------------|-----------|
| **Streaming** | ✅ Yes | ❌ No (batch only) | ✅ Yes |
| **Batch** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Continuous** | ✅ Yes | ⚠️ Periodic | ✅ Yes |
| **Triggered** | ✅ Yes | ✅ Yes | ✅ Yes |

---

## Real-World Scenarios

### Scenario 1: E-commerce Platform

**Requirements:**
- Source: MySQL database
- Volume: 10M customers, 100K updates/day
- Latency: < 5 minutes
- Need: SCD Type 2 history

**Solution:** **cdcApplyChanges** with Debezium
```
MySQL → Debezium → Kafka → DLT-META (cdcApplyChanges) → Delta
```

**Why:**
- Real-time requirement
- Native MySQL CDC available
- Incremental efficiency
- Streaming support

### Scenario 2: Third-Party Vendor Data

**Requirements:**
- Source: Daily CSV exports from vendor
- Volume: 500K products, 5K changes/day
- Latency: Daily is OK
- Need: Track price history

**Solution:** **applyChangesFromSnapshot**
```
Vendor → Daily CSV → DLT-META (applyChangesFromSnapshot) → Delta
```

**Why:**
- No control over source system
- Full snapshots provided
- No CDC capability at source
- Automatic delete detection needed

### Scenario 3: Internal Delta Pipeline

**Requirements:**
- Source: Delta table (upstream team manages)
- Volume: 2M orders, 50K updates/day
- Latency: < 10 minutes
- Need: Track order status changes

**Solution:** **Delta CDF**
```
Upstream Delta (CDF enabled) → DLT-META (readChangeFeed) → Target Delta
```

**Why:**
- Source is already Delta
- CDF easy to enable
- Low overhead
- Native Databricks integration
- Incremental and efficient

### Scenario 4: Legacy Mainframe

**Requirements:**
- Source: Mainframe nightly full extracts
- Volume: 5M accounts, unknown change rate
- Latency: Daily is OK
- Need: Compliance history (7 years)

**Solution:** **applyChangesFromSnapshot**
```
Mainframe → Nightly CSV → DLT-META (applyChangesFromSnapshot) → Delta SCD2
```

**Why:**
- Legacy system, no CDC capability
- Full dumps only option
- Automatic change detection
- SCD Type 2 for compliance

### Scenario 5: Event Streaming Platform

**Requirements:**
- Source: Kafka CDC topics (from DMS)
- Volume: 100K events/second
- Latency: Real-time
- Need: Keep full event history

**Solution:** **cdcApplyChanges** with Kafka
```
Database → DMS CDC → Kafka → DLT-META (cdcApplyChanges) → Delta
```

**Why:**
- Real-time streaming required
- CDC events have operation markers
- High throughput
- Event-driven architecture

---

## Hybrid Approaches

### Approach 1: Initial Load + Incremental CDC

**Scenario:** Large table, need to backfill history then switch to CDC

```python
# Phase 1: Historical backfill (one-time)
{
  "source_format": "snapshot",
  "landing_apply_changes_from_snapshot": {
    "keys": ["id"],
    "scd_type": "2"
  }
}

# Phase 2: Switch to streaming CDC (ongoing)
{
  "source_format": "kafka",
  "refinery_cdc_apply_changes": {
    "keys": ["id"],
    "sequence_by": "ts_ms",
    "scd_type": "2"
  }
}
```

### Approach 2: Multi-Source Merge

**Scenario:** Changes come from multiple sources

```json
{
  "landing_database_prod": "landing",
  "landing_table": "customers",

  // Main source: Kafka CDC
  "source_format": "kafka",

  // Additional source: Daily snapshots (append flow)
  "landing_append_flows": [
    {
      "name": "customers_snapshot_flow",
      "source_format": "cloudFiles",
      "source_details": {
        "source_path_prod": "/snapshots/customers/"
      }
    }
  ],

  // Apply changes from both sources
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "update_timestamp",
    "scd_type": "2"
  }
}
```

---

## Configuration Cheat Sheet

### cdcApplyChanges Configuration

```json
{
  "source_format": "kafka",  // or cloudFiles, eventhub
  "refinery_cdc_apply_changes": {
    "keys": ["id"],
    "sequence_by": "timestamp",
    "scd_type": "2",
    "apply_as_deletes": "op = 'D'",
    "apply_as_truncates": "op = 'T'",
    "except_column_list": ["op", "timestamp"],
    "track_history_except_column_list": ["metadata"]
  }
}
```

**Key Fields:**
- `keys` - Primary key(s)
- `sequence_by` - Timestamp or version for ordering
- `scd_type` - "1" or "2"
- `apply_as_deletes` - Expression to identify deletes
- `except_column_list` - Exclude CDC metadata columns

### applyChangesFromSnapshot Configuration

```json
{
  "source_format": "snapshot",
  "source_details": {
    "source_path_prod": "/snapshots/customers/",
    "snapshot_format": "csv"
  },
  "landing_apply_changes_from_snapshot": {
    "keys": ["customer_id"],
    "scd_type": "2",
    "track_history_column_list": ["email", "tier"]
  }
}
```

**Key Fields:**
- `source_format` - Must be "snapshot"
- `keys` - Primary key(s)
- `scd_type` - "1" or "2"
- `track_history_column_list` - Columns that trigger history

### Delta CDF Configuration

```json
{
  "source_format": "delta",
  "source_details": {
    "catalog": "upstream",
    "database": "source",
    "table": "customers"  // Must have CDF enabled
  },
  "landing_reader_options": {
    "readChangeFeed": "true",
    "startingVersion": "latest"
  },
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "_commit_timestamp",
    "scd_type": "2",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"]
  }
}
```

**Key Fields:**
- `source_format` - Must be "delta"
- `readChangeFeed` - Must be "true"
- `startingVersion` - "latest" or specific version
- Map `_change_type` to delete operation

---

## Best Practices by Method

### cdcApplyChanges Best Practices

1. ✅ **Validate operation values** - Ensure only valid operations
2. ✅ **Use timestamp for sequence_by** - Better than auto-increment
3. ✅ **Handle out-of-order** - Use `ignore_null_updates`
4. ✅ **Exclude metadata** - Use `except_column_list`
5. ✅ **Test deletes** - Verify `apply_as_deletes` works
6. ✅ **Monitor lag** - Track CDC delay from source

### applyChangesFromSnapshot Best Practices

1. ✅ **Consistent snapshot timing** - Same time each day
2. ✅ **Validate completeness** - Ensure snapshots are complete
3. ✅ **Use efficient formats** - Prefer Parquet over CSV
4. ✅ **Selective history** - Use `track_history_except_column_list`
5. ✅ **Monitor snapshot size** - Alert on anomalies
6. ✅ **Test delete detection** - Verify missing records handled

### Delta CDF Best Practices

1. ✅ **Enable CDF on source** - Before first write
2. ✅ **Set retention** - Balance recovery vs cost
3. ✅ **Use commit_timestamp** - Better than commit_version
4. ✅ **Filter preimages** - If not needed
5. ✅ **Vacuum regularly** - Clean old CDF data
6. ✅ **Enable on landing** - For downstream consumption

---

## Troubleshooting

### Common Issues

#### cdcApplyChanges

| Issue | Cause | Solution |
|-------|-------|----------|
| Out-of-order changes | Network delay, clock skew | Use `ignore_null_updates` |
| Missing deletes | Wrong `apply_as_deletes` expression | Verify expression matches data |
| Duplicate keys | Multiple updates same sequence_by | Use composite sequence_by |

#### applyChangesFromSnapshot

| Issue | Cause | Solution |
|-------|-------|----------|
| No changes detected | Keys don't match | Verify key columns identical |
| Too many updates | Metadata columns changing | Add to `track_history_except_column_list` |
| Missing snapshots | Source not available | Add monitoring, retry logic |

#### Delta CDF

| Issue | Cause | Solution |
|-------|-------|----------|
| CDF not available | Not enabled before writes | Enable, accept no historical changes |
| Storage growing | Long retention | Vacuum more frequently |
| Missing changes | Read from wrong version | Check `startingVersion` parameter |

---

## Migration Paths

### From Snapshot to CDC

**Current:** Using `applyChangesFromSnapshot`
**Goal:** Switch to real-time CDC

```
Phase 1: Baseline with snapshots
├─ Use applyChangesFromSnapshot
└─ Build target SCD2 tables

Phase 2: Parallel CDC testing
├─ Set up Debezium/DMS
├─ Run cdcApplyChanges in parallel
└─ Validate outputs match

Phase 3: Cutover
├─ Stop snapshot pipeline
├─ Switch to CDC pipeline
└─ Monitor for issues
```

### From CDC to Delta CDF

**Current:** Using `cdcApplyChanges` with Kafka
**Goal:** Simplify with Delta CDF

```
Phase 1: Enable CDF on landing
├─ Alter landing tables to enable CDF
└─ Continue CDC from Kafka to landing

Phase 2: Switch refinery source
├─ Update refinery to read from landing CDF
├─ Remove Kafka dependency for refinery
└─ Test thoroughly

Phase 3: Cleanup
├─ Simplify architecture
└─ Decommission if possible
```

---

## Summary

### Choose cdcApplyChanges When:
- ✅ Real-time streaming required
- ✅ Source has native CDC (Debezium, DMS)
- ✅ Kafka/Event Hub infrastructure exists
- ✅ Need sub-minute latency
- ✅ Large tables with small change rate

### Choose applyChangesFromSnapshot When:
- ✅ Source provides full table exports
- ✅ Legacy systems without CDC
- ✅ Third-party vendor data
- ✅ Batch processing acceptable
- ✅ Need automatic delete detection
- ✅ Small to medium tables (< 10M rows)

### Choose Delta CDF When:
- ✅ Source is Delta table
- ✅ Working within Databricks
- ✅ Can enable CDF on source
- ✅ Need incremental efficiency
- ✅ Want automatic change tracking
- ✅ Delta-to-Delta pipelines

---

## Quick Reference

```
Real-time Database CDC → cdcApplyChanges
Daily CSV Exports → applyChangesFromSnapshot
Delta Table with CDF → Delta CDF

Need history? → Use SCD Type 2 with any method
Only current state? → Use SCD Type 1 with any method
Very large tables? → Avoid applyChangesFromSnapshot

Within Databricks? → Prefer Delta CDF
External sources? → Use cdcApplyChanges or applyChangesFromSnapshot
```

---

## See Also

- [cdcApplyChanges Guide](CDC_CONFIGURATION.md)
- [applyChangesFromSnapshot Guide](CDC_APPLY_CHANGES_FROM_SNAPSHOT.md)
- [Delta CDF Guide](DELTA_CHANGE_DATA_FEED.md)
- [Onboarding Reference](ONBOARDING_FILE_REFERENCE.md)
