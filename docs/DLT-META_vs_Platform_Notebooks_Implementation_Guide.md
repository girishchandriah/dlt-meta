# DLT-META vs Platform Notebooks Implementation Guide

**Version:** 2.1 (Corrected with Organizational Guidance)
**Date:** February 23, 2025
**Author:** Platform Data Engineering Team

> **⚠️ CRITICAL CLARIFICATION:**
>
> This document compares two **FUNDAMENTALLY DIFFERENT** technologies:
>
> 1. **DLT-META** = Delta Live Tables (DLT) framework-based pipelines
> 2. **Platform Notebooks** = Spark Streaming + Delta Lake (NOT DLT)
>
> Platform Notebooks **DO NOT** use Delta Live Tables. They use standard Spark Streaming APIs with Delta Lake merge operations.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Organizational Recommendation](#organizational-recommendation)
3. [DLT Core Requirements and Constraints](#dlt-core-requirements-and-constraints)
4. [Architecture Comparison](#architecture-comparison)
5. [Implementation Approaches](#implementation-approaches)
6. [DLT-META Implementation Guide](#dlt-meta-implementation-guide)
7. [DLT-META Bottlenecks and Workarounds](#dlt-meta-bottlenecks-and-workarounds)
8. [Thought Process Framework](#thought-process-framework)
9. [Comparative Analysis](#comparative-analysis)
10. [References](#references)

---

## Executive Summary

This document compares two approaches for implementing data pipelines at LiveNation:

1. **DLT-META Framework** - Configuration-driven framework using Delta Live Tables (DLT) with JSON-based dataflow specifications
2. **Platform Notebooks** - YAML-configured Spark Streaming pipelines using Delta Lake merge operations

### Key Differences

| Aspect | DLT-META | Platform Notebooks |
|--------|----------|-------------------|
| **Technology** | Delta Live Tables (DLT) | Spark Streaming + Delta Lake |
| **Configuration** | JSON-driven dataflow specs | YAML-driven service configs |
| **Pipeline Definition** | DLT decorators (@dlt.table) | Spark readStream/writeStream |
| **Merge Operations** | dlt.apply_changes (CDC) | DeltaTable.merge() |
| **Reusability** | High (framework handles patterns) | High (reusable notebook services) |
| **Learning Curve** | Steep (DLT + framework concepts) | Moderate (Spark Streaming + Delta) |
| **Flexibility** | Constrained by DLT framework | Full Spark control |
| **Maintenance** | Centralized (framework updates) | Service-based (update notebook services) |

### Organizational Recommendation

**✅ DLT-META is the RECOMMENDED approach for new data pipelines at LiveNation.**

While both approaches are technically sound, this document recommends **DLT-META** as the organizational standard. See [Organizational Recommendation](#organizational-recommendation) for details.

---

## Organizational Recommendation

### 🎯 Standard Approach: DLT-META Framework

**For new data pipeline implementations, teams SHOULD use DLT-META unless there are compelling technical reasons to use an alternative approach.**

### Why DLT-META is the Organizational Standard

#### 1. **Low Organizational Adoption of Platform Notebooks**

Platform Notebooks, while technically capable, have **limited adoption** across LiveNation teams:

- **Few teams** are actively using Platform Notebooks for new implementations
- Most teams are unfamiliar with the Platform Notebooks service pattern
- Limited organizational knowledge base and tribal knowledge
- Fewer internal resources available for support and troubleshooting

#### 2. **Non-Standard Implementations**

Among teams that have adopted Platform Notebooks, implementations vary significantly:

- **Inconsistent configuration patterns** - Different YAML structures across teams
- **Custom notebook modifications** - Teams create modified versions of services
- **Lack of standardization** - No enforced patterns or best practices
- **Technical debt accumulation** - Custom implementations become hard to maintain
- **Knowledge silos** - Each team's implementation requires specialized knowledge

#### 3. **DLT-META Advantages for Organizations**

DLT-META provides organizational benefits beyond technical capabilities:

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Standardization** | All pipelines follow same JSON/YAML pattern | Easy to onboard new team members |
| **Consistency** | Framework enforces consistent structure | Reduced technical debt |
| **Documentation** | Well-documented framework with examples | Lower learning curve |
| **Community Support** | Active Databricks Labs community | Better external resources |
| **Maintainability** | Centralized framework updates benefit all pipelines | Easier to scale |
| **Governance** | Declarative configs easier to audit and approve | Better compliance |

#### 4. **When to Deviate from Standard**

You MAY use Platform Notebooks (or other approaches) when:

- **Self-referencing is critical** - Business logic requires reading target table
- **Heavy Python processing** - Complex transformations beyond SQL
- **Existing Platform ecosystem** - Already deeply integrated with Platform services
- **CCPA-specific features** - Require merge_pii() functionality
- **Executive approval** - Technical leadership approves deviation with justification

**Process for Deviation:**
1. Document technical justification
2. Get approval from data engineering leadership
3. Ensure team has expertise in chosen approach
4. Plan for long-term maintenance

### Migration Strategy for Existing Pipelines

**For teams currently using Platform Notebooks:**

- **Don't panic-migrate** - Existing pipelines can continue running
- **New pipelines** - Use DLT-META for new work
- **Major refactors** - Consider migrating to DLT-META when doing major updates
- **Evaluate case-by-case** - Some pipelines may have valid reasons to stay

---

## DLT Core Requirements and Constraints

> **⚠️ IMPORTANT:** This section applies **ONLY to DLT-META** which uses the Delta Live Tables framework. Platform Notebooks use standard Spark Streaming and are **NOT** subject to these DLT constraints.

### 1. Table Reference Rules

#### ❌ PROHIBITED: Self-Referencing
```python
# INVALID - Cannot reference the table being written to
@dlt.table(name="my_table")
def my_table():
    return spark.sql("""
        SELECT * FROM LIVE.my_table  -- ERROR: Self-reference
        WHERE condition
    """)
```

**Why:** DLT manages the pipeline DAG and cannot resolve circular dependencies.

#### ❌ PROHIBITED: Downstream Table References
```python
# INVALID - Cannot reference tables downstream in the DAG
@dlt.table(name="bronze_table")
def bronze():
    return spark.sql("""
        SELECT * FROM LIVE.silver_table  -- ERROR: silver doesn't exist yet
    """)

@dlt.table(name="silver_table")
def silver():
    return dlt.read("bronze_table")
```

#### ✅ ALLOWED: Upstream and External References
```python
# VALID - Reference upstream DLT tables
@dlt.table(name="silver_table")
def silver():
    return dlt.read("bronze_table")  # bronze is upstream

# VALID - Reference external tables
@dlt.table(name="enriched")
def enriched():
    landing_df = dlt.read("landing_table")
    return landing_df.join(
        spark.table("external_catalog.schema.dimension_table"),
        on="key"
    )
```

### 2. Streaming vs Batch Modes

#### Streaming Mode
- Used for continuous data ingestion
- Requires `readStream` for sources
- CDC `apply_changes` works in streaming mode

```python
@dlt.table(name="streaming_table")
def streaming():
    return spark.readStream.table("source_table")
```

#### Batch Mode
- Used for periodic processing
- Uses `read` for sources
- Better for data quality/quarantine checks

```python
@dlt.table(name="batch_table")
def batch():
    return spark.read.table("source_table")
```

### 3. CDC Apply Changes Constraints

```python
dlt.apply_changes(
    target="target_table",
    source="source_table",
    keys=["id"],                    # Merge keys
    sequence_by="updated_at",       # Determines record ordering
    stored_as_scd_type="1"          # SCD Type 1 or 2
)
```

**Critical Rules:**
- ❌ Cannot read from the target table in transformations
- ✅ CDC automatically handles INSERT/UPDATE/DELETE based on keys
- ✅ Sequence_by determines which record wins on conflict

### 4. View Naming Convention

DLT creates views based on data_flow_id:
- `source_{data_flow_id}` - DLT-managed input view
- `LIVE.{table_name}` - Reference to DLT tables in same pipeline

---

## Architecture Comparison

### Architecture 1: Platform Notebooks (Spark Streaming)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Reusable Notebook Services                    │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Service: stream_delta_transformation.py                 │  │
│  │  - Reads YAML configuration via dbutils.widgets         │  │
│  │  - Uses spark.readStream from source Delta table        │  │
│  │  - Applies SQL transformations                          │  │
│  │  - Uses DeltaTable.merge() for upserts                  │  │
│  │  - Includes merge_pii() for CCPA compliance             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Service: batch_delta_transformation.py                  │  │
│  │  - Batch processing version                              │  │
│  │  - Uses spark.read for batch sources                     │  │
│  │  - Same merge logic as streaming                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Service: refinery_from_json_merge_v1.py                │  │
│  │  - JSON data processing                                  │  │
│  │  - Streaming merge to refinery layer                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│              YAML Configuration (per pipeline)                   │
│  {                                                               │
│    "source_db": "landing_nonprod",                              │
│    "source_table": "source_table_name",                         │
│    "target_db": "treasury_nonprod",                             │
│    "target_table": "target_table_name",                         │
│    "id_column": "primary_key",                                  │
│    "transformation_sql": "SELECT ...",                          │
│    "merge_condition": "source.id = target.id"                   │
│  }                                                               │
└─────────────────────────────────────────────────────────────────┘

Characteristics:
- YAML-configured Spark Streaming
- Reusable notebook services (not DLT)
- Uses DeltaTable.merge() for CDC operations
- Standard Spark readStream/writeStream
- Full Spark DataFrame API control
- NOT using Delta Live Tables framework
```

### Architecture 2: DLT-META Framework

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DLT-META Framework                               │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  JSON Configuration (Onboarding Spec)                          │    │
│  │  {                                                              │    │
│  │    "data_flow_id": "204",                                       │    │
│  │    "source_format": "cloudFiles",                              │    │
│  │    "landing_table": "...",                                      │    │
│  │    "treasury_table": "...",                                     │    │
│  │    "treasury_transformation_json": "transformations.yaml",      │    │
│  │    "treasury_cdc_apply_changes": {...}                          │    │
│  │  }                                                              │    │
│  └────────────────────────┬───────────────────────────────────────┘    │
│                           │                                             │
│                           ▼                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  DLT-META Engine (Python Framework)                            │    │
│  │  - Reads JSON specs from onboarding table                      │    │
│  │  - Generates DLT tables dynamically                            │    │
│  │  - Manages CDC configurations                                  │    │
│  │  - Applies transformations from YAML                           │    │
│  └────────────────────────┬───────────────────────────────────────┘    │
│                           │                                             │
│                           ▼                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Generated DLT Pipeline                                         │    │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐                 │    │
│  │  │ Landing  │───▶│ Refinery │───▶│ Treasury │                 │    │
│  │  │  (Auto)  │    │  (Auto)  │    │  (Auto)  │                 │    │
│  │  └──────────┘    └──────────┘    └──────────┘                 │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘

Characteristics:
- Configuration-driven (JSON + YAML)
- Framework generates DLT pipeline code
- Reusable patterns (CDC, transformations, data quality)
- Standardized across all pipelines
```

---

## Implementation Approaches

### Sales Order Transaction Pipeline - Concrete Example

**Business Requirement:**
- Ingest sales order transactions from S3 (CSV files)
- Apply transformations (date conversions, void handling, operator type lookup)
- Join with sales_ord dimension to get sales_ord_id
- Write to treasury with CDC (handle updates for void transactions)
- Quarantine records without sales_ord_id

---

### Approach 1: Platform Notebooks Implementation

> **⚠️ IMPORTANT:** Platform Notebooks use **Spark Streaming + Delta Lake**, NOT Delta Live Tables (DLT).

#### File Structure
```
platform_notebooks/
├── notebook/
│   ├── stream_delta_transformation.py     # Main streaming service
│   ├── batch_delta_transformation.py      # Batch processing service
│   ├── refinery_from_json_merge_v1.py    # Refinery layer service
│   └── delta_from_s3.py                  # S3 ingestion service
└── configs/                               # YAML configurations per pipeline
    └── sales_ord_tran_config.yaml        # Example config
```

#### Implementation Pattern

**Step 1: Create YAML Configuration**

**File: `configs/sales_ord_tran_config.yaml`**

```yaml
# Configuration for sales order transaction pipeline
source_db: landing_nonprod
source_table: pfocusst_ff_src_hst_dsn_trans
target_db: treasury_nonprod
target_table: pfocusdb_sales_ord_tran
id_column: sales_ord_tran_id
merge_condition: |
  source.sales_ord_id = target.sales_ord_id AND
  source.event_id = target.event_id AND
  source.sales_ord_tran_id = target.sales_ord_tran_id AND
  source.sales_ord_tran_dt = target.sales_ord_tran_dt

transformation_sql: |
  SELECT
    CASE WHEN system = 'AU1' THEN 'AUS' ELSE UPPER(system) END as system,
    -- Void handling: zero out amounts for void transactions
    CASE WHEN TRIM(UPPER(voidflag)) = 'T' THEN 0.0000 ELSE facilityfee END as facility_fee,
    -- ... other transformations
    o.sales_ord_id
  FROM source s
  LEFT JOIN treasury.pfocusdb_sales_ord o
    ON s.system = o.host_sys_cd
    AND s.acctdate = o.host_acct_create_dt
  WHERE o.sales_ord_id IS NOT NULL

trigger: once  # or "processingTime='10 minutes'" for continuous
checkpoint_location: /dbfs/checkpoints/sales_ord_tran
```

**Step 2: Use Reusable Notebook Service**

The platform provides reusable notebook services. You **call** these services with your YAML config:

**Pseudocode for how it works (simplified):**

```python
# This is inside stream_delta_transformation.py (the reusable service)
import yaml
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Get config from widget parameter (compressed YAML)
param_yaml = yaml.load(dbutils.widgets.get("param_yaml"))

# Read source as stream
source_df = (
    spark.readStream
    .format("delta")
    .table(f"{param_yaml['source_db']}.{param_yaml['source_table']}")
)

# Apply SQL transformation
source_df.createOrReplaceTempView("source")
transformed_df = spark.sql(param_yaml['transformation_sql'])

# Define merge function
def merge_to_target(batch_df, batch_id):
    target_table = f"{param_yaml['target_db']}.{param_yaml['target_table']}"

    # Get existing Delta table
    delta_table = DeltaTable.forName(spark, target_table)

    # Perform merge (upsert)
    delta_table.alias("target").merge(
        batch_df.alias("source"),
        param_yaml['merge_condition']
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

    # CCPA privacy compliance (if configured)
    if param_yaml.get('enable_pii_merge'):
        merge_pii(param_yaml['target_db'], param_yaml['target_table'], param_yaml['env_suffix'])

# Execute streaming merge
(transformed_df.writeStream
    .foreachBatch(merge_to_target)
    .option("checkpointLocation", param_yaml['checkpoint_location'])
    .trigger(**param_yaml['trigger'])
    .start()
    .awaitTermination())
```

**Key Implementation Details:**

1. **No DLT decorators** - Uses standard Spark Streaming
2. **DeltaTable.merge()** - Manual CDC implementation (not dlt.apply_changes)
3. **Can self-reference** - Unlike DLT, you can read from the target table if needed
4. **CCPA integration** - Built-in merge_pii() function for privacy compliance
5. **Checkpoint management** - Manual checkpoint location specification

**Pros:**
- ✅ Full Spark DataFrame API control
- ✅ Can self-reference target tables (no DLT constraints)
- ✅ Reusable notebook services (don't write new notebooks per pipeline)
- ✅ YAML configuration per pipeline
- ✅ Built-in CCPA privacy features
- ✅ Easy to debug (standard Spark code)

**Cons:**
- ❌ Manual merge logic (no automatic CDC like DLT)
- ❌ Manual checkpoint management
- ❌ No built-in data quality checks (must implement manually)
- ❌ No automatic dependency resolution (manual orchestration via Control Panel)

---

### Approach 2: DLT-META Implementation

#### File Structure
```
dlt-meta/
├── cds/
│   └── conf/
│       └── onboarding/
│           ├── host_sales_ord_tran_0204/
│           │   └── onboarding_sales_ord_tran_landing_treasury.json
│           └── transformations/
│               ├── transformations_sales_ord_combined_treasury_nonprod.yaml
│               ├── transformations_sales_ord_quarantine_nonprod.yaml
│               └── ...
└── src/
    ├── dataflow_pipeline.py      # Framework engine
    ├── onboard_dataflowspec.py   # JSON processor
    └── ...
```

#### Configuration Files

**File: `onboarding_sales_ord_tran_landing_treasury.json`**

```json
[
  {
    "data_flow_id": "204",
    "data_flow_group": "sales_ord",
    "source_system": "jb_edw",
    "source_format": "cloudFiles",
    "source_details": {
      "source_database": "hostfile",
      "source_table": "dsn_sales_ord_tran",
      "source_path_nonprod": "s3://bucket/path/inbound",
      "source_schema_path": "/Volumes/.../dsn_sales_ord_tran.ddl"
    },
    "landing_catalog_nonprod": "dataservices_nonprod",
    "landing_database_nonprod": "landing_nonprod",
    "landing_table": "pfocusst_ff_src_hst_dsn_trans_dlt_v2",
    "landing_reader_options": {
      "cloudFiles.format": "csv",
      "header": "false",
      "delimiter": "|"
    },
    "treasury_catalog_nonprod": "dataservices_nonprod",
    "treasury_database_nonprod": "treasury_teradata_base_nonprod",
    "treasury_table": "pfocusdb_sales_ord_tran_dlt_v2",
    "treasury_transformation_json_nonprod": "/Volumes/.../transformations_sales_ord_combined_treasury_nonprod.yaml",
    "treasury_reader_options": {
      "readChangeFeed": "true"
    },
    "treasury_cdc_apply_changes": {
      "keys": ["sales_ord_id", "event_id", "sales_ord_tran_id", "sales_ord_tran_dt"],
      "sequence_by": "host_acct_create_dt",
      "scd_type": "1"
    },
    "version": "v3.0"
  },
  {
    "data_flow_id": "205",
    "source_format": "delta",
    "source_details": {
      "source_catalog_nonprod": "dataservices_nonprod",
      "source_database_nonprod": "landing_nonprod",
      "source_table_ref": "pfocusst_ff_src_hst_dsn_trans_dlt_v2"
    },
    "refinery_catalog_nonprod": "dataservices_nonprod",
    "refinery_database_nonprod": "refinery_nonprod",
    "refinery_table": "pfocusst_dsn_st_trans_quarantine_dlt_v2",
    "refinery_transformation_json_nonprod": "/Volumes/.../transformations_sales_ord_quarantine_nonprod.yaml",
    "refinery_reader_options": {
      "readChangeFeed": "true"
    },
    "refinery_cdc_apply_changes": {
      "keys": ["host_sys_cd", "host_vax_acct_num", "host_acct_create_dt", "event_id"],
      "sequence_by": "quarantine_ts",
      "scd_type": "1"
    },
    "version": "v3.0"
  }
]
```

**File: `transformations_sales_ord_combined_treasury_nonprod.yaml`**

```yaml
target_table: pfocusdb_sales_ord_tran_dlt_v2
transformation_name: Sales Order Transaction Combined Treasury Transformation
sql_query: |
  WITH q_extract AS (
    SELECT
      CASE
        WHEN f.system = 'AU1' THEN 'AUS'
        ELSE UPPER(f.system)
      END as system,
      -- Void handling: zero out amounts for void transactions
      CASE
        WHEN TRIM(UPPER(f.voidflag)) = 'T' THEN 0.0000
        ELSE f.facilityfee
      END as facility_fee,
      -- ... other transformations
    FROM source_204 f  -- DLT-managed view
  ),
  query AS (
    SELECT *,
      cdsudf_getoptype(vcode, system, acctdate) as void_optype
    FROM q_extract
  ),
  enriched_data AS (
    SELECT q.*, o.sales_ord_id
    FROM query q
    LEFT JOIN treasury.pfocusdb_sales_ord o
      ON q.system = o.host_sys_cd
      AND q.acctdate = o.host_acct_create_dt
    WHERE o.sales_ord_id IS NOT NULL
  )
  SELECT * FROM enriched_data
```

**Pros:**
- ✅ Configuration-driven (no coding for standard patterns)
- ✅ Standardized across all pipelines
- ✅ Framework handles CDC, data quality, quarantine patterns
- ✅ Centralized updates (fix framework once, all pipelines benefit)
- ✅ Easier onboarding (fill JSON, write SQL)

**Cons:**
- ❌ Steep learning curve (framework concepts)
- ❌ Limited flexibility (constrained by framework)
- ❌ Debugging harder (multiple layers of abstraction)
- ❌ Must understand both DLT + DLT-META

---

## DLT-META Implementation Guide

### Step-by-Step Process

#### Phase 1: Requirements Analysis

**Questions to Answer:**

1. **Data Sources**
   - Where is the data coming from? (S3, Kafka, JDBC, existing Delta table)
   - What format? (CSV, JSON, Parquet, Delta)
   - Streaming or batch?

2. **Layers Needed**
   - Landing only?
   - Landing → Refinery → Treasury?
   - Skip refinery (direct to treasury)?

3. **Transformations**
   - What business logic needs to be applied?
   - Any lookups/joins required?
   - Date conversions, data type changes?

4. **CDC Requirements**
   - What are the primary keys?
   - What column determines record order (sequence_by)?
   - SCD Type 1 or 2?

5. **Data Quality**
   - Any quarantine/validation rules?
   - What makes a record "bad"?

#### Phase 2: Design Decisions

**Decision Tree:**

```
┌─────────────────────────────────────────────────────────────────┐
│ Can I use existing sales_ord_id from dimension table?           │
│                                                                  │
│  YES                           NO                                │
│   │                             │                                │
│   ▼                             ▼                                │
│ Direct to Treasury          Need to generate                    │
│ (Flow 204 pattern)          surrogate key                       │
│                             (Flow 210 pattern)                   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Do I need to handle void/update logic?                          │
│                                                                  │
│  YES                           NO                                │
│   │                             │                                │
│   ▼                             ▼                                │
│ ❌ CANNOT self-join          Simple INSERT-only                 │
│ ✅ Use CDC with zeroed       Use append or CDC                  │
│    amounts in transformation                                     │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Are there records that should be quarantined?                   │
│                                                                  │
│  YES                           NO                                │
│   │                             │                                │
│   ▼                             ▼                                │
│ Create separate dataflow     Single dataflow                    │
│ (Flow 205 pattern)           (Flow 204 only)                    │
└─────────────────────────────────────────────────────────────────┘
```

#### Phase 3: Configuration

**Step 3.1: Create Onboarding JSON**

```json
{
  "data_flow_id": "XXX",           // Unique ID
  "data_flow_group": "domain",     // Grouping (sales_ord, customer, etc.)
  "source_system": "source_name",  // Source system identifier
  "source_format": "cloudFiles|delta|snapshot",

  // Source configuration
  "source_details": {
    "source_database": "...",
    "source_table": "...",
    "source_path_nonprod": "s3://...",
    "source_schema_path": "/Volumes/..."
  },

  // Target layer configuration
  "treasury_catalog_nonprod": "dataservices_nonprod",
  "treasury_database_nonprod": "treasury_teradata_base_nonprod",
  "treasury_table": "target_table_name",

  // Transformation reference
  "treasury_transformation_json_nonprod": "/Volumes/.../transformations.yaml",

  // CDC configuration
  "treasury_cdc_apply_changes": {
    "keys": ["key1", "key2"],      // Primary keys
    "sequence_by": "update_column", // Determines record order
    "scd_type": "1"                 // Type 1 or 2
  },

  "version": "v3.0"
}
```

**Step 3.2: Write Transformation YAML**

```yaml
target_table: target_table_name
transformation_name: Descriptive Name
description: "What this transformation does"
sql_query: |
  -- Use source_{data_flow_id} as the DLT-managed view
  WITH step1 AS (
    SELECT *
    FROM source_XXX  -- DLT creates this automatically
  ),
  step2 AS (
    -- Apply transformations
    SELECT * FROM step1
  )
  SELECT * FROM step2
```

**Step 3.3: Deploy**

```bash
# Deploy to nonprod
dltmeta deploy \
  --onboard_layer treasury \
  --onboard_file_path /path/to/onboarding.json \
  --env nonprod

# Trigger pipeline
databricks jobs run-now --job-id XXXXX
```

#### Phase 4: Testing

**Validation Checklist:**

- [ ] Pipeline creates all expected tables
- [ ] Source view `source_{data_flow_id}` exists
- [ ] Data flows through landing → treasury
- [ ] CDC merge works correctly (check for duplicates)
- [ ] Transformations produce expected results
- [ ] Quarantine captures bad records
- [ ] Performance is acceptable

---

## DLT-META Bottlenecks and Workarounds

While DLT-META is the recommended organizational standard, it has some limitations due to DLT framework constraints. This section provides practical workarounds.

### Bottleneck 1: Self-Referencing Prohibition

**Problem:** DLT does not allow reading from the target table during transformation.

**Common Use Case:** Void transaction handling that requires comparing incoming records with existing records.

#### ❌ What You CANNOT Do

```sql
-- INVALID in DLT-META
SELECT
  incoming.*,
  CASE
    WHEN incoming.void_flag = 'T' THEN existing.amount * -1
    ELSE incoming.amount
  END as final_amount
FROM source_204 incoming
LEFT JOIN LIVE.target_table existing  -- ERROR: Cannot self-reference
  ON incoming.key = existing.key
```

#### ✅ Workaround 1: Zero Amounts in Transformation (RECOMMENDED)

**Approach:** Transform void transactions to have zero amounts, then use CDC to merge.

```yaml
# Transformation YAML
sql_query: |
  SELECT
    transaction_id,
    -- Zero out amounts for voids
    CASE
      WHEN void_flag = 'T' THEN 0.0000
      ELSE amount
    END as amount,
    void_flag,
    transaction_date
  FROM source_204
```

```json
// CDC handles the merge
"treasury_cdc_apply_changes": {
  "keys": ["transaction_id"],
  "sequence_by": "transaction_date",
  "scd_type": "1"
}
```

**How it works:**
1. Original transaction: `amount=100, void_flag='F'` → INSERT
2. Void transaction: `amount=0, void_flag='T'` → UPDATE (overwrites)
3. CDC automatically handles the merge based on keys

**Pros:** Simple, works within DLT constraints
**Cons:** Cannot do complex calculations involving existing values

#### ✅ Workaround 2: External Reference Table

**Approach:** Use a separate reference table outside the DLT pipeline.

```sql
-- Step 1: Create external reference table (batch job, outside DLT)
CREATE OR REPLACE TABLE external_catalog.reference.transaction_state AS
SELECT transaction_id, amount, transaction_date
FROM treasury.target_table;

-- Step 2: Join with external reference in DLT transformation
sql_query: |
  SELECT
    incoming.*,
    CASE
      WHEN incoming.void_flag = 'T' THEN ref.amount * -1
      ELSE incoming.amount
    END as final_amount
  FROM source_204 incoming
  LEFT JOIN external_catalog.reference.transaction_state ref
    ON incoming.transaction_id = ref.transaction_id
```

**Pros:** Can access previous state for calculations
**Cons:**
- Requires separate batch job to update reference table
- Reference table may be stale
- More complex architecture

#### ✅ Workaround 3: Two-Stage Pipeline

**Approach:** Split into two DLT pipelines - one for initial load, one for processing.

```json
// Pipeline 1: Land data to staging
{
  "data_flow_id": "301",
  "landing_table": "staging_transactions",
  "treasury_table": "staging_transactions_processed"
}

// Pipeline 2: Process from staging (can read staging as external)
{
  "data_flow_id": "302",
  "source_format": "delta",
  "source_details": {
    "source_table_ref": "staging_transactions_processed"
  },
  "treasury_table": "final_transactions"
}
```

**Pros:** Clean separation, can reference staging table
**Cons:** More complex, two pipelines to manage

### Bottleneck 2: Limited Python Transformations

**Problem:** DLT-META is optimized for SQL transformations. Complex Python logic is harder to implement.

#### ✅ Workaround: SQL UDFs

**Approach:** Register Python functions as SQL UDFs.

```python
# Register UDF in init script or notebook
spark.udf.register("custom_transform", custom_transform_function)
```

```yaml
# Use in transformation YAML
sql_query: |
  SELECT
    transaction_id,
    custom_transform(raw_data) as transformed_data
  FROM source_204
```

**Pros:** Can use Python within SQL
**Cons:** UDFs can have performance overhead

### Bottleneck 3: Complex Joins with Multiple Dimensions

**Problem:** Need to join with multiple external dimension tables.

#### ✅ Workaround: Pre-Join Dimensions

**Approach:** Create a denormalized dimension view outside DLT.

```sql
-- Create consolidated dimension (outside DLT)
CREATE OR REPLACE VIEW external_catalog.dimensions.sales_ord_enriched AS
SELECT
  so.*,
  e.event_name,
  v.venue_name,
  p.performer_name
FROM treasury.sales_ord so
LEFT JOIN treasury.event e ON so.event_id = e.event_id
LEFT JOIN treasury.venue v ON e.venue_id = v.venue_id
LEFT JOIN treasury.performer p ON e.performer_id = p.performer_id;
```

```yaml
# DLT transformation uses consolidated view
sql_query: |
  SELECT
    t.*,
    d.event_name,
    d.venue_name,
    d.performer_name
  FROM source_204 t
  LEFT JOIN external_catalog.dimensions.sales_ord_enriched d
    ON t.sales_ord_id = d.sales_ord_id
```

**Pros:** Simpler transformation SQL, better performance
**Cons:** Requires maintaining dimension view

### Bottleneck 4: Debugging DLT Pipelines

**Problem:** DLT error messages can be cryptic with multiple layers of abstraction.

#### ✅ Workaround: Staged Validation

**Approach:** Test transformations incrementally.

```python
# Test transformation SQL directly in notebook before deploying
test_df = spark.sql("""
  -- Copy SQL from transformation YAML
  SELECT * FROM your_source_table
""")

test_df.display()  # Verify results
test_df.printSchema()  # Check schema
```

**Best Practices:**
- Start with simple SELECT * and add transformations incrementally
- Test SQL in Databricks SQL Editor first
- Use EXPLAIN to check query plan
- Check DLT event logs: `SELECT * FROM event_log(TABLE(your_target_table))`

### Bottleneck 5: Schema Evolution

**Problem:** Handling schema changes in source data.

#### ✅ Workaround: Schema Merge + Rescue Data Column

```json
"landing_reader_options": {
  "cloudFiles.format": "csv",
  "mergeSchema": "true",
  "cloudFiles.schemaEvolutionMode": "rescue",
  "cloudFiles.rescuedDataColumn": "_rescued_data"
}
```

**Then filter bad data to quarantine:**

```yaml
sql_query: |
  SELECT * FROM source_204
  WHERE _rescued_data IS NULL  -- Valid schema
```

```yaml
# Quarantine flow
sql_query: |
  SELECT * FROM source_204
  WHERE _rescued_data IS NOT NULL  -- Schema issues
```

### When Workarounds Are Not Enough

If DLT-META workarounds become too complex or hacky:

1. **Document the complexity** - Write up the technical debt
2. **Evaluate alternatives** - Consider Platform Notebooks or custom solution
3. **Get leadership approval** - Present trade-offs to data engineering leadership
4. **Exception process** - Follow deviation process from organizational standard

**Critical Question:** Is the workaround more complex than using Platform Notebooks directly?

If yes, you may have a valid case for deviation. Document and seek approval.

---

## Thought Process Framework

### When to Use DLT-META vs Platform Notebooks

> **🎯 ORGANIZATIONAL DEFAULT:** Start with DLT-META for all new pipelines. Only deviate with documented justification and leadership approval.

#### Decision Flowchart

```
┌─────────────────────────────────────────────────────────┐
│ Starting new data pipeline implementation               │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
         ┌─────────────────────┐
         │ Is self-referencing │
         │ absolutely required?│
         │ (read target table) │
         └─────────┬───────────┘
                   │
         ┌─────────┴─────────┐
         │                   │
        NO                  YES
         │                   │
         │          ┌────────▼────────┐
         │          │ Try workarounds │
         │          │ (see section 7) │
         │          └────────┬────────┘
         │                   │
         │          ┌────────┴────────┐
         │          │ Workarounds OK? │
         │          └────────┬────────┘
         │                   │
         │          ┌────────┴────────┐
         │          │                 │
         │         YES               NO
         │          │                 │
         ▼          ▼                 ▼
    ┌────────┐ ┌────────┐   ┌──────────────────┐
    │ DLT    │ │ DLT    │   │ Get leadership   │
    │ META   │ │ META   │   │ approval for     │
    │        │ │(use WA)│   │ Platform Notebook│
    └────────┘ └────────┘   └──────────────────┘
```

### ✅ DEFAULT: Use DLT-META

**DLT-META is the organizational standard. Use it unless you have an exception.**

**Why DLT-META is Default:**

1. **Organizational Standardization** - Most teams use this
2. **Better Documentation** - Well-documented with examples
3. **Easier Onboarding** - New team members can ramp up faster
4. **Consistent Patterns** - Framework enforces best practices
5. **Community Support** - Active Databricks Labs community
6. **Centralized Maintenance** - Framework updates benefit all pipelines

**DLT-META Fits Best When:**

- Pipeline follows landing → refinery → treasury pattern
- Transformations are primarily SQL-based
- CDC requirements don't need self-referencing
- Team is learning Databricks ecosystem
- Want automatic dependency management
- Need built-in data quality expectations

### 🚨 EXCEPTION: Use Platform Notebooks Only When...

**You need leadership approval to deviate from the organizational standard.**

**Valid Exceptions:**

1. **Self-Referencing Cannot Be Worked Around**
   - Tried workarounds from [Section 7](#dlt-meta-bottlenecks-and-workarounds)
   - Workarounds are too complex or create technical debt
   - Business logic fundamentally requires reading target table
   - **Example:** Complex reversal logic that requires comparing with 3+ previous transactions

2. **Heavy Python Processing Required**
   - Complex Python transformations beyond SQL UDFs
   - ML model integration that doesn't fit DLT patterns
   - Custom aggregation logic requiring Spark DataFrame API
   - **Example:** Real-time fraud detection with scikit-learn models

3. **CCPA-Specific Implementation**
   - Must use merge_pii() function for compliance
   - Already integrated with Platform CCPA services
   - Part of existing CCPA workflow
   - **Example:** Pipeline is part of CCPA erasure runner

4. **Existing Platform Ecosystem Investment**
   - Team has deep expertise in Platform Notebooks
   - Heavily integrated with Control Panel orchestration
   - Multiple existing pipelines in Platform Notebooks
   - **Example:** Team maintaining 20+ Platform Notebook pipelines

**Exception Approval Process:**

1. Document technical justification (why DLT-META won't work)
2. Document attempted workarounds
3. Present trade-offs to data engineering leadership
4. Get written approval
5. Document decision in pipeline README

### ⚠️ When NOT to Deviate

**Don't use Platform Notebooks for these reasons (use DLT-META instead):**

- ❌ "I'm more familiar with Spark Streaming" - Learn DLT-META
- ❌ "DLT is too constraining" - Work within constraints
- ❌ "Want more control" - Not a valid organizational reason
- ❌ "Faster to prototype" - Prototypes become production
- ❌ "Don't like JSON config" - Personal preference not valid

**Key Point:** Organizational standardization outweighs individual preferences.

### Decision Framework: Handling Void Transactions

**Problem:** Sales order transactions can be voided, requiring updates to existing records.

**Option 1: Self-Join (❌ DLT-META | ✅ Platform Notebooks)**

```sql
-- Platform Notebooks: VALID - Can read target table
SELECT
  incoming.key1, incoming.key2,
  CASE
    WHEN incoming.void_flag = 'T' THEN 0
    ELSE COALESCE(existing.amount, incoming.amount)
  END as amount
FROM incoming_source incoming
LEFT JOIN target_table existing
  ON incoming.key = existing.key  -- ✅ ALLOWED in Spark Streaming
```

```python
# Platform Notebooks: Using DeltaTable.merge with self-reference
from delta.tables import DeltaTable

target_df = spark.table(f"{target_db}.{target_table}")  # ✅ Can read target
incoming_df = spark.readStream.table(source_table)

# Can join target with incoming for complex logic
enriched_df = incoming_df.join(target_df, "key", "left_outer")

# Then merge back
DeltaTable.forName(spark, target_table).merge(
    enriched_df,
    "source.key = target.key"
).whenMatchedUpdateAll()\
 .whenNotMatchedInsertAll()\
 .execute()
```

> **Why this works:** Platform Notebooks use Spark Streaming, NOT DLT. Self-referencing is allowed.

**Option 2: Zero Amounts + CDC (✅ DLT-META | ✅ Platform Notebooks)**

```sql
-- DLT-META: REQUIRED approach (cannot self-reference)
-- Platform Notebooks: Alternative simple approach
SELECT
  key1, key2, key3,
  CASE
    WHEN void_flag = 'T' THEN 0.0000  -- Zero out for voids
    ELSE amount
  END as amount,
  void_flag,
  void_date
FROM source_XXX
```

**DLT-META CDC config:**
```json
"treasury_cdc_apply_changes": {
  "keys": ["key1", "key2", "key3"],
  "sequence_by": "updated_at",
  "scd_type": "1"
}
```

**Platform Notebooks merge:**
```python
DeltaTable.forName(spark, target).merge(
    transformed_df,
    "source.key1 = target.key1 AND source.key2 = target.key2"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**How It Works:**
1. First record arrives: `amount=100, void_flag='F'` → INSERT
2. Void record arrives: `amount=0, void_flag='T'` → UPDATE (based on keys)
3. Merge operation updates the existing row with new values

### Decision Framework: Quarantine vs Inline Filtering

**Option 1: Inline Filtering (Simple)**
```yaml
sql_query: |
  SELECT *
  FROM source_XXX
  WHERE sales_ord_id IS NOT NULL  -- Filter out bad records
```

**Pros:** Simple, single pipeline
**Cons:** Lose visibility into bad records

**Option 2: Separate Quarantine Pipeline (Recommended)**
```json
// Flow 1: Good records to treasury
{
  "data_flow_id": "204",
  "sql_query": "SELECT * FROM source_204 WHERE sales_ord_id IS NOT NULL"
}

// Flow 2: Bad records to quarantine
{
  "data_flow_id": "205",
  "sql_query": "SELECT * FROM source_205 WHERE sales_ord_id IS NULL"
}
```

**Pros:** Audit trail, data quality monitoring
**Cons:** More complex setup

---

## Comparative Analysis

### Implementation Complexity

| Task | Platform Notebooks | DLT-META |
|------|-------------------|----------|
| **Initial Setup** | Low (services already exist) | High (setup DLT framework) |
| **Adding New Pipeline** | Low (create YAML config) | Low (create JSON config) |
| **Modifying Transformation** | Low (edit YAML SQL) | Low (edit YAML SQL) |
| **Adding CDC** | Medium (manual DeltaTable.merge) | Low (dlt.apply_changes config) |
| **Debugging Issues** | Easy (standard Spark logs) | Hard (DLT + framework layers) |
| **Self-Referencing** | ✅ Allowed (Spark Streaming) | ❌ Prohibited (DLT constraint) |
| **Scaling to 100 Pipelines** | Medium (100 YAMLs + shared services) | Medium (100 JSONs + YAMLs) |

### Code Maintenance

**Platform Notebooks:**
```
1 Pipeline = 1 YAML Configuration (50-200 lines)
100 Pipelines = 100 YAML configs + ~5 reusable notebook services
Service Update = Update 1 notebook service, all pipelines benefit
```

**DLT-META:**
```
1 Pipeline = 1 JSON + 1 YAML (50-200 lines)
100 Pipelines = 100 JSONs + 100 YAMLs
Framework Update = Update 1 framework file
```

### Performance Considerations

**Performance is DIFFERENT** since they use different underlying technologies:

**DLT-META (uses DLT framework):**
- DLT manages optimization automatically
- Built-in automatic optimization
- DLT overhead for managing DAG
- Best for declarative streaming pipelines

**Platform Notebooks (uses Spark Streaming):**
- Full control over Spark optimization
- Manual tuning of shuffle partitions, memory, etc.
- Standard Spark Streaming performance characteristics
- Can leverage all Spark optimization techniques
- More flexibility but requires manual optimization

**Key Differences:**
- DLT-META: Higher-level abstraction, less tuning control
- Platform Notebooks: Lower-level control, more tuning options
- Choose based on: need for control vs. simplicity

---

## References

### DLT Documentation
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [CDC with apply_changes](https://docs.databricks.com/delta-live-tables/cdc.html)

### Internal Documentation
- [DLT-META GitHub Repository](https://github.com/databrickslabs/dlt-meta)
- Confluence: [Databricks Services](https://confluence.livenation.com/spaces/DS/pages/476944262/Databricks+Services)
- [ADR - Hash-Based Surrogate Keys](https://confluence.livenation.com/pages/viewpage.action?pageId=590628508)

### Example Implementations
- **DLT-META:** `/Users/Girish.Chandriah/ln_projects/dlt-meta/cds/conf/onboarding/host_sales_ord_tran_0204/`
- **Platform Notebooks:**
  - Services: `/Users/Girish.Chandriah/ln_projects/platform_notebooks/notebook/`
    - [stream_delta_transformation.py](../platform_notebooks/notebook/stream_delta_transformation.py)
    - [batch_delta_transformation.py](../platform_notebooks/notebook/batch_delta_transformation.py)
    - [refinery_from_json_merge_v1.py](../platform_notebooks/notebook/refinery_from_json_merge_v1.py)
  - README: [Platform Notebooks README](../platform_notebooks/README.md)

---

## Appendix: Common Patterns

### Pattern 1: Landing → Treasury (Skip Refinery)

**Use Case:** Simple ingestion with minimal transformation

```json
{
  "data_flow_id": "204",
  "landing_table": "bronze_table",
  "refinery_catalog_nonprod": null,  // Skip refinery
  "treasury_table": "gold_table",
  "treasury_transformation_json": "transformations.yaml"
}
```

### Pattern 2: Landing → Refinery → Treasury

**Use Case:** Complex transformations split into stages

```json
{
  "data_flow_id": "301",
  "landing_table": "bronze_table",
  "refinery_table": "silver_table",
  "refinery_transformation_json": "refinery_transform.yaml",
  "treasury_table": "gold_table",
  "treasury_transformation_json": "treasury_transform.yaml"
}
```

### Pattern 3: Quarantine + Main Pipeline

**Use Case:** Data quality with quarantine

```json
[
  {
    "data_flow_id": "204",  // Main flow
    "treasury_table": "main_table",
    "sql_query": "SELECT * WHERE valid = true"
  },
  {
    "data_flow_id": "205",  // Quarantine flow
    "refinery_table": "quarantine_table",
    "sql_query": "SELECT * WHERE valid = false"
  }
]
```

---

---

## Critical Corrections Summary

This document was corrected and enhanced with organizational guidance:

### ❌ INCORRECT (Version 1.0)
- Claimed Platform Notebooks use Delta Live Tables (DLT)
- Showed code examples with `@dlt.table`, `dlt.read()`, `dlt.apply_changes()`
- Stated DLT constraints apply to Platform Notebooks
- Showed fake implementation code

### ✅ CORRECTED (Version 2.0)
- **Platform Notebooks use Spark Streaming + Delta Lake** (NOT DLT)
- Uses `readStream`/`writeStream` and `DeltaTable.merge()`
- Uses YAML configuration with reusable notebook services
- Self-referencing IS allowed (major difference from DLT)
- Includes built-in CCPA privacy features (merge_pii function)

### ➕ ADDED (Version 2.1)
- **Organizational recommendation** - DLT-META is the standard
- **Comprehensive workarounds** - Solutions for DLT-META limitations
- **Exception approval process** - When and how to deviate from standard
- **Decision flowchart** - Clear guidance on when to use each approach
- **Low adoption context** - Explains why Platform Notebooks aren't widely used

### Key Takeaway

**DLT-META** and **Platform Notebooks** are fundamentally different:

| Aspect | DLT-META | Platform Notebooks |
|--------|----------|-------------------|
| **Framework** | Delta Live Tables | Spark Streaming |
| **CDC** | dlt.apply_changes | DeltaTable.merge() |
| **Self-Reference** | ❌ Prohibited | ✅ Allowed |
| **Decorators** | @dlt.table required | None (standard Spark) |
| **Organizational Status** | ✅ Standard | ⚠️ Exception-only |

---

**Document Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-02-23 | Initial version (contained inaccuracies about Platform Notebooks) |
| 2.0 | 2025-02-23 | **MAJOR CORRECTION**: Fixed fundamental errors about Platform Notebooks architecture. Platform Notebooks use Spark Streaming, NOT DLT. |
| 2.1 | 2025-02-23 | **ORGANIZATIONAL GUIDANCE**: Added organizational recommendation favoring DLT-META as standard. Added comprehensive bottleneck workarounds. Added exception approval process. |

