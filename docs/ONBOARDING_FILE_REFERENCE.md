# DLT-META Onboarding File Reference Guide

## Overview

Onboarding files define the **complete ETL configuration** for each table in DLT-META. They are JSON files that specify:
- Source configuration (where data comes from)
- Landing layer configuration (bronze/raw data)
- Refinery layer configuration (silver/cleaned data)
- Data quality expectations
- Transformations
- CDC settings

## File Structure

Onboarding files contain a **JSON array** where each object defines one dataflow (one table's configuration):

```json
[
  {
    "data_flow_id": "100",
    "data_flow_group": "A1",
    ...
  },
  {
    "data_flow_id": "101",
    "data_flow_group": "A1",
    ...
  }
]
```

## Key Elements Reference

### 1. Core Identifiers

#### `data_flow_id` (Required)
**Type:** String
**Description:** Unique identifier for this dataflow
**Example:** `"100"`, `"1001"`, `"sales_customer_001"`

**Best Practices:**
- Use numeric IDs with domain-based ranges (1000s for finance, 2000s for sales)
- Or use descriptive IDs: `"finance_sap_gl_001"`
- Must be unique across all dataflows

```json
"data_flow_id": "1001"
```

#### `data_flow_group` (Required)
**Type:** String
**Description:** Groups related dataflows together for pipeline execution
**Example:** `"finance_sap"`, `"sales_salesforce"`, `"A1"`

**Best Practices:**
- Use format: `{domain}_{source_system}`
- All tables in same group run in same pipeline
- Use for organizing 100s of ETLs

```json
"data_flow_group": "finance_sap"
```

#### `source_system` (Optional)
**Type:** String
**Description:** Name of the source system
**Example:** `"sap"`, `"salesforce"`, `"mysql"`

**Purpose:** Documentation and tracking

```json
"source_system": "mysql"
```

---

### 2. Source Configuration

#### `source_format` (Required)
**Type:** String
**Allowed Values:** `"cloudFiles"`, `"delta"`, `"eventhub"`, `"kafka"`, `"snapshot"`

**Description:** Type of source data format

```json
"source_format": "cloudFiles"
```

**Format Details:**

| Format | Use Case | Example |
|--------|----------|---------|
| `cloudFiles` | Auto Loader for files (CSV, JSON, Parquet) | S3, ADLS, DBFS files |
| `delta` | Read from existing Delta tables | Existing bronze/landing tables |
| `eventhub` | Azure Event Hubs streaming | Real-time events |
| `kafka` | Kafka streaming | Real-time events |
| `snapshot` | Snapshot-based CDC | Full table snapshots |

#### `source_details` (Required)
**Type:** Object
**Description:** Source-specific configuration

**For cloudFiles:**
```json
"source_details": {
  "source_database": "customers",        // Optional: Source database name
  "source_table": "customers",           // Optional: Source table name
  "source_path_prod": "/path/to/data",  // Required: Path to source files
  "source_schema_path": "/path/to/schema.ddl"  // Optional: DDL schema file
}
```

**For delta:**
```json
"source_details": {
  "catalog": "my_catalog",     // Optional: Unity Catalog name
  "database": "landing_db",    // Required: Database name
  "table": "customers",        // Required: Table name
  "path": "/path/to/delta"     // Optional: For non-UC tables
}
```

**For eventhub/kafka:**
```json
"source_details": {
  "connectionString": "...",   // Required: Connection string
  "eventhub.name": "my-hub",   // Required: Event hub name
  "kafka.bootstrap.servers": "..." // For Kafka
}
```

**Environment Suffixes:**
- `source_path_prod` - Production environment
- `source_path_nonprod` - Non-production environment
- `source_path_demo` - Demo environment

The onboarding process selects the appropriate suffix based on environment.

#### `source_metadata` (Optional)
**Type:** Object
**Description:** Auto Loader metadata configuration

```json
"source_metadata": {
  "include_autoloader_metadata_column": "True",
  "autoloader_metadata_col_name": "source_metadata",
  "select_metadata_cols": {
    "input_file_name": "_metadata.file_name",
    "input_file_path": "_metadata.file_path",
    "input_file_modification_time": "_metadata.file_modification_time"
  }
}
```

**Captures:**
- File name
- File path
- File modification time
- Other Auto Loader metadata

---

### 3. Landing Layer Configuration (Bronze)

#### Landing Target Details

```json
"landing_catalog_prod": "privacy_nonprod",
"landing_database_prod": "dltmeta_landing",
"landing_table": "customers",
"landing_table_comment": "customers bronze table",
"landing_table_path_prod": "/path/to/table"  // Optional: For non-UC
```

**Key Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `landing_catalog_<env>` | No | Unity Catalog name |
| `landing_database_<env>` | Yes | Target database/schema |
| `landing_table` | Yes | Target table name |
| `landing_table_comment` | No | Table description |
| `landing_table_path_<env>` | No | Table location (non-UC only) |

#### `landing_reader_options` (Optional)
**Type:** Object
**Description:** Spark reader options for Auto Loader

```json
"landing_reader_options": {
  "cloudFiles.format": "csv",              // File format
  "cloudFiles.inferColumnTypes": "true",   // Infer types
  "cloudFiles.rescuedDataColumn": "_rescued_data",  // Bad records column
  "header": "true",                        // CSV header
  "delimiter": ",",                        // CSV delimiter
  "multiLine": "true"                      // Multi-line JSON
}
```

**Common Options:**

| Option | Value | Use Case |
|--------|-------|----------|
| `cloudFiles.format` | `"csv"`, `"json"`, `"parquet"`, `"avro"` | File format |
| `cloudFiles.inferColumnTypes` | `"true"`, `"false"` | Auto-infer schema |
| `cloudFiles.rescuedDataColumn` | Column name | Store unparseable data |
| `cloudFiles.schemaLocation` | Path | Schema inference location |
| `header` | `"true"`, `"false"` | CSV has header row |
| `delimiter` | `","`, `"\t"`, etc. | CSV delimiter |

#### `landing_table_properties` (Optional)
**Type:** Object
**Description:** Delta table properties

```json
"landing_table_properties": {
  "pipelines.autoOptimize.managed": "true",
  "pipelines.reset.allowed": "false",
  "delta.enableChangeDataFeed": "true",
  "delta.deletedFileRetentionDuration": "interval 30 days"
}
```

**Common Properties:**

| Property | Value | Description |
|----------|-------|-------------|
| `pipelines.autoOptimize.managed` | `true`/`false` | Auto-optimize |
| `pipelines.reset.allowed` | `true`/`false` | Allow full refresh |
| `delta.enableChangeDataFeed` | `true`/`false` | Enable CDF |
| `delta.logRetentionDuration` | `interval X days` | Log retention |

#### `landing_cluster_by` / `landing_table_cluster_by` (Optional)
**Type:** Array
**Description:** Liquid clustering columns

```json
"landing_cluster_by": ["customer_id", "order_date"]
```

**Best Practices:**
- Use for high-cardinality columns frequently used in filters
- Limit to 3-4 columns
- Order matters (most selective first)

#### `landing_partition_columns` (Optional - Deprecated)
**Type:** Array
**Description:** Partition columns (prefer clustering)

```json
"landing_partition_columns": ["year", "month"]
```

**Note:** Use `cluster_by` instead for better performance.

---

### 4. Data Quality Expectations (DQE)

#### `landing_data_quality_expectations_json_<env>` (Optional)
**Type:** String (path)
**Description:** Path to JSON file containing landing layer DQE

```json
"landing_data_quality_expectations_json_prod": "/path/to/dqe/customers_landing_dqe.json"
```

**DQE File Format:**
```json
{
  "expect_all": {
    "valid_id": "customer_id IS NOT NULL",
    "valid_email": "email IS NOT NULL AND email LIKE '%@%'"
  },
  "expect_all_or_drop": {
    "future_date": "created_date <= CURRENT_DATE()"
  },
  "expect_or_quarantine": {
    "suspicious_amount": "order_amount >= 0 AND order_amount <= 1000000"
  }
}
```

**DQE Types:**

| Type | Behavior | Use Case |
|------|----------|----------|
| `expect_all` | Log violations, continue | Warnings |
| `expect_all_or_drop` | Drop violating rows | Critical validations |
| `expect_all_or_fail` | Fail pipeline | Must-pass validations |
| `expect_or_quarantine` | Send to quarantine table | Review needed |

---

### 5. Quarantine Configuration

#### Quarantine Target Details

```json
"landing_catalog_quarantine_prod": "privacy_nonprod",
"landing_database_quarantine_prod": "dltmeta_landing",
"landing_quarantine_table": "customers_quarantine",
"landing_quarantine_table_comment": "customers quarantine table",
"landing_quarantine_table_path_prod": "/path/to/quarantine"
```

**Purpose:** Store records that fail `expect_or_quarantine` validations

#### `landing_quarantine_table_properties` (Optional)
```json
"landing_quarantine_table_properties": {
  "pipelines.reset.allowed": "false",
  "pipelines.autoOptimize.managed": "true"
}
```

#### `landing_quarantine_table_cluster_by` (Optional)
```json
"landing_quarantine_table_cluster_by": ["customer_id"]
```

---

### 6. Append Flows (Advanced)

#### `landing_append_flows` (Optional)
**Type:** Array
**Description:** Additional data sources to append to main table

```json
"landing_append_flows": [
  {
    "name": "customer_landing_flow",
    "create_streaming_table": false,
    "source_format": "cloudFiles",
    "source_details": {
      "source_path_prod": "/path/to/additional/data"
    },
    "reader_options": {
      "cloudFiles.format": "json"
    },
    "once": false
  }
]
```

**Use Cases:**
- Multiple source paths feeding same table
- Incremental + historical data loads
- Multi-region data sources

**Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique flow name |
| `create_streaming_table` | No | Create streaming table |
| `source_format` | Yes | cloudFiles, delta, kafka, eventhub |
| `source_details` | Yes | Source configuration |
| `reader_options` | No | Reader options |
| `once` | No | One-time load vs continuous |

---

### 7. Refinery Layer Configuration (Silver)

#### Refinery Target Details

```json
"refinery_catalog_prod": "privacy_nonprod",
"refinery_database_prod": "dltmeta_refinery",
"refinery_table": "customers",
"refinery_table_comment": "customers silver table",
"refinery_table_path_prod": "/path/to/silver/table"
```

#### `refinery_source_format` (Optional)
**Type:** String
**Default:** `"delta"` (reads from landing)
**Values:** `"delta"`, `"snapshot"`

```json
"refinery_source_format": "delta"
```

#### `refinery_select_exp` (Optional)
**Type:** Array
**Description:** SELECT expressions for simple transformations

```json
"refinery_select_exp": [
  "*",
  "UPPER(customer_name) as customer_name_upper",
  "CASE WHEN country = 'US' THEN 'USA' ELSE country END as country"
]
```

#### `refinery_where_clause` (Optional)
**Type:** String or Array
**Description:** WHERE clause to filter data

```json
"refinery_where_clause": "status != 'DELETED' AND created_date >= '2020-01-01'"
```

Or as array:
```json
"refinery_where_clause": [
  "status != 'DELETED'",
  "created_date >= '2020-01-01'"
]
```

#### `refinery_transformation_json_<env>` (Optional)
**Type:** String (path)
**Description:** Path to complex SQL transformation file

```json
"refinery_transformation_json_prod": "/path/to/transformations/customer_360.json"
```

**Transformation File Format:**
```json
{
  "transformation_id": "customer_360",
  "sql_query": "
    SELECT
      c.*,
      o.total_orders,
      o.total_revenue
    FROM source_customers c
    LEFT JOIN (
      SELECT customer_id, COUNT(*) as total_orders, SUM(amount) as total_revenue
      FROM source_orders
      GROUP BY customer_id
    ) o ON c.customer_id = o.customer_id
  "
}
```

#### `refinery_table_properties` (Optional)
```json
"refinery_table_properties": {
  "pipelines.reset.allowed": "false",
  "pipelines.autoOptimize.zOrderCols": "customer_id, order_date",
  "delta.enableChangeDataFeed": "true"
}
```

#### `refinery_cluster_by` (Optional)
```json
"refinery_cluster_by": ["customer_id"]
```

#### `refinery_data_quality_expectations_json_<env>` (Optional)
```json
"refinery_data_quality_expectations_json_prod": "/path/to/dqe/customers_refinery_dqe.json"
```

---

### 8. CDC (Change Data Capture) Configuration

#### `refinery_cdc_apply_changes` (Optional)
**Type:** Object
**Description:** CDC configuration for SCD (Slowly Changing Dimension)

```json
"refinery_cdc_apply_changes": {
  "keys": ["customer_id"],
  "sequence_by": "update_timestamp",
  "scd_type": "2",
  "apply_as_deletes": "operation = 'D'",
  "apply_as_truncates": "operation = 'TRUNCATE'",
  "except_column_list": ["operation", "update_timestamp", "_rescued_data"],
  "column_list": null,
  "stored_as_scd_type": null,
  "track_history_column_list": null,
  "track_history_except_column_list": null,
  "where": null,
  "ignore_null_updates": false,
  "flow_name": "customers_cdc_flow",
  "once": false
}
```

**Field Details:**

| Field | Required | Description |
|-------|----------|-------------|
| `keys` | Yes | Primary key columns (list) |
| `sequence_by` | Yes | Column(s) to order changes |
| `scd_type` | Yes | `"1"` or `"2"` |
| `apply_as_deletes` | No | Expression to identify deletes |
| `apply_as_truncates` | No | Expression to identify truncates |
| `except_column_list` | No | Columns to exclude from target |
| `column_list` | No | Columns to include (default: all) |
| `track_history_column_list` | No | SCD2: columns to track history |
| `track_history_except_column_list` | No | SCD2: columns NOT to track |
| `ignore_null_updates` | No | Skip updates where all values are NULL |
| `where` | No | Filter condition |
| `flow_name` | No | Custom flow name |
| `once` | No | One-time processing |

**SCD Type 1 Example:**
```json
"refinery_cdc_apply_changes": {
  "keys": ["customer_id"],
  "sequence_by": "update_timestamp",
  "scd_type": "1",
  "apply_as_deletes": "operation = 'DELETE'"
}
```

**SCD Type 2 Example:**
```json
"refinery_cdc_apply_changes": {
  "keys": ["customer_id"],
  "sequence_by": "update_timestamp",
  "scd_type": "2",
  "apply_as_deletes": "operation = 'DELETE'",
  "track_history_except_column_list": ["_rescued_data", "operation"]
}
```

**Composite Sequence Key:**
```json
"sequence_by": "update_date, update_timestamp"
```

---

### 9. Apply Changes from Snapshot

#### `refinery_apply_changes_from_snapshot` (Optional)
**Type:** Object
**Description:** Snapshot-based CDC (declarative CDC)

```json
"refinery_apply_changes_from_snapshot": {
  "keys": ["customer_id"],
  "scd_type": "2",
  "track_history_column_list": ["status", "address"],
  "track_history_except_column_list": ["internal_notes"]
}
```

**Use Case:** When you receive full table snapshots and need to detect changes

---

### 10. Refinery Append Flows

#### `refinery_append_flows` (Optional)
**Type:** Array
**Description:** Additional sources to append to refinery table

```json
"refinery_append_flows": [
  {
    "name": "customers_historical_flow",
    "create_streaming_table": false,
    "source_format": "delta",
    "source_details": {
      "source_database": "legacy.historical",
      "source_table": "customers_archive"
    },
    "reader_options": {},
    "once": true
  }
]
```

---

### 11. Treasury Layer Configuration (Gold)

The treasury layer is for **business-level aggregations and analytics** (gold layer). It reads from refinery tables and creates business metrics.

#### Treasury Target Details

```json
"treasury_catalog_prod": "privacy_nonprod",
"treasury_database_prod": "dltmeta_treasury",
"treasury_table": "customer_summary",
"treasury_table_comment": "Customer aggregated metrics - gold layer",
"treasury_table_path_prod": "/path/to/gold/table"  // Optional: For non-UC
```

#### Treasury Source Configuration

Treasury **always reads from refinery/silver tables** (or other delta tables):

```json
"source_format": "delta",  // Treasury source is always delta
"source_details": {
  "catalog": "privacy_nonprod",
  "database": "dltmeta_refinery",
  "table": "customers"
}
```

Or specify in `treasury_source_details` if different from main source:

```json
"treasury_source_details": {
  "catalog": "privacy_nonprod",
  "database": "dltmeta_refinery",
  "table": "customers"
}
```

#### `treasury_transformation_json_<env>` (Required for Treasury)

**Type:** String (path)
**Description:** Path to SQL transformation file (typically aggregations)

```json
"treasury_transformation_json_prod": "/path/to/transformations/customer_summary.json"
```

**Treasury Transformation Example:**
```json
{
  "transformation_id": "customer_summary",
  "sql_query": "
    SELECT
      customer_tier,
      region,
      DATE_TRUNC('month', order_date) as month,
      COUNT(DISTINCT customer_id) as total_customers,
      SUM(order_amount) as total_revenue,
      AVG(order_amount) as avg_order_value,
      COUNT(order_id) as total_orders,
      SUM(order_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
    FROM source_customers c
    JOIN source_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'COMPLETED'
    GROUP BY customer_tier, region, DATE_TRUNC('month', order_date)
  "
}
```

#### `treasury_select_exp` (Optional)

Simple SELECT expressions (use `treasury_transformation_json` for complex queries):

```json
"treasury_select_exp": [
  "customer_tier",
  "COUNT(*) as total_customers",
  "SUM(lifetime_value) as total_ltv"
]
```

#### `treasury_where_clause` (Optional)

Filter conditions:

```json
"treasury_where_clause": "order_date >= '2023-01-01' AND status = 'ACTIVE'"
```

#### `treasury_table_properties` (Optional)

```json
"treasury_table_properties": {
  "pipelines.autoOptimize.managed": "true",
  "pipelines.autoOptimize.zOrderCols": "customer_tier,region",
  "pipelines.reset.allowed": "true"
}
```

#### `treasury_cluster_by` (Optional)

```json
"treasury_cluster_by": ["customer_tier", "region", "month"]
```

**Best Practice:** Use clustering on columns frequently used in dashboard filters.

#### `treasury_data_quality_expectations_json_<env>` (Optional)

```json
"treasury_data_quality_expectations_json_prod": "/path/to/dqe/customer_summary_dqe.json"
```

**Treasury DQE Example:**
```json
{
  "expect_all_or_fail": {
    "no_negative_revenue": "total_revenue >= 0",
    "customer_count_positive": "total_customers > 0"
  },
  "expect_all": {
    "reasonable_avg_order": "avg_order_value <= 10000"
  }
}
```

#### Treasury Layer Characteristics

| Characteristic | Value |
|----------------|-------|
| **Processing Mode** | Batch only (no streaming) |
| **Source** | Always Delta tables (typically refinery) |
| **Update Frequency** | Daily, hourly, or on-demand |
| **Typical Operations** | Aggregations, JOINs, window functions |
| **CDC Support** | No (read-only aggregations) |
| **Purpose** | Business metrics, KPIs, data marts |

---

## Complete Example: Full Medallion Architecture (Landing → Refinery → Treasury)

```json
[
  {
    "data_flow_id": "1001",
    "data_flow_group": "finance_sap",
    "source_system": "sap_erp",
    "source_format": "cloudFiles",

    "source_details": {
      "source_database": "sap_financials",
      "source_table": "customers",
      "source_path_prod": "/mnt/data/sap/customers",
      "source_schema_path": "/schemas/customers.ddl"
    },

    "landing_catalog_prod": "my_catalog",
    "landing_database_prod": "landing_layer",
    "landing_table": "sap_customers",
    "landing_table_comment": "SAP customer data - bronze layer",

    "landing_reader_options": {
      "cloudFiles.format": "csv",
      "cloudFiles.rescuedDataColumn": "_rescued_data",
      "header": "true",
      "inferSchema": "true"
    },

    "landing_table_properties": {
      "pipelines.autoOptimize.managed": "true"
    },

    "landing_cluster_by": ["customer_id", "country"],

    "landing_data_quality_expectations_json_prod": "/dqe/customers_landing.json",

    "landing_catalog_quarantine_prod": "my_catalog",
    "landing_database_quarantine_prod": "landing_layer",
    "landing_quarantine_table": "sap_customers_quarantine",
    "landing_quarantine_table_cluster_by": ["customer_id"],

    "refinery_catalog_prod": "my_catalog",
    "refinery_database_prod": "refinery_layer",
    "refinery_table": "customers",
    "refinery_table_comment": "Cleaned customer data - silver layer",

    "refinery_cdc_apply_changes": {
      "keys": ["customer_id"],
      "sequence_by": "last_modified_date",
      "scd_type": "2",
      "apply_as_deletes": "is_deleted = true",
      "except_column_list": ["is_deleted", "load_timestamp", "_rescued_data"]
    },

    "refinery_cluster_by": ["customer_id"],

    "refinery_table_properties": {
      "pipelines.autoOptimize.zOrderCols": "customer_id, country"
    },

    "refinery_transformation_json_prod": "/transformations/customer_enrichment.json",
    "refinery_data_quality_expectations_json_prod": "/dqe/customers_refinery.json",

    "treasury_catalog_prod": "my_catalog",
    "treasury_database_prod": "treasury_layer",
    "treasury_table": "customer_summary",
    "treasury_table_comment": "Customer aggregated metrics - gold layer",

    "treasury_transformation_json_prod": "/transformations/customer_summary.json",
    "treasury_data_quality_expectations_json_prod": "/dqe/customer_summary_dqe.json",

    "treasury_cluster_by": ["customer_tier", "country"],

    "treasury_table_properties": {
      "pipelines.autoOptimize.managed": "true",
      "pipelines.autoOptimize.zOrderCols": "customer_tier, country"
    }
  }
]
```

**Supporting Files:**

**`/transformations/customer_summary.json`:**
```json
{
  "transformation_id": "customer_summary",
  "sql_query": "
    SELECT
      CASE
        WHEN total_orders >= 100 THEN 'VIP'
        WHEN total_orders >= 10 THEN 'Standard'
        ELSE 'Basic'
      END as customer_tier,
      country,
      COUNT(*) as total_customers,
      SUM(total_orders) as total_orders_sum,
      AVG(total_orders) as avg_orders_per_customer,
      SUM(lifetime_value) as total_ltv,
      AVG(lifetime_value) as avg_ltv
    FROM source_customers
    WHERE __END_AT IS NULL  -- SCD2: Get only current records
    GROUP BY
      CASE
        WHEN total_orders >= 100 THEN 'VIP'
        WHEN total_orders >= 10 THEN 'Standard'
        ELSE 'Basic'
      END,
      country
  "
}
```

**`/dqe/customer_summary_dqe.json`:**
```json
{
  "expect_all_or_fail": {
    "positive_customer_count": "total_customers > 0",
    "non_negative_ltv": "total_ltv >= 0"
  },
  "expect_all": {
    "reasonable_avg_ltv": "avg_ltv <= 1000000",
    "valid_tier": "customer_tier IN ('Basic', 'Standard', 'VIP')"
  }
}
```

---

## Field Naming Convention

### Environment Suffixes

Fields with environment-specific values use suffixes:

- `_prod` - Production environment
- `_nonprod` - Non-production/dev environment
- `_qa` - QA environment
- `_demo` - Demo environment

**Example:**
```json
"landing_database_prod": "prod_landing",
"landing_database_nonprod": "dev_landing",
"landing_database_qa": "qa_landing"
```

The onboarding process selects the appropriate suffix based on the environment parameter.

### Layer Prefixes

- `landing_*` - Landing/bronze layer
- `refinery_*` - Refinery/silver layer
- `treasury_*` - Treasury/gold layer

---

## Required vs Optional Fields

### Minimal Configuration (Required Only)

```json
{
  "data_flow_id": "100",
  "data_flow_group": "A1",
  "source_format": "cloudFiles",
  "source_details": {
    "source_path_prod": "/path/to/data"
  },
  "landing_database_prod": "landing_db",
  "landing_table": "customers"
}
```

This creates:
- Landing table with auto-inferred schema
- No refinery layer
- No DQE
- No transformations

### Recommended Configuration

Include:
- Comments for documentation
- Cluster columns for performance
- DQE for data quality
- Refinery layer for cleaned data
- CDC for proper change tracking

---

## Common Patterns

### Pattern 1: Simple Ingestion (No Transformations)

```json
{
  "data_flow_id": "100",
  "data_flow_group": "raw_ingestion",
  "source_format": "cloudFiles",
  "source_details": {
    "source_path_prod": "/data/customers"
  },
  "landing_database_prod": "landing",
  "landing_table": "customers",
  "landing_reader_options": {
    "cloudFiles.format": "parquet"
  }
}
```

### Pattern 2: Landing + Refinery with CDC

```json
{
  "data_flow_id": "101",
  "data_flow_group": "customer_pipeline",
  "source_format": "cloudFiles",
  "source_details": {...},
  "landing_database_prod": "landing",
  "landing_table": "customers_raw",
  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_cdc_apply_changes": {
    "keys": ["id"],
    "sequence_by": "updated_at",
    "scd_type": "2"
  }
}
```

### Pattern 3: Complex Transformations

```json
{
  "data_flow_id": "102",
  "data_flow_group": "analytics",
  "source_format": "delta",
  "source_details": {
    "database": "landing",
    "table": "orders"
  },
  "refinery_database_prod": "analytics",
  "refinery_table": "customer_360",
  "refinery_transformation_json_prod": "/transforms/customer_360.json"
}
```

---

## Validation Rules

1. **Unique IDs**: `data_flow_id` must be unique across all onboarding files
2. **Required combos**: If refinery is defined, landing must be defined first
3. **CDC requires keys**: `refinery_cdc_apply_changes` requires `keys` and `sequence_by`
4. **Environment consistency**: Use same environment suffix throughout
5. **Path formats**: Paths must start with `/` or `dbfs:/` or `s3://` etc.

---

## Best Practices

1. ✅ **Use descriptive IDs and groups**
   ```json
   "data_flow_id": "1001",
   "data_flow_group": "finance_sap"
   ```

2. ✅ **Add comments for documentation**
   ```json
   "landing_table_comment": "Customer master data from SAP ERP"
   ```

3. ✅ **Use cluster_by for performance**
   ```json
   "landing_cluster_by": ["customer_id", "date"]
   ```

4. ✅ **Implement DQE at both layers**
   ```json
   "landing_data_quality_expectations_json_prod": "/dqe/landing.json",
   "refinery_data_quality_expectations_json_prod": "/dqe/refinery.json"
   ```

5. ✅ **Use quarantine tables for bad data**
   ```json
   "landing_quarantine_table": "customers_quarantine"
   ```

6. ✅ **Separate complex SQL to external files**
   ```json
   "refinery_transformation_json_prod": "/transforms/complex_join.json"
   ```

7. ✅ **Enable CDC for dimension tables**
   ```json
   "refinery_cdc_apply_changes": {
     "keys": ["id"],
     "scd_type": "2"
   }
   ```

---

## Troubleshooting

### Issue: Table not created
**Check:** Required fields present? Environment suffix correct?

### Issue: DQE not applied
**Check:** Path to DQE file correct and accessible?

### Issue: CDC not working
**Check:** `keys` and `sequence_by` defined? Columns exist in data?

### Issue: Transformation fails
**Check:** SQL syntax? Source table names correct? JOINs valid?

---

## Additional Resources

- [DQE Guide](DQE_GUIDE.md)
- [Transformations Guide](TRANSFORMATIONS_GUIDE.md)
- [CDC Configuration](CDC_CONFIGURATION.md)
- [Organizing Large Scale ETLs](ORGANIZING_LARGE_SCALE_ETLS.md)
