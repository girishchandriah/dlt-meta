# DLT-META Onboarding Quick Reference

## Medallion Architecture Overview

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   LANDING   │      │  REFINERY   │      │  TREASURY   │
│  (Bronze)   │─────▶│  (Silver)   │─────▶│   (Gold)    │
└─────────────┘      └─────────────┘      └─────────────┘
  Raw Data            Cleaned Data         Aggregated Data
  Streaming/Batch     Streaming/Batch      Batch Only

External Files  ──▶  Landing  ──▶  Refinery  ──▶  Treasury
(CSV/JSON/           Raw              CDC          Business
 Parquet)            Ingestion        Transforms   Metrics
```

## Essential Fields Cheat Sheet

### Core Identifiers (Required)
```json
{
  "data_flow_id": "1001",                    // Unique ID for this dataflow
  "data_flow_group": "finance_sap",          // Group for pipeline execution
  "source_system": "sap",                    // Source system name (optional)
  "source_format": "cloudFiles"              // cloudFiles | delta | kafka | eventhub
}
```

### Source Configuration (Required)
```json
"source_details": {
  "source_path_prod": "/path/to/data",       // Path to source files/table
  "source_schema_path": "/path/to/schema.ddl" // Optional: DDL schema
}
```

### Landing Layer (Required)
```json
"landing_database_prod": "landing_schema",   // Target schema
"landing_table": "customers",                // Target table name
"landing_reader_options": {                  // Spark reader options
  "cloudFiles.format": "csv",
  "header": "true"
}
```

### Refinery Layer (Optional - Silver)
```json
"refinery_database_prod": "refinery_schema",
"refinery_table": "customers",
"refinery_cdc_apply_changes": {              // CDC configuration
  "keys": ["customer_id"],
  "sequence_by": "update_timestamp",
  "scd_type": "2"
}
```

### Treasury Layer (Optional - Gold)
```json
"treasury_database_prod": "treasury_schema",
"treasury_table": "customer_summary",
"treasury_transformation_json_prod": "/transforms/customer_summary.json",
"treasury_data_quality_expectations_json_prod": "/dqe/customer_summary_dqe.json"
```

---

## Common Configurations

### 1. Simple CSV Ingestion
```json
{
  "data_flow_id": "100",
  "data_flow_group": "A1",
  "source_format": "cloudFiles",
  "source_details": {
    "source_path_prod": "/data/customers"
  },
  "landing_database_prod": "landing",
  "landing_table": "customers",
  "landing_reader_options": {
    "cloudFiles.format": "csv",
    "header": "true",
    "cloudFiles.rescuedDataColumn": "_rescued_data"
  }
}
```

### 2. With Data Quality (DQE)
```json
{
  ...,
  "landing_data_quality_expectations_json_prod": "/dqe/customers.json",
  "landing_quarantine_table": "customers_quarantine"
}
```

### 3. With CDC (SCD Type 2)
```json
{
  ...,
  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "update_timestamp",
    "scd_type": "2",
    "apply_as_deletes": "operation = 'D'",
    "except_column_list": ["operation", "_rescued_data"]
  }
}
```

### 4. With SQL Transformation
```json
{
  ...,
  "refinery_transformation_json_prod": "/transforms/customer_360.json",
  "refinery_select_exp": ["*", "UPPER(name) as name_upper"],
  "refinery_where_clause": "status != 'DELETED'"
}
```

### 5. Full Medallion: Landing → Refinery → Treasury
```json
{
  "data_flow_id": "100",
  "data_flow_group": "analytics",
  "source_format": "cloudFiles",
  "source_details": {"source_path_prod": "/data/orders"},

  // Bronze Layer
  "landing_database_prod": "landing",
  "landing_table": "orders",

  // Silver Layer
  "refinery_database_prod": "refinery",
  "refinery_table": "orders_cleaned",
  "refinery_transformation_json_prod": "/transforms/orders_clean.json",

  // Gold Layer
  "treasury_database_prod": "treasury",
  "treasury_table": "daily_revenue",
  "treasury_transformation_json_prod": "/transforms/revenue_summary.json"
}
```

---

## Field Reference

### Source Format Options
| Value | Use Case |
|-------|----------|
| `cloudFiles` | CSV, JSON, Parquet files (Auto Loader) |
| `delta` | Read from existing Delta table |
| `eventhub` | Azure Event Hubs |
| `kafka` | Kafka streams |
| `snapshot` | Full table snapshots |

### Reader Options (cloudFiles)
```json
"landing_reader_options": {
  "cloudFiles.format": "csv|json|parquet|avro",
  "cloudFiles.inferColumnTypes": "true|false",
  "cloudFiles.rescuedDataColumn": "_rescued_data",
  "header": "true|false",                    // CSV
  "delimiter": ",",                          // CSV
  "multiLine": "true|false"                  // JSON
}
```

### Performance Tuning
```json
"landing_cluster_by": ["customer_id", "date"],    // Liquid clustering
"landing_partition_columns": ["year", "month"],   // Partitioning (deprecated)
"landing_table_properties": {
  "pipelines.autoOptimize.managed": "true",
  "pipelines.autoOptimize.zOrderCols": "id,date"
}
```

### CDC Configuration
```json
"refinery_cdc_apply_changes": {
  "keys": ["id"],                                  // Primary key(s)
  "sequence_by": "updated_at",                     // Ordering column
  "scd_type": "1|2",                              // Type 1 or Type 2
  "apply_as_deletes": "operation = 'D'",          // Delete expression
  "except_column_list": ["op", "_rescued_data"],  // Exclude columns
  "track_history_column_list": ["status"],        // SCD2: columns to track
  "ignore_null_updates": false                    // Skip NULL updates
}
```

### Data Quality Expectations
```json
// In onboarding file:
"landing_data_quality_expectations_json_prod": "/dqe/customers.json"

// DQE file content:
{
  "expect_all": {                          // Log violations
    "valid_id": "id IS NOT NULL"
  },
  "expect_all_or_drop": {                  // Drop bad rows
    "valid_date": "date <= CURRENT_DATE()"
  },
  "expect_or_quarantine": {                // Send to quarantine
    "suspicious": "amount < 1000000"
  }
}
```

### Treasury Layer Configuration (Gold)
```json
"treasury_catalog_prod": "my_catalog",
"treasury_database_prod": "treasury",
"treasury_table": "customer_summary",
"treasury_table_comment": "Aggregated customer metrics",

"treasury_transformation_json_prod": "/transforms/customer_summary.json",
"treasury_data_quality_expectations_json_prod": "/dqe/customer_summary_dqe.json",

"treasury_cluster_by": ["customer_tier", "region"],
"treasury_table_properties": {
  "pipelines.autoOptimize.managed": "true"
}
```

**Treasury Transformation Example:**
```json
// File: /transforms/customer_summary.json
{
  "transformation_id": "customer_summary",
  "sql_query": "
    SELECT
      customer_tier,
      region,
      COUNT(*) as total_customers,
      SUM(lifetime_value) as total_ltv,
      AVG(order_count) as avg_orders
    FROM source_customers
    GROUP BY customer_tier, region
  "
}
```

**Note:** Treasury layer is **read-only/batch** - it aggregates data from refinery layer.

---

## Environment Suffixes

Use environment-specific suffixes for multi-environment deployments:

```json
"landing_database_prod": "prod_landing",
"landing_database_nonprod": "dev_landing",
"landing_database_qa": "qa_landing",
"landing_database_demo": "demo_landing"
```

During onboarding, specify which environment to use:
```bash
python src/cli.py onboard --environment prod
```

---

## Minimal vs Full Configuration

### Minimal (Just Landing - Bronze)
```json
{
  "data_flow_id": "100",
  "data_flow_group": "A1",
  "source_format": "cloudFiles",
  "source_details": {"source_path_prod": "/data"},
  "landing_database_prod": "landing",
  "landing_table": "customers"
}
```

### Minimal (Treasury Only - Gold)
```json
{
  "data_flow_id": "3001",
  "data_flow_group": "analytics",
  "source_format": "delta",
  "source_details": {
    "database": "refinery",
    "table": "orders_cleaned"
  },
  "treasury_database_prod": "treasury",
  "treasury_table": "daily_revenue",
  "treasury_transformation_json_prod": "/transforms/revenue.json"
}
```

### Full (Production-Ready - All Layers)
```json
{
  "data_flow_id": "1001",
  "data_flow_group": "finance_sap",
  "source_system": "sap",
  "source_format": "cloudFiles",

  "source_details": {
    "source_database": "sap_erp",
    "source_table": "customers",
    "source_path_prod": "/data/sap/customers",
    "source_schema_path": "/schemas/customers.ddl"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing",
  "landing_table": "sap_customers",
  "landing_table_comment": "SAP customer master data",

  "landing_reader_options": {
    "cloudFiles.format": "csv",
    "cloudFiles.rescuedDataColumn": "_rescued_data",
    "header": "true"
  },

  "landing_cluster_by": ["customer_id"],

  "landing_table_properties": {
    "pipelines.autoOptimize.managed": "true"
  },

  "landing_data_quality_expectations_json_prod": "/dqe/customers_landing.json",
  "landing_quarantine_table": "sap_customers_quarantine",

  "refinery_catalog_prod": "my_catalog",
  "refinery_database_prod": "refinery",
  "refinery_table": "customers",
  "refinery_table_comment": "Cleaned customer data",

  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "last_modified_date",
    "scd_type": "2",
    "apply_as_deletes": "is_deleted = true",
    "except_column_list": ["is_deleted", "_rescued_data"]
  },

  "refinery_cluster_by": ["customer_id"],

  "refinery_transformation_json_prod": "/transforms/customer_enrichment.json",
  "refinery_data_quality_expectations_json_prod": "/dqe/customers_refinery.json",

  "treasury_catalog_prod": "my_catalog",
  "treasury_database_prod": "treasury",
  "treasury_table": "customer_summary",
  "treasury_table_comment": "Customer aggregated metrics - gold layer",

  "treasury_transformation_json_prod": "/transforms/customer_summary.json",
  "treasury_data_quality_expectations_json_prod": "/dqe/customer_summary_dqe.json",

  "treasury_cluster_by": ["customer_tier"]
}
```

---

## Common Mistakes

❌ **Wrong:** Hardcoding environment
```json
"landing_database": "prod_landing"  // Won't work for other envs
```
✅ **Right:** Use environment suffix
```json
"landing_database_prod": "prod_landing",
"landing_database_nonprod": "dev_landing"
```

❌ **Wrong:** Missing required CDC fields
```json
"refinery_cdc_apply_changes": {
  "keys": ["id"]  // Missing sequence_by and scd_type
}
```
✅ **Right:** Complete CDC config
```json
"refinery_cdc_apply_changes": {
  "keys": ["id"],
  "sequence_by": "updated_at",
  "scd_type": "2"
}
```

❌ **Wrong:** Inline complex SQL
```json
"refinery_select_exp": ["SELECT c.*, o.total FROM customers c JOIN orders o..."]
```
✅ **Right:** External transformation file
```json
"refinery_transformation_json_prod": "/transforms/customer_orders.json"
```

---

## Quick Tips

### General
1. **Start Simple**: Begin with just landing layer, add refinery/treasury later
2. **Use Templates**: Copy existing onboarding files as starting point
3. **Test with Small Data**: Validate configuration with subset of data first
4. **Add DQE Incrementally**: Start with basic validations, add more over time
5. **Document with Comments**: Use `_comment` fields for documentation
6. **Use Clustering**: Add `cluster_by` for frequently filtered columns
7. **Enable Auto-Optimize**: Set `pipelines.autoOptimize.managed` to `true`
8. **Quarantine Bad Data**: Always specify quarantine table for DQE

### Layer-Specific
9. **Landing (Bronze)**: Keep raw, unmodified data; use `_rescued_data` column
10. **Refinery (Silver)**: Apply CDC, transformations, and business rules here
11. **Treasury (Gold)**: Use for aggregations, KPIs, and business metrics only
12. **Treasury is Read-Only**: Treasury layer uses batch processing (not streaming)

### Performance
13. **Landing**: Optimize for write throughput (minimal transformations)
14. **Refinery**: Balance read/write (some transformations OK)
15. **Treasury**: Optimize for read performance (heavy aggregations, clustering)

---

## File Organization

```
conf/
├── onboarding/
│   ├── finance/
│   │   ├── customers.json          ← One file per table
│   │   └── transactions.json
│   └── sales/
│       └── opportunities.json
├── dqe/
│   ├── finance/
│   │   ├── customers_landing_dqe.json      ← Bronze DQE
│   │   ├── customers_refinery_dqe.json     ← Silver DQE
│   │   └── customers_treasury_dqe.json     ← Gold DQE
│   └── shared/
│       └── email_validation.json            ← Reusable DQE
└── transformations/
    ├── refinery/
    │   └── finance/
    │       └── customer_360.json            ← Silver transformations
    └── treasury/
        └── finance/
            └── customer_summary.json        ← Gold aggregations
```

---

## Medallion Architecture Layers

| Layer | Name | Purpose | Typical Operations |
|-------|------|---------|-------------------|
| **Landing** | Bronze | Raw data ingestion | Auto Loader, Schema on Read |
| **Refinery** | Silver | Cleaned, enriched data | CDC, Joins, Enrichments, Deduplication |
| **Treasury** | Gold | Business-level aggregations | Aggregations, Metrics, KPIs |

**Pipeline Configuration:**

```json
{
  "layer": "landing_refinery_treasury",  // Process all three layers
  "landing.group": "A1",
  "refinery.group": "A1",
  "treasury.group": "A1"
}
```

---

## Treasury Layer Specifics

### Key Differences from Landing/Refinery

| Aspect | Landing/Refinery | Treasury |
|--------|-----------------|----------|
| **Processing Mode** | Streaming or Batch | **Batch Only** |
| **Source** | External files/streams | Refinery tables |
| **Purpose** | Capture/Clean data | Aggregate/Summarize |
| **Updates** | Incremental | Full refresh or incremental aggregations |
| **Schema** | Source-driven | Business metrics-driven |

### Treasury Configuration Example

```json
{
  "data_flow_id": "3001",
  "data_flow_group": "analytics_treasury",
  "source_format": "delta",  // Always delta (reads from refinery)

  "source_details": {
    "catalog": "my_catalog",
    "database": "refinery",
    "table": "orders_cleaned"
  },

  // No landing/refinery config needed
  "treasury_catalog_prod": "my_catalog",
  "treasury_database_prod": "treasury",
  "treasury_table": "daily_revenue",
  "treasury_table_comment": "Daily revenue by product category",

  "treasury_transformation_json_prod": "/transforms/daily_revenue.json",

  "treasury_cluster_by": ["date", "category"],

  "treasury_table_properties": {
    "pipelines.autoOptimize.managed": "true",
    "pipelines.autoOptimize.zOrderCols": "date,category"
  },

  "treasury_data_quality_expectations_json_prod": "/dqe/daily_revenue_dqe.json"
}
```

### Treasury Transformation Example

**File:** `/transforms/daily_revenue.json`

```json
{
  "transformation_id": "daily_revenue_summary",
  "sql_query": "
    SELECT
      DATE(order_timestamp) as order_date,
      product_category,
      COUNT(DISTINCT order_id) as total_orders,
      COUNT(DISTINCT customer_id) as unique_customers,
      SUM(order_amount) as total_revenue,
      AVG(order_amount) as avg_order_value,
      PERCENTILE(order_amount, 0.5) as median_order_value,
      MIN(order_timestamp) as first_order_time,
      MAX(order_timestamp) as last_order_time
    FROM source_orders_cleaned
    WHERE order_status = 'COMPLETED'
    GROUP BY DATE(order_timestamp), product_category
  "
}
```

### When to Use Treasury Layer

✅ **Use Treasury for:**
- Daily/monthly aggregations
- Business KPIs and metrics
- Executive dashboards
- Slowly changing aggregates
- Data marts for BI tools

❌ **Don't use Treasury for:**
- Raw data storage (use landing)
- Row-level data (use refinery)
- Real-time streaming (use refinery)
- Frequent updates (use refinery)
- CDC/SCD operations (use refinery)

### Pipeline Configuration for Treasury

To run a pipeline that includes treasury layer:

```json
{
  "layer": "landing_refinery_treasury",  // All three layers
  "landing.group": "sales",
  "refinery.group": "sales",
  "treasury.group": "sales_analytics",  // Can be different group
  "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec",
  "refinery.dataflowspecTable": "catalog.schema.refinery_dataflowspec",
  "treasury.dataflowspecTable": "catalog.schema.treasury_dataflowspec"
}
```

**Or just treasury layer:**

```json
{
  "layer": "treasury",  // Treasury only
  "treasury.group": "sales_analytics",
  "treasury.dataflowspecTable": "catalog.schema.treasury_dataflowspec"
}
```

---

## See Also

- [Full Reference Guide](ONBOARDING_FILE_REFERENCE.md) - Complete documentation
- [CDC Methods Comparison](CDC_METHODS_COMPARISON.md) - Compare all CDC approaches
- [cdcApplyChanges](CDC_CONFIGURATION.md) - Traditional CDC with operation columns
- [applyChangesFromSnapshot](CDC_APPLY_CHANGES_FROM_SNAPSHOT.md) - Snapshot-based CDC
- [Delta Change Data Feed](DELTA_CHANGE_DATA_FEED.md) - Delta CDF approach
- [DQE Guide](DQE_GUIDE.md) - Data quality expectations
- [Organizing Large Scale ETLs](ORGANIZING_LARGE_SCALE_ETLS.md) - 100s of tables
- [Template Files](../demo/conf/) - Example configurations
