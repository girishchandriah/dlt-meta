# Lakeflow Connect Guide
## Ingesting External Data into Delta Live Tables

**Version:** 1.0
**Date:** February 23, 2025
**Author:** Platform Data Engineering Team

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [What is Lakeflow Connect?](#what-is-lakeflow-connect)
3. [Architecture & Components](#architecture--components)
4. [Supported Data Sources](#supported-data-sources)
5. [Implementation Patterns](#implementation-patterns)
6. [Comparison with Existing Approaches](#comparison-with-existing-approaches)
7. [Use Cases & Examples](#use-cases--examples)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [References](#references)

---

## Executive Summary

**Lakeflow Connect** is Databricks' managed ingestion solution that simplifies loading data from external sources into Delta Live Tables (DLT) pipelines.

### Key Capabilities

| Aspect | Description |
|--------|-------------|
| **Purpose** | Simplified data ingestion into DLT pipelines |
| **Integration** | Native DLT integration with `dlt.read_stream()` |
| **Sources** | Kafka, S3, JDBC, REST APIs, and more |
| **Management** | Fully managed by Databricks |
| **Schema** | Automatic schema inference and evolution |
| **CDC** | Built-in Change Data Capture support |

### When to Use Lakeflow Connect

**✅ Use Lakeflow Connect When:**
- Ingesting from supported external sources (Kafka, S3, JDBC)
- Want fully managed ingestion (no maintenance burden)
- Need automatic schema evolution
- Building new DLT pipelines from scratch
- Want simplified configuration

**❌ Don't Use Lakeflow Connect When:**
- Using DLT-META framework (has its own ingestion patterns)
- Complex custom ingestion logic required
- Source not supported by Lakeflow Connect
- Already have working Platform Notebooks ingestion

---

## What is Lakeflow Connect?

### Overview

Lakeflow Connect is Databricks' answer to simplified data ingestion in DLT pipelines. It provides:

1. **Declarative Configuration** - Define what to ingest, not how
2. **Managed Connectors** - Pre-built connectors for common sources
3. **Automatic Schema Management** - Infer and evolve schemas automatically
4. **Native DLT Integration** - Seamlessly use in DLT pipelines

### How It Differs from Traditional Approaches

```
TRADITIONAL APPROACH                    LAKEFLOW CONNECT
(Platform Notebooks/DLT-META):          (Simplified):

┌──────────────────────┐                ┌──────────────────────┐
│ 1. Configure reader  │                │ 1. Define connection │
│    - Format settings │                │    - Source type     │
│    - Schema DDL      │                │    - Credentials     │
│    - Options         │                │    - Path/topic      │
└──────────┬───────────┘                └──────────┬───────────┘
           │                                       │
           ▼                                       ▼
┌──────────────────────┐                ┌──────────────────────┐
│ 2. Write read logic  │                │ 2. Use dlt.read()    │
│    - spark.readStream│                │    - Auto-configured │
│    - Error handling  │                │    - Auto schema     │
└──────────┬───────────┘                └──────────┬───────────┘
           │                                       │
           ▼                                       ▼
┌──────────────────────┐                ┌──────────────────────┐
│ 3. Schema management │                │ 3. Done!             │
│    - DDL files       │                │    (managed for you) │
│    - Evolution logic │                │                      │
└──────────────────────┘                └──────────────────────┘

Manual configuration                    Declarative config
Multiple steps                          Single step
You maintain                            Databricks maintains
```

### Key Benefits

1. **Simplified Configuration**
   - No need to write complex `readStream` code
   - No manual schema DDL files
   - No checkpoint management

2. **Automatic Schema Evolution**
   - Detects schema changes automatically
   - Adds new columns without breaking pipelines
   - Handles schema drift gracefully

3. **Built-in Best Practices**
   - Optimized read patterns
   - Automatic checkpointing
   - Error handling and retry logic

4. **Managed Maintenance**
   - Databricks handles connector updates
   - Security patches applied automatically
   - Performance optimizations included

---

## Architecture & Components

### Lakeflow Connect Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Lakeflow Connect Pipeline                    │
└─────────────────────────────────────────────────────────────────┘

EXTERNAL SOURCE                    LAKEFLOW CONNECT               DLT PIPELINE
┌──────────────┐                   ┌──────────────┐              ┌──────────┐
│              │                   │              │              │          │
│   Kafka      │──── Topic ───────▶│  Connector   │──── Stream ─▶│ Landing  │
│   Topic      │    Config         │  (Managed)   │   Reader    │ Table    │
│              │                   │              │              │          │
└──────────────┘                   └──────────────┘              └────┬─────┘
                                                                      │
┌──────────────┐                   ┌──────────────┐                  ▼
│              │                   │              │              ┌──────────┐
│   S3         │──── Path ────────▶│  Auto Loader │──── Stream ─▶│ Refinery │
│   Bucket     │    Options        │  (Managed)   │   Reader    │ Table    │
│              │                   │              │              │          │
└──────────────┘                   └──────────────┘              └────┬─────┘
                                                                      │
┌──────────────┐                   ┌──────────────┐                  ▼
│              │                   │              │              ┌──────────┐
│   JDBC       │──── Table ────────▶│  JDBC Read   │──── Batch ──▶│ Treasury │
│   Database   │    Query          │  (Managed)   │   Reader    │ Table    │
│              │                   │              │              │          │
└──────────────┘                   └──────────────┘              └──────────┘

         │                                │                            │
         └──── CONFIGURATION ─────────────┴───────── PIPELINE CODE ───┘
              (Connection settings)               (dlt.read_stream())
```

### Core Components

#### 1. **Connection Configuration**

```python
# Define a connection (stored in Databricks Workspace)
connection_config = {
  "name": "my-s3-connection",
  "connection_type": "s3",
  "options": {
    "path": "s3://my-bucket/data/",
    "format": "csv",
    "delimiter": "|"
  }
}
```

#### 2. **DLT Integration**

```python
import dlt

# Use connection in DLT pipeline
@dlt.table(
  name="landing_table"
)
def ingest_data():
  return (
    dlt.read_stream("my-s3-connection")  # ← Lakeflow Connect!
  )
```

#### 3. **Automatic Schema Management**

```
First Run:                       Schema Change:
┌───────────────┐                ┌───────────────┐
│ col1: string  │                │ col1: string  │
│ col2: int     │                │ col2: int     │
│ col3: date    │                │ col3: date    │
└───────────────┘                │ col4: string  │ ← NEW!
                                 └───────────────┘
     ↓                                ↓
Auto-inferred schema             Auto-evolved schema
No DDL file needed!              No pipeline break!
```

---

## Supported Data Sources

### 1. **Amazon S3 (Cloud Files / Auto Loader)**

**Use Case:** Ingesting CSV, JSON, Parquet files from S3

**Configuration:**

```python
# Create S3 connection
connection = {
  "name": "sales-order-s3",
  "connection_type": "s3",
  "options": {
    "path": "s3a://prd2612-us-west-2-prelanding/prod/sales_orders/",
    "format": "csv",
    "header": "false",
    "delimiter": "|",
    "cloudFiles.schemaLocation": "/checkpoint/schema",
    "cloudFiles.rescuedDataColumn": "_rescued_data"
  }
}

# Use in DLT pipeline
@dlt.table(name="sales_orders_landing")
def ingest_sales_orders():
  return dlt.read_stream("sales-order-s3")
```

**Key Features:**
- ✅ Incremental file processing (Auto Loader)
- ✅ Schema inference and evolution
- ✅ Handles schema drift with rescued data column
- ✅ Automatic checkpoint management

### 2. **Apache Kafka**

**Use Case:** Real-time streaming from Kafka topics

**Configuration:**

```python
connection = {
  "name": "event-kafka",
  "connection_type": "kafka",
  "options": {
    "kafka.bootstrap.servers": "kafka-broker:9092",
    "subscribe": "events-topic",
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN"
  }
}

@dlt.table(name="events_landing")
def ingest_events():
  return (
    dlt.read_stream("event-kafka")
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
  )
```

**Key Features:**
- ✅ Real-time streaming ingestion
- ✅ Automatic offset management
- ✅ Support for multiple topics
- ✅ Built-in authentication

### 3. **JDBC Databases**

**Use Case:** Batch or streaming from relational databases

**Configuration:**

```python
connection = {
  "name": "mysql-source",
  "connection_type": "jdbc",
  "options": {
    "url": "jdbc:mysql://db.example.com:3306/mydb",
    "dbtable": "orders",
    "user": "readonly_user",
    "password": "{{secrets/db/password}}",  # Use Databricks Secrets!
    "driver": "com.mysql.jdbc.Driver"
  }
}

@dlt.table(name="orders_landing")
def ingest_orders():
  return dlt.read("mysql-source")  # Batch read
```

**Key Features:**
- ✅ Support for MySQL, PostgreSQL, SQL Server, Oracle
- ✅ Incremental reads via watermark columns
- ✅ Connection pooling
- ✅ Secure credential management

### 4. **REST APIs**

**Use Case:** Ingesting data from REST endpoints

**Configuration:**

```python
connection = {
  "name": "api-source",
  "connection_type": "http",
  "options": {
    "url": "https://api.example.com/v1/data",
    "method": "GET",
    "headers": {
      "Authorization": "Bearer {{secrets/api/token}}"
    },
    "pagination": {
      "type": "link",
      "next_link_key": "next_page"
    }
  }
}

@dlt.table(name="api_data_landing")
def ingest_api_data():
  return dlt.read_stream("api-source")
```

**Key Features:**
- ✅ Automatic pagination handling
- ✅ Rate limiting support
- ✅ Retry logic with exponential backoff
- ✅ Support for various auth methods

### 5. **Azure Blob Storage / ADLS**

**Use Case:** Ingesting files from Azure storage

**Configuration:**

```python
connection = {
  "name": "azure-blob",
  "connection_type": "azure_blob",
  "options": {
    "path": "abfss://container@account.dfs.core.windows.net/data/",
    "format": "parquet",
    "cloudFiles.useNotifications": "true"
  }
}

@dlt.table(name="azure_data_landing")
def ingest_azure_data():
  return dlt.read_stream("azure-blob")
```

---

## Implementation Patterns

### Pattern 1: Simple File Ingestion (S3 → Landing)

**Use Case:** Replace Platform Notebooks' `delta_from_s3_uc` service

**Before (Platform Notebooks):**

```yaml
# In job_configs_airflow YAML
service_name: delta_from_s3_uc
service_config:
  s3_file_path: s3a://bucket/path/to/files
  target_db: landing.landing_nonprod
  target_table: my_table
  load_options:
    format: csv
    options:
      header: false
      sep: '|'
    target_table_schema: |
      col1 string,
      col2 int,
      col3 date
```

**After (Lakeflow Connect in DLT):**

```python
import dlt

# Define connection (one-time setup in Databricks)
# UI: Catalog → Connections → Create → S3

@dlt.table(
  name="my_table",
  comment="Landing table ingested via Lakeflow Connect"
)
def landing_table():
  return (
    dlt.read_stream("my-s3-connection")
    # Schema automatically inferred!
    # No DDL file needed!
  )
```

**Benefits:**
- 🎯 No YAML configuration
- 🎯 No schema DDL file
- 🎯 Automatic schema evolution
- 🎯 Native DLT integration

### Pattern 2: Streaming with Transformations

**Use Case:** Ingest + apply transformations in one pipeline

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(name="sales_orders_raw")
def ingest_raw():
  # Lakeflow Connect reads from S3
  return dlt.read_stream("sales-orders-s3")

@dlt.table(name="sales_orders_cleaned")
def clean_data():
  # Read from previous DLT table and transform
  return (
    dlt.read_stream("sales_orders_raw")
    .withColumn("order_date", F.to_date("order_date_str"))
    .withColumn("amount", F.col("amount").cast("decimal(18,4)"))
    .filter(F.col("status") != "CANCELLED")
  )
```

**Flow:**

```
S3 Files → [Lakeflow Connect] → sales_orders_raw (DLT)
                                       ↓
                            [DLT Transformation]
                                       ↓
                             sales_orders_cleaned (DLT)
```

### Pattern 3: Multi-Source Ingestion

**Use Case:** Combine data from multiple sources

```python
import dlt

# Source 1: S3 files
@dlt.table(name="orders_from_s3")
def ingest_s3_orders():
  return dlt.read_stream("orders-s3-connection")

# Source 2: Kafka stream
@dlt.table(name="orders_from_kafka")
def ingest_kafka_orders():
  return (
    dlt.read_stream("orders-kafka-connection")
    .selectExpr("CAST(value AS STRING) as json_data")
    .select(F.from_json("json_data", order_schema).alias("data"))
    .select("data.*")
  )

# Source 3: MySQL database
@dlt.table(name="orders_from_mysql")
def ingest_mysql_orders():
  return dlt.read("orders-mysql-connection")

# Combine all sources
@dlt.table(name="orders_unified")
def unify_orders():
  s3_orders = dlt.read("orders_from_s3")
  kafka_orders = dlt.read("orders_from_kafka")
  mysql_orders = dlt.read("orders_from_mysql")

  return s3_orders.unionByName(kafka_orders).unionByName(mysql_orders)
```

### Pattern 4: CDC with Apply Changes

**Use Case:** Ingest CDC data from database and apply to Delta table

```python
import dlt

@dlt.table(name="customer_cdc_raw")
def ingest_cdc():
  # Lakeflow Connect reads CDC stream
  return dlt.read_stream("customer-cdc-connection")

# Apply CDC changes to target table
dlt.create_streaming_table(name="customers")

dlt.apply_changes(
  target="customers",
  source="customer_cdc_raw",
  keys=["customer_id"],
  sequence_by="updated_timestamp",
  stored_as_scd_type="2",  # Type 2 SCD for history
  except_column_list=["_metadata"]
)
```

**Flow:**

```
MySQL CDC → [Lakeflow Connect] → customer_cdc_raw (DLT)
                                        ↓
                            [DLT apply_changes]
                                        ↓
                              customers (Delta Table)
                              - Maintains history
                              - Automatic CDC merge
```

---

## Comparison with Existing Approaches

### Platform Notebooks vs Lakeflow Connect

| Aspect | Platform Notebooks | Lakeflow Connect |
|--------|-------------------|------------------|
| **Configuration** | YAML files (complex) | Connection UI (simple) |
| **Schema Management** | Manual DDL files | Automatic inference |
| **Code Required** | Custom Python notebooks | Minimal DLT code |
| **Maintenance** | You maintain | Databricks maintains |
| **Schema Evolution** | Manual handling | Automatic |
| **Error Handling** | Custom logic | Built-in |
| **Checkpointing** | Manual setup | Automatic |
| **Best For** | Existing pipelines | New DLT pipelines |

### DLT-META vs Lakeflow Connect

| Aspect | DLT-META | Lakeflow Connect |
|--------|----------|------------------|
| **Approach** | JSON config framework | Native DLT feature |
| **Flexibility** | High (custom patterns) | Medium (standard patterns) |
| **Landing Layer** | cloudFiles in JSON | Connection object |
| **Organizational** | ✅ Recommended standard | Alternative for simple cases |
| **Use Case** | Enterprise-scale pipelines | Simple ingestion tasks |
| **Learning Curve** | Steep (framework + DLT) | Low (just DLT) |

### When to Use Each Approach

```
┌────────────────────────────────────────────────────────────────┐
│                    Decision Framework                           │
└────────────────────────────────────────────────────────────────┘

START: Need to ingest data
         │
         ▼
┌────────────────────┐
│ Using DLT-META     │
│ framework?         │
└────────┬───────────┘
         │
    YES  │  NO
         │   └──────────────────┐
         ▼                      ▼
┌────────────────────┐  ┌──────────────────┐
│ Use DLT-META       │  │ Building new     │
│ landing config     │  │ DLT pipeline?    │
│ (cloudFiles JSON)  │  └────────┬─────────┘
└────────────────────┘           │
                            YES  │  NO
                                 │   └────────────────┐
                                 ▼                    ▼
                        ┌────────────────┐  ┌─────────────────┐
                        │ Lakeflow       │  │ Platform        │
                        │ Connect        │  │ Notebooks       │
                        │ (recommended)  │  │ (keep existing) │
                        └────────────────┘  └─────────────────┘
```

---

## Use Cases & Examples

### Example 1: Sales Order Transaction Ingestion

**Scenario:** Replace Platform Notebooks S3 ingestion (Step 1 of job 0204)

**Current Implementation (Platform Notebooks):**

```yaml
job_steps:
  - job_step:
      step_name: step-1
      service_name: delta_from_s3_uc
      service_config:
        s3_file_path: s3a://.../jb_edw_dsn_sales_ord_tran_0204/file-ingest
        target_db: landing.landing_nonprod
        target_table: pfocusst_ff_src_hst_dsn_trans
        load_options:
          format: csv
          options:
            header: false
            sep: '|'
          target_table_schema: |
            system string,
            vaxacct string,
            accountdate string,
            ... (85 columns)
```

**With Lakeflow Connect:**

```python
# 1. Create connection (one-time, via Databricks UI)
# Catalog → Connections → Create → S3
# Name: sales-order-tran-s3
# Path: s3a://.../jb_edw_dsn_sales_ord_tran_0204/file-ingest
# Format: CSV, delimiter: |, no header

# 2. DLT Pipeline
import dlt

@dlt.table(
  name="pfocusst_ff_src_hst_dsn_trans",
  comment="Sales order transactions from JB EDW",
  table_properties={
    "quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def landing_sales_order_transactions():
  return dlt.read_stream("sales-order-tran-s3")
  # No schema DDL needed!
  # Automatically infers all 85 columns!
```

**Benefits:**
- ❌ No 85-column schema DDL to maintain
- ❌ No YAML configuration
- ✅ Automatic schema evolution (new columns auto-added)
- ✅ Simpler code (3 lines vs 85+ lines)

### Example 2: Real-Time Event Streaming from Kafka

**Scenario:** Ingest customer events from Kafka topic

**Implementation:**

```python
import dlt
from pyspark.sql import functions as F

# Step 1: Define connection (via UI or API)
# Name: customer-events-kafka
# Type: Kafka
# Bootstrap servers: kafka.livenation.com:9092
# Topic: customer.events
# Auth: SASL_SSL

# Step 2: DLT Pipeline
@dlt.table(
  name="customer_events_raw",
  comment="Raw customer events from Kafka"
)
def ingest_customer_events():
  return (
    dlt.read_stream("customer-events-kafka")
    .selectExpr(
      "CAST(key AS STRING) as event_key",
      "CAST(value AS STRING) as event_json",
      "timestamp as event_timestamp",
      "partition",
      "offset"
    )
  )

@dlt.table(
  name="customer_events_parsed"
)
def parse_events():
  from pyspark.sql.types import StructType, StructField, StringType, TimestampType

  event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType())
  ])

  return (
    dlt.read_stream("customer_events_raw")
    .select(
      F.from_json("event_json", event_schema).alias("event"),
      "event_timestamp",
      "offset"
    )
    .select("event.*", "event_timestamp", "offset")
  )
```

**Flow:**

```
Kafka Topic               DLT Pipeline
┌─────────────┐          ┌───────────────────────┐
│ customer.   │──Stream─▶│ customer_events_raw   │
│ events      │          └───────┬───────────────┘
└─────────────┘                  │
                                 ▼ parse JSON
                          ┌───────────────────────┐
                          │ customer_events_parsed│
                          └───────────────────────┘
```

### Example 3: Incremental JDBC Ingestion

**Scenario:** Daily incremental load from MySQL

**Implementation:**

```python
import dlt

# Connection configuration (via UI)
# Name: mysql-orders
# Type: JDBC
# URL: jdbc:mysql://db.example.com:3306/orders_db
# Table: orders
# Incremental column: updated_at

@dlt.table(
  name="orders_incremental"
)
def ingest_orders_incremental():
  return (
    dlt.read("mysql-orders")  # Batch read with watermark
    .filter("status IN ('CONFIRMED', 'SHIPPED')")
  )

# DLT automatically handles:
# - Tracking last watermark (max updated_at)
# - Only reading new/changed records
# - No duplicates
```

**How Incremental Works:**

```
First Run:                    Second Run:
┌──────────────────┐          ┌──────────────────┐
│ MySQL: orders    │          │ MySQL: orders    │
│ updated_at       │          │ updated_at       │
│ ─────────────    │          │ ─────────────    │
│ 2025-02-01       │  Read    │ 2025-02-01       │  Skip (already read)
│ 2025-02-02       │  All     │ 2025-02-02       │  Skip
│ 2025-02-03       │  ─────▶  │ 2025-02-03       │  Skip
│                  │          │ 2025-02-04       │  Read (new!)
│                  │          │ 2025-02-05       │  Read (new!)
└──────────────────┘          └──────────────────┘
    ↓                             ↓
Watermark: 2025-02-03        Watermark: 2025-02-05
```

### Example 4: Schema Evolution Handling

**Scenario:** Source schema changes over time

**Implementation:**

```python
import dlt

@dlt.table(
  name="evolving_data",
  comment="Table with automatic schema evolution"
)
def ingest_evolving_data():
  return (
    dlt.read_stream("evolving-s3-connection")
    # Lakeflow Connect automatically handles schema changes!
  )

# Month 1: File has columns A, B, C
# Month 2: File has columns A, B, C, D (new!)
# Month 3: File has columns A, B, C, D, E (new!)
#
# Pipeline continues without breaking!
# New columns automatically added to table
```

**With Rescued Data Column:**

```python
@dlt.table(
  name="evolving_data_safe",
  comment="Safe schema evolution with rescued data"
)
def ingest_with_rescue():
  return (
    dlt.read_stream("evolving-s3-connection")
    # Configure connection with:
    # cloudFiles.rescuedDataColumn: "_rescued_data"
  )

# If schema doesn't match:
# - Valid columns go to their columns
# - Invalid/extra data goes to _rescued_data
# - Pipeline doesn't break!
```

---

## Best Practices

### 1. Connection Management

**✅ DO:**

```python
# Store connections in Databricks Catalog
# Catalog → Connections → Create

# Use descriptive names
connection_name = "sales-orders-s3-prod"

# Use Databricks Secrets for credentials
"password": "{{secrets/prod-db/password}}"

# Document connection purpose
"comment": "Production sales orders from JB EDW S3 bucket"
```

**❌ DON'T:**

```python
# Don't hardcode credentials
"password": "mypassword123"

# Don't use generic names
connection_name = "connection1"

# Don't mix environments in one connection
connection_name = "orders-nonprod-and-prod"  # Bad!
```

### 2. Schema Management

**✅ DO:**

```python
# Let Lakeflow Connect infer schema
@dlt.table(name="auto_schema")
def ingest():
  return dlt.read_stream("my-connection")
  # Schema automatically managed!

# Use rescued data column for safety
connection_options = {
  "cloudFiles.rescuedDataColumn": "_rescued_data"
}

# Monitor schema evolution
@dlt.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
```

**❌ DON'T:**

```python
# Don't manually specify schema unless necessary
schema = StructType([
  StructField("col1", StringType()),
  # ... 100 columns
])
df = dlt.read_stream("connection").schema(schema)  # Defeats purpose!
```

### 3. Error Handling

**✅ DO:**

```python
# Use DLT expectations for data quality
@dlt.table(name="orders_validated")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
def ingest_orders():
  return dlt.read_stream("orders-connection")

# Monitor quarantine table
@dlt.table(name="orders_quarantine")
def quarantine():
  return dlt.read("orders_validated__expectations")
```

**❌ DON'T:**

```python
# Don't ignore errors silently
@dlt.table(name="orders")
def ingest():
  try:
    return dlt.read_stream("connection")
  except:
    pass  # Bad! No visibility into failures
```

### 4. Performance Optimization

**✅ DO:**

```python
# Use clustering for large tables
@dlt.table(
  name="large_table",
  cluster_by=["date", "customer_id"]  # Optimize queries
)
def ingest():
  return dlt.read_stream("large-data-connection")

# Partition by date for time-series data
@dlt.table(
  name="events",
  partition_cols=["event_date"]
)
def ingest_events():
  return (
    dlt.read_stream("events-connection")
    .withColumn("event_date", F.to_date("timestamp"))
  )

# Use appropriate trigger intervals
@dlt.table(
  name="streaming_data",
  trigger={"processingTime": "5 minutes"}  # Don't process too frequently
)
def ingest():
  return dlt.read_stream("connection")
```

### 5. Testing & Validation

**✅ DO:**

```python
# Test with small dataset first
# Set connection option: maxFilesPerTrigger: 10

# Validate row counts
@dlt.table(name="landing_validated")
@dlt.expect("has_data", "count(*) > 0")
def ingest():
  return dlt.read_stream("connection")

# Compare with source system
# Run reconciliation queries to verify data accuracy
```

---

## Troubleshooting

### Common Issues

#### Issue 1: Connection Fails to Authenticate

**Symptoms:**
```
Error: Failed to connect to source
Caused by: Authentication failed
```

**Solutions:**

1. **Check Databricks Secrets:**
   ```python
   # Verify secret exists
   dbutils.secrets.get(scope="my-scope", key="password")
   ```

2. **Verify IAM Permissions (S3):**
   ```bash
   # Cluster must have IAM role with S3 access
   # Check: Cluster → Configuration → AWS Attributes → Instance Profile
   ```

3. **Test Connection:**
   ```python
   # In notebook, test connection directly
   spark.read.format("csv").load("s3://bucket/path/")
   ```

#### Issue 2: Schema Inference Problems

**Symptoms:**
```
Error: Unable to infer schema
All columns are treated as strings
```

**Solutions:**

1. **Provide Schema Hints:**
   ```python
   # In connection options
   "inferSchema": "true",
   "samplingRatio": "1.0"  # Sample more data for better inference
   ```

2. **Use Schema DDL (Last Resort):**
   ```python
   from pyspark.sql.types import *

   schema = StructType([
     StructField("order_id", IntegerType()),
     StructField("amount", DecimalType(18, 4)),
     StructField("order_date", DateType())
   ])

   @dlt.table(name="with_schema")
   def ingest():
     return (
       dlt.read_stream("connection")
       .select(schema)
     )
   ```

#### Issue 3: Pipeline Performance Issues

**Symptoms:**
```
Pipeline takes too long to process
High memory usage
Frequent OOM errors
```

**Solutions:**

1. **Tune Auto Loader:**
   ```python
   # In S3 connection options
   "cloudFiles.maxFilesPerTrigger": "1000",
   "cloudFiles.maxBytesPerTrigger": "10g"
   ```

2. **Optimize Cluster:**
   ```python
   # Use larger cluster for initial load
   # Scale down for incremental processing

   # Enable optimized writes
   "pipelines.autoOptimize.zOrderCols": "date,customer_id"
   ```

3. **Batch vs Streaming:**
   ```python
   # For large historical loads, use batch
   @dlt.table(name="initial_load")
   def load():
     return dlt.read("connection")  # Batch

   # For ongoing processing, use streaming
   @dlt.table(name="incremental")
   def stream():
     return dlt.read_stream("connection")  # Streaming
   ```

#### Issue 4: Schema Evolution Breaks Pipeline

**Symptoms:**
```
Error: Schema mismatch
Cannot merge incompatible types
```

**Solutions:**

1. **Enable Schema Evolution:**
   ```python
   # In connection options
   "cloudFiles.schemaEvolutionMode": "addNewColumns",
   "mergeSchema": "true"
   ```

2. **Use Rescued Data Column:**
   ```python
   # In connection options
   "cloudFiles.rescuedDataColumn": "_rescued_data"

   # Monitor rescued data
   SELECT _rescued_data FROM table WHERE _rescued_data IS NOT NULL;
   ```

3. **Handle Type Changes:**
   ```python
   @dlt.table(name="with_type_handling")
   def ingest():
     return (
       dlt.read_stream("connection")
       .withColumn("amount", F.col("amount").cast("decimal(18,4)"))
     )
   ```

---

## Migration Path: Platform Notebooks → Lakeflow Connect

### Should You Migrate?

**✅ Consider Migration When:**
- Building new pipelines (not migrating existing)
- Want to simplify ingestion layer
- Schema changes frequently
- Want Databricks-managed solution

**❌ Don't Migrate When:**
- Using DLT-META framework (has its own patterns)
- Existing Platform Notebooks work well
- Complex custom ingestion logic
- Short on migration resources

### If You Decide to Migrate

**Phase 1: Proof of Concept**

```python
# Test Lakeflow Connect with one small pipeline
# Compare with existing Platform Notebooks output
# Validate schema, data quality, performance
```

**Phase 2: Parallel Run**

```python
# Run both Platform Notebooks AND Lakeflow Connect
# Write to different tables
# Compare results daily for 2 weeks
```

**Phase 3: Cutover**

```python
# After validation, switch to Lakeflow Connect
# Decommission Platform Notebooks
# Update downstream dependencies
```

---

## Comparison Summary Table

### Platform Notebooks vs DLT-META vs Lakeflow Connect

| Feature | Platform Notebooks | DLT-META | Lakeflow Connect |
|---------|-------------------|----------|------------------|
| **Configuration** | YAML (complex) | JSON (declarative) | UI + minimal code |
| **Schema Management** | Manual DDL | Manual DDL | Automatic |
| **Framework** | Spark Streaming | DLT Framework | DLT Native |
| **Maintenance** | You | You | Databricks |
| **Flexibility** | High | Medium | Low-Medium |
| **Learning Curve** | Medium | Steep | Low |
| **Org Standard** | Legacy | ✅ Current | Alternative |
| **Best For** | Existing pipelines | Enterprise DLT | New simple DLT |
| **Self-Reference** | ✅ Allowed | ❌ Prohibited | ❌ Prohibited |
| **Code Lines** | 1,500+ | 200 | 50 |

### Decision Matrix

```
┌────────────────────────────────────────────────────────────────┐
│              Which Approach Should I Use?                       │
└────────────────────────────────────────────────────────────────┘

┌─────────────────────┐
│ Building new        │
│ pipeline?           │
└──────┬──────────────┘
       │
    NO │ YES
       │  └────────────────────┐
       ▼                       ▼
┌───────────────┐     ┌────────────────────┐
│ Keep Platform │     │ Using DLT?         │
│ Notebooks     │     └──────┬─────────────┘
└───────────────┘            │
                         YES │ NO
                             │  └───────────────────┐
                             ▼                      ▼
                    ┌─────────────────┐    ┌────────────────┐
                    │ Enterprise-     │    │ Simple batch/  │
                    │ scale with many │    │ stream without │
                    │ pipelines?      │    │ DLT?           │
                    └──────┬──────────┘    └────────┬───────┘
                           │                        │
                      YES  │  NO                 YES│ NO
                           │   └─────────┐          │  └──────┐
                           ▼             ▼          ▼         ▼
                    ┌───────────┐  ┌─────────┐  ┌────────┐ ┌────┐
                    │ DLT-META  │  │Lakeflow │  │Platform│ │Ask │
                    │(Recommend)│  │Connect  │  │Notebooks│ │Team│
                    └───────────┘  └─────────┘  └────────┘ └────┘
```

---

## References

### Official Documentation

- [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/en/connect/index.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)

### Internal Documentation

- [DLT-META Implementation Guide](DLT-META_vs_Platform_Notebooks_Implementation_Guide.md)
- [Migration Guide: Sales Order Transaction 0204](Migration_Guide_Sales_Ord_Tran_0204.md)
- Confluence: [Databricks Services](https://confluence.livenation.com/spaces/DS/pages/476944262/Databricks+Services)

### Related Topics

- **DLT-META Framework** - Recommended organizational standard
- **Platform Notebooks** - Legacy approach for existing pipelines
- **Delta Live Tables** - Underlying framework for Lakeflow Connect
- **Auto Loader** - S3 ingestion engine used by Lakeflow Connect

---

## Quick Reference

### When to Use What?

```
┌──────────────────────────────────────────────────────────┐
│ QUICK DECISION GUIDE                                      │
└──────────────────────────────────────────────────────────┘

NEW Enterprise Pipeline with 10+ dataflows:
  → Use DLT-META ✅

NEW Simple Pipeline (1-3 dataflows):
  → Use Lakeflow Connect ✅

EXISTING Platform Notebooks Pipeline:
  → Keep as-is (don't migrate) ✅

EXISTING DLT-META Pipeline:
  → Keep using DLT-META (don't switch) ✅

Need Self-Referencing:
  → Use Platform Notebooks ⚠️ (or apply DLT-META workarounds)

Not Sure:
  → Ask data engineering team 💬
```

### Code Snippets

**Basic S3 Ingestion:**
```python
@dlt.table(name="landing")
def ingest():
  return dlt.read_stream("my-s3-connection")
```

**With Transformations:**
```python
@dlt.table(name="cleaned")
def ingest_and_clean():
  return (
    dlt.read_stream("connection")
    .filter("status != 'DELETED'")
    .withColumn("processed_at", F.current_timestamp())
  )
```

**With CDC:**
```python
@dlt.table(name="cdc_source")
def ingest():
  return dlt.read_stream("cdc-connection")

dlt.apply_changes(
  target="target_table",
  source="cdc_source",
  keys=["id"],
  sequence_by="updated_at"
)
```

---

## Conclusion

**Lakeflow Connect** is a powerful tool for **simplified data ingestion** in DLT pipelines. However, at LiveNation:

- **✅ DLT-META** remains the organizational standard for enterprise pipelines
- **✅ Lakeflow Connect** is a good option for new, simple DLT pipelines
- **✅ Platform Notebooks** should continue for existing pipelines

**Key Takeaway:** Lakeflow Connect reduces boilerplate code and automates schema management, but DLT-META's configuration-driven approach is better suited for large-scale, standardized implementations across the organization.

---

**Document Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-02-23 | Initial Lakeflow Connect guide created |
