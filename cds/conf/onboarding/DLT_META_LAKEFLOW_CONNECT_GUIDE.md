# dlt-meta with Lakeflow Connect: Complete Implementation Guide

## Table of Contents

1. [Overview](#overview)
2. [Lakeflow Connect with Federated Tables](#lakeflow-connect-with-federated-tables)
3. [Compute Requirements](#compute-requirements)
4. [Onboarding Instructions](#onboarding-instructions)
5. [Drawbacks and Solutions](#drawbacks-and-solutions)
6. [Kafka Implementation](#kafka-implementation)
7. [SSL Certificate Management](#ssl-certificate-management)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

---

## Overview

This guide provides comprehensive instructions for implementing dlt-meta with Lakeflow Connect, covering federated table ingestion from external databases and Kafka streaming sources. It addresses key architectural decisions, firewall requirements, and operational considerations for production deployments.

### Key Technologies

- **dlt-meta**: Declarative framework for Delta Live Tables pipelines
- **Lakeflow Connect**: Databricks federated query engine for external databases
- **Delta Live Tables (DLT)**: Declarative ETL framework for Delta Lake
- **Kafka**: Distributed streaming platform with Schema Registry integration

---

## Lakeflow Connect with Federated Tables

### What is Lakeflow Connect?

Lakeflow Connect enables Databricks to query external databases (Oracle, MySQL, PostgreSQL, SQL Server) without data movement. It uses Lakehouse Federation to create connections and federated tables/views that can be queried like native Delta tables.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Source System (Oracle/MySQL/PostgreSQL)                        │
│ - Production database with JOINs and complex queries           │
│ - Firewall-protected                                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                        JDBC Connection
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Databricks Lakeflow Connect (Federated Connection)             │
│ - Queries executed in source system                            │
│ - Results streamed to Databricks                               │
│ - Requires: Classic Compute (for firewall access)              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ dlt-meta DLT Pipeline                                           │
│ - Landing Layer: Raw ingestion                                 │
│ - Refinery Layer: CDC + transformations                        │
└─────────────────────────────────────────────────────────────────┘
```

### Benefits

✅ **No ETL infrastructure**: Query databases directly without intermediate ETL jobs
✅ **Reduced data movement**: Only changed records transferred
✅ **Simplified architecture**: Federated views replace complex staging layers
✅ **Real-time access**: Query production data without replication lag
✅ **Declarative configuration**: JSON-based dlt-meta specifications

### Limitations

❌ **Firewall restrictions**: Source databases must allow connections from Databricks
❌ **Performance overhead**: Network latency for remote queries
❌ **Classic compute required**: Cannot use serverless compute (firewall limitations)
❌ **Source database load**: Complex queries executed on source system

---

## Compute Requirements

### ⚠️ Critical: Classic Compute vs Serverless

| Feature | Serverless Compute | Classic Compute | Required For |
|---------|-------------------|-----------------|--------------|
| **Firewall Configuration** | Dynamic IP ranges (requires broad firewall rules) | Static IP ranges (predictable) | Classic Compute |
| **Lakeflow Connect** | ❌ Not supported (firewall issues) | ✅ Supported | **Classic Compute** |
| **Kafka with JKS** | ⚠️ Limited support | ✅ Full support | **Classic Compute** |
| **Volume Access** | ✅ Supported | ✅ Supported | Both |
| **Cost** | Pay-per-use (cheaper for bursty workloads) | Pay-per-hour (predictable) | Varies |

### Why Classic Compute is Required

**Firewall Security Requirements:**
- Most enterprise databases restrict connections to specific IP ranges
- Serverless compute uses dynamic IP addresses that change frequently
- Opening firewalls to all serverless IP ranges creates security risks
- Classic compute provides static, predictable IP ranges

**Current Implementation:**
```json
{
  "cluster_mode": "LEGACY",
  "cluster_policy": "classic_compute_policy",
  "num_workers": 2,
  "node_type_id": "i3.xlarge"
}
```

### Future Serverless Migration

**If your organization opens firewall to serverless IP ranges:**

1. Update DLT pipeline configuration to use serverless
2. Remove `cluster_mode: LEGACY` setting
3. Enable serverless channel: `channel: "CURRENT"`
4. Test federated connections still work
5. Monitor for connection timeouts

**Trade-offs:**
- ✅ Lower cost for variable workloads
- ✅ Faster startup times
- ❌ Requires broad firewall rules
- ❌ Less predictable IP addresses

---

## Onboarding Instructions

### Step 1: Prerequisites

#### 1.1 Database Access

- [ ] Database credentials (username, password)
- [ ] JDBC connection string
- [ ] Read permissions on source tables
- [ ] Network connectivity from Databricks workspace to database

#### 1.2 Databricks Configuration

- [ ] Unity Catalog enabled
- [ ] Classic compute cluster available
- [ ] dlt-meta CLI installed (`databricks labs install dlt-meta`)
- [ ] Appropriate IAM permissions for DLT pipeline creation

#### 1.3 Firewall Configuration

**Required Firewall Rules:**

```
Source: Databricks Classic Compute IP Range (e.g., 10.0.0.0/16)
Destination: Database Server IP/Hostname
Port: 1521 (Oracle), 3306 (MySQL), 5432 (PostgreSQL), 1433 (SQL Server)
Protocol: TCP
```

**How to find Databricks IP ranges:**

```sql
-- In Databricks SQL
SELECT * FROM system.networking.ip_access_list;
```

Or contact your Databricks account team for static IP ranges.

### Step 2: Create Federated Connection

#### 2.1 Using Databricks UI

1. Navigate to **Catalog** → **External Data** → **Connections**
2. Click **Create Connection**
3. Fill in details:
   - **Name**: `my_oracle_connection` (example)
   - **Connection Type**: Oracle / MySQL / PostgreSQL
   - **Host**: `db.example.com`
   - **Port**: `1521`
   - **Database**: `production_db`
   - **Username**: `readonly_user`
   - **Password**: Store in Databricks Secrets

#### 2.2 Using SQL

```sql
CREATE CONNECTION my_oracle_connection
TYPE oracle
OPTIONS (
  host 'db.example.com',
  port '1521',
  user 'readonly_user',
  password secret('db_credentials', 'oracle_password')
);
```

#### 2.3 Test Connection

```sql
-- List available schemas
SHOW SCHEMAS IN my_oracle_connection;

-- Test query
SELECT COUNT(*) FROM my_oracle_connection.member.customers LIMIT 10;
```

### Step 3: Create Federated Views in Source System

**Why create views in the source database?**

Creating views in the source system (Oracle/MySQL/PostgreSQL) offers several advantages over querying base tables directly:

✅ **Performance**: Indexes and query optimization done by source database
✅ **Security**: Limit columns/rows exposed to Databricks
✅ **Simplicity**: Complex JOINs encapsulated in single view
✅ **Change isolation**: Schema changes absorbed by view layer
✅ **Incremental filtering**: Date/timestamp filters reduce data transfer

#### 3.1 Example: Oracle View for CDC

**Execute in Oracle database (requires DBA or schema owner):**

```sql
-- Create view that returns only last 24 hours of changes
CREATE OR REPLACE VIEW VW_MEMBER_CUSTOMER_CDC AS
SELECT
    m.CUSTOMER_ID,
    m.FIRST_NAME,
    m.MIDDLE_NAME,
    m.LAST_NAME,
    c.UC_EMAIL_ADDRESS,
    m.CREATED,
    m.ACTIVE,
    m.GENDER,
    m.LAST_UPDATE,
    co.COUNTRY_ID,
    co.ABBREV as COUNTRY_ABBREV,
    m.PHONE_NUMBER as MBR_PHN_NUM,
    CURRENT_TIMESTAMP as EXTRACTION_TIMESTAMP
FROM
    MEMBER.MEMBERS m
LEFT OUTER JOIN
    MEMBER.CUSTOMERS c ON m.CUSTOMER_ID = c.CUSTOMER_ID
LEFT OUTER JOIN
    MEMBER.COUNTRY co ON m.COUNTRY_ID = co.COUNTRY_ID
WHERE
    m.LAST_UPDATE >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
    OR COALESCE(c.LAST_UPDATE, TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD'))
       >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

-- Grant read access to Databricks user
GRANT SELECT ON VW_MEMBER_CUSTOMER_CDC TO readonly_user;

-- Create indexes to optimize view queries
CREATE INDEX IDX_MEMBERS_LAST_UPDATE ON MEMBER.MEMBERS(LAST_UPDATE);
CREATE INDEX IDX_MEMBERS_CUSTOMER_ID ON MEMBER.MEMBERS(CUSTOMER_ID);
CREATE INDEX IDX_CUSTOMERS_CUSTOMER_ID ON MEMBER.CUSTOMERS(CUSTOMER_ID);
```

#### 3.2 Alternative: Databricks Federated View

If you cannot create views in the source database, create federated views in Databricks:

```sql
-- Create federated view in Databricks
CREATE OR REPLACE VIEW my_catalog.landing.vw_member_customer_cdc AS
SELECT
    m.CUSTOMER_ID,
    m.FIRST_NAME,
    m.MIDDLE_NAME,
    m.LAST_NAME,
    c.UC_EMAIL_ADDRESS,
    m.CREATED,
    m.ACTIVE,
    m.GENDER,
    m.LAST_UPDATE,
    co.COUNTRY_ID,
    co.ABBREV as COUNTRY_ABBREV
FROM
    my_oracle_connection.member.members m
LEFT OUTER JOIN
    my_oracle_connection.member.customers c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
LEFT OUTER JOIN
    my_oracle_connection.member.country co
    ON m.COUNTRY_ID = co.COUNTRY_ID
WHERE
    m.LAST_UPDATE >= current_timestamp() - INTERVAL '24' HOURS;
```

**Trade-offs:**

| Approach | Performance | Security | Maintenance |
|----------|------------|----------|-------------|
| **Source DB View** | ⚡ Fastest (source DB optimized) | ✅ Best (least exposure) | ⚠️ Requires DB access |
| **Databricks Federated View** | 🐌 Slower (network overhead) | ⚠️ Exposes base tables | ✅ Easy to update |

### Step 4: Configure dlt-meta JSON

#### 4.1 Snapshot CDC (Full Table Refresh)

Use when source table is small (< 10M rows) or full snapshots are preferred.

**File**: `lakeflow_member_db_snapshot.json`

```json
{
  "data_flow_id": "6001",
  "data_flow_group": "federated_oracle_member_db",
  "source_system": "oracle_member_db",
  "source_format": "snapshot",

  "source_details": {
    "snapshot_format": "delta",
    "catalog_nonprod": "my_oracle_connection",
    "database_nonprod": "member",
    "table": "VW_MEMBER_CUSTOMER_CDC"
  },

  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "memberdb_customers",

  "landing_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  },

  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_nonprod",
  "refinery_table": "memberdb_customers",
  "refinery_partition_columns": "LAST_UPDATE",

  "refinery_table_properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.targetFileSize": "256mb"
  },

  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "2",
    "track_history_except_column_list": ["EXTRACTION_TIMESTAMP"]
  }
}
```

**Key Settings:**
- `source_format: "snapshot"` - Full table refresh mode
- `landing_apply_changes_from_snapshot` - Deduplicates landing data
- `refinery_apply_changes_from_snapshot` - Maintains SCD Type 2 history

#### 4.2 Incremental CDC (Change Data Capture)

Use when source provides incremental changes (date-filtered queries).

**File**: `lakeflow_member_db_incremental.json`

```json
{
  "data_flow_id": "6002",
  "data_flow_group": "federated_oracle_members",
  "source_system": "oracle_member_db",
  "source_format": "delta",

  "source_details": {
    "source_catalog": "my_oracle_connection",
    "source_database": "member",
    "source_table": "VW_MEMBER_CUSTOMER_CDC"
  },

  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "member_customer_data",

  "landing_table_properties": {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true"
  },

  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_nonprod",
  "refinery_table": "member_customer_data",

  "refinery_cdc_apply_changes": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "2",
    "except_column_list": ["EXTRACTION_TIMESTAMP"]
  },

  "refinery_transformation_json_nonprod": "/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/cds/conf/onboarding/lakeflow_connect/transformations/memberdb_customers_refinery.yaml"
}
```

**Key Settings:**
- `source_format: "delta"` - Treats federated view as Delta source
- `refinery_cdc_apply_changes` - Uses `sequence_by` column for CDC
- `scd_type: "2"` - Maintains historical versions with `__START_AT` and `__END_AT`

### Step 5: Deploy dlt-meta Pipeline

#### 5.1 Deploy Command

```bash
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_db_incremental.json \
  --env nonprod \
  --layer landing_refinery \
  --cluster_mode LEGACY \
  --num_workers 2
```

**Important flags:**
- `--cluster_mode LEGACY` - Uses classic compute (required for Lakeflow Connect)
- `--num_workers 2` - Adjust based on data volume
- `--layer landing_refinery` - Creates both landing and refinery layers

#### 5.2 Verify Pipeline Creation

```bash
# List pipelines
databricks pipelines list --output json | jq '.[] | select(.name | contains("member_customer"))'

# Get pipeline details
databricks pipelines get --pipeline-id <pipeline-id>
```

### Step 6: Run and Monitor Pipeline

#### 6.1 Start Pipeline

```bash
# Manual run
databricks pipelines start --pipeline-id <pipeline-id>

# Schedule pipeline (add to pipeline settings)
databricks pipelines update --pipeline-id <pipeline-id> \
  --trigger "cron:0 */1 * * * ?" \
  --trigger-name "hourly_refresh"
```

#### 6.2 Monitor Execution

```sql
-- Check landing table
SELECT
    COUNT(*) as total_records,
    MIN(LAST_UPDATE) as oldest_update,
    MAX(LAST_UPDATE) as newest_update
FROM dataservices_nonprod.landing_nonprod.member_customer_data;

-- Check refinery table (SCD Type 2)
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    __START_AT,
    __END_AT,
    CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as STATUS
FROM dataservices_nonprod.refinery_nonprod.member_customer_data
WHERE CUSTOMER_ID = 123456
ORDER BY __START_AT DESC;

-- Check for duplicates (should be 0)
SELECT CUSTOMER_ID, COUNT(*) as cnt
FROM dataservices_nonprod.refinery_nonprod.member_customer_data
WHERE __END_AT IS NULL
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;
```

---

## Drawbacks and Solutions

### Drawback 1: Firewall Configuration Complexity

**Problem:**
- Enterprise databases are protected by strict firewall rules
- Databricks serverless uses dynamic IP ranges
- Security teams hesitant to open broad firewall rules

**Solution:**
- ✅ Use **classic compute** with static IP ranges
- ✅ Work with network/security teams to whitelist specific CIDR blocks
- ✅ Use VPC peering or AWS PrivateLink for secure connectivity
- ✅ Document firewall rules in runbook for auditing

**Implementation:**

```sql
-- Find Databricks compute IP ranges
SELECT
    ip_address,
    workspace_id,
    region
FROM system.networking.ip_access_list
WHERE cluster_type = 'CLASSIC';
```

Provide this list to your network team for firewall rule creation.

### Drawback 2: Source Database Performance Impact

**Problem:**
- Complex queries executed on source database
- JOINs and aggregations can slow down OLTP systems
- Production workload may be impacted

**Solutions:**

#### Solution 2.1: Create Optimized Views in Source Database

```sql
-- Oracle example with materialized view
CREATE MATERIALIZED VIEW VW_MEMBER_CUSTOMER_CDC_MV
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
AS
SELECT
    m.CUSTOMER_ID,
    m.FIRST_NAME,
    m.LAST_NAME,
    m.LAST_UPDATE
FROM MEMBER.MEMBERS m
WHERE m.LAST_UPDATE >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

-- Create index on materialized view
CREATE INDEX IDX_MV_LAST_UPDATE ON VW_MEMBER_CUSTOMER_CDC_MV(LAST_UPDATE);

-- Schedule refresh (every hour)
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'REFRESH_CUSTOMER_MV',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN DBMS_MVIEW.REFRESH(''VW_MEMBER_CUSTOMER_CDC_MV'', ''F''); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=1',
        enabled         => TRUE
    );
END;
/
```

**Benefits:**
- ⚡ Pre-computed JOINs reduce query time
- 📊 Indexed for fast incremental queries
- 🔄 Automatic refresh keeps data fresh

#### Solution 2.2: Use Read Replicas

- Query read replica instead of primary database
- Reduces load on production OLTP systems
- Acceptable replication lag (seconds to minutes)

```json
{
  "source_details": {
    "source_catalog": "my_oracle_replica_connection",
    "source_database": "member",
    "source_table": "VW_MEMBER_CUSTOMER_CDC"
  }
}
```

#### Solution 2.3: Query During Off-Peak Hours

Schedule pipeline to run during low-traffic periods:

```bash
databricks pipelines update --pipeline-id <pipeline-id> \
  --trigger "cron:0 0 2 * * ?" \
  --trigger-name "daily_2am_refresh"
```

### Drawback 3: Network Latency

**Problem:**
- Federated queries traverse network between Databricks and source database
- Large result sets slow down data transfer
- Query performance depends on network bandwidth

**Solutions:**

#### Solution 3.1: Incremental Queries Only

```sql
-- ❌ BAD: Full table scan
SELECT * FROM source_db.members;

-- ✅ GOOD: Incremental with date filter
SELECT * FROM source_db.members
WHERE last_update >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

#### Solution 3.2: Column Projection

```sql
-- ❌ BAD: SELECT *
SELECT * FROM source_db.members;

-- ✅ GOOD: Only required columns
SELECT customer_id, first_name, last_name, last_update
FROM source_db.members;
```

#### Solution 3.3: Partition Queries

```json
{
  "source_details": {
    "source_table": "VW_MEMBER_CUSTOMER_CDC",
    "partition_column": "CREATED_DATE",
    "partition_num": 10
  }
}
```

### Drawback 4: Schema Drift

**Problem:**
- Source database schema changes (add/remove/rename columns)
- Federated views don't auto-update
- Pipeline breaks on schema mismatch

**Solutions:**

#### Solution 4.1: Use Views as Abstraction Layer

**In source database:**

```sql
-- View isolates downstream consumers from base table changes
CREATE OR REPLACE VIEW VW_MEMBER_CUSTOMER_CDC AS
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    -- Map old column name to new
    EMAIL as UC_EMAIL_ADDRESS,
    -- Provide default for removed columns
    COALESCE(ACTIVE_FLAG, 1) as ACTIVE,
    LAST_UPDATE
FROM MEMBER.MEMBERS
WHERE LAST_UPDATE >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;
```

**Benefits:**
- Column renames absorbed by view
- Default values for missing columns
- Databricks pipeline unaffected

#### Solution 4.2: Schema Evolution in dlt-meta

```json
{
  "refinery_table_properties": {
    "delta.autoMerge.mergeSchema": "true",
    "delta.columnMapping.mode": "name"
  }
}
```

#### Solution 4.3: Monitoring and Alerts

```sql
-- Create monitor for schema changes
CREATE OR REPLACE VIEW schema_monitor AS
SELECT
    table_name,
    column_name,
    data_type,
    CURRENT_TIMESTAMP() as checked_at
FROM information_schema.columns
WHERE table_schema = 'MEMBER'
  AND table_name = 'MEMBERS';

-- Schedule daily check and alert on differences
```

### Drawback 5: Limited Data Type Support

**Problem:**
- Some Oracle/PostgreSQL data types not supported by Spark
- LOB, CLOB, BLOB columns may fail
- Custom types (arrays, JSON) need special handling

**Solutions:**

#### Solution 5.1: Cast Unsupported Types in View

```sql
-- Oracle example
CREATE OR REPLACE VIEW VW_MEMBER_CUSTOMER_CDC AS
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    -- Cast CLOB to VARCHAR
    CAST(NOTES as VARCHAR2(4000)) as NOTES,
    -- Serialize JSON to string
    JSON_SERIALIZE(METADATA_JSON) as METADATA_JSON_STR,
    -- Convert BLOB to base64 string
    UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE(PROFILE_IMAGE)) as PROFILE_IMAGE_B64
FROM MEMBER.MEMBERS;
```

#### Solution 5.2: Exclude Unsupported Columns

```json
{
  "select_exp": [
    "CUSTOMER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "LAST_UPDATE"
  ],
  "exclude_columns": ["BLOB_COLUMN", "CLOB_COLUMN"]
}
```

---

## Kafka Implementation

### Overview

dlt-meta supports Kafka integration with Schema Registry for Avro and Protobuf deserialization. This enables real-time streaming pipelines from Kafka topics to Delta tables.

### Critical Requirement: JKS Files Only

⚠️ **IMPORTANT**: Databricks DLT Kafka integration requires **JKS (Java KeyStore)** format for SSL certificates. PEM format is **not supported** in DLT pipelines.

**Why JKS only?**
- DLT uses Spark Structured Streaming Kafka source
- Kafka Spark connector requires JKS for SSL/TLS
- PEM certificates work in notebooks but **fail in DLT pipelines**

### JKS Certificate Setup

#### Step 1: Convert PEM to JKS (if needed)

If you have PEM certificates, convert them to JKS:

```bash
# Convert PEM to PKCS12
openssl pkcs12 -export \
  -in client-cert.pem \
  -inkey client-key.pem \
  -out keystore.p12 \
  -name kafka-client \
  -passout pass:changeit

# Convert PKCS12 to JKS
keytool -importkeystore \
  -srckeystore keystore.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit

# Import CA certificate to truststore
keytool -import \
  -file ca-cert.pem \
  -alias ca-root \
  -keystore truststore.jks \
  -storepass changeit \
  -noprompt
```

#### Step 2: Upload JKS Files to Databricks Volume

**Why Volumes instead of DBFS?**
- ✅ Unity Catalog governance
- ✅ Fine-grained access control
- ✅ No init scripts required
- ✅ Persistent across pipeline restarts

```bash
# Create volume (one-time setup)
databricks volumes create \
  --catalog dataservices_nonprod \
  --schema dlt_meta_resources \
  --name kafka_ssl_certs

# Upload JKS files
databricks fs cp truststore.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks

databricks fs cp keystore.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/keystore.jks

# Verify upload
databricks fs ls dbfs:/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/
```

#### Step 3: Store Passwords in Databricks Secrets

```bash
# Create secret scope (one-time setup)
databricks secrets create-scope kafka_ssl_nonprod

# Add passwords
databricks secrets put-secret kafka_ssl_nonprod truststore_password \
  --string-value "changeit"

databricks secrets put-secret kafka_ssl_nonprod keystore_password \
  --string-value "changeit"

databricks secrets put-secret kafka_ssl_nonprod key_password \
  --string-value "changeit"

# Verify secrets
databricks secrets list-secrets kafka_ssl_nonprod
```

### Kafka Configuration

#### Inbound: Kafka → Delta (Avro)

**File**: `kafka_avro_source.json`

```json
{
  "data_flow_id": "7001",
  "data_flow_group": "kafka_avro_ingest",
  "source_system": "kafka_payment_events",
  "source_format": "kafka",

  "source_details": {
    "kafka.bootstrap.servers": "broker1:9093,broker2:9093",
    "subscribe": "payment-events",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.location": "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks",
    "kafka.ssl.keystore.location": "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/keystore.jks",
    "kafka.ssl.truststore.secrets.scope": "kafka_ssl_nonprod",
    "kafka.ssl.truststore.secrets.key": "truststore_password",
    "kafka.ssl.keystore.secrets.scope": "kafka_ssl_nonprod",
    "kafka.ssl.keystore.secrets.key": "keystore_password",
    "kafka.ssl.key.secrets.scope": "kafka_ssl_nonprod",
    "kafka.ssl.key.secrets.key": "key_password",
    "schema.registry.url": "https://schema-registry:8081",
    "schema.registry.subject": "payment-events-value",
    "data_format": "avro",
    "mode": "PERMISSIVE"
  },

  "landing_reader_options": {
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "100000",
    "failOnDataLoss": "false"
  },

  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "payment_events",

  "landing_table_properties": {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
  }
}
```

**Key Configuration:**

| Parameter | Value | Description |
|-----------|-------|-------------|
| `kafka.ssl.truststore.location` | `/Volumes/.../truststore.jks` | Path to JKS truststore in volume |
| `kafka.ssl.keystore.location` | `/Volumes/.../keystore.jks` | Path to JKS keystore in volume |
| `kafka.ssl.*.secrets.scope` | `kafka_ssl_nonprod` | Databricks secret scope |
| `schema.registry.url` | `https://schema-registry:8081` | Schema Registry endpoint |
| `schema.registry.subject` | `payment-events-value` | Avro schema subject name |
| `data_format` | `avro` or `protobuf` | Deserialization format |

#### Outbound: Delta → Kafka (Avro)

**File**: `kafka_avro_sink.json`

```json
{
  "sinks": [
    {
      "name": "kafka_payment_export",
      "format": "kafka",
      "options": {
        "kafka.bootstrap.servers": "broker1:9093,broker2:9093",
        "topic": "payment-processed",
        "kafka.security.protocol": "SSL",
        "kafka.ssl.truststore.location": "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks",
        "kafka.ssl.keystore.location": "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/keystore.jks",
        "kafka.ssl.truststore.password": "{{secrets/kafka_ssl_nonprod/truststore_password}}",
        "kafka.ssl.keystore.password": "{{secrets/kafka_ssl_nonprod/keystore_password}}",
        "kafka.ssl.key.password": "{{secrets/kafka_ssl_nonprod/key_password}}",
        "schema.registry.url": "https://schema-registry:8081",
        "schema.registry.subject": "payment-processed-value",
        "data_format": "avro",
        "confluent.schema.registry.ssl.truststore.location": "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks",
        "confluent.schema.registry.ssl.truststore.password": "{{secrets/kafka_ssl_nonprod/truststore_password}}",
        "confluent.schema.registry.ssl.keystore.location": "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/keystore.jks",
        "confluent.schema.registry.ssl.keystore.password": "{{secrets/kafka_ssl_nonprod/keystore_password}}",
        "confluent.schema.registry.ssl.key.password": "{{secrets/kafka_ssl_nonprod/key_password}}"
      },
      "select_exp": [
        "payment_id as key",
        "payment_id",
        "customer_id",
        "amount",
        "status",
        "processed_timestamp"
      ],
      "where_clause": "status = 'COMPLETED'"
    }
  ]
}
```

**Important Notes:**

1. **`key` column is required**: Kafka messages need a key for partitioning
2. **Secret interpolation**: Use `{{secrets/scope/key}}` syntax for runtime resolution
3. **Schema Registry SSL**: Use `confluent.schema.registry.ssl.*` prefix
4. **No init scripts**: JKS files accessed directly from volumes

### Deploy Kafka Pipeline

```bash
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/kafka_ng/kafka_avro_source.json \
  --env nonprod \
  --layer landing \
  --cluster_mode LEGACY \
  --num_workers 4
```

**Notes:**
- Use `--cluster_mode LEGACY` for classic compute
- Increase `--num_workers` for high-throughput topics
- JKS files automatically accessible from volumes (no init scripts)

### Testing Kafka Integration

#### Test 1: Verify JKS Files Are Accessible

```sql
-- In Databricks SQL or notebook
LIST '/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/';

-- Should show:
-- truststore.jks
-- keystore.jks
```

#### Test 2: Test Kafka Connection (Notebook)

```python
# Test in notebook before deploying DLT
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9093") \
    .option("subscribe", "payment-events") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks") \
    .option("kafka.ssl.truststore.password", dbutils.secrets.get("kafka_ssl_nonprod", "truststore_password")) \
    .option("kafka.ssl.keystore.location", "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/keystore.jks") \
    .option("kafka.ssl.keystore.password", dbutils.secrets.get("kafka_ssl_nonprod", "keystore_password")) \
    .option("kafka.ssl.key.password", dbutils.secrets.get("kafka_ssl_nonprod", "key_password")) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show(truncate=False)
```

#### Test 3: Verify Schema Registry

```python
import requests

# Test Schema Registry connection
response = requests.get(
    "https://schema-registry:8081/subjects",
    cert=("/path/to/client-cert.pem", "/path/to/client-key.pem"),
    verify="/path/to/ca-cert.pem"
)

print("Available subjects:", response.json())

# Get specific schema
schema_response = requests.get(
    "https://schema-registry:8081/subjects/payment-events-value/versions/latest",
    cert=("/path/to/client-cert.pem", "/path/to/client-key.pem"),
    verify="/path/to/ca-cert.pem"
)

print("Schema:", schema_response.json())
```

---

## SSL Certificate Management

### Certificate Formats

| Format | Extension | Use Case | Supported in DLT? |
|--------|-----------|----------|-------------------|
| **JKS** | `.jks` | Java keystore/truststore | ✅ **Required** |
| **PKCS12** | `.p12`, `.pfx` | Cross-platform keystore | ⚠️ Convert to JKS |
| **PEM** | `.pem`, `.crt`, `.key` | OpenSSL format | ❌ Not in DLT |

### Security Best Practices

#### 1. Separate Secrets per Environment

```bash
# Nonprod
databricks secrets create-scope kafka_ssl_nonprod
databricks secrets create-scope db_credentials_nonprod

# Preprod
databricks secrets create-scope kafka_ssl_preprod
databricks secrets create-scope db_credentials_preprod

# Prod
databricks secrets create-scope kafka_ssl_prod
databricks secrets create-scope db_credentials_prod
```

#### 2. Use Service Accounts

```sql
-- Oracle example: create read-only service account
CREATE USER databricks_readonly IDENTIFIED BY <strong_password>;
GRANT CONNECT TO databricks_readonly;
GRANT SELECT ON MEMBER.MEMBERS TO databricks_readonly;
GRANT SELECT ON MEMBER.CUSTOMERS TO databricks_readonly;
```

#### 3. Rotate Certificates Regularly

```bash
# Update JKS files
databricks fs cp truststore_new.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks \
  --overwrite

# Update secrets
databricks secrets put-secret kafka_ssl_nonprod truststore_password \
  --string-value "new_password"

# Restart DLT pipeline
databricks pipelines restart --pipeline-id <pipeline-id>
```

#### 4. Restrict Volume Access

```sql
-- Grant read access only to service principals running DLT
GRANT READ FILES ON VOLUME dataservices_nonprod.dlt_meta_resources.kafka_ssl_certs
TO `service_principal_dlt_runner`;

-- Revoke access from users
REVOKE ALL PRIVILEGES ON VOLUME dataservices_nonprod.dlt_meta_resources.kafka_ssl_certs
FROM `user@example.com`;
```

---

## Troubleshooting

### Lakeflow Connect Issues

#### Issue 1: "Connection timed out"

**Symptoms:**
```
java.net.ConnectException: Connection timed out
```

**Causes:**
- Firewall blocking Databricks IP ranges
- Incorrect hostname/port in connection string
- Database server not accepting external connections

**Solutions:**

1. **Verify firewall rules:**
   ```bash
   # From Databricks cluster, test connectivity
   nc -zv db.example.com 1521

   # Or use telnet
   telnet db.example.com 1521
   ```

2. **Check database listener:**
   ```sql
   -- In Oracle
   SELECT * FROM v$listener_network;

   -- Check if TCP listener is active
   lsnrctl status
   ```

3. **Verify connection string:**
   ```sql
   -- Test connection in Databricks SQL
   SELECT * FROM my_oracle_connection.sys.dual;
   ```

#### Issue 2: "Table or view does not exist"

**Symptoms:**
```
org.apache.spark.sql.AnalysisException: Table or view 'VW_MEMBER_CUSTOMER_CDC' not found
```

**Solutions:**

1. **Check object exists in source database:**
   ```sql
   -- Oracle
   SELECT owner, object_name, object_type
   FROM all_objects
   WHERE object_name = 'VW_MEMBER_CUSTOMER_CDC';
   ```

2. **Verify grants:**
   ```sql
   -- Oracle
   SELECT * FROM all_tab_privs
   WHERE table_name = 'VW_MEMBER_CUSTOMER_CDC'
     AND grantee = 'DATABRICKS_READONLY';
   ```

3. **Check schema/database name:**
   ```json
   {
     "source_details": {
       "catalog": "my_oracle_connection",
       "database": "MEMBER",  // Case-sensitive!
       "table": "VW_MEMBER_CUSTOMER_CDC"
     }
   }
   ```

#### Issue 3: "No data in landing table"

**Symptoms:**
- Pipeline runs successfully
- Landing table empty or missing records

**Solutions:**

1. **Check view returns data:**
   ```sql
   SELECT COUNT(*) FROM my_oracle_connection.member.VW_MEMBER_CUSTOMER_CDC;
   ```

2. **Verify date filters:**
   ```sql
   -- Check if LAST_UPDATE is recent
   SELECT
       MIN(LAST_UPDATE) as oldest,
       MAX(LAST_UPDATE) as newest,
       COUNT(*) as total
   FROM my_oracle_connection.member.VW_MEMBER_CUSTOMER_CDC;
   ```

3. **Check pipeline logs:**
   ```bash
   databricks pipelines get-latest-update --pipeline-id <pipeline-id> | jq '.update.state'
   ```

### Kafka Issues

#### Issue 1: "SSLHandshakeException"

**Symptoms:**
```
javax.net.ssl.SSLHandshakeException: PKIX path building failed
```

**Causes:**
- Incorrect truststore path
- Missing CA certificate in truststore
- Wrong truststore password

**Solutions:**

1. **Verify JKS files:**
   ```bash
   # List certificates in truststore
   keytool -list -keystore truststore.jks -storepass changeit

   # Verify keystore
   keytool -list -keystore keystore.jks -storepass changeit
   ```

2. **Check volume path:**
   ```sql
   LIST '/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/';
   ```

3. **Test connection in notebook:**
   ```python
   # Use full /Volumes/ path
   .option("kafka.ssl.truststore.location", "/Volumes/dataservices_nonprod/dlt_meta_resources/kafka_ssl_certs/truststore.jks")
   ```

#### Issue 2: "Schema not found in Schema Registry"

**Symptoms:**
```
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Subject 'payment-events-value' not found
```

**Solutions:**

1. **List available subjects:**
   ```bash
   curl -X GET https://schema-registry:8081/subjects \
     --cert client-cert.pem \
     --key client-key.pem \
     --cacert ca-cert.pem
   ```

2. **Check subject naming:**
   ```json
   {
     "schema.registry.subject": "payment-events-value"  // Must match exactly
   }
   ```

3. **Register schema manually:**
   ```bash
   curl -X POST https://schema-registry:8081/subjects/payment-events-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"schema": "{\"type\": \"record\", ...}"}' \
     --cert client-cert.pem \
     --key client-key.pem
   ```

#### Issue 3: "PEM certificates not working in DLT"

**Symptoms:**
- Kafka connection works in notebook with PEM
- DLT pipeline fails with SSL errors

**Solution:**

⚠️ **DLT requires JKS format** - convert PEM to JKS:

```bash
# Step 1: Create PKCS12
openssl pkcs12 -export \
  -in client-cert.pem \
  -inkey client-key.pem \
  -CAfile ca-cert.pem \
  -out keystore.p12 \
  -name kafka-client \
  -passout pass:changeit

# Step 2: Convert to JKS
keytool -importkeystore \
  -srckeystore keystore.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit

# Step 3: Import CA to truststore
keytool -import \
  -file ca-cert.pem \
  -alias ca-root \
  -keystore truststore.jks \
  -storepass changeit \
  -noprompt

# Step 4: Upload to volume
databricks fs cp keystore.jks dbfs:/Volumes/.../kafka_ssl_certs/keystore.jks
databricks fs cp truststore.jks dbfs:/Volumes/.../kafka_ssl_certs/truststore.jks
```

---

## Best Practices

### 1. Configuration Management

#### Use Environment-Specific Files

```
cds/conf/onboarding/lakeflow_connect/
├── lakeflow_member_db_nonprod.json
├── lakeflow_member_db_preprod.json
└── lakeflow_member_db_prod.json
```

**Example structure:**

```json
{
  "data_flow_id": "6001",
  "source_details": {
    "catalog_nonprod": "my_oracle_connection_nonprod",
    "catalog_preprod": "my_oracle_connection_preprod",
    "catalog_prod": "my_oracle_connection_prod"
  },
  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_catalog_preprod": "dataservices_preprod",
  "landing_catalog_prod": "dataservices_prod"
}
```

#### Parameterize Secrets

```json
{
  "kafka.ssl.truststore.secrets.scope": "kafka_ssl_${env}",
  "kafka.ssl.truststore.secrets.key": "truststore_password"
}
```

### 2. Monitoring and Observability

#### Create Dashboard for Pipeline Health

```sql
-- Query: Pipeline execution history
SELECT
    pipeline_id,
    update_id,
    state,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes
FROM system.lakeflow.pipeline_updates
WHERE pipeline_id = '<pipeline-id>'
ORDER BY start_time DESC
LIMIT 100;
```

#### Set Up Alerts

```json
{
  "notifications": [
    {
      "email_recipients": ["data-team@example.com"],
      "alerts": ["on-update-failure", "on-update-success"]
    }
  ]
}
```

### 3. Performance Optimization

#### Landing Layer Optimization

```json
{
  "landing_table_properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.targetFileSize": "256mb",
    "delta.enableChangeDataFeed": "true"
  }
}
```

#### Refinery Layer Optimization

```json
{
  "refinery_table_properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.targetFileSize": "256mb"
  },
  "refinery_partition_columns": "event_date"
}
```

### 4. Data Quality

#### Add Expectations

```json
{
  "refinery_data_quality_expectations": {
    "rules": {
      "valid_customer_id": "CUSTOMER_ID IS NOT NULL",
      "valid_email": "UC_EMAIL_ADDRESS LIKE '%@%'",
      "recent_update": "LAST_UPDATE >= current_date() - INTERVAL 7 DAYS"
    },
    "action": "drop"
  }
}
```

### 5. Documentation

#### Maintain Runbook

Create runbook for each pipeline:

```markdown
# Pipeline: member_customer_data

## Purpose
Ingests customer data from Oracle Member DB to Databricks

## Source System
- Database: Oracle Member DB
- Connection: my_oracle_connection_nonprod
- View: VW_MEMBER_CUSTOMER_CDC
- Refresh: Every 1 hour

## Firewall Rules
- Source: Databricks Classic Compute (10.0.0.0/16)
- Destination: oracle-db.example.com
- Port: 1521

## Dependencies
- Databricks Secret Scope: db_credentials_nonprod
- Volume: dataservices_nonprod.dlt_meta_resources.kafka_ssl_certs
- DLT Pipeline ID: 12345-abcde-67890

## Contacts
- Database DBA: dba-team@example.com
- Network Team: network-ops@example.com
- Data Engineering: data-eng@example.com
```

### 6. Testing Strategy

#### Test Checklist

- [ ] Federated connection working
- [ ] View returns expected row count
- [ ] Date filters working correctly
- [ ] No duplicate records
- [ ] Pipeline deploys successfully
- [ ] Landing table populates
- [ ] Refinery CDC working (SCD Type 2)
- [ ] Transformations applied correctly
- [ ] Kafka sink (if applicable) producing messages
- [ ] Monitoring alerts configured

---

## Summary

### Key Takeaways

✅ **Lakeflow Connect** enables direct database queries without ETL infrastructure
✅ **Classic Compute required** for firewall compatibility
✅ **Views in source systems** optimize performance and reduce complexity
✅ **Kafka requires JKS files** - PEM format not supported in DLT
✅ **No init scripts needed** - JKS files stored in Databricks Volumes
✅ **Incremental queries** reduce network overhead and source database load
✅ **Databricks Secrets** for credential management

### Decision Matrix

| Use Case | Recommended Approach | Compute Type | Certificate Format |
|----------|---------------------|--------------|-------------------|
| Oracle/MySQL CDC | Lakeflow Connect + Federated View | Classic | N/A |
| Kafka Streaming | dlt-meta Kafka Source | Classic | JKS |
| Full Table Refresh | Snapshot CDC | Classic | N/A |
| Incremental CDC | Traditional CDC | Classic | N/A |
| Low-latency streaming | Kafka + Schema Registry | Classic | JKS |

### Additional Resources

- **dlt-meta Documentation**: https://databrickslabs.github.io/dlt-meta/
- **Lakeflow Connect Guide**: [FEDERATED_ORACLE_CDC_GUIDE.md](lakeflow_connect/FEDERATED_ORACLE_CDC_GUIDE.md)
- **Kafka Integration**: [README_KAFKA_SCHEMA_REGISTRY.md](kafka_ng/README_KAFKA_SCHEMA_REGISTRY.md)
- **Quick Start**: [QUICK_START.md](lakeflow_connect/QUICK_START.md)
- **Databricks DLT**: https://docs.databricks.com/delta-live-tables/
- **Lakehouse Federation**: https://docs.databricks.com/query-federation/

### Support

For questions or issues:
- **Internal Team**: https://jira.livenation.com/servicedesk/customer/portal/324/group/1242
- **dlt-meta GitHub**: https://github.com/databrickslabs/dlt-meta
- **Databricks Support**: Open support ticket in workspace

---

**Document Version**: 1.0
**Last Updated**: 2026-03-06
**Authors**: Data Platform Team
**Review Cycle**: Quarterly
