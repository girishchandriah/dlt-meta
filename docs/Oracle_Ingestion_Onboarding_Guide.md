# Oracle Database Ingestion - Onboarding Guide

**Version:** 1.0
**Date:** February 25, 2026
**Author:** Platform Data Engineering Team

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Oracle JDBC Connection Setup](#oracle-jdbc-connection-setup)
4. [Ingestion Patterns](#ingestion-patterns)
5. [Onboarding File Configuration](#onboarding-file-configuration)
6. [Complete Examples](#complete-examples)
7. [Best Practices](#best-practices)
8. [Performance Tuning](#performance-tuning)
9. [Troubleshooting](#troubleshooting)
10. [Security Considerations](#security-considerations)

---

## Overview

This guide provides comprehensive instructions for ingesting data from Oracle databases into Delta Live Tables using the DLT-META framework.

### Supported Ingestion Methods

| Method | Use Case | CDC Support | Performance |
|--------|----------|-------------|-------------|
| **Full Table Load** | Initial load, small tables | No | Good for < 1M rows |
| **Incremental Load** | Regular updates via watermark column | No | Excellent |
| **Query-Based Load** | Custom SQL queries | No | Depends on query |
| **CDC via Snapshot** | Change tracking without CDC flags | Yes (SCD Type 1/2) | Good |
| **CDC with Operation Column** | Tables with INSERT/UPDATE/DELETE flags | Yes (SCD Type 1/2) | Excellent |

### Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Oracle Database Ingestion                     │
└─────────────────────────────────────────────────────────────────┘

ORACLE DATABASE          DLT-META FRAMEWORK          DELTA LIVE TABLES
┌──────────────┐         ┌──────────────┐            ┌──────────────┐
│              │         │              │            │              │
│  Oracle      │ JDBC    │  Landing     │  Stream    │  Bronze      │
│  Table       │────────▶│  Reader      │───────────▶│  Table       │
│              │         │              │            │              │
└──────────────┘         └──────────────┘            └──────┬───────┘
                                                            │
                         ┌──────────────┐                  ▼
                         │              │            ┌──────────────┐
                         │  CDC Apply   │ Process    │  Silver      │
                         │  Changes     │◀───────────│  Refinery    │
                         │              │            │              │
                         └──────────────┘            └──────┬───────┘
                                                            │
                                                            ▼
                                                     ┌──────────────┐
                                                     │  Gold        │
                                                     │  Treasury    │
                                                     │              │
                                                     └──────────────┘
```

---

## Prerequisites

### 1. Oracle JDBC Driver

The Oracle JDBC driver must be available on the Databricks cluster. For DLT-META pipelines, there are multiple installation methods depending on your deployment approach.

**Recommended Driver Versions:**

| Oracle Version | JDBC Driver | Maven Coordinates |
|----------------|-------------|-------------------|
| Oracle 19c+ | ojdbc8.jar | `com.oracle.database.jdbc:ojdbc8:21.9.0.0` |
| Oracle 12c+ | ojdbc8.jar | `com.oracle.database.jdbc:ojdbc8:19.18.0.0` |
| Oracle 11g | ojdbc6.jar | `com.oracle.database.jdbc:ojdbc6:11.2.0.4` |

---

## Installing Oracle Libraries for DLT-META

Choose the installation method based on how you deploy DLT-META pipelines:

### Method 1: DLT Pipeline Configuration (Recommended for DLT-META)

This is the **best approach** for DLT-META pipelines as it configures libraries directly in the Delta Live Tables pipeline.

#### Option A: Using Databricks UI

1. **Navigate to Workflows → Delta Live Tables**

2. **Click "Create Pipeline"** or edit existing pipeline

3. **In the "Libraries" section:**
   - Click **"Add library"**
   - Select **"Maven"**
   - Enter Maven coordinates: `com.oracle.database.jdbc:ojdbc8:21.9.0.0`
   - Click **"Add"**

4. **Add your DLT-META notebook:**
   - Click **"Add library"** again
   - Select **"Notebook"**
   - Browse to your DLT-META pipeline notebook (e.g., `/Workspace/dlt-meta/dlt_meta_pipeline`)

5. **Configure pipeline settings:**
   ```
   Configuration:
     layer: landing
     landing.dataflowspecTable: catalog.schema.landing_dataflowspec_table
     landing.group: oracle_ingestion
   ```

6. **Save and start the pipeline**

#### Option B: Using Databricks Asset Bundles (DAB)

Add libraries to your `databricks.yml` or pipeline configuration file:

**File:** `demo/dabs/resources/oracle_pipeline.yml`

```yaml
resources:
  pipelines:
    oracle_landing_pipeline:
      name: Oracle Landing Pipeline
      catalog: ${var.catalog_name}
      schema: ${var.landing_schema}
      development: true
      photon: true
      serverless: false

      # Add Oracle JDBC library here
      libraries:
        - notebook:
            path: ${workspace.file_path}/notebooks/dlt_meta_pipeline
        - maven:
            coordinates: "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
            repo: "https://repo1.maven.org/maven2"

      configuration:
        layer: landing
        landing.dataflowspecTable: ${var.catalog_name}.${var.schema}.landing_dataflowspec_table
        landing.group: oracle_ingestion
```

**Deploy the bundle:**

```bash
# Validate
databricks bundle validate --profile=<your_profile>

# Deploy
databricks bundle deploy --target dev --profile=<your_profile>

# Run
databricks bundle run oracle_landing_pipeline -t dev --profile=<your_profile>
```

#### Option C: Using Databricks CLI

Create pipeline with Oracle JDBC library using CLI:

```bash
databricks pipelines create \
  --settings '{
    "name": "Oracle Landing Pipeline",
    "catalog": "my_catalog",
    "target": "landing_schema",
    "libraries": [
      {
        "notebook": {
          "path": "/Workspace/dlt-meta/dlt_meta_pipeline"
        }
      },
      {
        "maven": {
          "coordinates": "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
        }
      }
    ],
    "configuration": {
      "layer": "landing",
      "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec_table",
      "landing.group": "oracle_ingestion"
    },
    "photon": true,
    "continuous": false
  }' \
  --profile <your_profile>
```

#### Option D: Using Python SDK

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import *

w = WorkspaceClient()

pipeline = w.pipelines.create(
    name="Oracle Landing Pipeline",
    catalog="my_catalog",
    target="landing_schema",
    libraries=[
        PipelineLibrary(
            notebook=NotebookLibrary(
                path="/Workspace/dlt-meta/dlt_meta_pipeline"
            )
        ),
        PipelineLibrary(
            maven=MavenLibrary(
                coordinates="com.oracle.database.jdbc:ojdbc8:21.9.0.0"
            )
        )
    ],
    configuration={
        "layer": "landing",
        "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec_table",
        "landing.group": "oracle_ingestion"
    },
    photon=True,
    continuous=False
)

print(f"Pipeline created: {pipeline.pipeline_id}")
```

---

### Method 2: Cluster Libraries (For Interactive Development)

Use this method for **testing and development** in notebooks, **not recommended for production DLT pipelines**.

#### Option A: Databricks UI

1. **Go to Compute → Select your cluster**

2. **Click "Libraries" tab**

3. **Click "Install New"**

4. **Select "Maven"**
   - Coordinates: `com.oracle.database.jdbc:ojdbc8:21.9.0.0`
   - Repository: `https://repo1.maven.org/maven2` (default)

5. **Click "Install"**

6. **Restart cluster** (if running)

#### Option B: Databricks CLI

```bash
# Create cluster with Oracle JDBC library
databricks clusters create \
  --json '{
    "cluster_name": "Oracle Ingestion Cluster",
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "libraries": [
      {
        "maven": {
          "coordinates": "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
        }
      }
    ]
  }'
```

#### Option C: Cluster Configuration JSON

Add to your cluster configuration:

```json
{
  "cluster_name": "Oracle Ingestion Cluster",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "libraries": [
    {
      "maven": {
        "coordinates": "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
      }
    }
  ],
  "spark_conf": {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  }
}
```

---

### Method 3: Init Scripts (Advanced)

Use init scripts for **custom driver versions** or **multiple drivers**.

#### Step 1: Create Init Script

**File:** `/dbfs/init-scripts/install-oracle-jdbc.sh`

```bash
#!/bin/bash

# Install Oracle JDBC 8 driver
echo "Installing Oracle JDBC driver..."

# Option 1: Download from Maven Central
wget -q https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar \
  -O /databricks/jars/ojdbc8-21.9.0.0.jar

# Option 2: Download from Oracle (requires authentication)
# wget --http-user=oracle_user --http-password=oracle_pass \
#   https://download.oracle.com/otn-pub/otn_software/jdbc/219/ojdbc8.jar \
#   -O /databricks/jars/ojdbc8.jar

# Verify installation
if [ -f "/databricks/jars/ojdbc8-21.9.0.0.jar" ]; then
  echo "Oracle JDBC driver installed successfully"
  ls -lh /databricks/jars/ojdbc8-21.9.0.0.jar
else
  echo "ERROR: Failed to install Oracle JDBC driver"
  exit 1
fi

# Optional: Install additional Oracle drivers (for NLS, security, etc.)
# wget -q https://repo1.maven.org/maven2/com/oracle/database/nls/orai18n/21.9.0.0/orai18n-21.9.0.0.jar \
#   -O /databricks/jars/orai18n-21.9.0.0.jar

echo "Oracle JDBC installation complete"
```

#### Step 2: Upload Init Script

```bash
# Upload to DBFS
databricks fs cp install-oracle-jdbc.sh dbfs:/init-scripts/install-oracle-jdbc.sh
```

Or using Databricks CLI:

```python
# In Databricks notebook
dbutils.fs.put(
    "/init-scripts/install-oracle-jdbc.sh",
    """#!/bin/bash
echo "Installing Oracle JDBC driver..."
wget -q https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar \
  -O /databricks/jars/ojdbc8-21.9.0.0.jar
echo "Oracle JDBC driver installed successfully"
""",
    overwrite=True
)
```

#### Step 3: Configure Cluster with Init Script

**In Cluster Configuration:**

```json
{
  "cluster_name": "Oracle Ingestion Cluster",
  "init_scripts": [
    {
      "dbfs": {
        "destination": "dbfs:/init-scripts/install-oracle-jdbc.sh"
      }
    }
  ]
}
```

**Or via Databricks UI:**
1. Compute → Select cluster
2. Configuration → Advanced options
3. Init Scripts → Add
4. Type: DBFS
5. Path: `dbfs:/init-scripts/install-oracle-jdbc.sh`

---

### Method 4: Workspace Files (Unity Catalog Volumes)

For **Unity Catalog enabled workspaces**, store drivers in volumes.

#### Step 1: Create Volume for Drivers

```sql
-- Create volume for libraries
CREATE VOLUME IF NOT EXISTS my_catalog.shared_resources.jdbc_drivers;
```

#### Step 2: Upload Oracle JDBC Driver

```python
# In Databricks notebook
# Download driver
import requests

url = "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar"
response = requests.get(url)

# Save to volume
with open("/Volumes/my_catalog/shared_resources/jdbc_drivers/ojdbc8-21.9.0.0.jar", "wb") as f:
    f.write(response.content)

print("Oracle JDBC driver uploaded to Unity Catalog volume")
```

#### Step 3: Reference in DLT Pipeline

```yaml
# In DAB configuration
resources:
  pipelines:
    oracle_pipeline:
      libraries:
        - jar: /Volumes/my_catalog/shared_resources/jdbc_drivers/ojdbc8-21.9.0.0.jar
        - notebook:
            path: ${workspace.file_path}/notebooks/dlt_meta_pipeline
```

---

## Verification Steps

After installing Oracle JDBC drivers, verify the installation:

### Test 1: Check Driver Availability

```python
# In Databricks notebook
import os
import subprocess

# Check JDBC jars
result = subprocess.run(
    ["find", "/databricks/jars", "-name", "*oracle*", "-o", "-name", "*ojdbc*"],
    capture_output=True,
    text=True
)

print("Oracle JDBC drivers found:")
print(result.stdout)

# Check if driver class is available
try:
    from py4j.java_gateway import java_import
    java_import(spark._jvm, "oracle.jdbc.driver.OracleDriver")
    print("✅ Oracle JDBC driver class loaded successfully")
except Exception as e:
    print(f"❌ Failed to load Oracle JDBC driver: {e}")
```

### Test 2: Test Oracle Connection

```python
# Test Oracle connectivity
jdbc_url = "jdbc:oracle:thin:@//oracle-prod.company.com:1521/ORCL"
connection_properties = {
    "user": dbutils.secrets.get(scope="oracle_secrets", key="username"),
    "password": dbutils.secrets.get(scope="oracle_secrets", key="password"),
    "driver": "oracle.jdbc.driver.OracleDriver"
}

try:
    # Test query
    df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT 'SUCCESS' as status, SYSDATE as current_time FROM DUAL) test",
        properties=connection_properties
    )

    print("✅ Oracle connection successful!")
    df.show()

except Exception as e:
    print(f"❌ Oracle connection failed: {e}")
```

### Test 3: Verify in DLT Pipeline

After configuring Oracle libraries in your DLT pipeline:

1. **Start the pipeline**
2. **Check pipeline events for library installation:**
   ```
   Installing library: com.oracle.database.jdbc:ojdbc8:21.9.0.0
   Library installation successful
   ```
3. **Verify pipeline runs without JDBC driver errors**

---

## Troubleshooting Oracle Library Installation

### Issue 1: ClassNotFoundException: oracle.jdbc.driver.OracleDriver

**Symptoms:**
```
java.lang.ClassNotFoundException: oracle.jdbc.driver.OracleDriver
```

**Solutions:**

1. **Verify library is installed:**
   ```python
   # Check available JDBC drivers
   spark._jvm.java.sql.DriverManager.getDrivers()
   ```

2. **Restart cluster/pipeline after library installation**

3. **Check Maven coordinates are correct:**
   ```
   # Correct
   com.oracle.database.jdbc:ojdbc8:21.9.0.0

   # Wrong (missing version)
   com.oracle.database.jdbc:ojdbc8
   ```

### Issue 2: Driver Version Conflicts

**Symptoms:**
```
java.sql.SQLException: Unsupported feature
```

**Solution:** Match Oracle database version with JDBC driver version:

| Database Version | Use JDBC Driver |
|------------------|-----------------|
| Oracle 19c | ojdbc8 21.x or later |
| Oracle 12c | ojdbc8 19.x |
| Oracle 11g | ojdbc6 11.2.x |

### Issue 3: Library Not Available in DLT Pipeline

**Symptoms:**
```
Library not found during pipeline execution
```

**Solutions:**

1. **Add library to pipeline configuration, not cluster**
   - DLT pipelines use their own compute
   - Cluster libraries don't apply to DLT pipelines

2. **Verify library section in pipeline JSON:**
   ```json
   "libraries": [
     {
       "maven": {
         "coordinates": "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
       }
     }
   ]
   ```

### Issue 4: Maven Download Timeout

**Symptoms:**
```
Failed to download maven artifact
Connection timeout
```

**Solutions:**

1. **Use init script with direct download**
2. **Upload JAR to Unity Catalog volume**
3. **Configure proxy settings** (if behind corporate firewall)

---

## Best Practices for Oracle Libraries in DLT-META

### ✅ DO:

1. **Use Method 1 (DLT Pipeline Configuration)** for production pipelines
2. **Pin specific driver versions** (`21.9.0.0`, not `21.+` or `latest`)
3. **Test connectivity in notebook before creating pipeline**
4. **Store drivers in Unity Catalog volumes for easy sharing**
5. **Document driver version in onboarding files**

### ❌ DON'T:

1. **Don't rely on cluster libraries for DLT pipelines** (they won't be available)
2. **Don't use multiple conflicting JDBC driver versions**
3. **Don't hardcode driver paths** (use Maven coordinates)
4. **Don't skip verification tests**

---

## Complete Example: DLT-META Pipeline with Oracle JDBC

### Step 1: Install Oracle JDBC via DAB

**File:** `demo/dabs/resources/oracle_finance_pipeline.yml`

```yaml
resources:
  pipelines:
    oracle_finance_landing:
      name: Oracle Finance Landing Pipeline
      catalog: enterprise_data
      schema: landing_finance
      photon: true
      serverless: false

      libraries:
        # DLT-META notebook
        - notebook:
            path: /Workspace/dlt-meta/dlt_meta_pipeline

        # Oracle JDBC driver
        - maven:
            coordinates: "com.oracle.database.jdbc:ojdbc8:21.9.0.0"
            repo: "https://repo1.maven.org/maven2"

      configuration:
        layer: landing
        landing.dataflowspecTable: enterprise_data.dlt_meta_config.landing_dataflowspec_table
        landing.group: finance_oracle_gl
```

### Step 2: Create Onboarding File

**File:** `conf/onboarding/oracle/gl_accounts.json`

```json
[
  {
    "data_flow_id": "1001",
    "data_flow_group": "finance_oracle_gl",
    "source_system": "oracle_erp",
    "source_format": "delta",

    "source_details": {
      "jdbc_url": "jdbc:oracle:thin:@//oracle-finance:1521/FINPROD",
      "jdbc_table": "GL_SCHEMA.GL_ACCOUNTS",
      "jdbc_user": "{{secrets/oracle_secrets/username}}",
      "jdbc_password": "{{secrets/oracle_secrets/password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_finance",
    "landing_table": "oracle_gl_accounts"
  }
]
```

### Step 3: Deploy and Run

```bash
# Validate
databricks bundle validate --profile=prod

# Deploy
databricks bundle deploy --target prod --profile=prod

# Run onboarding
databricks bundle run onboard_oracle_finance -t prod --profile=prod

# Execute pipeline
databricks bundle run oracle_finance_landing -t prod --profile=prod
```

---

## Summary: Which Method to Use?

| Scenario | Recommended Method | Notes |
|----------|-------------------|-------|
| **Production DLT Pipelines** | Method 1 (DLT Pipeline Config) | ✅ Most reliable |
| **DAB Deployments** | Method 1 Option B (DAB YAML) | ✅ Best practice |
| **Interactive Development** | Method 2 (Cluster Libraries) | For testing only |
| **Custom Driver Versions** | Method 3 (Init Scripts) | Advanced use cases |
| **Shared Across Workspace** | Method 4 (UC Volumes) | Enterprise standard |

**For DLT-META pipelines, always use Method 1 (DLT Pipeline Configuration) for production workloads.**

### 2. Database Credentials

Store Oracle credentials in Databricks Secrets:

```bash
# Create secret scope (one-time setup)
databricks secrets create-scope --scope oracle_secrets

# Add credentials
databricks secrets put --scope oracle_secrets --key username
databricks secrets put --scope oracle_secrets --key password
```

### 3. Network Connectivity

Ensure Databricks workspace can connect to Oracle:
- **Cloud:** Configure VPC peering or private link
- **On-Premise:** Configure VPN or AWS Direct Connect
- **Firewall:** Allow outbound connections on Oracle port (default: 1521)

### 4. Oracle User Permissions

```sql
-- Minimum required privileges for read-only access
GRANT CONNECT TO dlt_meta_user;
GRANT SELECT ON schema.table_name TO dlt_meta_user;

-- For incremental loads with metadata queries
GRANT SELECT ON DBA_TABLES TO dlt_meta_user;
GRANT SELECT ON DBA_TAB_COLUMNS TO dlt_meta_user;

-- For CDC using Oracle SCN (System Change Number)
GRANT SELECT ON V$DATABASE TO dlt_meta_user;
GRANT SELECT ON V$ARCHIVED_LOG TO dlt_meta_user;
```

---

## Oracle JDBC Connection Setup

### Connection String Formats

#### Format 1: Basic Connection (Host:Port:SID)
```
jdbc:oracle:thin:@//hostname:1521/ORCL
```

#### Format 2: Service Name
```
jdbc:oracle:thin:@//hostname:1521/service_name
```

#### Format 3: TNS Names
```
jdbc:oracle:thin:@tns_name
```

#### Format 4: Oracle RAC (Multiple Hosts)
```
jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=service_name)))
```

### Connection Properties

```json
{
  "url": "jdbc:oracle:thin:@//oracle-prod.company.com:1521/ORCL",
  "user": "{{secrets/oracle_secrets/username}}",
  "password": "{{secrets/oracle_secrets/password}}",
  "driver": "oracle.jdbc.driver.OracleDriver",
  "fetchSize": "10000",
  "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
}
```

### Testing Connection

Test Oracle connectivity before creating onboarding files:

```python
# Test in Databricks notebook
from pyspark.sql import SparkSession

jdbc_url = "jdbc:oracle:thin:@//oracle-prod.company.com:1521/ORCL"
connection_properties = {
    "user": dbutils.secrets.get(scope="oracle_secrets", key="username"),
    "password": dbutils.secrets.get(scope="oracle_secrets", key="password"),
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Test connection
df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT 1 as test_col FROM DUAL) test_query",
    properties=connection_properties
)
df.show()

# If successful, shows:
# +--------+
# |test_col|
# +--------+
# |       1|
# +--------+
```

---

## Ingestion Patterns

### Pattern 1: Full Table Load (Batch)

**Use Case:** Small tables (< 1M rows), dimension tables, one-time loads

**Advantages:**
- ✅ Simple configuration
- ✅ No watermark column required
- ✅ Captures complete current state

**Disadvantages:**
- ❌ Not incremental (loads entire table each time)
- ❌ Higher load on source database
- ❌ Slower for large tables

**Onboarding Configuration:**

```json
{
  "data_flow_id": "1001",
  "data_flow_group": "finance_oracle",
  "source_system": "oracle_erp",
  "source_format": "delta",

  "source_details": {
    "source_database": "oracle_landing",
    "source_table": "customers",
    "jdbc_url": "jdbc:oracle:thin:@//oracle-prod:1521/ORCL",
    "jdbc_table": "ERP_SCHEMA.CUSTOMERS",
    "jdbc_user": "{{secrets/oracle_secrets/username}}",
    "jdbc_password": "{{secrets/oracle_secrets/password}}",
    "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
    "load_type": "full"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing_layer",
  "landing_table": "oracle_customers",
  "landing_table_comment": "Oracle ERP customer master data - full load"
}
```

### Pattern 2: Incremental Load (Watermark-Based)

**Use Case:** Large tables with timestamp or sequence columns

**Advantages:**
- ✅ Only loads new/changed records
- ✅ Reduces load on source database
- ✅ Fast for large tables

**Disadvantages:**
- ❌ Requires watermark column (timestamp or incrementing ID)
- ❌ Doesn't detect deletes
- ❌ Requires careful watermark management

**Requirements:**
- Table must have a monotonically increasing column:
  - Timestamp: `LAST_MODIFIED_DATE`, `UPDATED_AT`, `CREATE_TIMESTAMP`
  - Sequence: `ID`, `SEQUENCE_NUMBER`, `VERSION`

**Onboarding Configuration:**

```json
{
  "data_flow_id": "1002",
  "data_flow_group": "finance_oracle",
  "source_system": "oracle_erp",
  "source_format": "delta",

  "source_details": {
    "jdbc_url": "jdbc:oracle:thin:@//oracle-prod:1521/ORCL",
    "jdbc_table": "ERP_SCHEMA.TRANSACTIONS",
    "jdbc_user": "{{secrets/oracle_secrets/username}}",
    "jdbc_password": "{{secrets/oracle_secrets/password}}",
    "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
    "load_type": "incremental",
    "watermark_column": "LAST_MODIFIED_DATE",
    "watermark_start_value": "2023-01-01 00:00:00"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing_layer",
  "landing_table": "oracle_transactions",
  "landing_table_comment": "Oracle ERP transactions - incremental load by LAST_MODIFIED_DATE",

  "landing_reader_options": {
    "fetchSize": "50000",
    "numPartitions": "8",
    "partitionColumn": "TRANSACTION_ID",
    "lowerBound": "1",
    "upperBound": "10000000"
  }
}
```

**How Incremental Load Works:**

```
First Run:                           Second Run:
┌────────────────────────┐           ┌────────────────────────┐
│ Oracle: TRANSACTIONS   │           │ Oracle: TRANSACTIONS   │
│ LAST_MODIFIED_DATE     │           │ LAST_MODIFIED_DATE     │
│ ───────────────────    │           │ ───────────────────    │
│ 2023-01-01 10:00:00    │  Read     │ 2023-01-01 10:00:00    │  Skip
│ 2023-01-01 11:00:00    │  All      │ 2023-01-01 11:00:00    │  (already loaded)
│ 2023-01-01 12:00:00    │  ─────▶   │ 2023-01-01 12:00:00    │
│                        │           │ 2023-01-02 09:00:00    │  Read NEW
│                        │           │ 2023-01-02 10:00:00    │  ─────▶
└────────────────────────┘           └────────────────────────┘
    ↓                                     ↓
Watermark: 2023-01-01 12:00:00       Watermark: 2023-01-02 10:00:00
```

### Pattern 3: Query-Based Load (Custom SQL)

**Use Case:** Complex filtering, joins, transformations at source

**Advantages:**
- ✅ Filter data at source (reduces data transfer)
- ✅ Perform joins in Oracle (reduce Spark processing)
- ✅ Custom business logic

**Disadvantages:**
- ❌ Higher load on Oracle database
- ❌ Query performance depends on Oracle indexes
- ❌ Less portable

**Onboarding Configuration:**

```json
{
  "data_flow_id": "1003",
  "data_flow_group": "finance_oracle",
  "source_system": "oracle_erp",
  "source_format": "delta",

  "source_details": {
    "jdbc_url": "jdbc:oracle:thin:@//oracle-prod:1521/ORCL",
    "jdbc_query": "(SELECT o.ORDER_ID, o.ORDER_DATE, o.CUSTOMER_ID, c.CUSTOMER_NAME, o.AMOUNT FROM ERP_SCHEMA.ORDERS o JOIN ERP_SCHEMA.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID WHERE o.ORDER_DATE >= TO_DATE('2023-01-01', 'YYYY-MM-DD')) order_query",
    "jdbc_user": "{{secrets/oracle_secrets/username}}",
    "jdbc_password": "{{secrets/oracle_secrets/password}}",
    "jdbc_driver": "oracle.jdbc.driver.OracleDriver"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing_layer",
  "landing_table": "oracle_orders_with_customer",
  "landing_table_comment": "Oracle orders joined with customer names"
}
```

**Important:** Wrap custom queries in parentheses with an alias!

```sql
-- ✅ CORRECT
(SELECT * FROM SCHEMA.TABLE WHERE date >= '2023-01-01') my_query

-- ❌ WRONG (will fail)
SELECT * FROM SCHEMA.TABLE WHERE date >= '2023-01-01'
```

### Pattern 4: CDC with Apply Changes (Operation Column)

**Use Case:** Tables with INSERT/UPDATE/DELETE operation tracking

**Advantages:**
- ✅ Captures all changes (inserts, updates, deletes)
- ✅ Maintains history (SCD Type 2)
- ✅ No missed changes

**Requirements:**
- Table must have:
  - Operation column: `OPERATION`, `CHANGE_TYPE`, `CDC_FLAG` (values: I/U/D or INSERT/UPDATE/DELETE)
  - Sequence column: `UPDATED_TIMESTAMP`, `SCN`, `VERSION`
  - Primary key column(s)

**Onboarding Configuration:**

```json
{
  "data_flow_id": "1004",
  "data_flow_group": "finance_oracle",
  "source_system": "oracle_erp",
  "source_format": "delta",

  "source_details": {
    "jdbc_url": "jdbc:oracle:thin:@//oracle-prod:1521/ORCL",
    "jdbc_table": "ERP_SCHEMA.CUSTOMERS_CDC",
    "jdbc_user": "{{secrets/oracle_secrets/username}}",
    "jdbc_password": "{{secrets/oracle_secrets/password}}",
    "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
    "load_type": "incremental",
    "watermark_column": "CDC_TIMESTAMP",
    "watermark_start_value": "2023-01-01 00:00:00"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing_layer",
  "landing_table": "oracle_customers_cdc_raw",

  "refinery_catalog_prod": "my_catalog",
  "refinery_database_prod": "refinery_layer",
  "refinery_table": "oracle_customers",
  "refinery_table_comment": "Oracle customers with CDC - SCD Type 2",

  "refinery_cdc_apply_changes": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "CDC_TIMESTAMP",
    "scd_type": "2",
    "apply_as_deletes": "OPERATION = 'D'",
    "apply_as_truncates": "OPERATION = 'T'",
    "except_column_list": ["OPERATION", "CDC_TIMESTAMP"]
  }
}
```

**CDC Flow:**

```
Oracle CDC Table                Landing (Bronze)               Refinery (Silver)
┌─────────────────────┐        ┌──────────────────┐          ┌──────────────────┐
│ CUSTOMER_ID: 1001   │        │ All CDC records  │          │ CUSTOMER_ID: 1001│
│ NAME: John Doe      │ Load   │ with OPERATION   │  CDC     │ NAME: John Doe   │
│ OPERATION: I        │───────▶│ column           │─────────▶│ __START_AT: T1   │
│ CDC_TIMESTAMP: T1   │        │                  │  Apply   │ __END_AT: NULL   │
└─────────────────────┘        └──────────────────┘  Changes │                  │
                                                              │ (Current Record) │
┌─────────────────────┐                                      └──────────────────┘
│ CUSTOMER_ID: 1001   │                                      ┌──────────────────┐
│ NAME: Jane Doe      │                                      │ CUSTOMER_ID: 1001│
│ OPERATION: U        │                                      │ NAME: Jane Doe   │
│ CDC_TIMESTAMP: T2   │                                      │ __START_AT: T2   │
└─────────────────────┘                                      │ __END_AT: NULL   │
                                                              │                  │
                                                              │ (Current Record) │
                                                              └──────────────────┘
                                                              ┌──────────────────┐
                                                              │ CUSTOMER_ID: 1001│
                                                              │ NAME: John Doe   │
                                                              │ __START_AT: T1   │
                                                              │ __END_AT: T2     │
                                                              │                  │
                                                              │ (History Record) │
                                                              └──────────────────┘
```

### Pattern 5: CDC from Snapshot (No Operation Column)

**Use Case:** Tables without operation columns, periodic snapshot comparison

**Advantages:**
- ✅ Works with any table (no CDC column required)
- ✅ Automatically detects changes
- ✅ Maintains history (SCD Type 2)

**Disadvantages:**
- ❌ Cannot detect deletes reliably
- ❌ Requires full table read each time
- ❌ Higher resource usage

**Onboarding Configuration:**

```json
{
  "data_flow_id": "1005",
  "data_flow_group": "finance_oracle",
  "source_system": "oracle_erp",
  "source_format": "snapshot",

  "source_details": {
    "jdbc_url": "jdbc:oracle:thin:@//oracle-prod:1521/ORCL",
    "jdbc_table": "ERP_SCHEMA.PRODUCTS",
    "jdbc_user": "{{secrets/oracle_secrets/username}}",
    "jdbc_password": "{{secrets/oracle_secrets/password}}",
    "jdbc_driver": "oracle.jdbc.driver.OracleDriver"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing_layer",
  "landing_table": "oracle_products_snapshot",

  "refinery_catalog_prod": "my_catalog",
  "refinery_database_prod": "refinery_layer",
  "refinery_table": "oracle_products",
  "refinery_table_comment": "Oracle products with snapshot-based CDC - SCD Type 2",

  "refinery_apply_changes_from_snapshot": {
    "keys": ["PRODUCT_ID"],
    "scd_type": "2",
    "track_history_column_list": ["PRODUCT_NAME", "PRICE", "STATUS"]
  }
}
```

---

## Onboarding File Configuration

### Minimal Oracle Onboarding File

```json
[
  {
    "data_flow_id": "1001",
    "data_flow_group": "oracle_ingestion",
    "source_format": "delta",

    "source_details": {
      "jdbc_url": "jdbc:oracle:thin:@//hostname:1521/ORCL",
      "jdbc_table": "SCHEMA.TABLE_NAME",
      "jdbc_user": "{{secrets/oracle_secrets/username}}",
      "jdbc_password": "{{secrets/oracle_secrets/password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver"
    },

    "landing_database_prod": "landing_db",
    "landing_table": "oracle_table_name"
  }
]
```

### Complete Oracle Onboarding File (All Layers)

```json
[
  {
    "data_flow_id": "1010",
    "data_flow_group": "finance_oracle_gl",
    "source_system": "oracle_erp_prod",
    "source_format": "delta",

    "source_details": {
      "source_database": "oracle_finance",
      "source_table": "gl_journal_entries",
      "jdbc_url": "jdbc:oracle:thin:@//oracle-fin-prod.company.com:1521/FINPROD",
      "jdbc_table": "GL_SCHEMA.JOURNAL_ENTRIES",
      "jdbc_user": "{{secrets/oracle_secrets/gl_username}}",
      "jdbc_password": "{{secrets/oracle_secrets/gl_password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
      "load_type": "incremental",
      "watermark_column": "LAST_UPDATE_DATE",
      "watermark_start_value": "2023-01-01 00:00:00"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_finance",
    "landing_table": "oracle_gl_journal_entries",
    "landing_table_comment": "Oracle ERP GL journal entries - incremental load",

    "landing_reader_options": {
      "fetchSize": "100000",
      "numPartitions": "16",
      "partitionColumn": "JOURNAL_ID",
      "lowerBound": "1",
      "upperBound": "100000000",
      "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
    },

    "landing_cluster_by": ["LEDGER_ID", "ACCOUNTING_DATE"],

    "landing_table_properties": {
      "pipelines.autoOptimize.managed": "true",
      "delta.enableChangeDataFeed": "true"
    },

    "landing_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/gl_journal_entries_landing_dqe.json",

    "landing_catalog_quarantine_prod": "enterprise_data",
    "landing_database_quarantine_prod": "landing_finance",
    "landing_quarantine_table": "oracle_gl_journal_entries_quarantine",
    "landing_quarantine_table_cluster_by": ["JOURNAL_ID"],

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_finance",
    "refinery_table": "gl_journal_entries",
    "refinery_table_comment": "Cleaned GL journal entries with enrichments",

    "refinery_select_exp": [
      "*",
      "CAST(ACCOUNTING_DATE AS DATE) as accounting_date_clean",
      "UPPER(TRIM(JOURNAL_SOURCE)) as journal_source_clean",
      "CASE WHEN STATUS = 'P' THEN 'Posted' WHEN STATUS = 'U' THEN 'Unposted' ELSE 'Unknown' END as status_desc"
    ],

    "refinery_where_clause": "STATUS IS NOT NULL AND LEDGER_ID IS NOT NULL",

    "refinery_transformation_json_prod": "/Volumes/enterprise_data/dlt_meta/transformations/gl_enrichment.json",

    "refinery_cluster_by": ["LEDGER_ID", "ACCOUNTING_DATE"],

    "refinery_table_properties": {
      "pipelines.autoOptimize.zOrderCols": "JOURNAL_ID,ACCOUNTING_DATE"
    },

    "refinery_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/gl_journal_entries_refinery_dqe.json",

    "treasury_catalog_prod": "enterprise_data",
    "treasury_database_prod": "treasury_finance",
    "treasury_table": "gl_monthly_summary",
    "treasury_table_comment": "Monthly GL journal entries summary by ledger and account",

    "treasury_transformation_json_prod": "/Volumes/enterprise_data/dlt_meta/transformations/gl_monthly_summary.json",

    "treasury_cluster_by": ["ACCOUNTING_MONTH", "LEDGER_ID"],

    "treasury_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/gl_monthly_summary_dqe.json"
  }
]
```

### Important Source Details Fields

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `jdbc_url` | Yes | Oracle JDBC connection URL | `jdbc:oracle:thin:@//host:1521/ORCL` |
| `jdbc_table` | Yes* | Fully qualified table name | `SCHEMA.TABLE_NAME` |
| `jdbc_query` | Yes* | Custom SQL query | `(SELECT * FROM ...) query_alias` |
| `jdbc_user` | Yes | Database username (use secrets!) | `{{secrets/oracle_secrets/username}}` |
| `jdbc_password` | Yes | Database password (use secrets!) | `{{secrets/oracle_secrets/password}}` |
| `jdbc_driver` | Yes | JDBC driver class | `oracle.jdbc.driver.OracleDriver` |
| `load_type` | No | `full` or `incremental` | `incremental` |
| `watermark_column` | No | Column for incremental tracking | `LAST_MODIFIED_DATE` |
| `watermark_start_value` | No | Initial watermark value | `2023-01-01 00:00:00` |

*Either `jdbc_table` or `jdbc_query` required, not both

---

## Complete Examples

### Example 1: Full Table Load - Customer Master

**Scenario:** Small dimension table (100K rows), load once daily

**Onboarding File:** `conf/onboarding/oracle/customers.json`

```json
[
  {
    "data_flow_id": "2001",
    "data_flow_group": "crm_oracle",
    "source_system": "oracle_crm",
    "source_format": "delta",

    "source_details": {
      "source_database": "oracle_crm",
      "source_table": "customers",
      "jdbc_url": "jdbc:oracle:thin:@//crm-oracle-prod.company.com:1521/CRMPROD",
      "jdbc_table": "CRM_SCHEMA.CUSTOMERS",
      "jdbc_user": "{{secrets/oracle_secrets/crm_user}}",
      "jdbc_password": "{{secrets/oracle_secrets/crm_password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
      "load_type": "full"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_crm",
    "landing_table": "oracle_customers",
    "landing_table_comment": "Oracle CRM customer master - daily full load",

    "landing_reader_options": {
      "fetchSize": "10000"
    },

    "landing_cluster_by": ["CUSTOMER_ID"],

    "landing_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/customers_landing_dqe.json"
  }
]
```

**Data Quality File:** `/Volumes/enterprise_data/dlt_meta/dqe/customers_landing_dqe.json`

```json
{
  "expect_all_or_drop": {
    "valid_customer_id": "CUSTOMER_ID IS NOT NULL",
    "valid_customer_number": "CUSTOMER_NUMBER IS NOT NULL AND LENGTH(CUSTOMER_NUMBER) > 0"
  },
  "expect_all": {
    "valid_email": "EMAIL IS NULL OR EMAIL LIKE '%@%.%'",
    "valid_status": "STATUS IN ('ACTIVE', 'INACTIVE', 'SUSPENDED')"
  },
  "expect_or_quarantine": {
    "suspicious_creation_date": "CREATION_DATE <= CURRENT_DATE()"
  }
}
```

### Example 2: Incremental Load - Sales Orders

**Scenario:** Large fact table (50M rows), load new/updated records every hour

**Onboarding File:** `conf/onboarding/oracle/sales_orders.json`

```json
[
  {
    "data_flow_id": "2002",
    "data_flow_group": "sales_oracle",
    "source_system": "oracle_sales",
    "source_format": "delta",

    "source_details": {
      "source_database": "oracle_sales",
      "source_table": "orders",
      "jdbc_url": "jdbc:oracle:thin:@//sales-oracle-prod.company.com:1521/SALESPROD",
      "jdbc_table": "SALES_SCHEMA.ORDERS",
      "jdbc_user": "{{secrets/oracle_secrets/sales_user}}",
      "jdbc_password": "{{secrets/oracle_secrets/sales_password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
      "load_type": "incremental",
      "watermark_column": "LAST_MODIFIED_DATE",
      "watermark_start_value": "2023-01-01 00:00:00"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_sales",
    "landing_table": "oracle_orders",
    "landing_table_comment": "Oracle sales orders - incremental by LAST_MODIFIED_DATE",

    "landing_reader_options": {
      "fetchSize": "100000",
      "numPartitions": "32",
      "partitionColumn": "ORDER_ID",
      "lowerBound": "1",
      "upperBound": "100000000"
    },

    "landing_cluster_by": ["ORDER_DATE", "CUSTOMER_ID"],

    "landing_table_properties": {
      "pipelines.autoOptimize.managed": "true"
    },

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_sales",
    "refinery_table": "orders",
    "refinery_table_comment": "Cleaned and enriched sales orders",

    "refinery_select_exp": [
      "*",
      "CAST(ORDER_DATE AS DATE) as order_date_clean",
      "CAST(ORDER_AMOUNT AS DECIMAL(18,4)) as order_amount_clean"
    ],

    "refinery_where_clause": "ORDER_STATUS IS NOT NULL",

    "refinery_cluster_by": ["ORDER_DATE", "CUSTOMER_ID"]
  }
]
```

### Example 3: CDC with SCD Type 2 - Product Catalog

**Scenario:** Product table with operation column, maintain full history

**Onboarding File:** `conf/onboarding/oracle/products_cdc.json`

```json
[
  {
    "data_flow_id": "2003",
    "data_flow_group": "catalog_oracle",
    "source_system": "oracle_catalog",
    "source_format": "delta",

    "source_details": {
      "source_database": "oracle_catalog",
      "source_table": "products_cdc",
      "jdbc_url": "jdbc:oracle:thin:@//catalog-oracle-prod.company.com:1521/CATPROD",
      "jdbc_table": "CATALOG_SCHEMA.PRODUCTS_CDC",
      "jdbc_user": "{{secrets/oracle_secrets/catalog_user}}",
      "jdbc_password": "{{secrets/oracle_secrets/catalog_password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver",
      "load_type": "incremental",
      "watermark_column": "CDC_TIMESTAMP",
      "watermark_start_value": "2023-01-01 00:00:00"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_catalog",
    "landing_table": "oracle_products_cdc_raw",
    "landing_table_comment": "Oracle product CDC stream - raw",

    "landing_cluster_by": ["PRODUCT_ID"],

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_catalog",
    "refinery_table": "products",
    "refinery_table_comment": "Product catalog with full history - SCD Type 2",

    "refinery_cdc_apply_changes": {
      "keys": ["PRODUCT_ID"],
      "sequence_by": "CDC_TIMESTAMP",
      "scd_type": "2",
      "apply_as_deletes": "CDC_OPERATION = 'DELETE'",
      "except_column_list": ["CDC_OPERATION", "CDC_TIMESTAMP", "CDC_USER"]
    },

    "refinery_cluster_by": ["PRODUCT_ID", "CATEGORY_ID"]
  }
]
```

### Example 4: Custom Query - Multi-Table Join

**Scenario:** Denormalized view of orders with customer and product details

**Onboarding File:** `conf/onboarding/oracle/order_details_denorm.json`

```json
[
  {
    "data_flow_id": "2004",
    "data_flow_group": "analytics_oracle",
    "source_system": "oracle_analytics",
    "source_format": "delta",

    "source_details": {
      "source_database": "oracle_analytics",
      "source_table": "order_details_view",
      "jdbc_url": "jdbc:oracle:thin:@//analytics-oracle-prod.company.com:1521/ANPROD",
      "jdbc_query": "(SELECT o.ORDER_ID, o.ORDER_DATE, o.ORDER_AMOUNT, c.CUSTOMER_ID, c.CUSTOMER_NAME, c.CUSTOMER_EMAIL, p.PRODUCT_ID, p.PRODUCT_NAME, p.PRODUCT_CATEGORY FROM SALES_SCHEMA.ORDERS o JOIN CRM_SCHEMA.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID JOIN CATALOG_SCHEMA.PRODUCTS p ON o.PRODUCT_ID = p.PRODUCT_ID WHERE o.ORDER_DATE >= ADD_MONTHS(SYSDATE, -12)) order_details_query",
      "jdbc_user": "{{secrets/oracle_secrets/analytics_user}}",
      "jdbc_password": "{{secrets/oracle_secrets/analytics_password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_analytics",
    "landing_table": "oracle_order_details_denorm",
    "landing_table_comment": "Denormalized order details from Oracle - last 12 months",

    "landing_reader_options": {
      "fetchSize": "50000",
      "numPartitions": "16",
      "partitionColumn": "ORDER_ID",
      "lowerBound": "1",
      "upperBound": "10000000"
    },

    "landing_cluster_by": ["ORDER_DATE", "CUSTOMER_ID"]
  }
]
```

### Example 5: Snapshot-Based CDC - Inventory

**Scenario:** Inventory table without CDC columns, detect changes via snapshot comparison

**Onboarding File:** `conf/onboarding/oracle/inventory_snapshot.json`

```json
[
  {
    "data_flow_id": "2005",
    "data_flow_group": "supply_chain_oracle",
    "source_system": "oracle_scm",
    "source_format": "snapshot",

    "source_details": {
      "source_database": "oracle_scm",
      "source_table": "inventory",
      "jdbc_url": "jdbc:oracle:thin:@//scm-oracle-prod.company.com:1521/SCMPROD",
      "jdbc_table": "SCM_SCHEMA.INVENTORY",
      "jdbc_user": "{{secrets/oracle_secrets/scm_user}}",
      "jdbc_password": "{{secrets/oracle_secrets/scm_password}}",
      "jdbc_driver": "oracle.jdbc.driver.OracleDriver"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_scm",
    "landing_table": "oracle_inventory_snapshot",
    "landing_table_comment": "Oracle inventory snapshots",

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_scm",
    "refinery_table": "inventory",
    "refinery_table_comment": "Inventory with change history - snapshot-based SCD Type 2",

    "refinery_apply_changes_from_snapshot": {
      "keys": ["WAREHOUSE_ID", "PRODUCT_ID"],
      "scd_type": "2",
      "track_history_column_list": ["QUANTITY_ON_HAND", "QUANTITY_RESERVED", "LAST_UPDATED"]
    },

    "refinery_cluster_by": ["WAREHOUSE_ID", "PRODUCT_ID"]
  }
]
```

---

## Best Practices

### 1. Connection Management

**✅ DO:**

```json
{
  "jdbc_user": "{{secrets/oracle_secrets/username}}",
  "jdbc_password": "{{secrets/oracle_secrets/password}}"
}
```

**❌ DON'T:**

```json
{
  "jdbc_user": "myuser",
  "jdbc_password": "mypassword123"
}
```

**Connection Pooling:**

Add to `landing_reader_options`:

```json
{
  "oracle.jdbc.implicitStatementCacheSize": "25",
  "oracle.jdbc.maxCachedBufferSize": "50"
}
```

### 2. Incremental Load Strategy

**Choose the Right Watermark Column:**

| Watermark Type | Best For | Example |
|----------------|----------|---------|
| `LAST_MODIFIED_DATE` | ✅ Tables with update tracking | Dimension tables |
| `CREATED_DATE` | ⚠️ Insert-only tables | Fact tables |
| `SEQUENCE_NUMBER` | ✅ High-volume inserts | Log tables |
| `SCN` | ✅ Oracle change tracking | CDC enabled tables |

**Handle Null Watermarks:**

```sql
-- In your watermark column selection
COALESCE(LAST_MODIFIED_DATE, CREATED_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
```

### 3. Performance Optimization

**Partitioning Configuration:**

```json
"landing_reader_options": {
  "numPartitions": "32",
  "partitionColumn": "ORDER_ID",
  "lowerBound": "1",
  "upperBound": "100000000",
  "fetchSize": "100000"
}
```

**Guidelines:**

| Table Size | numPartitions | fetchSize |
|------------|---------------|-----------|
| < 1M rows | 4-8 | 10,000 |
| 1M-10M rows | 8-16 | 50,000 |
| 10M-100M rows | 16-32 | 100,000 |
| > 100M rows | 32-64 | 100,000-200,000 |

### 4. Oracle-Specific Settings

**Session Initialization:**

```json
"landing_reader_options": {
  "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF' NLS_NUMERIC_CHARACTERS='.,' TIME_ZONE='UTC'"
}
```

**Date/Time Handling:**

Oracle DATE and TIMESTAMP types can cause issues. Standardize format:

```sql
-- In custom query
TO_CHAR(order_date, 'YYYY-MM-DD HH24:MI:SS') as order_date_str
```

### 5. Schema Management

**Landing Layer:**

- Use Oracle schema exactly as-is (preserve all columns)
- Add metadata columns via `source_metadata`

```json
"source_metadata": {
  "include_autoloader_metadata_column": "True",
  "autoloader_metadata_col_name": "ingestion_metadata",
  "select_metadata_cols": {
    "ingestion_timestamp": "CURRENT_TIMESTAMP()",
    "source_system": "'oracle_erp'"
  }
}
```

**Refinery Layer:**

- Cast Oracle types to Spark types
- Clean and validate data
- Apply business rules

```json
"refinery_select_exp": [
  "CUSTOMER_ID",
  "CAST(CUSTOMER_NUMBER AS STRING) as customer_number",
  "TRIM(CUSTOMER_NAME) as customer_name",
  "CAST(CREATION_DATE AS TIMESTAMP) as creation_timestamp",
  "CASE WHEN STATUS = 'A' THEN 'ACTIVE' ELSE 'INACTIVE' END as status"
]
```

### 6. Data Quality

**Landing Layer DQE:**

```json
{
  "expect_all_or_drop": {
    "non_null_pk": "ORDER_ID IS NOT NULL",
    "valid_date": "ORDER_DATE IS NOT NULL"
  },
  "expect_or_quarantine": {
    "future_date_check": "ORDER_DATE <= CURRENT_DATE()",
    "reasonable_amount": "ORDER_AMOUNT >= 0 AND ORDER_AMOUNT < 1000000"
  }
}
```

### 7. Error Handling

**Quarantine Tables:**

Always configure quarantine tables for production:

```json
"landing_quarantine_table": "oracle_orders_quarantine",
"landing_quarantine_table_cluster_by": ["ORDER_ID"]
```

**Monitoring:**

Add alerting for quarantine table growth:

```sql
SELECT COUNT(*) as quarantine_count,
       MAX(ingestion_timestamp) as last_quarantine
FROM landing.oracle_orders_quarantine
WHERE ingestion_timestamp >= CURRENT_DATE()
```

---

## Performance Tuning

### 1. JDBC Read Performance

**Parallel Reads:**

Spark can read Oracle tables in parallel using partition columns:

```json
"landing_reader_options": {
  "numPartitions": "16",
  "partitionColumn": "ORDER_ID",
  "lowerBound": "1",
  "upperBound": "10000000"
}
```

**How it works:**

```
Oracle Table: ORDERS (10M rows, ORDER_ID: 1 to 10,000,000)
numPartitions = 4

Spark creates 4 parallel queries:

Task 1: WHERE ORDER_ID >= 1 AND ORDER_ID < 2,500,000
Task 2: WHERE ORDER_ID >= 2,500,000 AND ORDER_ID < 5,000,000
Task 3: WHERE ORDER_ID >= 5,000,000 AND ORDER_ID < 7,500,000
Task 4: WHERE ORDER_ID >= 7,500,000 AND ORDER_ID <= 10,000,000
```

**Requirements:**
- Partition column must be numeric (INT, LONG, DECIMAL)
- Partition column should have index in Oracle
- lowerBound/upperBound should cover the actual data range

### 2. Fetch Size Tuning

`fetchSize` controls how many rows are fetched per network round-trip:

| Scenario | Recommended fetchSize | Reason |
|----------|----------------------|--------|
| Few wide columns (100+) | 5,000-10,000 | Reduce memory usage |
| Many narrow columns (10-20) | 50,000-100,000 | Reduce network round-trips |
| BLOB/CLOB columns | 1,000-5,000 | Large object handling |

### 3. Oracle Indexes

Ensure Oracle tables have proper indexes:

```sql
-- For incremental loads
CREATE INDEX idx_orders_modified ON ORDERS(LAST_MODIFIED_DATE);

-- For partition columns
CREATE INDEX idx_orders_id ON ORDERS(ORDER_ID);

-- For CDC
CREATE INDEX idx_products_cdc_ts ON PRODUCTS_CDC(CDC_TIMESTAMP);
```

### 4. Network Optimization

**Use Oracle Net Services Configuration:**

In `tnsnames.ora`:

```
PROD_ORCL =
  (DESCRIPTION =
    (SDU=65535)
    (RECV_BUF_SIZE=1048576)
    (SEND_BUF_SIZE=1048576)
    (ADDRESS = (PROTOCOL = TCP)(HOST = oracle-prod)(PORT = 1521))
    (CONNECT_DATA = (SERVICE_NAME = ORCL))
  )
```

**JDBC URL Parameters:**

```
jdbc:oracle:thin:@//host:1521/ORCL?oracle.net.CONNECT_TIMEOUT=10000&oracle.jdbc.ReadTimeout=60000
```

### 5. Cluster Configuration

**Recommended Databricks Cluster Settings:**

For large Oracle ingestion workloads:

```json
{
  "cluster_name": "oracle-ingestion-cluster",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_E8ds_v4",
  "num_workers": 8,
  "spark_conf": {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.adaptive.enabled": "true"
  },
  "driver_node_type_id": "Standard_E8ds_v4"
}
```

---

## Troubleshooting

### Issue 1: Connection Timeout

**Symptoms:**

```
Error: Connection timed out
java.sql.SQLRecoverableException: IO Error: Connection timed out
```

**Solutions:**

1. **Check Network Connectivity:**

```bash
# From Databricks notebook
%sh
nc -zv oracle-prod.company.com 1521
```

2. **Increase Connection Timeout:**

```json
"jdbc_url": "jdbc:oracle:thin:@//host:1521/ORCL?oracle.net.CONNECT_TIMEOUT=30000"
```

3. **Verify Firewall Rules:**

Ensure Databricks workspace CIDR can reach Oracle on port 1521

### Issue 2: ORA-01017: invalid username/password

**Symptoms:**

```
Error: ORA-01017: invalid username/password; logon denied
```

**Solutions:**

1. **Verify Secrets:**

```python
# Test in notebook
username = dbutils.secrets.get(scope="oracle_secrets", key="username")
password = dbutils.secrets.get(scope="oracle_secrets", key="password")
print(f"Username: {username}")
print(f"Password: {'*' * len(password)}")
```

2. **Check Account Status:**

```sql
-- In Oracle
SELECT username, account_status, expiry_date
FROM dba_users
WHERE username = 'DLT_META_USER';
```

3. **Reset Password:**

```sql
ALTER USER dlt_meta_user IDENTIFIED BY new_password;
```

### Issue 3: ORA-00942: table or view does not exist

**Symptoms:**

```
Error: ORA-00942: table or view does not exist
```

**Solutions:**

1. **Verify Table Name (case-sensitive):**

```json
"jdbc_table": "SCHEMA.TABLE_NAME"  // Use UPPERCASE for Oracle
```

2. **Check Grants:**

```sql
-- In Oracle
GRANT SELECT ON schema.table_name TO dlt_meta_user;
```

3. **Verify Schema:**

```sql
-- List tables accessible to user
SELECT owner, table_name
FROM all_tables
WHERE owner = 'SCHEMA_NAME';
```

### Issue 4: Memory Errors (OutOfMemoryError)

**Symptoms:**

```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

1. **Reduce fetchSize:**

```json
"landing_reader_options": {
  "fetchSize": "5000"  // Reduced from 100000
}
```

2. **Increase Partitions:**

```json
"landing_reader_options": {
  "numPartitions": "64"  // Increased from 16
}
```

3. **Use Larger Cluster:**

Increase worker memory or add more workers

### Issue 5: Incremental Load Missing Records

**Symptoms:**

Records are missing in incremental loads

**Solutions:**

1. **Check Watermark Column Nulls:**

```sql
-- In Oracle, find records with NULL watermark
SELECT COUNT(*) FROM schema.table_name
WHERE watermark_column IS NULL;
```

2. **Use COALESCE in Watermark:**

```json
"watermark_column": "COALESCE(LAST_MODIFIED_DATE, CREATED_DATE)"
```

3. **Verify Watermark Persistence:**

```sql
-- Check watermark table in Databricks
SELECT * FROM landing.watermark_tracking
WHERE table_name = 'oracle_table_name';
```

### Issue 6: Date Format Issues

**Symptoms:**

```
Error: Invalid date format
java.sql.SQLException: Invalid date format
```

**Solutions:**

1. **Set Session Date Format:**

```json
"landing_reader_options": {
  "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
}
```

2. **Use TO_CHAR in Query:**

```sql
(SELECT
  ORDER_ID,
  TO_CHAR(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS') as ORDER_DATE,
  TO_CHAR(LAST_MODIFIED_DATE, 'YYYY-MM-DD HH24:MI:SS.FF6') as LAST_MODIFIED_DATE
FROM schema.orders) order_query
```

### Issue 7: Performance Degradation

**Symptoms:**

Pipeline runs are slow or taking longer over time

**Solutions:**

1. **Optimize Oracle Query:**

```sql
-- Add index on watermark column
CREATE INDEX idx_table_watermark ON schema.table_name(watermark_column);

-- Analyze table statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('SCHEMA', 'TABLE_NAME');
```

2. **Check Databricks Optimization:**

```json
"landing_table_properties": {
  "pipelines.autoOptimize.managed": "true",
  "delta.autoOptimize.optimizeWrite": "true",
  "delta.autoOptimize.autoCompact": "true"
}
```

3. **Review Partition Strategy:**

```python
# Check partition distribution in notebook
display(spark.read.table("landing.oracle_table").groupBy(spark_partition_id()).count())
```

---

## Security Considerations

### 1. Credential Management

**Always use Databricks Secrets:**

```bash
# Create secrets (CLI)
databricks secrets create-scope --scope oracle_secrets
databricks secrets put --scope oracle_secrets --key prod_username
databricks secrets put --scope oracle_secrets --key prod_password
```

**Access Control:**

```bash
# Grant access to specific users/groups
databricks secrets put-acl --scope oracle_secrets \
  --principal data-engineering-team --permission READ
```

### 2. Network Security

**Use Private Link / VPC Peering:**

```
Databricks VPC          Oracle VPC
┌──────────────┐        ┌──────────────┐
│              │        │              │
│  Cluster     │◀──────▶│  Oracle DB   │
│              │ Private│              │
└──────────────┘ Link   └──────────────┘
```

**Restrict Oracle Access:**

```sql
-- In Oracle, limit access by IP
BEGIN
  DBMS_NETWORK_ACL_ADMIN.CREATE_ACL (
    acl => 'databricks_acl.xml',
    description => 'Allow access from Databricks',
    principal => 'DLT_META_USER',
    is_grant => TRUE,
    privilege => 'connect'
  );

  DBMS_NETWORK_ACL_ADMIN.ASSIGN_ACL (
    acl => 'databricks_acl.xml',
    host => 'databricks-nat-gateway-ip'
  );
END;
/
```

### 3. Principle of Least Privilege

**Grant minimum required permissions:**

```sql
-- Create read-only user
CREATE USER dlt_meta_reader IDENTIFIED BY secure_password;
GRANT CREATE SESSION TO dlt_meta_reader;

-- Grant SELECT only on required tables
GRANT SELECT ON schema.table1 TO dlt_meta_reader;
GRANT SELECT ON schema.table2 TO dlt_meta_reader;

-- Grant SELECT on metadata tables (for incremental)
GRANT SELECT ON DBA_TABLES TO dlt_meta_reader;
GRANT SELECT ON DBA_TAB_COLUMNS TO dlt_meta_reader;
```

### 4. Audit Logging

**Enable Oracle Audit:**

```sql
-- Audit all access from DLT-META user
AUDIT SELECT ON schema.sensitive_table BY dlt_meta_reader;

-- Review audit logs
SELECT username, action_name, obj_name, timestamp
FROM dba_audit_trail
WHERE username = 'DLT_META_READER'
ORDER BY timestamp DESC;
```

### 5. Data Masking

**Mask sensitive data at source:**

```sql
-- Custom query with masking
(SELECT
  CUSTOMER_ID,
  SUBSTR(CREDIT_CARD_NUMBER, 1, 4) || '****' || SUBSTR(CREDIT_CARD_NUMBER, -4) as CREDIT_CARD_MASKED,
  REGEXP_REPLACE(EMAIL, '^(.{3}).*(@.*)$', '\1***\2') as EMAIL_MASKED
FROM schema.customers) customer_query
```

### 6. Encryption

**Use SSL/TLS for JDBC Connection:**

```json
{
  "jdbc_url": "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=oracle-prod)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=ORCL))(SECURITY=(SSL_SERVER_CERT_DN=\"CN=oracle-prod.company.com\")))",
  "oracle.net.ssl_version": "1.2",
  "oracle.net.ssl_cipher_suites": "(TLS_RSA_WITH_AES_256_CBC_SHA256)"
}
```

---

## Additional Resources

### Internal Documentation

- [DLT-META Onboarding File Reference](ONBOARDING_FILE_REFERENCE.md)
- [DLT-META Quick Reference](ONBOARDING_QUICK_REFERENCE.md)
- [CDC Methods Comparison](CDC_METHODS_COMPARISON.md)
- [Organizing Large Scale ETLs](ORGANIZING_LARGE_SCALE_ETLS.md)

### External Documentation

- [Oracle JDBC Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/)
- [Databricks JDBC Documentation](https://docs.databricks.com/external-data/jdbc.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)

### Support

- **Data Engineering Team:** [Jira Service Desk](https://jira.company.com/servicedesk/dlt-meta)
- **Confluence:** [DLT-META Oracle Integration](https://confluence.company.com/display/DE/Oracle+Integration)

---

## Appendix

### A. Oracle to Spark Data Type Mapping

| Oracle Type | Spark Type | Notes |
|-------------|------------|-------|
| VARCHAR2 | STRING | |
| CHAR | STRING | Trailing spaces preserved |
| NUMBER | DECIMAL | Precision preserved |
| NUMBER(p,0) where p < 10 | INT | |
| NUMBER(p,0) where p >= 10 | LONG | |
| DATE | TIMESTAMP | Oracle DATE includes time |
| TIMESTAMP | TIMESTAMP | |
| CLOB | STRING | |
| BLOB | BINARY | |
| RAW | BINARY | |

### B. Common Oracle Session Settings

```sql
ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6';
ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,';
ALTER SESSION SET TIME_ZONE='UTC';
```

### C. Sample Databricks Notebook for Testing

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle Connection Test

# COMMAND ----------

# Configuration
jdbc_url = "jdbc:oracle:thin:@//oracle-prod.company.com:1521/ORCL"
jdbc_properties = {
    "user": dbutils.secrets.get(scope="oracle_secrets", key="username"),
    "password": dbutils.secrets.get(scope="oracle_secrets", key="password"),
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# COMMAND ----------

# Test connection
test_query = "(SELECT 'SUCCESS' as status, SYSDATE as current_time FROM DUAL) test"
df = spark.read.jdbc(url=jdbc_url, table=test_query, properties=jdbc_properties)
display(df)

# COMMAND ----------

# Query table schema
schema_query = """
(SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
 FROM all_tab_columns
 WHERE owner = 'SCHEMA_NAME' AND table_name = 'TABLE_NAME'
 ORDER BY column_id) schema_query
"""
df_schema = spark.read.jdbc(url=jdbc_url, table=schema_query, properties=jdbc_properties)
display(df_schema)

# COMMAND ----------

# Query row count
count_query = "(SELECT COUNT(*) as row_count FROM SCHEMA.TABLE_NAME) count_query"
df_count = spark.read.jdbc(url=jdbc_url, table=count_query, properties=jdbc_properties)
display(df_count)

# COMMAND ----------

# Test incremental query
incremental_query = """
(SELECT * FROM SCHEMA.TABLE_NAME
 WHERE LAST_MODIFIED_DATE >= TO_DATE('2024-01-01', 'YYYY-MM-DD')
 AND ROWNUM <= 100) inc_query
"""
df_inc = spark.read.jdbc(url=jdbc_url, table=incremental_query, properties=jdbc_properties)
display(df_inc)
```

---

**Document Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-25 | Initial Oracle ingestion onboarding guide created |
