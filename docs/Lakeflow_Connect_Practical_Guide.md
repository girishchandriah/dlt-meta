# Lakeflow Connect: Practical How-To Guide

**Version:** 1.0
**Last Updated:** 2026-02-23
**Audience:** Data Engineers implementing data pipelines

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Step-by-Step Tutorials](#step-by-step-tutorials)
3. [Connection Types Deep Dive](#connection-types-deep-dive)
4. [Real-World Scenarios](#real-world-scenarios)
5. [Integration with DLT-META](#integration-with-dlt-meta)
6. [Advanced Patterns](#advanced-patterns)
7. [Performance Tuning](#performance-tuning)
8. [Security Best Practices](#security-best-practices)
9. [Monitoring & Operations](#monitoring--operations)
10. [Troubleshooting Cookbook](#troubleshooting-cookbook)

---

## Quick Start

### What is Lakeflow Connect?

**Lakeflow Connect** is Databricks' managed service for ingesting data from external sources into Delta Live Tables (DLT) pipelines. Think of it as "Auto Loader on steroids" with support for multiple sources beyond just cloud storage.

### 5-Minute Example

```python
import dlt

# Step 1: Create a connection (via Databricks UI or API)
# Catalog → Connections → Create → S3
# Name: "my-data-source"
# Path: "s3://my-bucket/data/"

# Step 2: Use in DLT pipeline (that's it!)
@dlt.table(name="my_table")
def ingest():
    return dlt.read_stream("my-data-source")
```

**What just happened?**
- ✅ No schema DDL files needed (auto-inferred)
- ✅ No checkpoint management (automatic)
- ✅ No complex readStream configuration
- ✅ Schema evolution handled automatically

---

## Step-by-Step Tutorials

### Tutorial 1: Your First Lakeflow Connect Pipeline (S3 CSV)

**Goal:** Ingest CSV files from S3 into a Delta table using Lakeflow Connect.

#### Prerequisites

- Databricks workspace with Unity Catalog enabled
- AWS S3 bucket with CSV files
- IAM role with S3 read permissions attached to cluster

#### Step 1: Create S3 Connection

**Via Databricks UI:**

1. Navigate to **Catalog Explorer**
2. Click **Connections** in left sidebar
3. Click **Create Connection**
4. Fill in details:
   ```
   Connection Type: Amazon S3
   Connection Name: customer-data-s3
   Storage Credential: <select or create>
   Path: s3://my-bucket/customers/
   Format: CSV
   Additional Options:
     - header: true
     - inferSchema: true
     - delimiter: ,
   ```
5. Click **Create**

**Via API (Alternative):**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

connection = w.connections.create(
    name="customer-data-s3",
    connection_type="AMAZON_S3",
    options={
        "path": "s3://my-bucket/customers/",
        "format": "csv",
        "header": "true",
        "inferSchema": "true",
        "delimiter": ","
    }
)
```

#### Step 2: Create DLT Pipeline

Create a notebook `customer_pipeline.py`:

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="customers_landing",
    comment="Customer data from S3 CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def ingest_customers():
    """
    Ingest customer CSV files from S3.
    Schema is automatically inferred from files.
    """
    return dlt.read_stream("customer-data-s3")


@dlt.table(
    name="customers_cleaned",
    comment="Cleaned customer data",
    table_properties={"quality": "silver"}
)
def clean_customers():
    """
    Apply data quality rules and transformations.
    """
    return (
        dlt.read_stream("customers_landing")
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    )
```

#### Step 3: Deploy DLT Pipeline

**Via Databricks UI:**

1. Navigate to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   ```
   Pipeline Name: customer-ingestion-pipeline
   Product Edition: Advanced
   Source Code: /path/to/customer_pipeline.py
   Storage Location: s3://my-bucket/dlt-storage/
   Target Schema: landing
   Cluster Mode: Enhanced Autoscaling
   ```
4. Click **Create**
5. Click **Start** to run the pipeline

**Via CLI:**

```bash
databricks pipelines create --json '{
  "name": "customer-ingestion-pipeline",
  "storage": "s3://my-bucket/dlt-storage/",
  "target": "landing",
  "continuous": false,
  "libraries": [
    {"notebook": {"path": "/path/to/customer_pipeline"}}
  ],
  "clusters": [
    {
      "label": "default",
      "num_workers": 2
    }
  ]
}'
```

#### Step 4: Monitor and Verify

```sql
-- Check landing table
SELECT COUNT(*) FROM landing.customers_landing;

-- Check schema
DESCRIBE TABLE landing.customers_landing;

-- Verify data quality
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as missing_emails
FROM landing.customers_cleaned;
```

#### Step 5: Verify Schema Evolution

Add a new column to your CSV file (e.g., `phone_number`) and upload to S3:

```python
# Run pipeline again
# New column automatically added to table!

# Verify:
DESCRIBE TABLE landing.customers_landing;
-- You'll see 'phone_number' column added automatically
```

**✅ Tutorial Complete!** You've ingested CSV files from S3 with automatic schema evolution.

---

### Tutorial 2: Real-Time Streaming from Kafka

**Goal:** Stream real-time events from Kafka into Delta tables.

#### Prerequisites

- Kafka cluster accessible from Databricks
- Kafka topic with streaming data
- Network connectivity to Kafka brokers
- Databricks Secrets configured with Kafka credentials

#### Step 1: Configure Databricks Secrets

```bash
# Create secret scope
databricks secrets create-scope kafka-prod

# Add Kafka bootstrap servers
databricks secrets put-secret kafka-prod bootstrap-servers
# Enter value: kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092

# Add SASL credentials (if using SASL)
databricks secrets put-secret kafka-prod sasl-username
databricks secrets put-secret kafka-prod sasl-password
```

#### Step 2: Create Kafka Connection

**Via Databricks UI:**

1. **Catalog** → **Connections** → **Create Connection**
2. Connection details:
   ```
   Type: Apache Kafka
   Name: events-kafka-prod
   Bootstrap Servers: {{secrets/kafka-prod/bootstrap-servers}}
   Topic: user-events
   Security Protocol: SASL_SSL
   SASL Mechanism: PLAIN
   SASL Username: {{secrets/kafka-prod/sasl-username}}
   SASL Password: {{secrets/kafka-prod/sasl-password}}
   Additional Options:
     - startingOffsets: earliest
     - kafka.request.timeout.ms: 60000
     - kafka.session.timeout.ms: 60000
   ```

**Via API:**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

connection = w.connections.create(
    name="events-kafka-prod",
    connection_type="KAFKA",
    options={
        "kafka.bootstrap.servers": "{{secrets/kafka-prod/bootstrap-servers}}",
        "subscribe": "user-events",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            'username="{{secrets/kafka-prod/sasl-username}}" '
            'password="{{secrets/kafka-prod/sasl-password}}";'
        ),
        "startingOffsets": "earliest"
    }
)
```

#### Step 3: Create DLT Pipeline for Kafka Streaming

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Define event schema
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("properties", StringType(), True)
])

@dlt.table(
    name="events_raw",
    comment="Raw events from Kafka",
    table_properties={
        "quality": "bronze"
    }
)
def ingest_kafka_events():
    """
    Ingest raw Kafka events.
    Events are in binary format with key and value.
    """
    return (
        dlt.read_stream("events-kafka-prod")
        .selectExpr(
            "CAST(key AS STRING) as event_key",
            "CAST(value AS STRING) as event_json",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        )
    )


@dlt.table(
    name="events_parsed",
    comment="Parsed events with structured schema",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_json", "event_json IS NOT NULL")
def parse_events():
    """
    Parse JSON events into structured columns.
    """
    return (
        dlt.read_stream("events_raw")
        .withColumn("event", F.from_json("event_json", event_schema))
        .select(
            "event.*",
            "event_key",
            "kafka_timestamp",
            "partition",
            "offset"
        )
    )


@dlt.table(
    name="events_enriched",
    comment="Enriched events with derived fields",
    table_properties={"quality": "gold"}
)
def enrich_events():
    """
    Add derived fields and business logic.
    """
    return (
        dlt.read_stream("events_parsed")
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("is_weekend",
            F.when(F.dayofweek("event_timestamp").isin([1, 7]), True).otherwise(False)
        )
        .filter(F.col("event_type").isin(["page_view", "click", "purchase"]))
    )
```

#### Step 4: Set Up Streaming Pipeline

```python
# Deploy as continuous streaming pipeline
databricks pipelines create --json '{
  "name": "kafka-events-streaming",
  "storage": "s3://my-bucket/kafka-streaming/",
  "target": "events",
  "continuous": true,
  "channel": "PREVIEW",
  "libraries": [
    {"notebook": {"path": "/pipelines/kafka_events_pipeline"}}
  ],
  "clusters": [
    {
      "label": "default",
      "num_workers": 4,
      "autoscale": {
        "min_workers": 2,
        "max_workers": 8,
        "mode": "ENHANCED"
      }
    }
  ]
}'
```

#### Step 5: Monitor Streaming Metrics

```sql
-- Check ingestion rate
SELECT
    event_date,
    event_hour,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM events.events_enriched
WHERE event_date = CURRENT_DATE()
GROUP BY event_date, event_hour
ORDER BY event_hour DESC;

-- Monitor lag (latest Kafka offset vs processed offset)
SELECT
    partition,
    MAX(offset) as max_offset_processed,
    CURRENT_TIMESTAMP() as check_time
FROM events.events_raw
GROUP BY partition;
```

**✅ Tutorial Complete!** Real-time Kafka streaming with DLT and Lakeflow Connect.

---

### Tutorial 3: Incremental JDBC Ingestion from MySQL

**Goal:** Incrementally load data from MySQL database using watermark columns.

#### Prerequisites

- MySQL database accessible from Databricks
- Table with timestamp or incremental column (e.g., `updated_at`)
- Database user with SELECT permission
- JDBC driver available in cluster

#### Step 1: Configure Database Secrets

```bash
# Create secret scope for database credentials
databricks secrets create-scope mysql-prod

# Add credentials
databricks secrets put-secret mysql-prod jdbc-url
# Value: jdbc:mysql://db.example.com:3306/orders_db

databricks secrets put-secret mysql-prod username
# Value: readonly_user

databricks secrets put-secret mysql-prod password
# Value: <secure-password>
```

#### Step 2: Create JDBC Connection

**Via UI:**

```
Type: JDBC
Name: mysql-orders-prod
JDBC URL: {{secrets/mysql-prod/jdbc-url}}
Driver: com.mysql.cj.jdbc.Driver
Username: {{secrets/mysql-prod/username}}
Password: {{secrets/mysql-prod/password}}
Table: orders
Incremental Column: updated_at
Additional Options:
  - fetchsize: 10000
  - numPartitions: 8
```

**Via API:**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

connection = w.connections.create(
    name="mysql-orders-prod",
    connection_type="JDBC",
    options={
        "url": "{{secrets/mysql-prod/jdbc-url}}",
        "driver": "com.mysql.cj.jdbc.Driver",
        "dbtable": "orders",
        "user": "{{secrets/mysql-prod/username}}",
        "password": "{{secrets/mysql-prod/password}}",
        "fetchsize": "10000",
        "numPartitions": "8"
    }
)
```

#### Step 3: Create Incremental Load Pipeline

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="orders_incremental",
    comment="Incremental orders from MySQL",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def ingest_orders():
    """
    Incrementally load orders based on updated_at timestamp.
    DLT automatically tracks watermark and only loads new/updated records.
    """
    return (
        dlt.read("mysql-orders-prod")  # Batch read with automatic watermark
        .filter(F.col("status").isin(["CONFIRMED", "SHIPPED", "DELIVERED"]))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


@dlt.table(
    name="orders_with_details",
    comment="Orders joined with customer data"
)
def enrich_orders():
    """
    Join orders with customer dimension.
    """
    orders = dlt.read("orders_incremental")
    customers = spark.table("customers_dim")  # Assume this exists

    return (
        orders
        .join(customers, "customer_id", "left")
        .select(
            orders["*"],
            customers["customer_name"],
            customers["customer_segment"],
            customers["customer_region"]
        )
    )


@dlt.table(
    name="orders_daily_summary",
    comment="Daily order summary metrics"
)
def daily_summary():
    """
    Aggregate daily metrics for reporting.
    """
    return (
        dlt.read("orders_with_details")
        .groupBy(
            F.to_date("order_date").alias("date"),
            "customer_segment"
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("order_amount").alias("total_amount"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
    )
```

#### Step 4: Configure Incremental Loading

**How Incremental Works:**

```
Run 1 (Initial Load):
┌─────────────────────────┐
│ MySQL: orders table     │
│ updated_at              │
│ ─────────────────       │
│ 2025-02-01 10:00:00     │ ← Load all
│ 2025-02-01 11:30:00     │ ← Load all
│ 2025-02-01 15:45:00     │ ← Load all
└─────────────────────────┘
Watermark saved: 2025-02-01 15:45:00

Run 2 (Incremental):
┌─────────────────────────┐
│ MySQL: orders table     │
│ updated_at              │
│ ─────────────────       │
│ 2025-02-01 10:00:00     │ ← Skip (before watermark)
│ 2025-02-01 11:30:00     │ ← Skip
│ 2025-02-01 15:45:00     │ ← Skip
│ 2025-02-02 09:15:00     │ ← Load (NEW!)
│ 2025-02-02 14:22:00     │ ← Load (NEW!)
└─────────────────────────┘
Watermark updated: 2025-02-02 14:22:00
```

#### Step 5: Schedule Pipeline

```python
# Create scheduled pipeline
databricks pipelines create --json '{
  "name": "mysql-orders-incremental",
  "storage": "s3://my-bucket/mysql-incremental/",
  "target": "orders",
  "continuous": false,
  "libraries": [
    {"notebook": {"path": "/pipelines/mysql_incremental_pipeline"}}
  ],
  "clusters": [
    {
      "label": "default",
      "num_workers": 4
    }
  ]
}'

# Schedule to run every hour
databricks jobs create --json '{
  "name": "Orders Incremental Load",
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "America/Los_Angeles",
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "run_pipeline",
      "pipeline_task": {
        "pipeline_id": "<pipeline_id>"
      }
    }
  ]
}'
```

**✅ Tutorial Complete!** Incremental MySQL ingestion with automatic watermark tracking.

---

## Connection Types Deep Dive

### Amazon S3 / Cloud Files

**Best For:**
- CSV, JSON, Parquet files in S3
- Large historical data loads
- Auto Loader use cases

**Configuration Options:**

```python
{
    # Required
    "path": "s3://bucket/path/",
    "format": "csv|json|parquet|avro",

    # Schema Inference
    "inferSchema": "true",
    "header": "true",  # For CSV

    # Auto Loader Options
    "cloudFiles.format": "csv",
    "cloudFiles.schemaLocation": "s3://bucket/schema/",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "cloudFiles.rescuedDataColumn": "_rescued_data",

    # Performance
    "cloudFiles.maxFilesPerTrigger": "1000",
    "cloudFiles.maxBytesPerTrigger": "10g",

    # File Notifications (faster than directory listing)
    "cloudFiles.useNotifications": "true",
    "cloudFiles.queueUrl": "https://sqs.us-west-2.amazonaws.com/...",

    # CSV Specific
    "delimiter": ",",
    "quote": "\"",
    "escape": "\\",
    "multiLine": "false",

    # Partitioning
    "recursiveFileLookup": "true",
    "pathGlobFilter": "*.csv"
}
```

**Example:**

```python
@dlt.table(name="s3_data")
def ingest():
    return dlt.read_stream("s3-connection")
```

---

### Apache Kafka

**Best For:**
- Real-time event streaming
- CDC (Change Data Capture) streams
- High-throughput data pipelines

**Configuration Options:**

```python
{
    # Required
    "kafka.bootstrap.servers": "broker1:9092,broker2:9092",
    "subscribe": "topic-name",  # OR "subscribePattern": "topic-.*"

    # Offset Management
    "startingOffsets": "earliest|latest|{json}",
    "endingOffsets": "latest|{json}",  # For batch processing

    # Security - PLAINTEXT
    "kafka.security.protocol": "PLAINTEXT",

    # Security - SSL
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.location": "/path/to/truststore.jks",
    "kafka.ssl.truststore.password": "{{secrets/scope/key}}",
    "kafka.ssl.keystore.location": "/path/to/keystore.jks",
    "kafka.ssl.keystore.password": "{{secrets/scope/key}}",

    # Security - SASL
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN|SCRAM-SHA-256|SCRAM-SHA-512",
    "kafka.sasl.jaas.config": "...",

    # Performance
    "maxOffsetsPerTrigger": "10000",
    "kafka.fetch.max.bytes": "52428800",  # 50MB
    "kafka.max.partition.fetch.bytes": "1048576",  # 1MB

    # Reliability
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "60000",
    "failOnDataLoss": "false"
}
```

**Example:**

```python
@dlt.table(name="kafka_events")
def ingest():
    return (
        dlt.read_stream("kafka-connection")
        .selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
    )
```

---

### JDBC Databases

**Best For:**
- Relational databases (MySQL, PostgreSQL, SQL Server, Oracle)
- Batch and incremental loads
- Historical data migration

**Configuration Options:**

```python
{
    # Required
    "url": "jdbc:mysql://host:port/database",
    "driver": "com.mysql.cj.jdbc.Driver",
    "dbtable": "schema.table_name",  # OR "query": "(SELECT ... FROM ...) AS t"
    "user": "{{secrets/scope/username}}",
    "password": "{{secrets/scope/password}}",

    # Incremental Loading
    "incrementalColumn": "updated_at",  # Column for watermark tracking

    # Performance
    "fetchsize": "10000",  # Rows per fetch
    "numPartitions": "8",  # Parallel connections
    "partitionColumn": "id",  # For parallel reads
    "lowerBound": "0",
    "upperBound": "1000000",

    # Connection Pooling
    "connectionTimeout": "30000",
    "idleTimeout": "600000",
    "maxLifetime": "1800000",

    # SQL Server Specific
    "encrypt": "true",
    "trustServerCertificate": "false",

    # PostgreSQL Specific
    "ssl": "true",
    "sslmode": "require",

    # Query Optimization
    "predicates": [
        "date >= '2025-01-01' AND date < '2025-02-01'",
        "date >= '2025-02-01' AND date < '2025-03-01'"
    ]
}
```

**Example:**

```python
# Batch load
@dlt.table(name="jdbc_data_batch")
def ingest_batch():
    return dlt.read("jdbc-connection")

# Streaming with CDC
@dlt.table(name="jdbc_data_streaming")
def ingest_streaming():
    return dlt.read_stream("jdbc-cdc-connection")
```

---

### Azure Event Hubs

**Best For:**
- Azure-native streaming
- IoT device data
- Real-time analytics on Azure

**Configuration Options:**

```python
{
    # Required
    "eventhubs.connectionString": "{{secrets/scope/connection-string}}",
    "eventhubs.consumerGroup": "$Default",

    # Or use individual fields:
    "eventhubs.namespace": "my-namespace",
    "eventhubs.name": "my-eventhub",
    "eventhubs.accessKeyName": "RootManageSharedAccessKey",
    "eventhubs.accessKey": "{{secrets/scope/key}}",

    # Offset Management
    "eventhubs.startingPosition": {
        "offset": "-1",  # Latest
        "seqNo": "-1",
        "enqueuedTime": "2025-01-01T00:00:00.000Z",
        "isInclusive": "true"
    },

    # Performance
    "eventhubs.maxEventsPerTrigger": "10000",
    "eventhubs.receiverTimeout": "PT60S",
    "eventhubs.operationTimeout": "PT60S"
}
```

**Example:**

```python
@dlt.table(name="eventhub_events")
def ingest():
    return (
        dlt.read_stream("eventhub-connection")
        .selectExpr(
            "CAST(body AS STRING) as event_data",
            "enqueuedTime",
            "offset",
            "sequenceNumber",
            "properties"
        )
    )
```

---

## Real-World Scenarios

### Scenario 1: Multi-Source Customer 360

**Challenge:** Combine customer data from S3 (CSV), Kafka (events), and MySQL (transactions).

**Solution:**

```python
import dlt
from pyspark.sql import functions as F

# Source 1: Customer profile from S3
@dlt.table(name="customer_profiles_raw")
def ingest_profiles():
    return dlt.read_stream("customer-profiles-s3")

# Source 2: Customer events from Kafka
@dlt.table(name="customer_events_raw")
def ingest_events():
    return (
        dlt.read_stream("customer-events-kafka")
        .selectExpr(
            "CAST(value AS STRING) as event_json",
            "timestamp as kafka_timestamp"
        )
    )

# Source 3: Customer transactions from MySQL
@dlt.table(name="customer_transactions_raw")
def ingest_transactions():
    return dlt.read("customer-transactions-mysql")

# Unified Customer 360 View
@dlt.table(
    name="customer_360",
    comment="Unified customer view from all sources"
)
def create_customer_360():
    # Get latest profile
    profiles = (
        dlt.read("customer_profiles_raw")
        .groupBy("customer_id")
        .agg(
            F.max("profile_updated_at").alias("latest_update"),
            F.first("email").alias("email"),
            F.first("phone").alias("phone"),
            F.first("address").alias("address")
        )
    )

    # Aggregate events (last 30 days)
    events = (
        dlt.read("customer_events_raw")
        .filter(F.col("kafka_timestamp") >= F.date_sub(F.current_date(), 30))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("event_count_30d"),
            F.countDistinct("event_type").alias("distinct_event_types"),
            F.max("kafka_timestamp").alias("last_event_timestamp")
        )
    )

    # Aggregate transactions
    transactions = (
        dlt.read("customer_transactions_raw")
        .groupBy("customer_id")
        .agg(
            F.sum("amount").alias("total_spent"),
            F.count("*").alias("transaction_count"),
            F.avg("amount").alias("avg_transaction_value"),
            F.max("transaction_date").alias("last_transaction_date")
        )
    )

    # Join all sources
    return (
        profiles
        .join(events, "customer_id", "left")
        .join(transactions, "customer_id", "left")
        .withColumn("customer_segment",
            F.when(F.col("total_spent") > 10000, "VIP")
            .when(F.col("total_spent") > 1000, "Premium")
            .otherwise("Standard")
        )
    )
```

---

### Scenario 2: CDC from Database to Delta Lake

**Challenge:** Real-time CDC from MySQL to Delta Lake with full history tracking (SCD Type 2).

**Solution:**

```python
import dlt

# Step 1: Ingest CDC stream
@dlt.table(
    name="customers_cdc_raw",
    comment="Raw CDC events from MySQL"
)
def ingest_cdc():
    return (
        dlt.read_stream("mysql-cdc-connection")
        .selectExpr(
            "after.*",  # New values
            "before.*",  # Old values (for deletes/updates)
            "op as operation",  # c=create, u=update, d=delete
            "ts_ms as cdc_timestamp"
        )
    )

# Step 2: Apply CDC to target table (SCD Type 2)
dlt.create_streaming_table(
    name="customers_scd2",
    comment="Customer data with full history (SCD Type 2)"
)

dlt.apply_changes(
    target="customers_scd2",
    source="customers_cdc_raw",
    keys=["customer_id"],
    sequence_by="cdc_timestamp",
    apply_as_deletes=F.expr("operation = 'd'"),
    apply_as_truncates=F.expr("operation = 't'"),
    stored_as_scd_type="2",
    track_history_column_list=["email", "phone", "address"],
    column_list=[
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "address",
        "status"
    ]
)

# Step 3: Create current view (SCD Type 1)
@dlt.table(name="customers_current")
def current_customers():
    return (
        dlt.read("customers_scd2")
        .filter(F.col("__END_AT").isNull())  # Only current records
        .drop("__START_AT", "__END_AT")
    )
```

---

### Scenario 3: Late-Arriving Data Handling

**Challenge:** Handle events that arrive out of order (e.g., mobile app events sent when device comes online).

**Solution:**

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="events_with_watermark",
    comment="Events with watermark for late arrivals"
)
def ingest_with_watermark():
    return (
        dlt.read_stream("events-kafka")
        .selectExpr(
            "CAST(value AS STRING) as event_json",
            "timestamp as kafka_timestamp"
        )
        .select(
            F.from_json("event_json", event_schema).alias("event"),
            "kafka_timestamp"
        )
        .select("event.*", "kafka_timestamp")
        .withWatermark("event_timestamp", "1 hour")  # Allow 1 hour delay
    )

@dlt.table(name="events_hourly_aggregates")
def aggregate_events():
    return (
        dlt.read_stream("events_with_watermark")
        .groupBy(
            F.window("event_timestamp", "1 hour"),
            "event_type"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "unique_users"
        )
    )
```

---

## Integration with DLT-META

### Can Lakeflow Connect Replace DLT-META?

**Short Answer:** No, they serve different purposes.

**Comparison:**

| Aspect | DLT-META | Lakeflow Connect |
|--------|----------|------------------|
| **Purpose** | Full pipeline framework | Data ingestion only |
| **Configuration** | JSON-driven pipelines | Connection-driven readers |
| **Landing Layer** | cloudFiles + transformations | Managed connections |
| **Refinery/Treasury** | SQL transformations + CDC | N/A (use standard DLT) |
| **DQ Expectations** | Built into framework | Use DLT expectations |
| **Metadata Management** | Onboarding tables | Connection catalog |

### Using Lakeflow Connect WITH DLT-META

**Option 1: Replace Landing Layer Only**

```python
# Instead of DLT-META's cloudFiles config:
{
    "source_format": "cloudFiles",
    "source_details": {
        "source_path": "s3://...",
        "format": "csv",
        "schema": "schema.ddl"
    }
}

# Use Lakeflow Connect in custom DLT code:
@dlt.table(name="landing_table")
def landing():
    return dlt.read_stream("lakeflow-connection")
    # No schema DDL needed!

# Then use DLT-META for Refinery/Treasury layers
```

**Option 2: Hybrid Approach**

```python
# Use Lakeflow Connect for complex sources (Kafka, JDBC)
# Use DLT-META for S3 files (more control)

# Landing: Lakeflow Connect
@dlt.table(name="events_landing")
def landing():
    return dlt.read_stream("kafka-connection")

# Refinery: DLT-META patterns
# (Use DLT-META's refinery_transformations.json)
```

### Migration Considerations

**Should You Migrate DLT-META to Lakeflow Connect?**

**✅ Migrate Landing Layer If:**
- Schema changes frequently (manual DDL maintenance is painful)
- Want Databricks-managed connectors
- Building new pipelines (not retrofitting existing)
- Sources match Lakeflow Connect capabilities

**❌ Don't Migrate If:**
- Current DLT-META works well
- Custom landing logic required
- Team trained on DLT-META
- Organization standard is DLT-META

---

## Advanced Patterns

### Pattern 1: Dynamic Connection Selection

**Use Case:** Switch between dev/qa/prod connections based on environment.

```python
import dlt
import os

# Get environment
env = os.getenv("ENVIRONMENT", "dev")

# Map environment to connection name
connection_map = {
    "dev": "orders-s3-dev",
    "qa": "orders-s3-qa",
    "prod": "orders-s3-prod"
}

connection_name = connection_map[env]

@dlt.table(name="orders")
def ingest():
    return dlt.read_stream(connection_name)
```

### Pattern 2: Parallel Multi-Topic Kafka Ingestion

**Use Case:** Ingest from multiple Kafka topics in parallel.

```python
import dlt

topics = ["events", "clicks", "impressions", "conversions"]

# Create table for each topic
for topic in topics:
    @dlt.table(name=f"{topic}_raw")
    def ingest_topic(topic_name=topic):  # Capture topic in closure
        connection_name = f"kafka-{topic_name}"
        return (
            dlt.read_stream(connection_name)
            .selectExpr(
                "CAST(value AS STRING) as event_json",
                "timestamp",
                f"'{topic_name}' as source_topic"
            )
        )

# Union all topics
@dlt.table(name="all_events_unified")
def unify_events():
    dfs = [dlt.read(f"{topic}_raw") for topic in topics]
    return dfs[0].unionByName(*dfs[1:])
```

### Pattern 3: Schema Registry Integration (Kafka)

**Use Case:** Use Confluent Schema Registry for Avro deserialization.

```python
import dlt
from pyspark.sql import functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient

# Configure Schema Registry
schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}
sr_client = SchemaRegistryClient(schema_registry_conf)

@dlt.table(name="kafka_avro_events")
def ingest_avro():
    return (
        dlt.read_stream("kafka-avro-connection")
        .selectExpr(
            "CAST(value AS BINARY) as avro_bytes",
            "topic",
            "partition",
            "offset"
        )
        # Deserialize Avro using Schema Registry
        # Note: Use from_avro() function with schema registry
        .withColumn("event",
            F.from_avro("avro_bytes", "event_schema", {
                "mode": "PERMISSIVE",
                "schemaRegistryUrl": "http://schema-registry:8081"
            })
        )
        .select("event.*", "topic", "partition", "offset")
    )
```

### Pattern 4: Fan-Out to Multiple Sinks

**Use Case:** Ingest once, write to multiple destinations (Delta, Kafka, S3).

```python
import dlt

# Source: Ingest from Kafka
@dlt.table(name="events_source")
def ingest():
    return dlt.read_stream("kafka-events")

# Sink 1: Delta table (default DLT behavior)
@dlt.table(name="events_bronze")
def write_to_delta():
    return dlt.read_stream("events_source")

# Sink 2: Write to different Kafka topic
dlt.create_sink(
    name="events_kafka_sink",
    format="kafka",
    options={
        "kafka.bootstrap.servers": "kafka:9092",
        "topic": "processed_events"
    }
)

@dlt.table(name="events_kafka_output")
def write_to_kafka():
    return (
        dlt.read_stream("events_source")
        .selectExpr("to_json(struct(*)) as value")
    )

dlt.append_flow(
    name="kafka_sink_flow",
    target="events_kafka_sink",
    comment="Stream processed events to Kafka"
)(lambda: dlt.read_stream("events_kafka_output"))

# Sink 3: Write to S3 as Parquet
@dlt.table(
    name="events_s3_export",
    table_properties={
        "location": "s3://export-bucket/events/",
        "format": "parquet"
    }
)
def write_to_s3():
    return dlt.read_stream("events_source")
```

---

## Performance Tuning

### Tuning S3 Auto Loader

```python
# High-throughput S3 ingestion
connection_options = {
    "path": "s3://large-bucket/data/",
    "format": "parquet",

    # Increase processing rate
    "cloudFiles.maxFilesPerTrigger": "10000",  # Process more files per micro-batch
    "cloudFiles.maxBytesPerTrigger": "100g",   # Process more data per trigger

    # Use file notifications (faster than listing)
    "cloudFiles.useNotifications": "true",
    "cloudFiles.queueUrl": "https://sqs.../queue",

    # Schema hints for faster inference
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaHints": "id INT, timestamp TIMESTAMP",

    # Partitioning optimization
    "recursiveFileLookup": "true",
    "pathGlobFilter": "*.parquet",

    # Memory optimization
    "cloudFiles.maxFileAge": "7d"  # Ignore files older than 7 days
}
```

### Tuning Kafka Ingestion

```python
# High-throughput Kafka streaming
connection_options = {
    "kafka.bootstrap.servers": "kafka:9092",
    "subscribe": "high-volume-topic",

    # Increase throughput
    "maxOffsetsPerTrigger": "1000000",  # Process more records per batch
    "kafka.fetch.max.bytes": "104857600",  # 100MB
    "kafka.max.partition.fetch.bytes": "10485760",  # 10MB

    # Optimize network
    "kafka.fetch.min.bytes": "1048576",  # 1MB min fetch
    "kafka.fetch.wait.max.ms": "500",    # Wait 500ms to accumulate data

    # Consumer tuning
    "kafka.max.poll.records": "10000",
    "kafka.max.poll.interval.ms": "600000",  # 10 minutes

    # Reliability
    "failOnDataLoss": "false",  # Don't fail on log compaction
    "kafka.session.timeout.ms": "60000"
}

# Pipeline configuration
@dlt.table(
    name="high_volume_kafka",
    cluster_by=["event_date", "partition"],  # Optimize queries
    trigger={"processingTime": "30 seconds"}  # Process every 30 seconds
)
def ingest():
    return dlt.read_stream("high-volume-kafka-connection")
```

### Tuning JDBC Ingestion

```python
# Optimized JDBC batch loading
connection_options = {
    "url": "jdbc:mysql://db:3306/mydb",
    "dbtable": "large_table",
    "user": "...",
    "password": "...",

    # Parallel reads
    "numPartitions": "16",  # 16 parallel JDBC connections
    "partitionColumn": "id",
    "lowerBound": "0",
    "upperBound": "10000000",

    # Fetch optimization
    "fetchsize": "100000",  # Large fetch size for batch

    # Connection pooling
    "connectionTimeout": "60000",
    "maxLifetime": "1800000",

    # Query pushdown
    "predicates": [
        "MOD(id, 16) = 0",
        "MOD(id, 16) = 1",
        # ... 16 predicates for parallel reads
    ]
}
```

### Cluster Sizing Guidelines

| Data Volume | Workers | Node Type | Notes |
|-------------|---------|-----------|-------|
| < 1 GB/day | 2-4 | Standard | Single-node for dev/test |
| 1-10 GB/day | 4-8 | Standard | Small production |
| 10-100 GB/day | 8-16 | Memory Optimized | Medium production |
| 100+ GB/day | 16-32 | Memory/Compute Optimized | Large production |
| Streaming (low latency) | 4-8 | Compute Optimized | Fast processing |

---

## Security Best Practices

### 1. Never Hardcode Credentials

**❌ Bad:**
```python
connection_options = {
    "password": "mypassword123",  # NEVER!
    "kafka.bootstrap.servers": "kafka:9092"
}
```

**✅ Good:**
```python
connection_options = {
    "password": "{{secrets/prod-db/password}}",  # Use Databricks Secrets
    "kafka.bootstrap.servers": "{{secrets/kafka-prod/brokers}}"
}
```

### 2. Secrets Management

```bash
# Create secret scope
databricks secrets create-scope prod-db

# Add secrets
databricks secrets put-secret prod-db password
databricks secrets put-secret prod-db jdbc-url
databricks secrets put-secret prod-db username

# List secrets (values are hidden)
databricks secrets list-secrets prod-db

# Use in connections
# Syntax: {{secrets/<scope>/<key>}}
```

### 3. Network Security

```python
# Use VPC peering or PrivateLink for database connections
# Kafka: Use SASL_SSL for production
connection_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="{{secrets/kafka/username}}" '
        'password="{{secrets/kafka/password}}";'
    )
}

# JDBC: Use SSL connections
connection_options = {
    "url": "jdbc:mysql://db:3306/mydb?useSSL=true",
    "sslMode": "VERIFY_IDENTITY",
    "sslCA": "/path/to/ca.pem"
}
```

### 4. IAM Roles (AWS)

```python
# S3: Use IAM instance profiles (no access keys!)
# Configure cluster with IAM role
{
  "aws_attributes": {
    "instance_profile_arn": "arn:aws:iam::123456789:instance-profile/DatabricksS3Access"
  }
}

# Connection (no credentials needed)
connection_options = {
    "path": "s3://bucket/data/",
    # IAM role provides access automatically
}
```

### 5. Data Masking

```python
# Mask sensitive columns in transit
@dlt.table(name="customers_masked")
def ingest_with_masking():
    return (
        dlt.read_stream("customers-connection")
        .withColumn("ssn_masked",
            F.concat(F.lit("XXX-XX-"), F.substring("ssn", -4, 4))
        )
        .withColumn("credit_card_masked",
            F.concat(F.lit("****-****-****-"), F.substring("credit_card", -4, 4))
        )
        .drop("ssn", "credit_card")  # Drop original sensitive columns
    )
```

---

## Monitoring & Operations

### 1. Pipeline Monitoring

```sql
-- Monitor pipeline runs
SELECT
    update_id,
    state,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes
FROM system.lakeflow.pipeline_updates
WHERE pipeline_id = '<pipeline_id>'
ORDER BY start_time DESC
LIMIT 10;

-- Check table sizes
SELECT
    table_name,
    size_in_bytes / 1024 / 1024 / 1024 as size_gb,
    num_files,
    last_updated
FROM system.information_schema.tables
WHERE table_schema = 'landing'
ORDER BY size_gb DESC;

-- Monitor data quality
SELECT
    expectation,
    passed_count,
    failed_count,
    ROUND(passed_count * 100.0 / (passed_count + failed_count), 2) as pass_rate_pct
FROM event_log(TABLE(landing.customers))
WHERE event_type = 'expectation_violation'
GROUP BY expectation;
```

### 2. Connection Health Checks

```python
# Test connection in notebook
def test_connection(connection_name):
    """Test if connection is working."""
    try:
        df = spark.read.format("lakeflow_connect") \
            .option("connection", connection_name) \
            .load() \
            .limit(1)
        print(f"✅ Connection '{connection_name}' is working")
        return True
    except Exception as e:
        print(f"❌ Connection '{connection_name}' failed: {str(e)}")
        return False

# Test all connections
connections = [
    "customer-s3",
    "events-kafka",
    "orders-mysql"
]

for conn in connections:
    test_connection(conn)
```

### 3. Alerting Setup

```python
# Create alerts for pipeline failures
databricks alerts create --json '{
  "name": "DLT Pipeline Failed",
  "query_id": "...",
  "condition": {
    "operand": {
      "column": { "name": "state" }
    },
    "op": "EQUAL",
    "threshold": { "value": { "string_value": "FAILED" } }
  },
  "rearm": 3600,
  "parent": "..."
}'
```

### 4. Cost Monitoring

```sql
-- Monitor DBU consumption by pipeline
SELECT
    pipeline_id,
    pipeline_name,
    DATE(usage_date) as date,
    SUM(usage_quantity) as total_dbus,
    SUM(usage_quantity * list_price) as estimated_cost
FROM system.billing.usage
WHERE usage_metadata.pipeline_id IS NOT NULL
GROUP BY pipeline_id, pipeline_name, DATE(usage_date)
ORDER BY date DESC, estimated_cost DESC;
```

---

## Troubleshooting Cookbook

### Issue: Connection Authentication Fails

**Symptoms:**
```
Error: Failed to authenticate with connection 'my-connection'
```

**Diagnosis:**
```python
# Check if secret exists
try:
    secret = dbutils.secrets.get(scope="my-scope", key="password")
    print("✅ Secret exists")
except:
    print("❌ Secret not found")

# Test direct connection
spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://...") \
    .option("user", "...") \
    .option("password", "...") \
    .option("dbtable", "test_table") \
    .load() \
    .limit(1) \
    .show()
```

**Solution:**
1. Verify secret scope and key names
2. Check IAM permissions (for S3/AWS)
3. Verify network connectivity
4. Check firewall rules

---

### Issue: Schema Mismatch Error

**Symptoms:**
```
Error: Schema mismatch detected
Cannot merge incompatible types: STRING and INT for column 'id'
```

**Diagnosis:**
```python
# Check current table schema
spark.sql("DESCRIBE TABLE landing.my_table").show()

# Check incoming data schema
df = dlt.read_stream("my-connection").limit(10)
df.printSchema()
```

**Solution:**

```python
# Option 1: Enable schema evolution
connection_options = {
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "mergeSchema": "true"
}

# Option 2: Use rescued data column
connection_options = {
    "cloudFiles.rescuedDataColumn": "_rescued_data"
}

# Option 3: Explicit type casting
@dlt.table(name="with_casting")
def ingest():
    return (
        dlt.read_stream("connection")
        .withColumn("id", F.col("id").cast("string"))  # Force type
    )
```

---

### Issue: Pipeline Running Slow

**Symptoms:**
- Pipeline takes hours to complete
- Micro-batches processing slowly
- High memory usage

**Diagnosis:**

```sql
-- Check micro-batch sizes
SELECT
    batch_id,
    num_input_rows,
    input_rows_per_second,
    process_rows_per_second,
    duration_ms
FROM event_log(TABLE(landing.my_table))
WHERE event_type = 'batch_processed'
ORDER BY batch_id DESC
LIMIT 10;
```

**Solution:**

```python
# 1. Increase processing limits
connection_options = {
    "cloudFiles.maxFilesPerTrigger": "10000",  # Process more files
    "maxOffsetsPerTrigger": "1000000",  # For Kafka
}

# 2. Add clustering
@dlt.table(
    name="optimized_table",
    cluster_by=["date", "customer_id"]  # Add clustering
)
def ingest():
    return dlt.read_stream("connection")

# 3. Increase cluster size
# Edit pipeline → Cluster → Increase workers

# 4. Use appropriate trigger interval
@dlt.table(
    name="batched_processing",
    trigger={"processingTime": "5 minutes"}  # Don't process too frequently
)
def ingest():
    return dlt.read_stream("connection")
```

---

### Issue: Duplicate Records

**Symptoms:**
- Same records appear multiple times
- Primary key violations

**Diagnosis:**
```sql
-- Check for duplicates
SELECT
    id,
    COUNT(*) as count
FROM landing.my_table
GROUP BY id
HAVING COUNT(*) > 1;
```

**Solution:**

```python
# Use merge operation for idempotency
@dlt.table(name="deduped_table")
def dedupe():
    from pyspark.sql.window import Window

    return (
        dlt.read_stream("my_table_raw")
        .withColumn("row_num",
            F.row_number().over(
                Window.partitionBy("id").orderBy(F.desc("updated_at"))
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
```

---

### Issue: Memory Out of Errors

**Symptoms:**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

**Solution:**

```python
# 1. Reduce batch size
connection_options = {
    "cloudFiles.maxFilesPerTrigger": "100",  # Reduce from 1000
    "cloudFiles.maxBytesPerTrigger": "1g"    # Limit bytes
}

# 2. Enable spill to disk
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Use larger cluster with more memory
# OR use memory-optimized node types

# 4. Partition data better
@dlt.table(
    name="partitioned_table",
    partition_cols=["date"],  # Partition by date
    cluster_by=["customer_id"]
)
def ingest():
    return (
        dlt.read_stream("connection")
        .withColumn("date", F.to_date("timestamp"))
    )
```

---

## Conclusion

Lakeflow Connect simplifies data ingestion in Delta Live Tables by:

1. **Eliminating boilerplate** - No complex readStream code
2. **Automating schema management** - Schema inference and evolution
3. **Providing managed connectors** - Databricks maintains and updates
4. **Enabling rapid development** - Focus on business logic, not plumbing

### When to Use Lakeflow Connect

**✅ Use for:**
- New DLT pipelines
- Supported source types (S3, Kafka, JDBC, EventHubs)
- Schemas that change frequently
- Teams new to Spark/DLT

**❌ Don't use for:**
- Complex custom ingestion logic
- Existing working pipelines (don't retrofit)
- Unsupported source types
- When DLT-META framework is organizational standard

### Next Steps

1. **Try the tutorials** - Start with Tutorial 1 (S3 CSV)
2. **Review your use case** - Match to real-world scenarios
3. **Test in dev** - Create proof of concept
4. **Measure performance** - Compare with existing approach
5. **Plan migration** - If beneficial, plan gradual rollout

---

## Resources

### Documentation
- [Databricks Lakeflow Connect Official Docs](https://docs.databricks.com/en/connect/)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)
- [DLT-META Framework Guide](https://github.com/databrickslabs/dlt-meta)

### Related Guides
- [Lakeflow Connect Overview](Lakeflow_Connect_Guide.md)
- [DLT-META Sinks Guide](DLT_META_SINKS_GUIDE.md)
- [DLT-META vs Platform Notebooks](DLT-META_vs_Platform_Notebooks_Implementation_Guide.md)

---

**Document Version:** 1.0
**Last Updated:** 2026-02-23
**Maintained By:** Data Engineering Team
