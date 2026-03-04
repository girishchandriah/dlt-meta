# DLT-META Sinks: Comprehensive Guide

**Version:** 1.0
**Last Updated:** 2026-02-23

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Supported Sink Formats](#supported-sink-formats)
4. [Configuration Reference](#configuration-reference)
5. [Implementation Examples](#implementation-examples)
6. [Advanced Features](#advanced-features)
7. [Potential Enhancements](#potential-enhancements)
8. [Implementation Guide](#implementation-guide)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

---

## Overview

### What are Sinks in DLT-META?

Sinks in DLT-META enable data fanout from Lakeflow Declarative Pipelines to external systems. They allow you to write processed data from any layer (Landing, Refinery, or Treasury) to multiple destinations simultaneously.

### Key Features

- **Multi-destination support**: Write to multiple sinks from a single source
- **Layer-agnostic**: Available in Landing, Refinery, and Treasury layers
- **Transformation support**: Apply column selection and filtering before writing
- **Secrets management**: Integrated with Databricks Secrets for secure credential handling
- **Format support**: Delta tables, Kafka topics, Azure Event Hubs

### Use Cases

1. **Data Distribution**: Send processed data to downstream consumers
2. **Real-time Analytics**: Stream transformed data to Kafka for real-time processing
3. **Multi-cloud Replication**: Replicate data across regions or clouds
4. **Event-Driven Architecture**: Trigger downstream processes via event streaming
5. **Data Lake Export**: Write curated data to external Delta tables

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   DLT-META Pipeline                         │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐        │
│  │  Landing   │───>│  Refinery  │───>│  Treasury  │        │
│  └──────┬─────┘    └──────┬─────┘    └──────┬─────┘        │
│         │                  │                  │              │
│         │                  │                  │              │
│         ▼                  ▼                  ▼              │
│    ┌────────────────────────────────────────────┐           │
│    │         DLTSinkWriter (Fanout)             │           │
│    └────────────────────────────────────────────┘           │
│         │                  │                  │              │
│         ▼                  ▼                  ▼              │
│    ┌─────────┐      ┌──────────┐      ┌──────────┐         │
│    │  Delta  │      │  Kafka   │      │EventHub  │         │
│    │  Sink   │      │  Sink    │      │  Sink    │         │
│    └─────────┘      └──────────┘      └──────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### Key Classes

#### `DLTSink` Dataclass ([src/dataflow_spec.py:149-155](src/dataflow_spec.py#L149-L155))

Represents sink configuration metadata:

```python
@dataclass
class DLTSink:
    name: str                    # Unique sink name
    format: str                  # Sink format: delta, kafka, eventhub
    options: map                 # Format-specific options
    select_exp: list            # Optional: Column selection expressions
    where_clause: str           # Optional: Row filtering condition
```

#### `DLTSinkWriter` Class ([src/pipeline_writers.py:60-89](src/pipeline_writers.py#L60-L89))

Handles writing data to sinks:

```python
class DLTSinkWriter:
    def __init__(self, dlt_sink: DLTSink, source_view_name):
        """Initialize sink writer with configuration and source view."""

    def read_input_view(self):
        """Read and transform data from source view."""

    def write_to_sink(self):
        """Write transformed data to sink destination."""
```

### Data Flow

1. **Read Phase**: Pipeline processes data through layers
2. **Sink Invocation**: After creating main tables, sinks are triggered ([src/dataflow_pipeline.py:242-245](src/dataflow_pipeline.py#L242-L245))
3. **Transformation**: Apply `select_exp` and `where_clause` filters
4. **Write Phase**: Use `dlt.create_sink()` and `dlt.append_flow()` APIs
5. **Streaming**: Continuously stream transformed data to destination

---

## Supported Sink Formats

### 1. Delta Sink

Write to external Delta tables (Unity Catalog or DBFS paths).

#### Configuration

```json
{
  "name": "external_delta_sink",
  "format": "delta",
  "options": {
    "tablename": "catalog.database.table_name"
  },
  "select_exp": ["col1", "col2", "col3"],
  "where_clause": "col1 IS NOT NULL"
}
```

#### Options Reference

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `tablename` | Yes | Fully qualified table name | `"landing.customers_export"` |

#### Use Cases

- Export curated data to external catalogs
- Create materialized views in different databases
- Data replication across Unity Catalog metastores

---

### 2. Kafka Sink

Stream data to Apache Kafka topics with PLAINTEXT or SSL security.

#### Configuration

```json
{
  "name": "kafka_streaming_sink",
  "format": "kafka",
  "options": {
    "kafka_sink_servers_secret_scope_name": "kafka_secrets",
    "kafka_sink_servers_secret_scope_key": "bootstrap_servers",
    "kafka.security.protocol": "PLAINTEXT",
    "topic": "processed_events"
  },
  "select_exp": ["to_json(struct(*)) as value"],
  "where_clause": "status = 'ACTIVE'"
}
```

#### Options Reference

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `kafka_sink_servers_secret_scope_name` | Yes | Databricks secret scope for Kafka servers | `"kafka_secrets"` |
| `kafka_sink_servers_secret_scope_key` | Yes | Secret key for bootstrap servers | `"bootstrap_servers"` |
| `topic` | Yes | Target Kafka topic name | `"customer_events"` |
| `kafka.security.protocol` | Yes | Security protocol | `"PLAINTEXT"` or `"SSL"` |

#### SSL Configuration (Optional)

For SSL-enabled Kafka:

```json
{
  "options": {
    "kafka_sink_servers_secret_scope_name": "kafka_secrets",
    "kafka_sink_servers_secret_scope_key": "bootstrap_servers",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.location": "/path/to/truststore.jks",
    "kafka.ssl.keystore.location": "/path/to/keystore.jks",
    "kafka.ssl.truststore.secrets.scope": "kafka_secrets",
    "kafka.ssl.truststore.secrets.key": "truststore_password",
    "kafka.ssl.keystore.secrets.scope": "kafka_secrets",
    "kafka.ssl.keystore.secrets.key": "keystore_password",
    "topic": "secure_topic"
  }
}
```

#### Implementation Details

**Secrets Resolution** ([src/dataflow_spec.py:530-569](src/dataflow_spec.py#L530-L569)):

- Bootstrap servers retrieved from Databricks Secrets
- SSL certificates and passwords managed via Secrets
- Credentials injected at runtime, never hardcoded

**Data Format**:

- Kafka expects messages as binary `value` column
- Use `to_json(struct(*))` to serialize rows
- Select specific columns: `["id", "name", "to_json(struct(*)) as value"]`

---

### 3. EventHub Sink

Stream data to Azure Event Hubs (automatically converted to Kafka format).

#### Configuration

```json
{
  "name": "eventhub_streaming_sink",
  "format": "eventhub",
  "options": {
    "eventhub.namespace": "my-eventhub-namespace",
    "eventhub.port": "9093",
    "eventhub.name": "events-hub",
    "eventhub.accessKeyName": "RootManageSharedAccessKey",
    "eventhub.accessKeySecretName": "eventhub-key",
    "eventhub.secretsScopeName": "eventhub_secrets"
  },
  "select_exp": ["to_json(struct(*)) as value"]
}
```

#### Options Reference

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `eventhub.namespace` | Yes | Event Hubs namespace | `"prod-events"` |
| `eventhub.port` | Yes | Connection port | `"9093"` |
| `eventhub.name` | Yes | Event Hub name | `"customer-events"` |
| `eventhub.accessKeyName` | Yes | Shared Access Key name | `"RootManageSharedAccessKey"` |
| `eventhub.accessKeySecretName` | No | Secret name (defaults to accessKeyName) | `"eventhub-key"` |
| `eventhub.secretsScopeName` | Yes | Databricks secret scope | `"eventhub_secrets"` |

#### Implementation Details

**Format Conversion** ([src/dataflow_spec.py:570-607](src/dataflow_spec.py#L570-L607)):

- EventHub sink internally converts to Kafka format
- Connection string built from namespace and access keys
- SASL/SSL configuration auto-generated
- Final format changed to `kafka` before sink creation

---

## Configuration Reference

### Sink Structure in Onboarding File

Sinks are configured in the onboarding JSON under layer-specific keys:

```json
{
  "data_flow_id": "100",
  "data_flow_group": "A1",

  "landing_sinks": [
    { "name": "sink1", "format": "delta", "options": {...} }
  ],

  "refinery_sinks": [
    { "name": "sink2", "format": "kafka", "options": {...} }
  ],

  "treasury_sinks": [
    { "name": "sink3", "format": "eventhub", "options": {...} }
  ]
}
```

### Mandatory Fields

All sinks must include ([src/dataflow_spec.py:195](src/dataflow_spec.py#L195)):

1. `name` - Unique identifier for the sink
2. `format` - Sink type: `delta`, `kafka`, or `eventhub`
3. `options` - Format-specific configuration (JSON object)

### Optional Fields

- `select_exp` (list) - Column selection expressions (default: all columns)
- `where_clause` (string) - Row filtering condition (default: no filter)

### Validation

**Supported Formats** ([src/dataflow_spec.py:196](src/dataflow_spec.py#L196)):
```python
supported_sink_formats = ["delta", "kafka", "eventhub"]
```

**Missing Mandatory Attributes**:
```
Exception: mandatory missing keys= {'name', 'format', 'options'} for sink
```

---

## Implementation Examples

### Example 1: Landing Layer with Multiple Sinks

Write IoT events to both Delta table and Kafka topic:

```json
{
  "data_flow_id": "103",
  "data_flow_group": "iot_events",
  "source_format": "kafka",
  "source_details": {
    "subscribe": "iot_raw_topic",
    "kafka.bootstrap.servers": "kafka-broker:9092"
  },
  "landing_database": "landing",
  "landing_table": "iot_events",
  "landing_sinks": [
    {
      "name": "iot_delta_export",
      "format": "delta",
      "options": {
        "tablename": "external_catalog.iot.events"
      },
      "select_exp": ["device_id", "timestamp", "temperature", "humidity"],
      "where_clause": "temperature IS NOT NULL AND humidity IS NOT NULL"
    },
    {
      "name": "iot_kafka_downstream",
      "format": "kafka",
      "options": {
        "kafka_sink_servers_secret_scope_name": "kafka_prod",
        "kafka_sink_servers_secret_scope_key": "bootstrap_servers",
        "kafka.security.protocol": "PLAINTEXT",
        "topic": "iot_processed_events"
      },
      "select_exp": ["to_json(struct(*)) as value"],
      "where_clause": "device_id IS NOT NULL"
    }
  ]
}
```

### Example 2: Refinery Layer with Transformation

Transform customer data and stream to Kafka:

```json
{
  "data_flow_id": "200",
  "data_flow_group": "customers",
  "refinery_database": "refinery",
  "refinery_table": "customers_cleaned",
  "refinery_sinks": [
    {
      "name": "customer_kafka_sink",
      "format": "kafka",
      "options": {
        "kafka_sink_servers_secret_scope_name": "kafka_secrets",
        "kafka_sink_servers_secret_scope_key": "bootstrap_servers",
        "kafka.security.protocol": "SSL",
        "kafka.ssl.truststore.location": "/dbfs/kafka/truststore.jks",
        "kafka.ssl.keystore.location": "/dbfs/kafka/keystore.jks",
        "kafka.ssl.truststore.secrets.scope": "kafka_secrets",
        "kafka.ssl.truststore.secrets.key": "truststore_pwd",
        "kafka.ssl.keystore.secrets.scope": "kafka_secrets",
        "kafka.ssl.keystore.secrets.key": "keystore_pwd",
        "topic": "customers_processed"
      },
      "select_exp": [
        "customer_id",
        "email",
        "phone",
        "to_json(struct(*)) as value"
      ],
      "where_clause": "email IS NOT NULL AND opt_in_marketing = true"
    }
  ]
}
```

### Example 3: Treasury Layer with EventHub

Export aggregated metrics to Azure Event Hubs:

```json
{
  "data_flow_id": "300",
  "data_flow_group": "analytics",
  "treasury_database": "treasury",
  "treasury_table": "daily_sales_metrics",
  "treasury_sinks": [
    {
      "name": "sales_eventhub_sink",
      "format": "eventhub",
      "options": {
        "eventhub.namespace": "analytics-prod",
        "eventhub.port": "9093",
        "eventhub.name": "sales-metrics",
        "eventhub.accessKeyName": "SalesMetricsKey",
        "eventhub.secretsScopeName": "eventhub_prod"
      },
      "select_exp": ["to_json(struct(*)) as value"],
      "where_clause": "total_sales > 0"
    }
  ]
}
```

### Example 4: Multiple Kafka Sinks with Different Topics

Fan out to multiple downstream topics:

```json
{
  "landing_sinks": [
    {
      "name": "high_priority_events",
      "format": "kafka",
      "options": {
        "kafka_sink_servers_secret_scope_name": "kafka_prod",
        "kafka_sink_servers_secret_scope_key": "bootstrap_servers",
        "kafka.security.protocol": "PLAINTEXT",
        "topic": "high_priority_events"
      },
      "select_exp": ["to_json(struct(*)) as value"],
      "where_clause": "priority = 'HIGH'"
    },
    {
      "name": "low_priority_events",
      "format": "kafka",
      "options": {
        "kafka_sink_servers_secret_scope_name": "kafka_prod",
        "kafka_sink_servers_secret_scope_key": "bootstrap_servers",
        "kafka.security.protocol": "PLAINTEXT",
        "topic": "low_priority_events"
      },
      "select_exp": ["to_json(struct(*)) as value"],
      "where_clause": "priority = 'LOW'"
    }
  ]
}
```

---

## Advanced Features

### Column Selection with `select_exp`

Transform data before writing to sink:

```json
"select_exp": [
  "customer_id",
  "UPPER(email) as email",
  "CONCAT(first_name, ' ', last_name) as full_name",
  "CAST(created_at as STRING) as created_timestamp",
  "to_json(struct(*)) as value"
]
```

**Key Points**:
- Supports full Spark SQL expressions
- Can rename columns with `AS` alias
- Apply functions: `UPPER()`, `CONCAT()`, `CAST()`, etc.
- For Kafka/EventHub: must include `value` column

### Row Filtering with `where_clause`

Filter rows before writing:

```json
"where_clause": "status = 'ACTIVE' AND created_at >= current_date() - INTERVAL 30 DAYS"
```

**Key Points**:
- Supports full Spark SQL WHERE syntax
- Can use functions: `current_date()`, `date_sub()`, etc.
- Multiple conditions with `AND`/`OR`
- Use parentheses for complex logic

### Combining Transformations

```json
{
  "name": "filtered_kafka_sink",
  "format": "kafka",
  "options": {...},
  "select_exp": [
    "customer_id",
    "UPPER(email) as email",
    "to_json(struct(customer_id, email, created_at)) as value"
  ],
  "where_clause": "email IS NOT NULL AND created_at >= '2024-01-01'"
}
```

### Secrets Management Best Practices

1. **Never hardcode credentials** in onboarding files
2. **Use Databricks Secrets** for all sensitive data:
   ```bash
   databricks secrets create-scope kafka_prod
   databricks secrets put-secret kafka_prod bootstrap_servers
   ```
3. **Reference secrets** via scope and key names
4. **Rotate secrets** regularly without code changes

---

## Potential Enhancements

### 1. HTTP/REST API Sink

**Description**: Stream data to HTTP endpoints via POST/PUT requests.

**Configuration Example**:
```json
{
  "name": "webhook_sink",
  "format": "http",
  "options": {
    "url": "https://api.example.com/events",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer ${secret:api_secrets:token}"
    },
    "batch_size": 100,
    "timeout_seconds": 30
  },
  "select_exp": ["to_json(struct(*)) as body"]
}
```

**Use Cases**:
- Trigger webhooks on data changes
- Integrate with external APIs
- Send alerts to monitoring systems

**Implementation Considerations**:
- Use Spark `foreachBatch()` for batching
- Handle retries and error responses
- Support authentication (Bearer, Basic, API Key)
- Implement rate limiting

---

### 2. JDBC Sink

**Description**: Write to relational databases (MySQL, PostgreSQL, SQL Server).

**Configuration Example**:
```json
{
  "name": "postgres_sink",
  "format": "jdbc",
  "options": {
    "jdbc_url_secret_scope": "db_secrets",
    "jdbc_url_secret_key": "postgres_url",
    "table": "processed_events",
    "mode": "append",
    "batch_size": 1000,
    "isolation_level": "READ_COMMITTED"
  }
}
```

**Use Cases**:
- Export to data warehouses
- Feed transactional databases
- Integrate with legacy systems

**Implementation Considerations**:
- Use Spark JDBC connector
- Handle connection pooling
- Support upsert/merge operations
- Manage schema evolution

---

### 3. S3/ADLS File Sink

**Description**: Write to cloud storage as Parquet, CSV, or JSON files.

**Configuration Example**:
```json
{
  "name": "s3_parquet_sink",
  "format": "cloudfiles",
  "options": {
    "path": "s3://analytics-bucket/processed/events/",
    "format": "parquet",
    "partition_by": ["year", "month", "day"],
    "compression": "snappy",
    "mode": "append"
  }
}
```

**Use Cases**:
- Archive processed data
- Export to data lakes
- Create backups

**Implementation Considerations**:
- Support partitioning strategies
- Handle file compaction
- Enable compression options
- Support multiple formats (Parquet, CSV, JSON, Avro)

---

### 4. Snowflake Sink

**Description**: Stream data directly to Snowflake tables.

**Configuration Example**:
```json
{
  "name": "snowflake_sink",
  "format": "snowflake",
  "options": {
    "sfUrl": "account.snowflakecomputing.com",
    "sfUser_secret_scope": "snowflake_secrets",
    "sfUser_secret_key": "username",
    "sfPassword_secret_scope": "snowflake_secrets",
    "sfPassword_secret_key": "password",
    "sfDatabase": "ANALYTICS",
    "sfSchema": "PROCESSED",
    "dbtable": "EVENTS",
    "sfWarehouse": "COMPUTE_WH"
  }
}
```

**Use Cases**:
- Feed Snowflake data warehouse
- Enable BI tool integration
- Support analytics workloads

**Implementation Considerations**:
- Use Snowflake Spark connector
- Handle authentication (OAuth, Key Pair)
- Support MERGE operations
- Optimize with stages

---

### 5. MongoDB Sink

**Description**: Write to MongoDB collections.

**Configuration Example**:
```json
{
  "name": "mongodb_sink",
  "format": "mongodb",
  "options": {
    "connection_string_secret_scope": "mongo_secrets",
    "connection_string_secret_key": "connection_string",
    "database": "analytics",
    "collection": "events",
    "write_concern": "majority",
    "batch_size": 500
  }
}
```

**Use Cases**:
- Feed document databases
- Support flexible schemas
- Enable real-time queries

---

### 6. Elasticsearch Sink

**Description**: Index data in Elasticsearch for search and analytics.

**Configuration Example**:
```json
{
  "name": "elasticsearch_sink",
  "format": "elasticsearch",
  "options": {
    "es_nodes": "elasticsearch.example.com:9200",
    "es_index": "events",
    "es_mapping_id": "event_id",
    "es_nodes_wan_only": "true",
    "es_username_secret_scope": "es_secrets",
    "es_username_secret_key": "username",
    "es_password_secret_scope": "es_secrets",
    "es_password_secret_key": "password"
  }
}
```

**Use Cases**:
- Enable full-text search
- Power dashboards (Kibana)
- Log aggregation

---

### 7. Enhanced Error Handling

**Features**:
- Dead-letter queue for failed writes
- Automatic retries with exponential backoff
- Error metrics and alerts
- Failure recovery mechanisms

**Configuration Example**:
```json
{
  "options": {
    "enable_retry": true,
    "max_retries": 3,
    "retry_backoff_seconds": 60,
    "dead_letter_path": "dbfs:/dead_letters/sink_name/",
    "enable_metrics": true
  }
}
```

---

### 8. Data Quality Checks for Sinks

**Features**:
- Validate data before writing
- Schema enforcement
- Record count validation
- Checksum verification

**Configuration Example**:
```json
{
  "sink_quality_expectations": {
    "expect_all": {
      "valid_email": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
      "valid_amount": "amount > 0"
    }
  }
}
```

---

### 9. Sink Monitoring & Metrics

**Features**:
- Record throughput metrics
- Latency tracking
- Error rate monitoring
- Integration with Databricks monitoring

**Metrics to Track**:
- Records written per second
- Bytes written per second
- Write latency (p50, p95, p99)
- Error count and rate
- Backpressure indicators

---

### 10. Dynamic Partitioning for File Sinks

**Features**:
- Partition output files by columns
- Time-based partitioning
- Custom partition functions

**Configuration Example**:
```json
{
  "options": {
    "partition_by": ["year", "month", "region"],
    "partition_overwrite_mode": "dynamic",
    "max_records_per_file": 1000000
  }
}
```

---

## Implementation Guide

### Adding a New Sink Format

Follow these steps to add support for a new sink format:

#### Step 1: Update Supported Formats

**File**: [src/dataflow_spec.py](src/dataflow_spec.py#L196)

```python
supported_sink_formats = ["delta", "kafka", "eventhub", "http", "jdbc"]
```

#### Step 2: Add Options Parsing Logic

**File**: [src/dataflow_spec.py](src/dataflow_spec.py) - `get_sinks()` method

```python
def get_sinks(sinks, spark) -> list[DLTSink]:
    # ... existing code ...

    if format == "http":
        http_options_json = json_sink['options']
        # Parse HTTP-specific options
        if 'headers' in http_options_json:
            # Handle header templating with secrets
            headers = http_options_json['headers']
            for key, value in headers.items():
                if '${secret:' in value:
                    # Extract and resolve secret
                    pass

    # ... rest of the method ...
```

#### Step 3: Implement Sink Writer Logic

**File**: [src/pipeline_writers.py](src/pipeline_writers.py) - `DLTSinkWriter` class

For simple sinks using DLT's `create_sink()`:
```python
def write_to_sink(self):
    """Write to Sink."""
    dlt.create_sink(
        name=self.dlt_sink.name,
        format=self.dlt_sink.format,
        options=self.dlt_sink.options
    )
    dlt.append_flow(
        name=f"{self.dlt_sink.name}_flow",
        target=self.dlt_sink.name,
        comment=f"Sink flow for {self.dlt_sink.name}"
    )(self.read_input_view)
```

For custom sinks not supported by DLT:
```python
def write_to_custom_sink(self):
    """Write to custom sink using foreachBatch."""
    input_df = self.read_input_view()

    def write_batch(batch_df, batch_id):
        # Custom write logic here
        # e.g., HTTP POST, custom JDBC logic, etc.
        pass

    input_df.writeStream \
        .foreachBatch(write_batch) \
        .trigger(processingTime='1 minute') \
        .start()
```

#### Step 4: Add Unit Tests

**File**: `tests/test_pipeline_writers.py`

```python
def test_http_sink_writer(self):
    dlt_sink = DLTSink(
        name="http_test_sink",
        format="http",
        options={
            "url": "https://api.example.com/events",
            "method": "POST"
        },
        select_exp=["to_json(struct(*)) as body"],
        where_clause="status = 'ACTIVE'"
    )
    sink_writer = DLTSinkWriter(dlt_sink, "test_view")
    # Test sink writer logic
```

#### Step 5: Update Documentation

1. Add configuration example to this guide
2. Update README.md with new sink format
3. Add example onboarding file to `examples/`

---

## Best Practices

### 1. Naming Conventions

- **Sink Names**: Use descriptive, unique names
  - Good: `customer_kafka_prod_sink`
  - Bad: `sink1`
- **Topics/Tables**: Include layer and purpose
  - Good: `refinery_customers_processed`
  - Bad: `output`

### 2. Security

- **Always use Databricks Secrets** for credentials
- **Never commit** credentials to version control
- **Rotate secrets** regularly
- **Use least-privilege** access for sink destinations

### 3. Performance

- **Batch Size**: Tune based on sink capacity
  - Kafka: 100-1000 records per batch
  - Delta: 10,000+ records per batch
- **Partitioning**: Use appropriate partitioning for file sinks
- **Parallelism**: Configure Spark partitions based on sink throughput

### 4. Monitoring

- **Track metrics**: Write throughput, latency, errors
- **Set up alerts**: For sink failures and backpressure
- **Log sampling**: Log sample records for debugging

### 5. Error Handling

- **Implement retries**: For transient failures
- **Dead-letter queues**: For permanent failures
- **Graceful degradation**: Continue processing even if one sink fails

### 6. Testing

- **Unit tests**: Test sink configuration parsing
- **Integration tests**: Verify end-to-end sink writing
- **Performance tests**: Validate throughput under load

---

## Troubleshooting

### Common Issues

#### 1. Sink Not Writing Data

**Symptoms**:
- No data appears in sink destination
- DLT pipeline runs without errors

**Possible Causes**:
- `where_clause` filters out all records
- `select_exp` results in empty dataframe
- Incorrect sink configuration

**Solutions**:
```python
# Debug by checking source view
source_df = dlt.read_stream("source_view")
print(source_df.count())

# Check after transformations
transformed_df = source_df.selectExpr(*select_exp).where(where_clause)
print(transformed_df.count())
```

#### 2. Kafka Authentication Errors

**Symptoms**:
```
ERROR: Failed to construct kafka consumer
org.apache.kafka.common.KafkaException: Failed to connect to broker
```

**Solutions**:
- Verify secret scope and key names
- Check Kafka bootstrap servers are reachable
- Validate security protocol matches Kafka cluster config
- For SSL: verify certificate paths and passwords

#### 3. Delta Sink Permission Errors

**Symptoms**:
```
ERROR: Permission denied: user does not have MODIFY permission on table
```

**Solutions**:
- Grant MODIFY permission on target table
- Verify Unity Catalog permissions
- Check if external location is accessible

#### 4. EventHub Connection Issues

**Symptoms**:
```
ERROR: Connection refused to Event Hubs endpoint
```

**Solutions**:
- Verify Event Hubs namespace is correct
- Check shared access key has Send permission
- Validate secret scope contains correct connection string
- Ensure port 9093 is accessible

#### 5. Schema Mismatch Errors

**Symptoms**:
```
ERROR: Column 'value' not found in dataframe
```

**Solutions**:
- For Kafka/EventHub: Ensure `select_exp` includes `value` column
- Use `to_json(struct(*))` to serialize all columns:
  ```json
  "select_exp": ["to_json(struct(*)) as value"]
  ```

#### 6. Memory Issues with Large Batches

**Symptoms**:
```
ERROR: OutOfMemoryError: GC overhead limit exceeded
```

**Solutions**:
- Reduce batch size in sink options
- Increase executor memory
- Enable adaptive query execution:
  ```python
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  ```

### Debugging Tips

#### Enable Debug Logging

```python
import logging
logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.DEBUG)
```

#### Inspect Sink Configuration

```python
# In pipeline code
print(f"Sink config: {self.dlt_sink}")
print(f"Options: {json.dumps(self.dlt_sink.options, indent=2)}")
```

#### Test Sink Independently

Create a test notebook to validate sink configuration:

```python
# Test Kafka sink
from pyspark.sql.functions import to_json, struct

test_df = spark.range(10).selectExpr(
    "id",
    "to_json(struct(*)) as value"
)

test_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test_topic") \
    .save()
```

---

## Conclusion

DLT-META sinks provide a powerful mechanism for data fanout and integration with external systems. By following the patterns and best practices outlined in this guide, you can:

1. **Integrate** with diverse downstream systems
2. **Scale** to handle high-throughput streaming workloads
3. **Secure** sensitive data with proper secrets management
4. **Monitor** sink health and performance
5. **Extend** with new sink formats as needed

For questions or contributions, please refer to the [DLT-META repository](https://github.com/databrickslabs/dlt-meta).

---

## Appendix

### A. Complete Configuration Schema

```json
{
  "landing_sinks": [
    {
      "name": "string (required)",
      "format": "delta|kafka|eventhub (required)",
      "options": {
        "format-specific-options": "value"
      },
      "select_exp": ["array", "of", "expressions"],
      "where_clause": "SQL WHERE condition"
    }
  ],
  "refinery_sinks": [...],
  "treasury_sinks": [...]
}
```

### B. Secrets Management Commands

```bash
# Create secret scope
databricks secrets create-scope <scope-name>

# Add secret
databricks secrets put-secret <scope-name> <key-name>

# List secrets in scope
databricks secrets list-secrets <scope-name>

# Delete secret
databricks secrets delete-secret <scope-name> <key-name>
```

### C. Example Onboarding Files

See the `examples/` directory for complete onboarding file examples:
- [examples/kafka-sink-onboarding.template](../examples/kafka-sink-onboarding.template)
- [tests/resources/onboarding_sink.json](../tests/resources/onboarding_sink.json)

### D. Related Documentation

- [DLT-META Documentation](https://databrickslabs.github.io/dlt-meta/)
- [Databricks DLT create_sink API](https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-sink)
- [Onboarding File Reference](ONBOARDING_FILE_REFERENCE.md)

---

**Document Version**: 1.0
**Last Updated**: 2026-02-23
**Maintained By**: DLT-META Contributors
