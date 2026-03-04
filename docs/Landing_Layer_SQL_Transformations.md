# Landing Layer SQL Transformations - Feature Guide

**Version:** 1.0
**Date:** February 26, 2026
**Feature:** SQL transformations in Landing Layer

---

## Overview

DLT-META now supports **SQL transformations at the Landing layer**, enabling you to apply transformations (such as Avro deserialization, JSON parsing, column selection, etc.) during data ingestion.

### What's New

Previously, SQL transformations were only available in:
- ✅ **Refinery layer** (`refinery_transformation_json_{env}`)
- ✅ **Treasury layer** (`treasury_transformation_json_{env}`)

Now supported:
- ✅ **Landing layer** (`landing_transformation_json_{env}`)

---

## Why Use Landing Layer Transformations?

### Use Cases

1. **Avro/Protobuf Deserialization from Kafka**
   - Deserialize binary data at ingestion time
   - Store structured data in landing tables
   - Make landing data immediately queryable

2. **JSON Parsing**
   - Extract fields from JSON strings
   - Flatten nested JSON structures
   - Parse semi-structured data

3. **Data Type Conversions**
   - Cast columns to proper types
   - Parse timestamp strings
   - Convert formats

4. **Column Selection and Renaming**
   - Select specific columns from source
   - Rename columns for consistency
   - Add computed columns

5. **Basic Filtering**
   - Filter out invalid records early
   - Remove test/debug data
   - Apply business logic at ingestion

### Benefits

| Benefit | Description |
|---------|-------------|
| **Query Performance** | Landing data is structured, not raw binary |
| **Storage Efficiency** | Only store needed columns and formats |
| **Easier Debugging** | Can query landing tables with SQL |
| **Single Processing** | Avro deserialization happens once |
| **Partition Efficiency** | Partition by business date columns |

---

## Configuration

### Onboarding File

Add the `landing_transformation_json_{env}` field to your onboarding file:

```json
{
  "data_flow_id": "3001",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "my-topic",
    "kafka.bootstrap.servers": "kafka-broker:9093"
  },

  "landing_catalog_nonprod": "my_catalog",
  "landing_database_nonprod": "landing",
  "landing_table": "my_table",

  "landing_transformation_json_nonprod": "/Volumes/my_catalog/dlt_meta/transformations/landing_transform.yaml",
  "landing_transformation_json_prod": "/Volumes/my_catalog/dlt_meta/transformations/landing_transform_prod.yaml"
}
```

### Transformation File (YAML or JSON)

**YAML Format (Recommended):**

```yaml
target_table: my_table
transformation_name: Landing Transformation
description: "Description of what this transformation does"
sql_query: |
  SELECT
    column1,
    column2,
    CAST(column3 AS INT) as column3_int
  FROM stream(kafka_source)
```

**JSON Format:**

```json
{
  "target_table": "my_table",
  "sql_query": "SELECT column1, column2 FROM stream(kafka_source)"
}
```

---

## Complete Example: Kafka + Avro + Schema Registry

### Onboarding File

```json
{
  "data_flow_id": "3001",
  "data_flow_group": "streaming_events",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "payment-events",
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": "..."
  },

  "landing_catalog_nonprod": "enterprise_data",
  "landing_database_nonprod": "landing_payments",
  "landing_table": "kafka_payment_events",
  "landing_table_comment": "Deserialized payment events from Kafka",

  "landing_transformation_json_nonprod": "/Volumes/enterprise_data/dlt_meta/transformations/avro_deserialize.yaml",

  "landing_partition_columns": "payment_date",

  "refinery_catalog_nonprod": "enterprise_data",
  "refinery_database_nonprod": "refinery_payments",
  "refinery_table": "payment_events_cleaned",

  "refinery_transformation_json_nonprod": "/Volumes/enterprise_data/dlt_meta/transformations/clean_payments.yaml"
}
```

### Landing Transformation (Avro Deserialization)

```yaml
target_table: kafka_payment_events
sql_query: |
  SELECT
    payment.payment_id,
    payment.customer_id,
    payment.amount,
    payment.currency,
    CAST(payment.transaction_timestamp AS TIMESTAMP) as transaction_timestamp,
    DATE(CAST(payment.transaction_timestamp AS TIMESTAMP)) as payment_date,
    topic,
    partition,
    offset
  FROM (
    SELECT
      topic,
      partition,
      offset,
      from_avro(
        value,
        'payment-events-value',
        map('url', 'https://schema-registry:8081',
            'basic.auth.user.info', '{{secrets/schema_registry/credentials}}')
      ) as payment
    FROM stream(kafka_source)
  )
```

### Refinery Transformation (Cleaning)

```yaml
target_table: payment_events_cleaned
sql_query: |
  SELECT
    payment_id,
    customer_id,
    CAST(amount AS DECIMAL(18,2)) as amount,
    UPPER(currency) as currency,
    transaction_timestamp,
    payment_date
  FROM LIVE.kafka_payment_events
  WHERE amount > 0
    AND currency IN ('USD', 'EUR', 'GBP')
```

---

## Architecture Patterns

### Pattern 1: Transform at Landing (Recommended for Avro/Binary Formats)

```
Kafka (Avro Binary)
    ↓ landing_transformation_json
Landing (Deserialized Structured Data)
    ↓ refinery_transformation_json
Refinery (Cleaned & Validated)
    ↓ treasury_transformation_json
Treasury (Aggregated)
```

**Benefits:**
- ✅ Avro deserialization happens once
- ✅ Landing data is queryable
- ✅ Efficient partitioning

### Pattern 2: Raw Landing, Transform at Refinery (Historical Pattern)

```
Kafka (Avro Binary)
    ↓ (no transformation)
Landing (Raw Binary: key, value, topic, partition, offset)
    ↓ refinery_transformation_json (with Avro deserialization)
Refinery (Deserialized & Cleaned)
    ↓ treasury_transformation_json
Treasury (Aggregated)
```

**Benefits:**
- ✅ Flexibility to reprocess with different schemas
- ✅ Landing is smallest (raw binary)

---

## SQL Query Requirements

### Required Syntax

The SQL query in landing transformations must:

1. **Use `stream(kafka_source)` as source reference**
   ```sql
   FROM stream(kafka_source)
   ```

2. **Return all required columns**
   - For Kafka: topic, partition, offset, timestamp recommended
   - For your business logic: all needed columns

3. **Be valid Spark SQL**
   - Use Spark SQL functions
   - Follow Spark SQL syntax

### Available Functions

All Spark SQL functions are available:
- `from_avro()` - Avro deserialization
- `get_json_object()` - JSON parsing
- `CAST()` - Type conversion
- `DATE()`, `TIMESTAMP()` - Date/time functions
- String functions, math functions, etc.

### Placeholder Support

You can use placeholders in SQL queries:
- `{landing_catalog}` - Replaced with landing catalog name
- `{landing_database}` - Replaced with landing database name
- `{refinery_catalog}` - Replaced with refinery catalog (for joins)
- `{refinery_database}` - Replaced with refinery database
- `{{secrets/scope/key}}` - Replaced with Databricks secret value

---

## Best Practices

### 1. Keep Landing Transformations Simple

✅ **DO:**
- Deserialize binary formats (Avro, Protobuf)
- Parse JSON to extract fields
- Select necessary columns
- Add partition columns

❌ **DON'T:**
- Complex business logic (use Refinery)
- Joins with other tables (use Refinery)
- Aggregations (use Treasury)
- Heavy computations

### 2. Partition by Business Date

```yaml
sql_query: |
  SELECT
    *,
    DATE(event_timestamp) as event_date  -- For partitioning
  FROM ...
```

Then in onboarding file:
```json
{
  "landing_partition_columns": "event_date"
}
```

### 3. Include Kafka Metadata

Always preserve Kafka metadata for debugging:

```sql
SELECT
  -- Your deserialized data
  payment.payment_id,
  payment.amount,
  -- Kafka metadata
  topic as kafka_topic,
  partition as kafka_partition,
  offset as kafka_offset,
  timestamp as kafka_timestamp
FROM ...
```

### 4. Use PERMISSIVE Mode for Schema Evolution

```sql
from_avro(
  value,
  'topic-value',
  map('mode', 'PERMISSIVE')  -- Allows schema evolution
)
```

### 5. Test Transformations First

Test your SQL in a notebook before adding to landing:

```python
# Test in notebook
test_df = spark.readStream.format("kafka").option(...).load().limit(10)

transformed_df = spark.sql("""
  SELECT ...
  FROM test_df
""")

display(transformed_df)
```

---

## Troubleshooting

### Issue 1: "stream(kafka_source) not found"

**Cause:** Incorrect source reference in SQL

**Solution:** Use `stream(kafka_source)` exactly as shown:
```sql
FROM stream(kafka_source)
```

### Issue 2: Transformation file not loaded

**Check:**
1. File path is correct
2. File exists at specified path
3. File has `.yaml`, `.yml`, or `.json` extension
4. File contains `sql_query` field

**Verify:**
```python
# Check file exists
dbutils.fs.ls("/Volumes/.../transformations/")

# Read file content
dbutils.fs.head("/Volumes/.../transformations/landing_transform.yaml")
```

### Issue 3: SQL syntax error

**Check logs:**
```python
# Pipeline event logs
spark.sql("SELECT * FROM event_log('my_pipeline')").display()
```

**Common issues:**
- Missing `FROM stream(kafka_source)`
- Invalid Spark SQL syntax
- Typo in column names

### Issue 4: No data in landing table

**Check:**
1. Source has data (check Kafka topic)
2. SQL transformation returns data
3. No WHERE clause filtering all records

**Test:**
```sql
-- Simplify query to test
SELECT * FROM stream(kafka_source) LIMIT 10
```

---

## Migration Guide

### Migrating from Raw Landing to Transformed Landing

**Before (Raw Landing):**

```json
{
  "landing_table": "kafka_events_raw",

  "refinery_table": "events",
  "refinery_transformation_json_prod": "/Volumes/.../refinery_avro_deserialize.yaml"
}
```

Refinery transformation:
```yaml
sql_query: |
  SELECT
    from_avro(value, ...) as event
  FROM LIVE.kafka_events_raw
```

**After (Transformed Landing):**

```json
{
  "landing_table": "kafka_events",
  "landing_transformation_json_prod": "/Volumes/.../landing_avro_deserialize.yaml",

  "refinery_table": "events_cleaned",
  "refinery_transformation_json_prod": "/Volumes/.../refinery_clean.yaml"
}
```

Landing transformation:
```yaml
sql_query: |
  SELECT
    from_avro(value, ...) as event
  FROM stream(kafka_source)
```

Refinery transformation:
```yaml
sql_query: |
  SELECT * FROM LIVE.kafka_events WHERE event_id IS NOT NULL
```

---

## Code Changes Summary

### Modified Files

1. **`src/dataflow_spec.py`**
   - Added `sqlQuery: str` field to `LandingDataflowSpec` dataclass

2. **`src/dataflow_pipeline.py`**
   - Modified `read_landing()` to apply SQL transformations if provided

3. **`src/onboard_dataflowspec.py`**
   - Added `sqlQuery` to landing dataflow spec schema
   - Added transformation file reading logic for landing layer
   - Updated landing row tuple to include SQL query

---

## Examples

See example files:
- **Onboarding:** `/cds/conf/onboarding/kafka_ng/onboarding_with_landing_transform.json`
- **Landing Transform:** `/cds/conf/transformations/kafka_ng/landing_avro_deserialize.yaml`
- **Refinery Transform:** `/cds/conf/transformations/kafka_ng/refinery_clean_payments.yaml`

---

## Support

For questions or issues with landing layer transformations:
1. Check this guide
2. Review example configurations
3. Check DLT pipeline event logs
4. Contact Data Engineering team

---

**Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-26 | Initial release of landing layer SQL transformations |
