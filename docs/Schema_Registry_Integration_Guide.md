# Schema Registry Integration Guide - DLT-META

**Version:** 1.0
**Date:** February 26, 2026
**Author:** Platform Data Engineering Team

---

## Table of Contents

1. [Overview](#overview)
2. [Schema Registry Basics](#schema-registry-basics)
3. [Configuration Methods](#configuration-methods)
4. [Avro Format Integration](#avro-format-integration)
5. [JSON Schema Integration](#json-schema-integration)
6. [Protobuf Integration](#protobuf-integration)
7. [Authentication](#authentication)
8. [Complete Examples](#complete-examples)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

---

## Overview

**Confluent Schema Registry** is a centralized service for managing and validating schemas for Kafka messages. It supports:

- **Avro** (most common)
- **JSON Schema**
- **Protobuf**

### Why Use Schema Registry?

| Benefit | Description |
|---------|-------------|
| **Schema Evolution** | Safely evolve schemas over time with compatibility checks |
| **Data Validation** | Ensure data quality at write time |
| **Storage Efficiency** | Store schema once, reference by ID in messages |
| **Interoperability** | Share schemas across teams and applications |
| **Type Safety** | Strong typing for data structures |

### Architecture Flow

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   Producer   │         │    Kafka     │         │   Consumer   │
│              │         │    Broker    │         │  (DLT-META)  │
└──────┬───────┘         └──────┬───────┘         └──────┬───────┘
       │                        │                        │
       │ 1. Register Schema     │ 2. Write Message      │
       ├───────────────────────▶│   (with schema ID)    │
       │                        ├──────────────────────▶│
       │                        │                        │
       │                        │ 3. Read Message       │
       │                        │ 4. Get Schema by ID   │
       │                        │◀──────────────────────┤
       │                        │                        │
       ▼                        ▼                        ▼
┌──────────────────────────────────────────────────────────┐
│              Confluent Schema Registry                   │
│  - Stores schemas                                         │
│  - Validates compatibility                                │
│  - Returns schemas by ID                                  │
└──────────────────────────────────────────────────────────┘
```

---

## Schema Registry Basics

### Message Wire Format (Avro)

When using Schema Registry with Avro, Kafka messages have this structure:

```
┌─────────┬──────────────┬───────────────────┐
│ Magic   │  Schema ID   │   Avro Data       │
│ Byte    │  (4 bytes)   │   (Variable)      │
│ (0x00)  │              │                   │
└─────────┴──────────────┴───────────────────┘
```

- **Magic Byte (0x00)**: Indicates Confluent wire format
- **Schema ID**: 4-byte integer referencing schema in registry
- **Avro Data**: Binary encoded data according to schema

### Schema Registry Endpoints

| Endpoint | Purpose | Example |
|----------|---------|---------|
| `/subjects` | List all subjects | `GET /subjects` |
| `/subjects/{subject}/versions` | List schema versions | `GET /subjects/payment-events-value/versions` |
| `/schemas/ids/{id}` | Get schema by ID | `GET /schemas/ids/1` |
| `/config` | Get global compatibility | `GET /config` |

---

## Configuration Methods

### Method 1: Using Spark Built-in Functions (Recommended)

**Databricks Runtime 11.3+** includes native support for Schema Registry with `from_avro()` and `to_avro()` functions.

#### Onboarding File Configuration

```json
{
  "data_flow_id": "3001",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "payment-events",
    "kafka.bootstrap.servers": "kafka-broker:9093"
  },

  "landing_table": "kafka_payment_events_raw",

  "refinery_table": "payment_events_deserialized",
  "refinery_transformation_json_prod": "/Volumes/.../transformations/avro_deserialize.json"
}
```

#### Transformation JSON File

`/Volumes/.../transformations/avro_deserialize.json`:

```json
{
  "transformation_type": "avro_deserialize",
  "schema_registry_url": "https://schema-registry.company.com:8081",
  "schema_registry_auth": {
    "basic.auth.credentials.source": "USER_INFO",
    "basic.auth.user.info": "{{secrets/schema_registry/api_key}}:{{secrets/schema_registry/api_secret}}"
  },
  "subject_name": "payment-events-value",
  "value_column": "value",
  "key_column": "key"
}
```

### Method 2: Direct SQL Function Approach

Use Spark SQL `from_avro()` function directly in `refinery_select_exp`.

#### Onboarding File with Inline Deserialization

```json
{
  "data_flow_id": "3002",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "user-events",
    "kafka.bootstrap.servers": "kafka-broker:9093"
  },

  "landing_table": "kafka_user_events_raw",

  "refinery_table": "user_events",
  "refinery_select_exp": [
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp",
    "from_avro(value, 'user-events-value', map('mode', 'PERMISSIVE', 'url', 'https://schema-registry:8081', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}')) as event"
  ],
  "refinery_select_exp_unpacked": [
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "event.user_id",
    "event.event_type",
    "event.event_timestamp",
    "event.properties"
  ]
}
```

### Method 3: Custom Python UDF (Advanced)

For complex deserialization logic or non-Avro formats.

#### Python Notebook

```python
# Register UDF for deserialization
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json

# Initialize Schema Registry client
sr_config = {
    'url': 'https://schema-registry:8081',
    'basic.auth.user.info': dbutils.secrets.get('schema_registry', 'credentials')
}
sr_client = SchemaRegistryClient(sr_config)

# Create deserializer
avro_deserializer = AvroDeserializer(sr_client)

@udf(StringType())
def deserialize_avro(value_bytes):
    if value_bytes is None:
        return None
    try:
        deserialized = avro_deserializer(value_bytes, None)
        return json.dumps(deserialized)
    except Exception as e:
        return json.dumps({"error": str(e)})

# Register UDF
spark.udf.register("deserialize_avro_udf", deserialize_avro)
```

#### Onboarding File

```json
{
  "refinery_select_exp": [
    "deserialize_avro_udf(value) as event_json",
    "topic",
    "partition",
    "offset"
  ]
}
```

---

## Avro Format Integration

### Example 1: Basic Avro Deserialization with Schema Registry

**Scenario:** Ingest Avro-encoded payment events

#### Step 1: Configure Secrets

```bash
# Store Schema Registry credentials
databricks secrets create-scope --scope schema_registry

databricks secrets put --scope schema_registry --key api_key
# Enter: SCHEMA_REGISTRY_API_KEY

databricks secrets put --scope schema_registry --key api_secret
# Enter: SCHEMA_REGISTRY_API_SECRET

# Create combined credential
databricks secrets put --scope schema_registry --key credentials
# Enter: SCHEMA_REGISTRY_API_KEY:SCHEMA_REGISTRY_API_SECRET
```

#### Step 2: Onboarding File

`conf/onboarding/kafka/payment_events_avro.json`:

```json
[
  {
    "data_flow_id": "5001",
    "data_flow_group": "payment_streaming",
    "source_system": "kafka_prod",
    "source_format": "kafka",

    "source_details": {
      "subscribe": "payment.events.avro",
      "kafka.bootstrap.servers": "kafka-broker:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "PLAIN",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_payments",
    "landing_table": "kafka_payment_events_raw",
    "landing_table_comment": "Raw Kafka messages with Avro-encoded values",

    "landing_reader_options": {
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "100000"
    },

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_payments",
    "refinery_table": "payment_events",
    "refinery_table_comment": "Deserialized payment events from Avro",

    "refinery_select_exp": [
      "topic",
      "partition",
      "offset",
      "timestamp as kafka_timestamp",
      "from_avro(value, 'payment.events.avro-value', map('url', 'https://schema-registry.company.com:8081', 'basic.auth.credentials.source', 'USER_INFO', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}')) as payment"
    ],

    "refinery_select_exp_final": [
      "payment.payment_id",
      "payment.customer_id",
      "payment.amount",
      "payment.currency",
      "payment.payment_method",
      "payment.transaction_timestamp",
      "payment.status",
      "topic",
      "partition",
      "offset",
      "kafka_timestamp"
    ]
  }
]
```

### Example 2: Avro with Schema Evolution

**Scenario:** Handle multiple schema versions

#### Step 1: Check Schema Evolution

```bash
# Get all schema versions for a subject
curl -X GET https://schema-registry:8081/subjects/payment.events.avro-value/versions \
  -u $API_KEY:$API_SECRET

# Response: [1, 2, 3, 4]

# Get specific version
curl -X GET https://schema-registry:8081/subjects/payment.events.avro-value/versions/4 \
  -u $API_KEY:$API_SECRET
```

#### Step 2: Onboarding File with Version Handling

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'payment.events.avro-value', map('url', 'https://schema-registry:8081', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}', 'mode', 'PERMISSIVE')) as payment"
  ],

  "refinery_transformation_json_prod": "/Volumes/.../transformations/payment_schema_migration.json"
}
```

#### Transformation JSON for Schema Migration

`payment_schema_migration.json`:

```json
{
  "transformation_type": "select",
  "select_columns": [
    "COALESCE(payment.payment_id, payment.transaction_id) as payment_id",
    "payment.customer_id",
    "payment.amount",
    "COALESCE(payment.currency, 'USD') as currency",
    "payment.payment_method",
    "COALESCE(payment.transaction_timestamp, payment.created_at) as transaction_timestamp",
    "payment.status"
  ]
}
```

### Example 3: Key and Value Deserialization

**Scenario:** Both Kafka key and value are Avro-encoded

```json
{
  "refinery_select_exp": [
    "from_avro(key, 'payment.events.avro-key', map('url', 'https://schema-registry:8081', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}')) as key_data",
    "from_avro(value, 'payment.events.avro-value', map('url', 'https://schema-registry:8081', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}')) as value_data",
    "topic",
    "partition",
    "offset"
  ],

  "refinery_select_exp_final": [
    "key_data.customer_id as key_customer_id",
    "key_data.payment_date as key_payment_date",
    "value_data.*",
    "topic",
    "partition",
    "offset"
  ]
}
```

---

## JSON Schema Integration

**JSON Schema** is an alternative to Avro, using JSON with schema validation.

### Example: JSON Schema Deserialization

#### Onboarding File

```json
{
  "data_flow_id": "5010",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "events.json-schema",
    "kafka.bootstrap.servers": "kafka-broker:9093"
  },

  "landing_table": "kafka_events_raw",

  "refinery_table": "events_json_schema",
  "refinery_select_exp": [
    "CAST(value AS STRING) as value_str",
    "topic",
    "partition",
    "offset"
  ],

  "refinery_transformation_json_prod": "/Volumes/.../transformations/json_schema_validate.json"
}
```

#### Transformation with Schema Validation

`json_schema_validate.json`:

```json
{
  "transformation_type": "json_schema_validate",
  "schema_registry_url": "https://schema-registry:8081",
  "schema_registry_auth": {
    "basic.auth.user.info": "{{secrets/schema_registry/credentials}}"
  },
  "subject_name": "events.json-schema-value",
  "json_column": "value_str",
  "validation_mode": "PERMISSIVE",
  "parse_columns": [
    "$.event_id as event_id",
    "$.user_id as user_id",
    "$.event_type as event_type",
    "$.timestamp as event_timestamp",
    "$.properties as properties"
  ]
}
```

---

## Protobuf Integration

**Protobuf** is a binary serialization format from Google, more compact than JSON.

### Example: Protobuf Deserialization

#### Prerequisites

```python
# Install protobuf library in Databricks
%pip install protobuf confluent-kafka[protobuf]
```

#### Python UDF for Protobuf

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Configure Schema Registry
sr_config = {
    'url': 'https://schema-registry:8081',
    'basic.auth.user.info': dbutils.secrets.get('schema_registry', 'credentials')
}
sr_client = SchemaRegistryClient(sr_config)

# Create Protobuf deserializer
protobuf_deserializer = ProtobufDeserializer(sr_client)

# Define output schema
payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

@udf(payment_schema)
def deserialize_protobuf(value_bytes):
    if value_bytes is None:
        return None
    try:
        message = protobuf_deserializer(value_bytes, None)
        return (
            message.payment_id,
            message.customer_id,
            message.amount,
            message.currency,
            message.timestamp
        )
    except Exception as e:
        return None

spark.udf.register("deserialize_protobuf_udf", deserialize_protobuf)
```

#### Onboarding File

```json
{
  "refinery_select_exp": [
    "deserialize_protobuf_udf(value) as payment",
    "topic",
    "partition",
    "offset"
  ],

  "refinery_select_exp_final": [
    "payment.payment_id",
    "payment.customer_id",
    "payment.amount",
    "payment.currency",
    "payment.timestamp as transaction_timestamp",
    "topic",
    "partition",
    "offset"
  ]
}
```

---

## Authentication

### Authentication Method 1: Basic Auth (Most Common)

**For Confluent Cloud or secured Schema Registry**

#### Configure Secrets

```bash
databricks secrets put --scope schema_registry --key credentials
# Enter: API_KEY:API_SECRET
```

#### Configuration

```json
{
  "from_avro": {
    "options": {
      "url": "https://schema-registry.company.com:8081",
      "basic.auth.credentials.source": "USER_INFO",
      "basic.auth.user.info": "{{secrets/schema_registry/credentials}}"
    }
  }
}
```

### Authentication Method 2: SSL/TLS with Client Certificates

**For Schema Registry with mTLS**

#### Upload Certificates

```python
dbutils.fs.put(
    "/Volumes/.../schema_registry/client-cert.pem",
    client_cert_content
)
dbutils.fs.put(
    "/Volumes/.../schema_registry/client-key.pem",
    client_key_content
)
```

#### Configuration

```json
{
  "from_avro": {
    "options": {
      "url": "https://schema-registry:8081",
      "ssl.certificate.location": "/Volumes/.../schema_registry/client-cert.pem",
      "ssl.key.location": "/Volumes/.../schema_registry/client-key.pem"
    }
  }
}
```

### Authentication Method 3: No Authentication (Development)

```json
{
  "from_avro": {
    "options": {
      "url": "http://schema-registry:8081"
    }
  }
}
```

---

## Complete Examples

### Complete Example 1: Confluent Cloud Kafka + Schema Registry

**Full end-to-end configuration for Confluent Cloud**

#### Onboarding File

```json
[
  {
    "data_flow_id": "6001",
    "data_flow_group": "confluent_streaming",
    "source_system": "confluent_cloud",
    "source_format": "kafka",

    "source_details": {
      "subscribe": "orders.avro.v1",
      "kafka.bootstrap.servers": "pkc-abc123.us-east-1.aws.confluent.cloud:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "PLAIN",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/confluent/api_key}}\" password=\"{{secrets/confluent/api_secret}}\";"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_orders",
    "landing_table": "kafka_orders_raw_avro",
    "landing_table_comment": "Raw Kafka orders from Confluent Cloud (Avro)",

    "landing_reader_options": {
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "50000",
      "kafka.request.timeout.ms": "60000"
    },

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_orders",
    "refinery_table": "orders",
    "refinery_table_comment": "Deserialized orders from Avro format",

    "refinery_select_exp": [
      "topic",
      "partition",
      "offset",
      "timestamp as kafka_timestamp",
      "from_avro(value, 'orders.avro.v1-value', map('url', 'https://psrc-abc123.us-east-1.aws.confluent.cloud', 'basic.auth.credentials.source', 'USER_INFO', 'basic.auth.user.info', '{{secrets/confluent/schema_registry_api_key}}:{{secrets/confluent/schema_registry_api_secret}}')) as order_data"
    ],

    "refinery_select_exp_final": [
      "order_data.order_id",
      "order_data.customer_id",
      "order_data.order_date",
      "order_data.total_amount",
      "order_data.currency",
      "order_data.status",
      "order_data.items",
      "order_data.shipping_address",
      "DATE(order_data.order_date) as order_date_partition",
      "topic",
      "partition",
      "offset",
      "kafka_timestamp"
    ],

    "refinery_partition_columns": "order_date_partition",

    "refinery_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/orders_refinery_dqe.json"
  }
]
```

#### Data Quality Expectations

`orders_refinery_dqe.json`:

```json
{
  "expect_all_or_drop": {
    "valid_order_id": "order_id IS NOT NULL AND LENGTH(order_id) > 0",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_amount": "total_amount > 0",
    "valid_currency": "currency IN ('USD', 'EUR', 'GBP')"
  },
  "expect_or_quarantine": {
    "recent_order": "order_date >= current_date() - INTERVAL 30 DAYS"
  }
}
```

### Complete Example 2: Multi-Topic Avro Ingestion

**Ingest multiple related topics with different schemas**

```json
[
  {
    "data_flow_id": "6010",
    "source_format": "kafka",

    "source_details": {
      "subscribePattern": "ecommerce\\..*\\.avro",
      "kafka.bootstrap.servers": "kafka-broker:9093"
    },

    "landing_table": "kafka_ecommerce_events_raw",

    "refinery_table": "ecommerce_events_unified",
    "refinery_transformation_json_prod": "/Volumes/.../transformations/multi_topic_avro_deserialize.json"
  }
]
```

#### Transformation JSON

`multi_topic_avro_deserialize.json`:

```json
{
  "transformation_type": "conditional_deserialize",
  "schema_registry_url": "https://schema-registry:8081",
  "schema_registry_auth": {
    "basic.auth.user.info": "{{secrets/schema_registry/credentials}}"
  },
  "topic_schema_mapping": [
    {
      "topic_pattern": "ecommerce.orders.avro",
      "subject": "ecommerce.orders.avro-value",
      "select_columns": [
        "event.order_id",
        "event.customer_id",
        "event.total_amount",
        "'order' as event_type"
      ]
    },
    {
      "topic_pattern": "ecommerce.customers.avro",
      "subject": "ecommerce.customers.avro-value",
      "select_columns": [
        "event.customer_id",
        "event.customer_name",
        "event.email",
        "'customer' as event_type"
      ]
    },
    {
      "topic_pattern": "ecommerce.products.avro",
      "subject": "ecommerce.products.avro-value",
      "select_columns": [
        "event.product_id",
        "event.product_name",
        "event.price",
        "'product' as event_type"
      ]
    }
  ]
}
```

---

## Best Practices

### 1. Schema Subject Naming Convention

**Follow Confluent's naming strategy:**

| Strategy | Format | Example |
|----------|--------|---------|
| **TopicNameStrategy** (default) | `{topic}-value`, `{topic}-key` | `payment-events-value` |
| **RecordNameStrategy** | `{namespace}.{name}` | `com.company.Payment` |
| **TopicRecordNameStrategy** | `{topic}-{namespace}.{name}` | `payment-events-com.company.Payment` |

**Configure in onboarding file:**

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'payment-events-value', map('url', 'https://schema-registry:8081'))"
  ]
}
```

### 2. Schema Evolution Best Practices

**Use compatible schema changes:**

✅ **FORWARD compatible:**
- Add optional fields (with defaults)
- Delete optional fields

✅ **BACKWARD compatible:**
- Add optional fields (with defaults)
- Delete required fields

✅ **FULL compatible:**
- Add optional fields (with defaults)

❌ **Breaking changes:**
- Change field type
- Rename field (without alias)
- Add required field without default
- Delete required field

### 3. Error Handling

**Use PERMISSIVE mode for schema evolution:**

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'events-value', map('url', 'https://schema-registry:8081', 'mode', 'PERMISSIVE')) as event"
  ]
}
```

**Modes:**

- `PERMISSIVE` (default): Set corrupted fields to null, continue processing
- `DROPMALFORMED`: Drop rows with schema mismatches
- `FAILFAST`: Fail pipeline on first schema mismatch

### 4. Performance Optimization

**Cache Schema Registry responses:**

```json
{
  "landing_reader_options": {
    "schema.registry.cache.capacity": "1000",
    "schema.registry.cache.expiry.secs": "300"
  }
}
```

**Batch schema lookups:**

```scala
// In custom transformation
spark.conf.set("spark.sql.avro.compression.codec", "snappy")
spark.conf.set("spark.sql.avro.deflate.level", "5")
```

### 5. Monitoring and Alerting

**Track schema validation failures:**

```sql
-- Create monitoring view
CREATE OR REPLACE VIEW schema_validation_failures AS
SELECT
  DATE(kafka_timestamp) as failure_date,
  topic,
  COUNT(*) as failure_count,
  COLLECT_SET(error_message) as error_types
FROM landing.kafka_events_raw
WHERE _rescued_data IS NOT NULL
GROUP BY DATE(kafka_timestamp), topic
HAVING failure_count > 100
```

### 6. Security Best Practices

**Never hardcode credentials:**

```json
// ❌ WRONG
{
  "basic.auth.user.info": "my-api-key:my-api-secret"
}

// ✅ CORRECT
{
  "basic.auth.user.info": "{{secrets/schema_registry/credentials}}"
}
```

**Use dedicated service accounts:**

```bash
# Create dedicated Schema Registry API key for DLT-META
confluent api-key create --resource sr-12345 \
  --description "DLT-META Schema Registry Access"
```

---

## Troubleshooting

### Issue 1: Schema Not Found

**Symptoms:**

```
org.apache.spark.sql.avro.AvroTypeException: Cannot find schema for subject: payment-events-value
```

**Solutions:**

1. **Verify subject name:**

```bash
curl -X GET https://schema-registry:8081/subjects \
  -u $API_KEY:$API_SECRET | jq
```

2. **Check subject naming strategy:**

```bash
# Get schema for specific subject
curl -X GET https://schema-registry:8081/subjects/payment-events-value/versions/latest \
  -u $API_KEY:$API_SECRET
```

3. **Ensure schema is registered:**

```bash
# Register schema manually if needed
curl -X POST https://schema-registry:8081/subjects/payment-events-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u $API_KEY:$API_SECRET \
  -d @schema.json
```

### Issue 2: Authentication Failed

**Symptoms:**

```
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Unauthorized; error code: 401
```

**Solutions:**

1. **Verify credentials:**

```python
# Test Schema Registry access
import requests
from requests.auth import HTTPBasicAuth

api_key = dbutils.secrets.get('schema_registry', 'api_key')
api_secret = dbutils.secrets.get('schema_registry', 'api_secret')

response = requests.get(
    'https://schema-registry:8081/subjects',
    auth=HTTPBasicAuth(api_key, api_secret)
)
print(response.status_code)
print(response.json())
```

2. **Check secret format:**

```bash
# Credential format should be: API_KEY:API_SECRET (colon-separated)
databricks secrets get --scope schema_registry --key credentials
```

### Issue 3: Schema Compatibility Error

**Symptoms:**

```
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible
```

**Solutions:**

1. **Check compatibility mode:**

```bash
# Get current compatibility mode
curl -X GET https://schema-registry:8081/config/payment-events-value \
  -u $API_KEY:$API_SECRET

# Set compatibility mode
curl -X PUT https://schema-registry:8081/config/payment-events-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u $API_KEY:$API_SECRET \
  -d '{"compatibility": "FORWARD"}'
```

2. **Test compatibility before registering:**

```bash
curl -X POST https://schema-registry:8081/compatibility/subjects/payment-events-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u $API_KEY:$API_SECRET \
  -d @new_schema.json
```

### Issue 4: Slow Deserialization Performance

**Symptoms:**

Pipeline processing is slow due to Schema Registry lookups

**Solutions:**

1. **Enable schema caching:**

```python
# Set Spark configuration
spark.conf.set("spark.sql.avro.schema.cache.size", "1000")
```

2. **Increase parallelism:**

```json
{
  "landing_reader_options": {
    "minPartitions": "32"
  }
}
```

3. **Use local Schema Registry cache:**

```bash
# Deploy Schema Registry cache sidecar (advanced)
```

### Issue 5: Deserialization Errors

**Symptoms:**

```
org.apache.avro.AvroTypeException: Found bytes, expecting union
```

**Solutions:**

1. **Check wire format:**

```python
# Verify magic byte (should be 0x00 for Confluent format)
from pyspark.sql.functions import expr

df.selectExpr(
    "hex(substring(value, 1, 1)) as magic_byte",
    "conv(hex(substring(value, 2, 4)), 16, 10) as schema_id"
).show()
```

2. **Handle non-Confluent format:**

```json
{
  "refinery_select_exp": [
    "CASE WHEN substring(value, 1, 1) = X'00' THEN from_avro(value, 'subject', options) ELSE null END as parsed"
  ]
}
```

### Issue 6: Missing Schema Registry URL

**Symptoms:**

```
java.lang.IllegalArgumentException: requirement failed: Schema Registry URL must be specified
```

**Solutions:**

Ensure URL is specified in options:

```json
{
  "from_avro": {
    "options": {
      "url": "https://schema-registry:8081"
    }
  }
}
```

---

## Additional Resources

### Documentation

- [Confluent Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Databricks Avro Functions](https://docs.databricks.com/sql/language-manual/functions/from_avro.html)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)

### Tools

- [Schema Registry UI](https://github.com/lensesio/schema-registry-ui)
- [Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html)
- [Avro Tools](https://avro.apache.org/releases.html)

### Schema Registry REST API

```bash
# List all subjects
curl -X GET https://schema-registry:8081/subjects

# Get all versions for a subject
curl -X GET https://schema-registry:8081/subjects/{subject}/versions

# Get specific schema version
curl -X GET https://schema-registry:8081/subjects/{subject}/versions/{version}

# Get schema by ID
curl -X GET https://schema-registry:8081/schemas/ids/{id}

# Register new schema
curl -X POST https://schema-registry:8081/subjects/{subject}/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{...}"}'

# Test compatibility
curl -X POST https://schema-registry:8081/compatibility/subjects/{subject}/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{...}"}'

# Delete subject
curl -X DELETE https://schema-registry:8081/subjects/{subject}
```

---

## Appendix: Sample Avro Schema

```json
{
  "type": "record",
  "name": "PaymentEvent",
  "namespace": "com.company.payments",
  "doc": "Payment event schema",
  "fields": [
    {
      "name": "payment_id",
      "type": "string",
      "doc": "Unique payment identifier"
    },
    {
      "name": "customer_id",
      "type": "string",
      "doc": "Customer identifier"
    },
    {
      "name": "amount",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 18,
        "scale": 2
      },
      "doc": "Payment amount"
    },
    {
      "name": "currency",
      "type": "string",
      "default": "USD",
      "doc": "Currency code (ISO 4217)"
    },
    {
      "name": "payment_method",
      "type": {
        "type": "enum",
        "name": "PaymentMethod",
        "symbols": ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
      },
      "doc": "Payment method"
    },
    {
      "name": "transaction_timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Transaction timestamp in milliseconds"
    },
    {
      "name": "status",
      "type": "string",
      "doc": "Payment status"
    },
    {
      "name": "metadata",
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "default": null,
      "doc": "Additional metadata"
    }
  ]
}
```

---

**Document Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-26 | Initial Schema Registry integration guide created |
