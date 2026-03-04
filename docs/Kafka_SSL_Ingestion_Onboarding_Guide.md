# Kafka with SSL Ingestion - Onboarding Guide

**Version:** 1.0
**Date:** February 26, 2026
**Author:** Platform Data Engineering Team

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [SSL/TLS Configuration](#ssltls-configuration)
4. [SASL Authentication Mechanisms](#sasl-authentication-mechanisms)
5. [Ingestion Patterns](#ingestion-patterns)
6. [Onboarding File Configuration](#onboarding-file-configuration)
7. [Complete Examples](#complete-examples)
8. [Best Practices](#best-practices)
9. [Performance Tuning](#performance-tuning)
10. [Troubleshooting](#troubleshooting)
11. [Security Considerations](#security-considerations)

---

## Overview

This guide provides comprehensive instructions for ingesting streaming data from Apache Kafka clusters with SSL/TLS encryption and SASL authentication into Delta Live Tables using the DLT-META framework.

### Supported Security Configurations

| Security Protocol | SASL Mechanism | Use Case | Authentication |
|-------------------|----------------|----------|----------------|
| **SSL** | None | SSL encryption only | Certificate-based |
| **SASL_SSL** | PLAIN | SSL + username/password | Username/Password |
| **SASL_SSL** | SCRAM-SHA-256 | SSL + SCRAM | Username/Password |
| **SASL_SSL** | SCRAM-SHA-512 | SSL + SCRAM (stronger) | Username/Password |
| **SASL_SSL** | GSSAPI (Kerberos) | SSL + Kerberos | Kerberos principal |
| **SASL_PLAINTEXT** | PLAIN | SASL without encryption | Username/Password (not recommended) |
| **PLAINTEXT** | None | No security (dev only) | None |

### Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                  Kafka with SSL/TLS Ingestion                    │
└─────────────────────────────────────────────────────────────────┘

KAFKA CLUSTER            DLT-META FRAMEWORK          DELTA LIVE TABLES
┌──────────────┐         ┌──────────────┐            ┌──────────────┐
│              │         │              │            │              │
│  Kafka       │ SSL/    │  Kafka       │  Stream    │  Landing     │
│  Topic       │ TLS +   │  Reader      │───────────▶│  (Bronze)    │
│              │ SASL    │              │            │              │
└──────────────┘         └──────────────┘            └──────┬───────┘
      │                                                      │
      │ Encrypted                                            ▼
      │ Connection                                    ┌──────────────┐
      │                                               │  Refinery    │
      └──────────────────────────────────────────────│  (Silver)    │
        Broker 1, 2, 3...                            │              │
                                                     └──────┬───────┘
                                                            │
                                                            ▼
                                                     ┌──────────────┐
                                                     │  Treasury    │
                                                     │  (Gold)      │
                                                     │              │
                                                     └──────────────┘
```

---

## Prerequisites

### 1. Kafka Cluster Access

Ensure you have the following information from your Kafka cluster administrator:

**Broker Information:**
- Bootstrap servers (broker addresses and ports)
- SSL/TLS port (typically 9093)
- Kafka version

**Example:**
```
kafka-broker-1.company.com:9093
kafka-broker-2.company.com:9093
kafka-broker-3.company.com:9093
```

### 2. SSL/TLS Certificates

For SSL/TLS encryption, you need:

**Option A: Truststore (Server certificate validation)**
- Truststore file (`.jks` or `.p12`)
- Truststore password
- Certificate Authority (CA) certificate

**Option B: PEM Certificates**
- CA certificate (`.pem` or `.crt`)
- Optional: Client certificate and private key for mutual TLS

### 3. SASL Credentials

For SASL authentication:

**PLAIN/SCRAM:**
- Username
- Password
- SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)

**Kerberos (GSSAPI):**
- Kerberos principal
- Keytab file
- Kerberos configuration file (`krb5.conf`)

### 4. Databricks Secrets Setup

Store all credentials in Databricks Secrets:

```bash
# Create secret scope (one-time setup)
databricks secrets create-scope --scope kafka_secrets

# Add credentials
databricks secrets put --scope kafka_secrets --key kafka_username
databricks secrets put --scope kafka_secrets --key kafka_password

# For SSL certificates (base64 encoded)
databricks secrets put --scope kafka_secrets --key truststore_password
```

### 5. Network Connectivity

Ensure Databricks workspace can connect to Kafka:
- **Cloud:** Configure VPC peering, private link, or NAT gateway
- **On-Premise:** Configure VPN or Direct Connect
- **Firewall:** Allow outbound connections on Kafka SSL port (typically 9093)

### 6. Topic Access Permissions

Verify Kafka ACLs grant read permissions:

```bash
# Check ACLs for your user/principal
kafka-acls --bootstrap-server kafka-broker:9093 \
  --command-config client.properties \
  --list --principal User:your_username
```

Required permissions:
- `READ` on topic
- `DESCRIBE` on topic
- `READ` on consumer group

---

## SSL/TLS Configuration

### Configuration Method 1: Using Truststore (Recommended)

**When to use:** Corporate Kafka clusters with provided truststore files

#### Step 1: Upload Truststore to Unity Catalog Volume

```python
# In Databricks notebook
dbutils.fs.mkdirs("/Volumes/my_catalog/shared_resources/kafka_ssl")

# Upload truststore file
dbutils.fs.put(
    "/Volumes/my_catalog/shared_resources/kafka_ssl/kafka.truststore.jks",
    truststore_content,
    overwrite=True
)
```

#### Step 2: Configure Onboarding File

```json
{
  "data_flow_id": "3001",
  "source_format": "kafka",
  "source_details": {
    "subscribe": "my-topic",
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.ssl.truststore.location": "/Volumes/my_catalog/shared_resources/kafka_ssl/kafka.truststore.jks",
    "kafka.ssl.truststore.password": "{{secrets/kafka_secrets/truststore_password}}"
  }
}
```

### Configuration Method 2: Using PEM Certificates

**When to use:** Modern Kafka clusters providing PEM/CRT certificates

#### Step 1: Upload Certificates to Unity Catalog Volume

```python
# Upload CA certificate
dbutils.fs.put(
    "/Volumes/my_catalog/shared_resources/kafka_ssl/ca-cert.pem",
    ca_cert_content,
    overwrite=True
)
```

#### Step 2: Configure Onboarding File

```json
{
  "source_details": {
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.ssl.ca.location": "/Volumes/my_catalog/shared_resources/kafka_ssl/ca-cert.pem"
  }
}
```

### Configuration Method 3: Mutual TLS (mTLS)

**When to use:** High-security environments requiring client certificate authentication

#### Step 1: Upload Client Certificates

```python
# Upload client certificate and private key
dbutils.fs.put("/Volumes/.../kafka_ssl/client-cert.pem", client_cert)
dbutils.fs.put("/Volumes/.../kafka_ssl/client-key.pem", client_key)
dbutils.fs.put("/Volumes/.../kafka_ssl/ca-cert.pem", ca_cert)
```

#### Step 2: Configure Onboarding File

```json
{
  "source_details": {
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.ca.location": "/Volumes/.../kafka_ssl/ca-cert.pem",
    "kafka.ssl.certificate.location": "/Volumes/.../kafka_ssl/client-cert.pem",
    "kafka.ssl.key.location": "/Volumes/.../kafka_ssl/client-key.pem",
    "kafka.ssl.key.password": "{{secrets/kafka_secrets/key_password}}"
  }
}
```

### SSL/TLS Options Reference

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `kafka.security.protocol` | Yes | Security protocol | `SASL_SSL`, `SSL` |
| `kafka.ssl.truststore.location` | No* | Truststore path | `/Volumes/.../kafka.truststore.jks` |
| `kafka.ssl.truststore.password` | No* | Truststore password | `{{secrets/kafka_secrets/password}}` |
| `kafka.ssl.ca.location` | No* | CA certificate path (PEM) | `/Volumes/.../ca-cert.pem` |
| `kafka.ssl.certificate.location` | No | Client cert (mTLS) | `/Volumes/.../client-cert.pem` |
| `kafka.ssl.key.location` | No | Client key (mTLS) | `/Volumes/.../client-key.pem` |
| `kafka.ssl.key.password` | No | Key password | `{{secrets/kafka_secrets/key_pass}}` |
| `kafka.ssl.endpoint.identification.algorithm` | No | Hostname verification | `https` (default), `""` (disable) |

*Either truststore or CA location required for SSL

---

## SASL Authentication Mechanisms

### SASL/PLAIN (Username/Password)

**Most common for cloud Kafka (Confluent Cloud, AWS MSK, etc.)**

#### Configuration:

```json
{
  "source_details": {
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
  }
}
```

**Important:** The JAAS config string must:
- End with semicolon (`;`)
- Use escaped quotes around secret references
- Use `required` keyword

### SASL/SCRAM-SHA-256 or SCRAM-SHA-512

**More secure than PLAIN, salted challenge-response**

#### Configuration:

```json
{
  "source_details": {
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
  }
}
```

For SCRAM-SHA-512, change `SCRAM-SHA-256` to `SCRAM-SHA-512`.

### SASL/GSSAPI (Kerberos)

**Enterprise environments with Active Directory/Kerberos**

#### Step 1: Upload Kerberos Configuration

```python
# Upload keytab and krb5.conf
dbutils.fs.put("/Volumes/.../kafka_ssl/kafka.keytab", keytab_content)
dbutils.fs.put("/Volumes/.../kafka_ssl/krb5.conf", krb5_conf_content)
```

#### Step 2: Configure Onboarding File

```json
{
  "source_details": {
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "GSSAPI",
    "kafka.sasl.kerberos.service.name": "kafka",
    "kafka.sasl.jaas.config": "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/Volumes/.../kafka_ssl/kafka.keytab\" principal=\"kafka-client@REALM.COM\";"
  }
}
```

#### Step 3: Set Kerberos Configuration in DLT Pipeline

Add to pipeline configuration:

```yaml
configuration:
  spark.executorEnv.KRB5_CONFIG: /Volumes/.../kafka_ssl/krb5.conf
  spark.yarn.appMasterEnv.KRB5_CONFIG: /Volumes/.../kafka_ssl/krb5.conf
```

### SASL Mechanism Comparison

| Mechanism | Security Level | Use Case | Pros | Cons |
|-----------|----------------|----------|------|------|
| **PLAIN** | Low-Medium | Cloud Kafka, simple auth | Easy setup | Credentials in clear text over SSL |
| **SCRAM-SHA-256** | Medium | General purpose | Better than PLAIN | Requires server support |
| **SCRAM-SHA-512** | High | Sensitive data | Most secure SCRAM | Requires server support |
| **GSSAPI** | Very High | Enterprise AD integration | Strong auth, SSO | Complex setup |

---

## Ingestion Patterns

### Pattern 1: Streaming Ingestion with SSL/SASL

**Use Case:** Real-time event streaming from secure Kafka clusters

**Advantages:**
- ✅ Real-time data ingestion
- ✅ Encrypted data in transit
- ✅ Authenticated access
- ✅ Automatic offset management

**Configuration:**

```json
{
  "data_flow_id": "3001",
  "data_flow_group": "streaming_events",
  "source_system": "kafka_prod",
  "source_format": "kafka",

  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/dlt_meta/schemas/events.ddl",
    "subscribe": "payment-events",
    "kafka.bootstrap.servers": "kafka-1:9093,kafka-2:9093,kafka-3:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";",
    "kafka.ssl.ca.location": "/Volumes/my_catalog/shared_resources/kafka_ssl/ca-cert.pem"
  },

  "landing_reader_options": {
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "100000",
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "60000"
  },

  "landing_catalog_prod": "enterprise_data",
  "landing_database_prod": "landing_events",
  "landing_table": "kafka_payment_events",
  "landing_table_comment": "Real-time payment events from Kafka"
}
```

### Pattern 2: Multi-Topic Ingestion

**Use Case:** Ingest from multiple related Kafka topics

**Configuration:**

```json
{
  "data_flow_id": "3002",
  "source_format": "kafka",

  "source_details": {
    "subscribePattern": "customer-.*",
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
  },

  "landing_table": "kafka_customer_events"
}
```

**Topic Selection Options:**

| Option | Description | Example |
|--------|-------------|---------|
| `subscribe` | Single topic | `"payment-events"` |
| `subscribePattern` | Topic regex pattern | `"customer-.*"` |
| `assign` | Specific partitions | `"{'topic1': [0, 1], 'topic2': [0]}"` |

### Pattern 3: Batch Processing (Bounded Stream)

**Use Case:** Process specific offset ranges or catch up on historical data

**Configuration:**

```json
{
  "source_details": {
    "subscribe": "transactions",
    "kafka.bootstrap.servers": "kafka-broker:9093"
  },

  "landing_reader_options": {
    "startingOffsets": "{\"transactions\": {\"0\": 1000, \"1\": 1000}}",
    "endingOffsets": "{\"transactions\": {\"0\": 2000, \"1\": 2000}}",
    "maxOffsetsPerTrigger": "50000"
  }
}
```

### Pattern 4: Late-Arriving Data with Watermarking

**Use Case:** Handle out-of-order events with event time processing

**Configuration:**

```json
{
  "source_details": {
    "subscribe": "clickstream",
    "kafka.bootstrap.servers": "kafka-broker:9093"
  },

  "landing_reader_options": {
    "startingOffsets": "latest"
  },

  "refinery_select_exp": [
    "*",
    "CAST(event_timestamp AS TIMESTAMP) as event_time"
  ],

  "refinery_transformation_json_prod": "/Volumes/.../transformations/clickstream_watermark.json"
}
```

**Transformation JSON** (`clickstream_watermark.json`):

```json
{
  "transformation": "withWatermark",
  "watermark_column": "event_time",
  "watermark_delay": "10 minutes"
}
```

---

## Onboarding File Configuration

### Minimal Kafka with SSL Onboarding File

```json
[
  {
    "data_flow_id": "3001",
    "data_flow_group": "kafka_ingestion",
    "source_format": "kafka",

    "source_details": {
      "subscribe": "my-topic",
      "kafka.bootstrap.servers": "kafka-broker:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "PLAIN",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
    },

    "landing_database_prod": "landing_db",
    "landing_table": "kafka_my_topic"
  }
]
```

### Complete Kafka Onboarding File (All Layers)

```json
[
  {
    "data_flow_id": "3010",
    "data_flow_group": "streaming_analytics",
    "source_system": "kafka_prod_cluster",
    "source_format": "kafka",

    "source_details": {
      "source_database": "kafka_events",
      "source_table": "user_activity",
      "source_schema_path": "/Volumes/enterprise_data/dlt_meta/schemas/user_activity.ddl",
      "subscribe": "user-activity-prod",
      "kafka.bootstrap.servers": "kafka-1.prod:9093,kafka-2.prod:9093,kafka-3.prod:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "SCRAM-SHA-512",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/prod_username}}\" password=\"{{secrets/kafka_secrets/prod_password}}\";",
      "kafka.ssl.truststore.location": "/Volumes/enterprise_data/shared_resources/kafka_ssl/prod.truststore.jks",
      "kafka.ssl.truststore.password": "{{secrets/kafka_secrets/truststore_password}}"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_streaming",
    "landing_table": "kafka_user_activity",
    "landing_table_comment": "User activity events from Kafka - real-time stream",

    "landing_reader_options": {
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "500000",
      "kafka.request.timeout.ms": "120000",
      "kafka.session.timeout.ms": "90000",
      "kafka.heartbeat.interval.ms": "30000",
      "kafka.max.poll.records": "10000",
      "kafka.fetch.min.bytes": "1048576",
      "kafka.fetch.max.wait.ms": "500",
      "failOnDataLoss": "false"
    },

    "landing_partition_columns": "event_date",

    "landing_table_properties": {
      "pipelines.autoOptimize.managed": "true",
      "delta.enableChangeDataFeed": "true"
    },

    "landing_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/user_activity_landing_dqe.json",

    "landing_catalog_quarantine_prod": "enterprise_data",
    "landing_database_quarantine_prod": "landing_streaming",
    "landing_quarantine_table": "kafka_user_activity_quarantine",

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_streaming",
    "refinery_table": "user_activity",
    "refinery_table_comment": "Cleaned and enriched user activity events",

    "refinery_select_exp": [
      "CAST(key AS STRING) as event_key",
      "get_json_object(value, '$.user_id') as user_id",
      "get_json_object(value, '$.event_type') as event_type",
      "get_json_object(value, '$.timestamp') as event_timestamp_raw",
      "CAST(get_json_object(value, '$.timestamp') AS TIMESTAMP) as event_timestamp",
      "get_json_object(value, '$.properties') as properties",
      "topic as kafka_topic",
      "partition as kafka_partition",
      "offset as kafka_offset",
      "timestamp as kafka_timestamp"
    ],

    "refinery_where_clause": "user_id IS NOT NULL AND event_type IS NOT NULL",

    "refinery_transformation_json_prod": "/Volumes/enterprise_data/dlt_meta/transformations/user_activity_enrichment.json",

    "refinery_partition_columns": "event_date",

    "refinery_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/user_activity_refinery_dqe.json",

    "treasury_catalog_prod": "enterprise_data",
    "treasury_database_prod": "treasury_analytics",
    "treasury_table": "user_activity_hourly_summary",
    "treasury_table_comment": "Hourly aggregated user activity metrics",

    "treasury_transformation_json_prod": "/Volumes/enterprise_data/dlt_meta/transformations/user_activity_hourly_summary.json",

    "treasury_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/user_activity_treasury_dqe.json"
  }
]
```

### Important Source Details Fields

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `subscribe` | Yes* | Kafka topic name | `"payment-events"` |
| `subscribePattern` | Yes* | Regex pattern for topics | `"payment-.*"` |
| `kafka.bootstrap.servers` | Yes | Broker addresses | `"broker1:9093,broker2:9093"` |
| `kafka.security.protocol` | Yes | Security protocol | `"SASL_SSL"`, `"SSL"` |
| `kafka.sasl.mechanism` | No** | SASL mechanism | `"PLAIN"`, `"SCRAM-SHA-256"` |
| `kafka.sasl.jaas.config` | No** | JAAS configuration | See examples above |
| `kafka.ssl.truststore.location` | No*** | Truststore path | `/Volumes/.../truststore.jks` |
| `kafka.ssl.ca.location` | No*** | CA certificate path | `/Volumes/.../ca-cert.pem` |
| `source_schema_path` | No | DDL schema file | `/Volumes/.../schema.ddl` |

*Either `subscribe` or `subscribePattern` required
**Required when `security.protocol` is `SASL_*`
***Required for SSL/SASL_SSL

---

## Complete Examples

### Example 1: Real-Time Order Events (SASL_SSL with SCRAM)

**Scenario:** Ingest order events from Confluent Cloud Kafka

**Onboarding File:** `conf/onboarding/kafka/order_events.json`

```json
[
  {
    "data_flow_id": "4001",
    "data_flow_group": "ecommerce_streaming",
    "source_system": "confluent_cloud",
    "source_format": "kafka",

    "source_details": {
      "source_database": "kafka_orders",
      "source_table": "orders",
      "source_schema_path": "/Volumes/enterprise_data/dlt_meta/schemas/order_events.ddl",
      "subscribe": "ecommerce.orders.v1",
      "kafka.bootstrap.servers": "pkc-abc123.us-east-1.aws.confluent.cloud:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "PLAIN",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/confluent_api_key}}\" password=\"{{secrets/kafka_secrets/confluent_api_secret}}\";"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_ecommerce",
    "landing_table": "kafka_order_events",
    "landing_table_comment": "Real-time order events from Confluent Cloud",

    "landing_reader_options": {
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "100000",
      "kafka.request.timeout.ms": "60000",
      "kafka.session.timeout.ms": "60000"
    },

    "landing_partition_columns": "order_date",

    "landing_data_quality_expectations_json_prod": "/Volumes/enterprise_data/dlt_meta/dqe/order_events_landing_dqe.json"
  }
]
```

**Schema File:** `/Volumes/enterprise_data/dlt_meta/schemas/order_events.ddl`

```sql
CREATE TABLE IF NOT EXISTS kafka_order_events (
  key STRING,
  value STRING,
  topic STRING,
  partition INT,
  offset LONG,
  timestamp TIMESTAMP,
  timestampType INT
)
```

**Data Quality File:** `/Volumes/enterprise_data/dlt_meta/dqe/order_events_landing_dqe.json`

```json
{
  "expect_all_or_drop": {
    "non_null_value": "value IS NOT NULL",
    "valid_json": "get_json_object(value, '$.order_id') IS NOT NULL"
  },
  "expect_or_quarantine": {
    "valid_kafka_offset": "offset >= 0",
    "recent_timestamp": "timestamp >= current_timestamp() - INTERVAL 7 DAYS"
  }
}
```

### Example 2: IoT Sensor Data (SSL with mTLS)

**Scenario:** Ingest IoT sensor telemetry with mutual TLS authentication

**Onboarding File:** `conf/onboarding/kafka/iot_sensors.json`

```json
[
  {
    "data_flow_id": "4002",
    "data_flow_group": "iot_telemetry",
    "source_system": "kafka_iot_prod",
    "source_format": "kafka",

    "source_details": {
      "source_schema_path": "/Volumes/enterprise_data/dlt_meta/schemas/iot_telemetry.ddl",
      "subscribePattern": "sensors\\..*\\.telemetry",
      "kafka.bootstrap.servers": "kafka-iot-1:9093,kafka-iot-2:9093",
      "kafka.security.protocol": "SSL",
      "kafka.ssl.ca.location": "/Volumes/enterprise_data/shared_resources/kafka_ssl/iot-ca.pem",
      "kafka.ssl.certificate.location": "/Volumes/enterprise_data/shared_resources/kafka_ssl/iot-client-cert.pem",
      "kafka.ssl.key.location": "/Volumes/enterprise_data/shared_resources/kafka_ssl/iot-client-key.pem",
      "kafka.ssl.key.password": "{{secrets/kafka_secrets/iot_key_password}}"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_iot",
    "landing_table": "kafka_iot_telemetry",
    "landing_table_comment": "IoT sensor telemetry from Kafka - multiple topics",

    "landing_reader_options": {
      "startingOffsets": "earliest",
      "maxOffsetsPerTrigger": "1000000",
      "kafka.max.partition.fetch.bytes": "10485760"
    },

    "landing_partition_columns": "sensor_date",

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_iot",
    "refinery_table": "iot_telemetry",

    "refinery_select_exp": [
      "CAST(key AS STRING) as sensor_id",
      "get_json_object(value, '$.temperature') as temperature",
      "get_json_object(value, '$.humidity') as humidity",
      "get_json_object(value, '$.timestamp') as reading_timestamp",
      "topic as kafka_topic",
      "SPLIT(topic, '\\\\.')[1] as sensor_location",
      "partition as kafka_partition",
      "offset as kafka_offset"
    ]
  }
]
```

### Example 3: AWS MSK with IAM Authentication

**Scenario:** Ingest from Amazon MSK using IAM authentication

**Onboarding File:** `conf/onboarding/kafka/msk_logs.json`

```json
[
  {
    "data_flow_id": "4003",
    "data_flow_group": "aws_logs",
    "source_system": "aws_msk_prod",
    "source_format": "kafka",

    "source_details": {
      "subscribe": "application-logs",
      "kafka.bootstrap.servers": "b-1.msk-prod.kafka.us-east-1.amazonaws.com:9098,b-2.msk-prod.kafka.us-east-1.amazonaws.com:9098",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "AWS_MSK_IAM",
      "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
      "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_logs",
    "landing_table": "kafka_application_logs",

    "landing_reader_options": {
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "200000"
    }
  }
]
```

**Note:** AWS MSK IAM authentication requires:
1. Databricks cluster IAM role with MSK permissions
2. AWS MSK IAM library in cluster libraries

### Example 4: Multi-Environment Configuration

**Scenario:** Same pipeline across dev/qa/prod with different credentials

**Onboarding File:** `conf/onboarding/kafka/customer_events_multi_env.json`

```json
[
  {
    "data_flow_id": "4004",
    "data_flow_group": "customer_events",
    "source_format": "kafka",

    "source_details": {
      "subscribe": "customer.events",
      "kafka.bootstrap.servers": "kafka-prod:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "SCRAM-SHA-256",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username_prod}}\" password=\"{{secrets/kafka_secrets/password_prod}}\";",
      "kafka.ssl.ca.location": "/Volumes/enterprise_data/shared_resources/kafka_ssl/prod-ca.pem"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_events",
    "landing_table": "kafka_customer_events",

    "landing_catalog_nonprod": "dev_data",
    "landing_database_nonprod": "landing_events",
    "landing_table": "kafka_customer_events",

    "source_details_nonprod": {
      "kafka.bootstrap.servers": "kafka-nonprod:9093",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username_nonprod}}\" password=\"{{secrets/kafka_secrets/password_nonprod}}\";",
      "kafka.ssl.ca.location": "/Volumes/dev_data/shared_resources/kafka_ssl/nonprod-ca.pem"
    }
  }
]
```

### Example 5: JSON Parsing and Enrichment

**Scenario:** Parse nested JSON from Kafka and enrich with reference data

**Onboarding File:** `conf/onboarding/kafka/transaction_events.json`

```json
[
  {
    "data_flow_id": "4005",
    "data_flow_group": "payment_transactions",
    "source_format": "kafka",

    "source_details": {
      "subscribe": "transactions",
      "kafka.bootstrap.servers": "kafka-broker:9093",
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "PLAIN",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
    },

    "landing_catalog_prod": "enterprise_data",
    "landing_database_prod": "landing_payments",
    "landing_table": "kafka_transactions_raw",

    "refinery_catalog_prod": "enterprise_data",
    "refinery_database_prod": "refinery_payments",
    "refinery_table": "transactions",

    "refinery_select_exp": [
      "get_json_object(value, '$.transaction_id') as transaction_id",
      "get_json_object(value, '$.customer_id') as customer_id",
      "CAST(get_json_object(value, '$.amount') AS DECIMAL(18,2)) as amount",
      "get_json_object(value, '$.currency') as currency",
      "CAST(get_json_object(value, '$.timestamp') AS TIMESTAMP) as transaction_timestamp",
      "get_json_object(value, '$.payment_method') as payment_method",
      "get_json_object(value, '$.merchant.id') as merchant_id",
      "get_json_object(value, '$.merchant.name') as merchant_name",
      "CAST(get_json_object(value, '$.metadata.risk_score') AS INT) as risk_score",
      "DATE(CAST(get_json_object(value, '$.timestamp') AS TIMESTAMP)) as transaction_date"
    ],

    "refinery_transformation_json_prod": "/Volumes/enterprise_data/dlt_meta/transformations/transaction_enrichment.json"
  }
]
```

**Transformation JSON** (`transaction_enrichment.json`):

```json
{
  "transformation_type": "join",
  "join_type": "left",
  "join_tables": [
    {
      "table": "enterprise_data.refinery_reference.merchants",
      "alias": "m"
    }
  ],
  "join_condition": "merchant_id = m.merchant_id",
  "select_columns": [
    "transaction_id",
    "customer_id",
    "amount",
    "currency",
    "transaction_timestamp",
    "payment_method",
    "merchant_id",
    "merchant_name",
    "m.merchant_category",
    "m.merchant_country",
    "risk_score",
    "transaction_date"
  ]
}
```

---

## Best Practices

### 1. Credential Management

**✅ DO:**

```json
{
  "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
}
```

**❌ DON'T:**

```json
{
  "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"myuser\" password=\"mypassword123\";"
}
```

**Rotate Credentials Regularly:**

```bash
# Update secrets periodically
databricks secrets put --scope kafka_secrets --key username
databricks secrets put --scope kafka_secrets --key password
```

### 2. SSL Certificate Management

**Organize Certificates by Environment:**

```
/Volumes/my_catalog/shared_resources/kafka_ssl/
├── prod/
│   ├── ca-cert.pem
│   ├── truststore.jks
│   └── client-cert.pem
├── nonprod/
│   ├── ca-cert.pem
│   └── truststore.jks
└── dev/
    └── ca-cert.pem
```

**Monitor Certificate Expiration:**

```python
# Check certificate expiration
from datetime import datetime
import ssl

def check_cert_expiry(cert_path):
    cert_dict = ssl._ssl._test_decode_cert(cert_path)
    expiry_date = datetime.strptime(cert_dict['notAfter'], '%b %d %H:%M:%S %Y %Z')
    days_until_expiry = (expiry_date - datetime.now()).days
    print(f"Certificate expires in {days_until_expiry} days")
    return days_until_expiry

check_cert_expiry('/Volumes/.../kafka_ssl/ca-cert.pem')
```

### 3. Consumer Group Naming

Use descriptive, environment-specific consumer group names:

```json
{
  "landing_reader_options": {
    "kafka.group.id": "dlt-meta-prod-payment-events-v1"
  }
}
```

**Naming Convention:**
```
dlt-meta-{environment}-{topic}-{version}
```

### 4. Offset Management Strategy

**For Production:**

```json
{
  "landing_reader_options": {
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
  }
}
```

**For Initial Load / Backfill:**

```json
{
  "landing_reader_options": {
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "1000000"
  }
}
```

**For Specific Offset:**

```json
{
  "landing_reader_options": {
    "startingOffsets": "{\"my-topic\": {\"0\": 100000, \"1\": 100000}}"
  }
}
```

### 5. Schema Management

**Option A: DDL Schema File (Recommended)**

```sql
-- events.ddl
CREATE TABLE kafka_events (
  key STRING COMMENT 'Kafka message key',
  value STRING COMMENT 'Kafka message value (JSON)',
  topic STRING COMMENT 'Kafka topic name',
  partition INT COMMENT 'Kafka partition',
  offset LONG COMMENT 'Kafka offset',
  timestamp TIMESTAMP COMMENT 'Kafka message timestamp',
  timestampType INT COMMENT 'Timestamp type (0=CreateTime, 1=LogAppendTime)'
) COMMENT 'Raw Kafka messages'
```

**Option B: Schema Inference (Development Only)**

```json
{
  "source_details": {
    "subscribe": "my-topic"
  }
}
```

### 6. Error Handling and Quarantine

**Always configure quarantine tables for production:**

```json
{
  "landing_quarantine_table": "kafka_events_quarantine",
  "landing_data_quality_expectations_json_prod": "/Volumes/.../dqe.json"
}
```

**Monitor Quarantine Tables:**

```sql
SELECT
  COUNT(*) as quarantine_count,
  COUNT(DISTINCT kafka_partition) as affected_partitions,
  MIN(kafka_offset) as min_offset,
  MAX(kafka_offset) as max_offset,
  MAX(timestamp) as last_quarantine_time
FROM landing.kafka_events_quarantine
WHERE DATE(timestamp) = CURRENT_DATE()
```

### 7. Backpressure and Rate Limiting

**Control ingestion rate to prevent overwhelming downstream:**

```json
{
  "landing_reader_options": {
    "maxOffsetsPerTrigger": "100000",
    "minPartitions": "8",
    "kafka.fetch.min.bytes": "1048576",
    "kafka.fetch.max.wait.ms": "500"
  }
}
```

### 8. JSON Parsing Best Practices

**Use `get_json_object` for simple paths:**

```sql
get_json_object(value, '$.user_id') as user_id
```

**Use `from_json` for complex nested structures:**

```sql
from_json(value, schema_of_json('{"user_id": 123, "data": {...}}')) as parsed
```

**Handle malformed JSON:**

```sql
CASE
  WHEN get_json_object(value, '$') IS NOT NULL
  THEN get_json_object(value, '$.user_id')
  ELSE NULL
END as user_id
```

---

## Performance Tuning

### 1. Kafka Consumer Configuration

**Optimize Fetch Parameters:**

```json
{
  "landing_reader_options": {
    "kafka.fetch.min.bytes": "1048576",
    "kafka.fetch.max.wait.ms": "500",
    "kafka.max.partition.fetch.bytes": "10485760",
    "kafka.max.poll.records": "10000"
  }
}
```

**Parameter Guidelines:**

| Parameter | Small Messages (<1KB) | Large Messages (>10KB) | High Throughput |
|-----------|----------------------|------------------------|-----------------|
| `fetch.min.bytes` | 524288 (512KB) | 2097152 (2MB) | 1048576 (1MB) |
| `fetch.max.wait.ms` | 500 | 1000 | 500 |
| `max.partition.fetch.bytes` | 5242880 (5MB) | 52428800 (50MB) | 10485760 (10MB) |
| `max.poll.records` | 10000 | 1000 | 5000 |

### 2. Parallel Processing

**Partition Parallelism:**

Spark processes each Kafka partition in parallel. Optimize partition count:

```
Ideal Partitions = 2-3x Spark Cores
```

**Example:**
- Spark cluster: 32 cores
- Kafka topic: 64-96 partitions (optimal)

**Configure minPartitions:**

```json
{
  "landing_reader_options": {
    "minPartitions": "32"
  }
}
```

### 3. Batch Size Tuning

**Control records per micro-batch:**

```json
{
  "landing_reader_options": {
    "maxOffsetsPerTrigger": "500000"
  }
}
```

**Guidelines:**

| Use Case | maxOffsetsPerTrigger | Trigger Interval |
|----------|---------------------|------------------|
| Low Latency | 10,000-50,000 | 10-30 seconds |
| High Throughput | 100,000-1,000,000 | 1-5 minutes |
| Balanced | 100,000-500,000 | 30-60 seconds |

### 4. SSL/TLS Performance

**SSL adds overhead. Optimize with:**

```json
{
  "landing_reader_options": {
    "kafka.send.buffer.bytes": "524288",
    "kafka.receive.buffer.bytes": "1048576"
  }
}
```

### 5. Network Optimization

**For high-throughput scenarios:**

```json
{
  "source_details": {
    "kafka.socket.receive.buffer.bytes": "1048576",
    "kafka.socket.send.buffer.bytes": "524288"
  },
  "landing_reader_options": {
    "kafka.connections.max.idle.ms": "540000"
  }
}
```

### 6. Compression

**If Kafka uses compression, configure consumer:**

```json
{
  "landing_reader_options": {
    "kafka.compression.type": "snappy"
  }
}
```

**Supported compression types:**
- `none` (no compression)
- `gzip` (highest compression, more CPU)
- `snappy` (balanced)
- `lz4` (fast, low CPU)
- `zstd` (modern, efficient)

### 7. Delta Lake Optimization

**Enable Auto Optimize for streaming tables:**

```json
{
  "landing_table_properties": {
    "pipelines.autoOptimize.managed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
  }
}
```

### 8. Cluster Configuration

**Recommended DLT Pipeline Settings for Kafka:**

```yaml
# DAB configuration
resources:
  pipelines:
    kafka_streaming_pipeline:
      name: Kafka Streaming Pipeline
      catalog: enterprise_data
      schema: landing_streaming
      photon: true
      serverless: false

      configuration:
        layer: landing_refinery
        landing.dataflowspecTable: enterprise_data.dlt_meta_config.landing_dataflowspec_table
        landing.group: kafka_streaming

      cluster:
        - num_workers: 8
          node_type_id: Standard_E8ds_v4
          spark_conf:
            spark.databricks.delta.optimizeWrite.enabled: "true"
            spark.databricks.delta.autoCompact.enabled: "true"
            spark.sql.adaptive.enabled: "true"
            spark.sql.adaptive.coalescePartitions.enabled: "true"
```

---

## Troubleshooting

### Issue 1: SSL Handshake Failure

**Symptoms:**

```
org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed
javax.net.ssl.SSLHandshakeException: PKIX path building failed
```

**Causes:**
- Invalid or expired certificates
- Incorrect truststore path
- Certificate chain incomplete

**Solutions:**

1. **Verify Certificate Path:**

```python
# Check if certificate exists
dbutils.fs.ls("/Volumes/my_catalog/shared_resources/kafka_ssl/")
```

2. **Test SSL Connection:**

```bash
# From Databricks notebook
%sh
openssl s_client -connect kafka-broker:9093 -showcerts
```

3. **Validate Truststore:**

```bash
%sh
keytool -list -v -keystore /Volumes/.../kafka.truststore.jks -storepass password
```

4. **Disable Hostname Verification (Development Only):**

```json
{
  "source_details": {
    "kafka.ssl.endpoint.identification.algorithm": ""
  }
}
```

### Issue 2: SASL Authentication Failed

**Symptoms:**

```
org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed
```

**Solutions:**

1. **Verify Credentials:**

```python
# Test secret access
username = dbutils.secrets.get(scope="kafka_secrets", key="username")
password = dbutils.secrets.get(scope="kafka_secrets", key="password")
print(f"Username: {username}")
print(f"Password length: {len(password)}")
```

2. **Check JAAS Config Syntax:**

```json
// ✅ CORRECT - Note the semicolon and quotes
"kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"

// ❌ WRONG - Missing semicolon
"kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\""

// ❌ WRONG - Wrong quote escaping
"kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='pass';"
```

3. **Verify SASL Mechanism:**

```json
{
  "kafka.sasl.mechanism": "PLAIN",  // Must match Kafka broker configuration
  "kafka.security.protocol": "SASL_SSL"
}
```

### Issue 3: Connection Timeout

**Symptoms:**

```
org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata
```

**Solutions:**

1. **Check Network Connectivity:**

```bash
%sh
nc -zv kafka-broker 9093
telnet kafka-broker 9093
```

2. **Increase Timeouts:**

```json
{
  "landing_reader_options": {
    "kafka.request.timeout.ms": "120000",
    "kafka.session.timeout.ms": "90000",
    "kafka.heartbeat.interval.ms": "30000",
    "kafka.connections.max.idle.ms": "540000"
  }
}
```

3. **Verify Broker Addresses:**

```python
# Check DNS resolution
%sh
nslookup kafka-broker-1.company.com
```

### Issue 4: Offset Out of Range

**Symptoms:**

```
org.apache.kafka.clients.consumer.OffsetOutOfRangeException: Fetch position for partition my-topic-0 is out of range
```

**Solutions:**

1. **Reset to Latest Offsets:**

```json
{
  "landing_reader_options": {
    "startingOffsets": "latest"
  }
}
```

2. **Handle Data Loss Gracefully:**

```json
{
  "landing_reader_options": {
    "failOnDataLoss": "false"
  }
}
```

3. **Check Kafka Retention:**

```bash
# Query Kafka topic configuration
kafka-configs --bootstrap-server kafka-broker:9093 \
  --describe --entity-type topics --entity-name my-topic
```

### Issue 5: Slow Streaming Performance

**Symptoms:**

Pipeline processing is slower than Kafka ingestion rate, causing lag

**Solutions:**

1. **Increase Parallelism:**

```json
{
  "landing_reader_options": {
    "minPartitions": "64",
    "maxOffsetsPerTrigger": "1000000"
  }
}
```

2. **Add More Workers:**

```yaml
# Increase DLT pipeline cluster size
cluster:
  - num_workers: 16  # Increased from 8
```

3. **Enable Auto Scaling:**

```yaml
cluster:
  - autoscale:
      min_workers: 8
      max_workers: 32
```

4. **Optimize Delta Writes:**

```json
{
  "landing_table_properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
  }
}
```

### Issue 6: JSON Parsing Errors

**Symptoms:**

Quarantine table filling up with unparseable JSON

**Solutions:**

1. **Validate JSON Structure:**

```sql
-- Check for malformed JSON
SELECT
  value,
  CASE WHEN get_json_object(value, '$') IS NULL THEN 'INVALID' ELSE 'VALID' END as json_status
FROM landing.kafka_events
WHERE get_json_object(value, '$') IS NULL
LIMIT 100
```

2. **Add Rescued Data Column:**

```json
{
  "landing_reader_options": {
    "_rescued_data": "_rescued_data"
  }
}
```

3. **Use Try-Catch in Transformation:**

```sql
-- In refinery_select_exp
CASE
  WHEN get_json_object(value, '$.user_id') IS NOT NULL
  THEN get_json_object(value, '$.user_id')
  ELSE 'UNKNOWN'
END as user_id
```

### Issue 7: Certificate Expiry

**Symptoms:**

```
javax.net.ssl.SSLException: Certificate expired
```

**Solutions:**

1. **Check Certificate Expiration:**

```bash
%sh
openssl x509 -in /Volumes/.../kafka_ssl/ca-cert.pem -noout -enddate
```

2. **Renew Certificates:**

Contact your Kafka administrator for new certificates and update:

```python
# Upload new certificate
dbutils.fs.put("/Volumes/.../kafka_ssl/ca-cert-new.pem", new_cert_content, overwrite=True)
```

3. **Update Onboarding File:**

```json
{
  "kafka.ssl.ca.location": "/Volumes/.../kafka_ssl/ca-cert-new.pem"
}
```

### Issue 8: Consumer Group Lag

**Symptoms:**

Consumer group falling behind Kafka topic

**Solutions:**

1. **Monitor Consumer Lag:**

```bash
# Check lag using Kafka tools
kafka-consumer-groups --bootstrap-server kafka-broker:9093 \
  --describe --group dlt-meta-prod-events \
  --command-config client.properties
```

2. **Increase Processing Rate:**

```json
{
  "landing_reader_options": {
    "maxOffsetsPerTrigger": "2000000",
    "minPartitions": "64"
  }
}
```

3. **Scale Horizontally:**

Increase Kafka topic partitions and Spark workers proportionally

---

## Security Considerations

### 1. Credential Management

**Use Databricks Secrets for All Credentials:**

```bash
# Create dedicated scope per environment
databricks secrets create-scope --scope kafka_prod_secrets
databricks secrets create-scope --scope kafka_nonprod_secrets

# Add credentials
databricks secrets put --scope kafka_prod_secrets --key username
databricks secrets put --scope kafka_prod_secrets --key password
databricks secrets put --scope kafka_prod_secrets --key truststore_password
```

**Access Control:**

```bash
# Grant read access to specific groups
databricks secrets put-acl --scope kafka_prod_secrets \
  --principal data-engineering-team --permission READ

# Deny access to dev team
databricks secrets put-acl --scope kafka_prod_secrets \
  --principal dev-team --permission DENY
```

### 2. Certificate Security

**Store Certificates in Unity Catalog Volumes:**

```
/Volumes/
  └── enterprise_data/
      └── shared_resources/
          └── kafka_ssl/
              ├── prod/
              │   ├── ca-cert.pem (chmod 644)
              │   ├── truststore.jks (chmod 644)
              │   └── client-key.pem (chmod 600)
              └── nonprod/
                  └── ...
```

**Set Appropriate Permissions:**

```sql
-- Grant read access on volume
GRANT READ FILES ON VOLUME enterprise_data.shared_resources TO `data-engineering-team`;

-- Revoke write access
REVOKE WRITE FILES ON VOLUME enterprise_data.shared_resources FROM `dev-team`;
```

### 3. Network Security

**Use Private Link / VPC Peering:**

```
Databricks VPC (10.0.0.0/16)
        │
        │ Private Link
        │
        ▼
Kafka VPC (172.16.0.0/16)
        │
        └── Kafka Brokers (172.16.1.x)
```

**Firewall Rules:**

- Allow outbound from Databricks CIDR to Kafka brokers on port 9093
- Deny all other traffic

### 4. Kafka ACLs (Access Control Lists)

**Principle of Least Privilege:**

```bash
# Grant minimal permissions
kafka-acls --bootstrap-server kafka-broker:9093 \
  --add --allow-principal User:dlt_meta_user \
  --operation Read --topic payment-events

kafka-acls --bootstrap-server kafka-broker:9093 \
  --add --allow-principal User:dlt_meta_user \
  --operation Describe --topic payment-events

kafka-acls --bootstrap-server kafka-broker:9093 \
  --add --allow-principal User:dlt_meta_user \
  --operation Read --group dlt-meta-prod-payments
```

### 5. Audit Logging

**Enable Kafka Audit Logs:**

```properties
# Kafka broker configuration
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
audit.log.enabled=true
```

**Monitor Access in Databricks:**

```sql
-- Query DLT pipeline event logs
SELECT
  timestamp,
  event_type,
  message
FROM event_log('enterprise_data.landing_streaming.kafka_payment_events')
WHERE event_type IN ('flow_definition', 'flow_progress')
ORDER BY timestamp DESC
LIMIT 100
```

### 6. Data Encryption

**At-Rest Encryption:**

Enable Delta Lake encryption:

```json
{
  "landing_table_properties": {
    "delta.encryption.enabled": "true"
  }
}
```

**In-Transit Encryption:**

Always use SSL/TLS:

```json
{
  "kafka.security.protocol": "SASL_SSL"  // ✅
}
```

Never use:

```json
{
  "kafka.security.protocol": "PLAINTEXT"  // ❌ Unencrypted
}
```

### 7. Sensitive Data Masking

**Mask PII in Landing Layer:**

```json
{
  "refinery_select_exp": [
    "user_id",
    "CONCAT(SUBSTR(email, 1, 3), '***@', SPLIT(email, '@')[1]) as email_masked",
    "CONCAT('***-***-', SUBSTR(phone, -4)) as phone_masked",
    "get_json_object(value, '$.non_pii_data') as data"
  ]
}
```

### 8. Secrets Rotation

**Implement Regular Rotation:**

```bash
#!/bin/bash
# rotate_kafka_secrets.sh

# Rotate every 90 days
NEW_PASSWORD=$(generate_secure_password)

# Update Kafka user password
kafka-configs --bootstrap-server kafka-broker:9093 \
  --alter --entity-type users --entity-name dlt_meta_user \
  --add-config "SCRAM-SHA-256=[password=$NEW_PASSWORD]"

# Update Databricks secret
databricks secrets put --scope kafka_secrets --key password <<< "$NEW_PASSWORD"

echo "Password rotated successfully"
```

### 9. Monitoring and Alerting

**Set Up Alerts for Security Events:**

```sql
-- Alert on authentication failures
CREATE OR REPLACE ALERT kafka_auth_failures
SCHEDULE CRON "*/15 * * * *"
AS
SELECT COUNT(*) as failure_count
FROM event_log('kafka_payment_events')
WHERE event_type = 'flow_definition'
  AND message LIKE '%Authentication failed%'
  AND timestamp >= current_timestamp() - INTERVAL 15 MINUTES
HAVING failure_count > 0
```

### 10. Compliance

**GDPR/CCPA Considerations:**

- Store Kafka offsets to support right-to-erasure
- Implement data lineage tracking
- Enable Change Data Feed for audit trails

```json
{
  "landing_table_properties": {
    "delta.enableChangeDataFeed": "true"
  }
}
```

---

## Additional Resources

### Internal Documentation

- [DLT-META Onboarding File Reference](ONBOARDING_FILE_REFERENCE.md)
- [DLT-META Quick Reference](ONBOARDING_QUICK_REFERENCE.md)
- [Streaming Best Practices](STREAMING_BEST_PRACTICES.md)
- [Data Quality Expectations Guide](DATA_QUALITY_GUIDE.md)

### External Documentation

- [Apache Kafka Security](https://kafka.apache.org/documentation/#security)
- [Databricks Structured Streaming + Kafka](https://docs.databricks.com/structured-streaming/kafka.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)

### Kafka Tools

- [Kafka CLI Tools](https://kafka.apache.org/documentation/#tools)
- [kcat (kafkacat)](https://github.com/edenhill/kcat) - Kafka debugging tool
- [Conduktor](https://www.conduktor.io/) - Kafka GUI

### Support

- **Data Engineering Team:** [Jira Service Desk](https://jira.company.com/servicedesk/dlt-meta)
- **Confluence:** [DLT-META Kafka Integration](https://confluence.company.com/display/DE/Kafka+Integration)

---

## Appendix

### A. Kafka Message Format

**Standard Kafka Message Structure:**

```
┌────────────────────────────────────┐
│ Kafka Message                      │
├────────────────────────────────────┤
│ key: STRING (optional)             │
│ value: BINARY (typically JSON)     │
│ topic: STRING                      │
│ partition: INT                     │
│ offset: LONG                       │
│ timestamp: TIMESTAMP               │
│ timestampType: INT                 │
│ headers: MAP<STRING, BINARY>       │
└────────────────────────────────────┘
```

### B. Common Kafka Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 9092 | PLAINTEXT | Unencrypted (dev only) |
| 9093 | SSL / SASL_SSL | Encrypted with authentication |
| 9094 | SASL_PLAINTEXT | SASL without encryption |
| 2181 | Zookeeper | Older Kafka versions |

### C. SSL/TLS Handshake Process

```
Client (Databricks)                Kafka Broker
        │                               │
        │─────── ClientHello ──────────▶│
        │                               │
        │◀────── ServerHello ───────────│
        │◀────── Certificate ───────────│
        │◀────── ServerHelloDone ───────│
        │                               │
        │─────── ClientKeyExchange ────▶│
        │─────── ChangeCipherSpec ─────▶│
        │─────── Finished ──────────────▶│
        │                               │
        │◀────── ChangeCipherSpec ──────│
        │◀────── Finished ───────────────│
        │                               │
        │═══════ Encrypted Data ════════│
```

### D. Sample Testing Notebook

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka SSL Connection Test

# COMMAND ----------

# Configuration
kafka_brokers = "kafka-broker-1:9093,kafka-broker-2:9093"
topic = "test-topic"
username = dbutils.secrets.get(scope="kafka_secrets", key="username")
password = dbutils.secrets.get(scope="kafka_secrets", key="password")

# COMMAND ----------

# Test 1: Basic connectivity
kafka_options = {
    "kafka.bootstrap.servers": kafka_brokers,
    "subscribe": topic,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";',
    "kafka.ssl.ca.location": "/Volumes/.../kafka_ssl/ca-cert.pem",
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

try:
    df = spark.readStream.format("kafka").options(**kafka_options).load()
    print("✅ Kafka connection successful!")
    display(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp"))
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")

# COMMAND ----------

# Test 2: Read last 10 messages
kafka_batch_options = kafka_options.copy()
kafka_batch_options["startingOffsets"] = "earliest"
kafka_batch_options["endingOffsets"] = "latest"

df_batch = spark.read.format("kafka").options(**kafka_batch_options).load()
print(f"Total messages in topic: {df_batch.count()}")
display(df_batch.selectExpr("CAST(value AS STRING)").limit(10))

# COMMAND ----------

# Test 3: Parse JSON values
from pyspark.sql.functions import get_json_object

df_parsed = df_batch.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("user_id", get_json_object("json_value", "$.user_id")) \
    .withColumn("event_type", get_json_object("json_value", "$.event_type"))

display(df_parsed.limit(10))
```

### E. Environment-Specific Configuration Template

```json
{
  "environments": {
    "prod": {
      "kafka.bootstrap.servers": "kafka-prod-1:9093,kafka-prod-2:9093,kafka-prod-3:9093",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username_prod}}\" password=\"{{secrets/kafka_secrets/password_prod}}\";",
      "kafka.ssl.ca.location": "/Volumes/enterprise_data/kafka_ssl/prod/ca-cert.pem",
      "landing_catalog": "enterprise_data",
      "landing_database": "landing_prod"
    },
    "nonprod": {
      "kafka.bootstrap.servers": "kafka-nonprod:9093",
      "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username_nonprod}}\" password=\"{{secrets/kafka_secrets/password_nonprod}}\";",
      "kafka.ssl.ca.location": "/Volumes/dev_data/kafka_ssl/nonprod/ca-cert.pem",
      "landing_catalog": "dev_data",
      "landing_database": "landing_nonprod"
    },
    "dev": {
      "kafka.bootstrap.servers": "kafka-dev:9092",
      "kafka.security.protocol": "PLAINTEXT",
      "landing_catalog": "dev_data",
      "landing_database": "landing_dev"
    }
  }
}
```

---

**Document Version History:**

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-26 | Initial Kafka with SSL ingestion onboarding guide created |
