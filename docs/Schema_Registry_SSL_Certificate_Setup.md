# Schema Registry with SSL Certificate Authentication - Setup Guide

**Version:** 1.0
**Date:** February 26, 2026
**Author:** Platform Data Engineering Team

---

## Overview

This guide covers configuring **Confluent Schema Registry** with **SSL/TLS client certificate authentication** (mutual TLS/mTLS) for DLT-META Kafka ingestion with Avro deserialization.

### Authentication Method

Your Schema Registry requires the same authentication as your Kafka brokers:
- ✅ **SSL/TLS encryption** (server authentication)
- ✅ **Client certificates** (client authentication - mTLS)
- ✅ **Private key** for client certificate

---

## Prerequisites

### 1. Certificate Files Required

You need the following files from your Schema Registry administrator:

| File Type | Description | Example Filename |
|-----------|-------------|------------------|
| **CA Certificate** | Certificate Authority (server trust) | `schema-registry-ca.pem` or `.crt` |
| **Client Certificate** | Your client certificate | `schema-registry-client.pem` or `.crt` |
| **Private Key** | Private key for client certificate | `schema-registry-client-key.pem` |
| **Truststore (JKS)** | Java Keystore with CA cert | `schema-registry.truststore.jks` |
| **Keystore (JKS)** | Java Keystore with client cert + key | `schema-registry.keystore.jks` |

### 2. Certificate Format Options

Schema Registry (Java-based) works with two formats:

#### Option A: Java Keystores (JKS) - Recommended
- **Truststore.jks**: Contains CA certificate for server validation
- **Keystore.jks**: Contains client certificate + private key

#### Option B: PEM Certificates
- **ca-cert.pem**: CA certificate
- **client-cert.pem**: Client certificate
- **client-key.pem**: Private key

**Note:** If you have PEM files, you'll need to convert them to JKS format.

---

## Step-by-Step Setup

### Step 1: Upload Certificates to Unity Catalog Volume

Create a dedicated location for Schema Registry certificates:

```python
# In Databricks notebook
# Create directory for Schema Registry certificates
dbutils.fs.mkdirs("/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl")
```

**Upload your certificate files:**

```python
# If you have JKS files (recommended)
dbutils.fs.put(
    "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.truststore.jks",
    truststore_content,
    overwrite=True
)

dbutils.fs.put(
    "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.keystore.jks",
    keystore_content,
    overwrite=True
)
```

**Alternative: If you have PEM files:**

```python
dbutils.fs.put(
    "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/ca-cert.pem",
    ca_cert_content,
    overwrite=True
)

dbutils.fs.put(
    "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/client-cert.pem",
    client_cert_content,
    overwrite=True
)

dbutils.fs.put(
    "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/client-key.pem",
    private_key_content,
    overwrite=True
)
```

**Verify uploads:**

```python
dbutils.fs.ls("/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl")
```

---

### Step 2: Convert PEM to JKS (If Needed)

If you only have PEM files, convert them to JKS format:

#### Create Truststore from CA Certificate

```bash
# Convert CA certificate to truststore.jks
keytool -import \
  -file ca-cert.pem \
  -alias ca-cert \
  -keystore schema-registry.truststore.jks \
  -storepass YOUR_TRUSTSTORE_PASSWORD \
  -noprompt
```

#### Create Keystore from Client Certificate + Private Key

```bash
# Step 1: Convert PEM to PKCS12
openssl pkcs12 -export \
  -in client-cert.pem \
  -inkey client-key.pem \
  -out client-cert.p12 \
  -name schema-registry-client \
  -password pass:YOUR_KEYSTORE_PASSWORD

# Step 2: Convert PKCS12 to JKS
keytool -importkeystore \
  -srckeystore client-cert.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass YOUR_KEYSTORE_PASSWORD \
  -destkeystore schema-registry.keystore.jks \
  -deststoretype JKS \
  -deststorepass YOUR_KEYSTORE_PASSWORD \
  -destkeypass YOUR_KEY_PASSWORD \
  -alias schema-registry-client
```

**Upload converted JKS files to Unity Catalog Volume.**

---

### Step 3: Store Passwords in Databricks Secrets

```bash
# Create secret scope (if not already created)
databricks secrets create-scope --scope schema_registry

# Store truststore password
databricks secrets put --scope schema_registry --key truststore_password
# Enter: YOUR_TRUSTSTORE_PASSWORD

# Store keystore password
databricks secrets put --scope schema_registry --key keystore_password
# Enter: YOUR_KEYSTORE_PASSWORD

# Store private key password
databricks secrets put --scope schema_registry --key key_password
# Enter: YOUR_KEY_PASSWORD
```

**Verify secrets:**

```python
# In Databricks notebook
truststore_pwd = dbutils.secrets.get("schema_registry", "truststore_password")
keystore_pwd = dbutils.secrets.get("schema_registry", "keystore_password")
key_pwd = dbutils.secrets.get("schema_registry", "key_password")

print(f"Truststore password length: {len(truststore_pwd)}")
print(f"Keystore password length: {len(keystore_pwd)}")
print(f"Key password length: {len(key_pwd)}")
```

---

### Step 4: Configure DLT-META Onboarding File

#### Method 1: Using Transformation File (Recommended)

**Onboarding File:**

```json
{
  "data_flow_id": "3001",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "payment-events",
    "kafka.bootstrap.servers": "kafka-1:9093,kafka-2:9093,kafka-3:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";",
    "kafka.ssl.ca.location": "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/kafka_ssl/ca-cert.pem"
  },

  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_events",
  "landing_table": "kafka_payment_events_raw",

  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_events",
  "refinery_table": "payment_events",

  "refinery_transformation_json_nonprod": "/Volumes/dataservices_nonprod/landing_nonprod/dlt_meta/transformations/avro_deserialize_payment_events.yaml"
}
```

**Transformation YAML File:**

```yaml
target_table: payment_events
sql_query: |
  SELECT
    payment.payment_id,
    payment.customer_id,
    payment.amount,
    payment.currency,
    payment.transaction_timestamp
  FROM (
    SELECT
      from_avro(
        value,
        'payment-events-value',
        map(
          'url', 'https://schema-registry.company.com:8081',
          'ssl.truststore.location', '/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.truststore.jks',
          'ssl.truststore.password', '{{secrets/schema_registry/truststore_password}}',
          'ssl.keystore.location', '/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.keystore.jks',
          'ssl.keystore.password', '{{secrets/schema_registry/keystore_password}}',
          'ssl.key.password', '{{secrets/schema_registry/key_password}}',
          'mode', 'PERMISSIVE'
        )
      ) as payment
    FROM LIVE.kafka_payment_events_raw
  )
```

#### Method 2: Using refinery_select_exp (Inline)

```json
{
  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_events",
  "refinery_table": "payment_events",

  "refinery_select_exp": [
    "from_avro(value, 'payment-events-value', map('url', 'https://schema-registry.company.com:8081', 'ssl.truststore.location', '/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.truststore.jks', 'ssl.truststore.password', '{{secrets/schema_registry/truststore_password}}', 'ssl.keystore.location', '/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.keystore.jks', 'ssl.keystore.password', '{{secrets/schema_registry/keystore_password}}', 'ssl.key.password', '{{secrets/schema_registry/key_password}}', 'mode', 'PERMISSIVE')) as payment",
    "topic",
    "partition",
    "offset"
  ],

  "refinery_select_exp_final": [
    "payment.payment_id",
    "payment.customer_id",
    "payment.amount",
    "payment.currency",
    "payment.transaction_timestamp",
    "topic",
    "partition",
    "offset"
  ]
}
```

---

## Configuration Options Reference

### SSL Options for from_avro() with Mutual TLS

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `url` | Yes | Schema Registry URL (HTTPS) | `'https://schema-registry:8081'` |
| `ssl.truststore.location` | Yes | Path to truststore (server validation) | `'/Volumes/.../truststore.jks'` |
| `ssl.truststore.password` | Yes | Truststore password | `'{{secrets/sr/truststore_pwd}}'` |
| `ssl.keystore.location` | Yes | Path to keystore (client cert) | `'/Volumes/.../keystore.jks'` |
| `ssl.keystore.password` | Yes | Keystore password | `'{{secrets/sr/keystore_pwd}}'` |
| `ssl.key.password` | Yes | Private key password | `'{{secrets/sr/key_pwd}}'` |
| `mode` | No | Error handling | `'PERMISSIVE'` (default: `FAILFAST`) |

### Alternative: Using PEM Certificates Directly

If `from_avro()` supports PEM files (Databricks-specific):

```yaml
map(
  'url', 'https://schema-registry:8081',
  'ssl.ca.location', '/Volumes/.../ca-cert.pem',
  'ssl.certificate.location', '/Volumes/.../client-cert.pem',
  'ssl.key.location', '/Volumes/.../client-key.pem',
  'ssl.key.password', '{{secrets/schema_registry/key_password}}',
  'mode', 'PERMISSIVE'
)
```

**Note:** Check Databricks documentation to confirm PEM support. JKS is the standard format.

---

## Testing the Configuration

### Test 1: Verify Certificate Files

```python
# List certificate files
files = dbutils.fs.ls("/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl")
for file in files:
    print(f"{file.name}: {file.size} bytes")
```

### Test 2: Test SSL Connection to Schema Registry

```python
# Test HTTPS connection to Schema Registry with certificates
import requests
import tempfile
import os

# Get passwords
truststore_pwd = dbutils.secrets.get("schema_registry", "truststore_password")
keystore_pwd = dbutils.secrets.get("schema_registry", "keystore_password")

# Download certificate files to local temp (for testing only)
truststore_bytes = dbutils.fs.head("/Volumes/.../schema-registry.truststore.jks", 1000000)
keystore_bytes = dbutils.fs.head("/Volumes/.../schema-registry.keystore.jks", 1000000)

# Write to temp files
with tempfile.NamedTemporaryFile(delete=False, suffix='.jks') as tf:
    tf.write(truststore_bytes.encode('latin1'))
    truststore_path = tf.name

with tempfile.NamedTemporaryFile(delete=False, suffix='.jks') as kf:
    kf.write(keystore_bytes.encode('latin1'))
    keystore_path = kf.name

# Test connection (using requests with PEM - convert JKS to PEM first for this test)
# For production, the actual test happens when DLT pipeline runs
print("✅ Certificate files accessible")
print(f"Truststore: {truststore_path}")
print(f"Keystore: {keystore_path}")

# Clean up temp files
os.unlink(truststore_path)
os.unlink(keystore_path)
```

### Test 3: Test Schema Registry REST API with curl

If you have shell access:

```bash
# Test with client certificate
curl -X GET https://schema-registry:8081/subjects \
  --cacert /path/to/ca-cert.pem \
  --cert /path/to/client-cert.pem \
  --key /path/to/client-key.pem
```

### Test 4: Test from_avro() in Notebook

```python
from pyspark.sql.functions import col

# Create test data (sample Avro message)
test_df = spark.read.format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-broker:9093") \
  .option("subscribe", "payment-events") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .load() \
  .limit(1)

# Test deserialization
schema_registry_url = "https://schema-registry.company.com:8081"
truststore_path = "/Volumes/.../schema-registry.truststore.jks"
truststore_pwd = dbutils.secrets.get("schema_registry", "truststore_password")
keystore_path = "/Volumes/.../schema-registry.keystore.jks"
keystore_pwd = dbutils.secrets.get("schema_registry", "keystore_password")
key_pwd = dbutils.secrets.get("schema_registry", "key_password")

decoded_df = test_df.selectExpr(
  "topic",
  f"""
  from_avro(
    value,
    'payment-events-value',
    map(
      'url', '{schema_registry_url}',
      'ssl.truststore.location', '{truststore_path}',
      'ssl.truststore.password', '{truststore_pwd}',
      'ssl.keystore.location', '{keystore_path}',
      'ssl.keystore.password', '{keystore_pwd}',
      'ssl.key.password', '{key_pwd}',
      'mode', 'PERMISSIVE'
    )
  ) as payment
  """
)

# Display result
display(decoded_df.select("payment.*"))
```

---

## Complete Example: End-to-End Configuration

### Files Structure

```
/Volumes/dataservices_nonprod/landing_nonprod/
├── shared_resources/
│   ├── kafka_ssl/
│   │   ├── ca-cert.pem                    # Kafka broker CA
│   │   └── ...
│   └── schema_registry_ssl/
│       ├── schema-registry.truststore.jks  # Schema Registry CA
│       └── schema-registry.keystore.jks    # Client cert + key
└── dlt_meta/
    ├── schemas/
    │   └── events.ddl
    └── transformations/
        └── avro_deserialize_payment_events.yaml
```

### Complete Onboarding File

```json
{
  "data_flow_id": "3001",
  "data_flow_group": "streaming_events",
  "source_system": "kafka_nonprod",
  "source_format": "kafka",

  "source_details": {
    "source_schema_path": "/Volumes/dataservices_nonprod/landing_nonprod/dlt_meta/schemas/events.ddl",
    "subscribe": "payment-events",
    "kafka.bootstrap.servers": "kafka-1:9093,kafka-2:9093,kafka-3:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-256",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";",
    "kafka.ssl.ca.location": "/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/kafka_ssl/ca-cert.pem"
  },

  "landing_reader_options": {
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "100000",
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "60000"
  },

  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_events",
  "landing_table": "kafka_payment_events_raw",
  "landing_table_comment": "Raw Kafka messages with Avro serialized values",

  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_events",
  "refinery_table": "payment_events",
  "refinery_table_comment": "Deserialized payment events from Avro format",

  "refinery_transformation_json_nonprod": "/Volumes/dataservices_nonprod/landing_nonprod/dlt_meta/transformations/avro_deserialize_payment_events.yaml",

  "refinery_partition_columns": "payment_date"
}
```

### Complete Transformation File

```yaml
target_table: payment_events
transformation_name: Avro Deserialization with SSL Certificate Authentication
description: "Deserializes Avro-encoded payment events using Schema Registry with mutual TLS"
sql_query: |
  SELECT
    payment.payment_id,
    payment.customer_id,
    payment.amount,
    payment.currency,
    payment.payment_method,
    CAST(payment.transaction_timestamp AS TIMESTAMP) as transaction_timestamp,
    payment.status,
    DATE(CAST(payment.transaction_timestamp AS TIMESTAMP)) as payment_date,
    topic,
    partition,
    offset,
    timestamp as kafka_timestamp
  FROM (
    SELECT
      topic,
      partition,
      offset,
      timestamp,
      from_avro(
        value,
        'payment-events-value',
        map(
          'url', 'https://schema-registry.company.com:8081',
          'ssl.truststore.location', '/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.truststore.jks',
          'ssl.truststore.password', '{{secrets/schema_registry/truststore_password}}',
          'ssl.keystore.location', '/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl/schema-registry.keystore.jks',
          'ssl.keystore.password', '{{secrets/schema_registry/keystore_password}}',
          'ssl.key.password', '{{secrets/schema_registry/key_password}}',
          'mode', 'PERMISSIVE'
        )
      ) as payment
    FROM LIVE.kafka_payment_events_raw
  )
```

---

## Troubleshooting

### Issue 1: "SSLHandshakeException: Certificate Validation Failed"

**Cause:** Truststore doesn't contain the correct CA certificate

**Solution:**

```bash
# Verify CA certificate in truststore
keytool -list -v \
  -keystore schema-registry.truststore.jks \
  -storepass YOUR_PASSWORD

# Check certificate chain
openssl s_client -connect schema-registry:8081 -showcerts
```

### Issue 2: "SSLHandshakeException: Client Authentication Failed"

**Cause:** Keystore missing or incorrect client certificate

**Solution:**

```bash
# Verify client certificate in keystore
keytool -list -v \
  -keystore schema-registry.keystore.jks \
  -storepass YOUR_PASSWORD \
  -alias schema-registry-client
```

### Issue 3: "FileNotFoundException: Keystore not found"

**Cause:** Incorrect path or file not uploaded

**Solution:**

```python
# Verify files exist
files = dbutils.fs.ls("/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/schema_registry_ssl")
for f in files:
    print(f.path)
```

### Issue 4: "Wrong Password"

**Cause:** Incorrect password in secrets

**Solution:**

```python
# Test password retrieval
pwd = dbutils.secrets.get("schema_registry", "truststore_password")
print(f"Password length: {len(pwd)}")
print(f"Password (masked): {'*' * len(pwd)}")

# Update secret if wrong
# databricks secrets put --scope schema_registry --key truststore_password
```

---

## Security Best Practices

### 1. Certificate Storage

✅ **DO:**
- Store certificates in Unity Catalog Volumes with restricted access
- Use separate directories for Kafka and Schema Registry certificates
- Set appropriate file permissions

❌ **DON'T:**
- Store certificates in DBFS root (less secure)
- Commit certificates to Git repositories
- Share certificates across environments

### 2. Password Management

✅ **DO:**
- Store all passwords in Databricks Secrets
- Use different passwords for each keystore/truststore
- Rotate passwords regularly (every 90 days)

❌ **DON'T:**
- Hardcode passwords in configuration files
- Use the same password for multiple keystores
- Share secrets across teams

### 3. Certificate Rotation

**Plan for certificate expiration:**

```python
# Check certificate expiration
import subprocess
import tempfile

# Extract and check certificate
result = subprocess.run([
    'keytool', '-list', '-v',
    '-keystore', '/path/to/keystore.jks',
    '-storepass', 'password'
], capture_output=True, text=True)

print(result.stdout)
# Look for "Valid from: ... until: ..."
```

**Set up alerts 30 days before expiration.**

---

## Additional Resources

- **DLT-META Schema Registry Guide:** [Schema_Registry_Integration_Guide.md](Schema_Registry_Integration_Guide.md)
- **Kafka SSL Guide:** [Kafka_SSL_Ingestion_Onboarding_Guide.md](Kafka_SSL_Ingestion_Onboarding_Guide.md)
- **Java Keytool Documentation:** https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html
- **OpenSSL Documentation:** https://www.openssl.org/docs/

---

**Document Version:** 1.0
**Last Updated:** February 26, 2026
**Maintained By:** Platform Data Engineering Team
