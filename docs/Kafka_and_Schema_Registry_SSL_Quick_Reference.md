# Kafka + Schema Registry SSL Configuration - Quick Reference

**When Both Use SSL Certificates for Authentication**

---

## 🔐 Certificate Requirements

| Component | Certificates Needed |
|-----------|-------------------|
| **Kafka Broker** | • CA cert (server trust)<br>• Client cert (optional mTLS)<br>• Private key (optional mTLS) |
| **Schema Registry** | • CA cert (server trust)<br>• Client cert (client authentication)<br>• Private key (client authentication) |

---

## 📁 File Structure

```
/Volumes/dataservices_nonprod/landing_nonprod/shared_resources/

├── kafka_ssl/                           # Kafka certificates
│   ├── ca-cert.pem                      # Kafka broker CA
│   ├── kafka.truststore.jks             # (if using JKS)
│   ├── client-cert.pem                  # (optional for mTLS)
│   └── client-key.pem                   # (optional for mTLS)
│
└── schema_registry_ssl/                 # Schema Registry certificates
    ├── schema-registry.truststore.jks   # Schema Registry CA (server trust)
    └── schema-registry.keystore.jks     # Client cert + key (authentication)
```

---

## 🔑 Secrets Setup

```bash
# Kafka secrets
databricks secrets create-scope --scope kafka_secrets
databricks secrets put --scope kafka_secrets --key username
databricks secrets put --scope kafka_secrets --key password

# Schema Registry secrets
databricks secrets create-scope --scope schema_registry
databricks secrets put --scope schema_registry --key truststore_password
databricks secrets put --scope schema_registry --key keystore_password
databricks secrets put --scope schema_registry --key key_password
```

---

## 📋 Complete Onboarding File

```json
{
  "data_flow_id": "3001",
  "data_flow_group": "streaming_events",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "payment-events",

    "kafka.bootstrap.servers": "kafka-1:9093,kafka-2:9093",
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

---

## 🔄 Transformation File with Schema Registry SSL

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

---

## 🔍 Side-by-Side Comparison

### Kafka Configuration (in source_details)

```json
{
  "kafka.bootstrap.servers": "kafka-1:9093",
  "kafka.security.protocol": "SASL_SSL",
  "kafka.sasl.mechanism": "SCRAM-SHA-256",
  "kafka.sasl.jaas.config": "...username/password...",
  "kafka.ssl.ca.location": "/Volumes/.../kafka_ssl/ca-cert.pem"
}
```

**Authentication:**
- SASL (username/password) for authentication
- SSL (CA cert) for encryption

---

### Schema Registry Configuration (in from_avro map)

```yaml
map(
  'url', 'https://schema-registry:8081',
  'ssl.truststore.location', '/Volumes/.../schema-registry.truststore.jks',
  'ssl.truststore.password', '{{secrets/schema_registry/truststore_password}}',
  'ssl.keystore.location', '/Volumes/.../schema-registry.keystore.jks',
  'ssl.keystore.password', '{{secrets/schema_registry/keystore_password}}',
  'ssl.key.password', '{{secrets/schema_registry/key_password}}'
)
```

**Authentication:**
- Client certificate (in keystore) for authentication
- SSL (truststore with CA) for encryption

---

## 🎯 Key Differences

| Aspect | Kafka | Schema Registry |
|--------|-------|-----------------|
| **Authentication** | SASL (username/password) | Client certificate (mTLS) |
| **Encryption** | SSL with CA cert | SSL with CA cert |
| **Configuration Location** | `source_details` in onboarding file | `from_avro()` map in transformation |
| **Certificate Format** | PEM files | JKS keystores (or PEM if supported) |
| **Required Files** | 1 file (CA cert) | 2 files (truststore + keystore) |

---

## ✅ Pre-Flight Checklist

Before running your DLT pipeline:

- [ ] **Kafka certificates uploaded** to `/Volumes/.../kafka_ssl/`
- [ ] **Schema Registry certificates uploaded** to `/Volumes/.../schema_registry_ssl/`
- [ ] **Kafka secrets created** in `kafka_secrets` scope
- [ ] **Schema Registry secrets created** in `schema_registry` scope
- [ ] **Onboarding file configured** with correct paths
- [ ] **Transformation file created** with from_avro SSL options
- [ ] **Subject name verified** in Schema Registry (e.g., `payment-events-value`)
- [ ] **Network connectivity tested** to both Kafka and Schema Registry

---

## 🧪 Testing Commands

### Test Kafka Connection

```python
# Test Kafka read (without deserialization)
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-1:9093") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
  .option("kafka.sasl.jaas.config", "...") \
  .option("kafka.ssl.ca.location", "/Volumes/.../kafka_ssl/ca-cert.pem") \
  .option("subscribe", "payment-events") \
  .load()

display(kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").limit(5))
```

### Test Schema Registry Connection

```bash
# Using curl with client certificate
curl -X GET https://schema-registry:8081/subjects \
  --cacert /path/to/ca-cert.pem \
  --cert /path/to/client-cert.pem \
  --key /path/to/client-key.pem
```

### Test End-to-End (Kafka + Schema Registry)

```python
# Read from Kafka and deserialize with Schema Registry
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-1:9093") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
  .option("kafka.sasl.jaas.config", f'...username/password...') \
  .option("kafka.ssl.ca.location", "/Volumes/.../kafka_ssl/ca-cert.pem") \
  .option("subscribe", "payment-events") \
  .load()

# Deserialize using Schema Registry
decoded_df = kafka_df.selectExpr(
  "topic",
  f"""
  from_avro(
    value,
    'payment-events-value',
    map(
      'url', 'https://schema-registry:8081',
      'ssl.truststore.location', '/Volumes/.../schema-registry.truststore.jks',
      'ssl.truststore.password', '{dbutils.secrets.get("schema_registry", "truststore_password")}',
      'ssl.keystore.location', '/Volumes/.../schema-registry.keystore.jks',
      'ssl.keystore.password', '{dbutils.secrets.get("schema_registry", "keystore_password")}',
      'ssl.key.password', '{dbutils.secrets.get("schema_registry", "key_password")}',
      'mode', 'PERMISSIVE'
    )
  ) as payment
  """
)

display(decoded_df.select("payment.*").limit(5))
```

---

## 🚨 Common Issues

### Issue 1: Kafka connects, but Schema Registry fails

**Symptom:** Can read raw messages from Kafka, but `from_avro()` throws SSL error

**Cause:** Schema Registry certificates not configured or incorrect

**Solution:** Verify Schema Registry SSL options in `from_avro()` map

---

### Issue 2: "Schema not found" error

**Symptom:** SSL connection works, but schema subject not found

**Cause:** Incorrect subject name or schema not registered

**Solution:**
```bash
# List all subjects
curl -X GET https://schema-registry:8081/subjects \
  --cert client-cert.pem --key client-key.pem --cacert ca-cert.pem

# Verify your topic name convention:
# Topic: payment-events → Subject: payment-events-value
```

---

### Issue 3: Secrets not interpolating

**Symptom:** Literal string `{{secrets/...}}` appears in error message

**Cause:** DLT-META not interpolating secrets correctly

**Solution:** Ensure you're using the exact format: `{{secrets/scope_name/key_name}}`

---

## 📚 Related Documentation

- **[Schema Registry SSL Setup Guide](Schema_Registry_SSL_Certificate_Setup.md)** - Detailed SSL certificate configuration
- **[Kafka SSL Ingestion Guide](Kafka_SSL_Ingestion_Onboarding_Guide.md)** - Complete Kafka SSL configuration
- **[Schema Registry Integration Guide](Schema_Registry_Integration_Guide.md)** - Full Schema Registry documentation
- **[Schema Registry Links](Schema_Registry_Links_and_References.md)** - Official documentation links

---

**Quick Access:**
- Kafka SSL Guide: `/docs/Kafka_SSL_Ingestion_Onboarding_Guide.md`
- Schema Registry SSL: `/docs/Schema_Registry_SSL_Certificate_Setup.md`
- Sample Files: `/cds/conf/onboarding/kafka_ng/`

---

**Version:** 1.0 | **Last Updated:** February 26, 2026
