# Schema Registry Quick Reference

## 🚀 Quick Start

### 1. Store Schema Registry Credentials

```bash
databricks secrets create-scope --scope schema_registry
databricks secrets put --scope schema_registry --key api_key
databricks secrets put --scope schema_registry --key api_secret
```

### 2. Basic Avro Deserialization in Onboarding File

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'your-topic-value', map('url', 'https://schema-registry:8081', 'basic.auth.user.info', '{{secrets/schema_registry/api_key}}:{{secrets/schema_registry/api_secret}}')) as event"
  ],
  "refinery_select_exp_final": [
    "event.field1",
    "event.field2",
    "event.field3"
  ]
}
```

---

## 📝 Common Configuration Patterns

### Pattern 1: Confluent Cloud

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'topic-name-value', map('url', 'https://psrc-xxxxx.us-east-1.aws.confluent.cloud', 'basic.auth.credentials.source', 'USER_INFO', 'basic.auth.user.info', '{{secrets/confluent/schema_registry_key}}:{{secrets/confluent/schema_registry_secret}}')) as data"
  ]
}
```

### Pattern 2: Self-Hosted with Basic Auth

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'events-value', map('url', 'https://schema-registry.company.com:8081', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}')) as event"
  ]
}
```

### Pattern 3: No Authentication (Dev Only)

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'events-value', map('url', 'http://localhost:8081')) as event"
  ]
}
```

### Pattern 4: Key + Value Deserialization

```json
{
  "refinery_select_exp": [
    "from_avro(key, 'topic-key', map('url', 'https://schema-registry:8081')) as key_data",
    "from_avro(value, 'topic-value', map('url', 'https://schema-registry:8081')) as value_data"
  ]
}
```

---

## 🔧 Configuration Options

### from_avro() Function Syntax

```sql
from_avro(
  column,                    -- Binary column (key or value)
  subject_name,              -- Schema Registry subject
  options_map                -- Configuration map
)
```

### Options Map Keys

| Key | Required | Description | Example |
|-----|----------|-------------|---------|
| `url` | Yes | Schema Registry URL | `https://schema-registry:8081` |
| `basic.auth.credentials.source` | No | Auth source | `USER_INFO` |
| `basic.auth.user.info` | No | Credentials | `{{secrets/sr/key}}:{{secrets/sr/secret}}` |
| `mode` | No | Error handling | `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST` |

---

## 🎯 Subject Naming Conventions

### TopicNameStrategy (Default)

```
Topic: payment-events
Subject (value): payment-events-value
Subject (key): payment-events-key
```

### RecordNameStrategy

```
Avro namespace: com.company
Avro name: Payment
Subject: com.company.Payment
```

### TopicRecordNameStrategy

```
Topic: payment-events
Avro: com.company.Payment
Subject: payment-events-com.company.Payment
```

---

## 🐛 Testing Schema Registry Connection

### Test 1: List All Subjects

```bash
curl -X GET https://schema-registry:8081/subjects \
  -u $API_KEY:$API_SECRET
```

### Test 2: Get Schema for Subject

```bash
curl -X GET https://schema-registry:8081/subjects/payment-events-value/versions/latest \
  -u $API_KEY:$API_SECRET
```

### Test 3: Get Schema by ID

```bash
curl -X GET https://schema-registry:8081/schemas/ids/123 \
  -u $API_KEY:$API_SECRET
```

### Test 4: Databricks Notebook Test

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

if response.status_code == 200:
    print("✅ Schema Registry connection successful!")
    print(f"Available subjects: {response.json()}")
else:
    print(f"❌ Connection failed: {response.status_code}")
    print(response.text)
```

---

## 🔍 Troubleshooting Quick Fixes

### Error: "Schema not found"

```bash
# Check if subject exists
curl -X GET https://schema-registry:8081/subjects | grep "your-topic"

# List all versions
curl -X GET https://schema-registry:8081/subjects/your-topic-value/versions
```

**Fix:** Ensure subject name matches exactly (case-sensitive)

### Error: "Unauthorized (401)"

```python
# Verify credentials format
creds = dbutils.secrets.get('schema_registry', 'credentials')
print(f"Credentials length: {len(creds)}")
print(f"Contains colon: {':' in creds}")
```

**Fix:** Format must be `API_KEY:API_SECRET` (colon-separated, no spaces)

### Error: "Connection timeout"

```bash
# Test network connectivity
nc -zv schema-registry-hostname 8081
```

**Fix:** Check VPC peering, firewall rules, or private link configuration

### Error: "Deserialization failed"

```python
# Check message format (should start with 0x00 for Confluent format)
df.selectExpr("hex(substring(value, 1, 1)) as magic_byte").show()
```

**Fix:** Ensure messages use Confluent wire format (magic byte = 0x00)

---

## 📊 Complete Working Example

### Onboarding File

```json
{
  "data_flow_id": "3001",
  "source_format": "kafka",

  "source_details": {
    "subscribe": "payment-events",
    "kafka.bootstrap.servers": "kafka-broker:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{{secrets/kafka_secrets/username}}\" password=\"{{secrets/kafka_secrets/password}}\";"
  },

  "landing_catalog_prod": "my_catalog",
  "landing_database_prod": "landing",
  "landing_table": "kafka_payments_raw",

  "refinery_catalog_prod": "my_catalog",
  "refinery_database_prod": "refinery",
  "refinery_table": "payments",

  "refinery_select_exp": [
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp",
    "from_avro(value, 'payment-events-value', map('url', 'https://schema-registry:8081', 'basic.auth.user.info', '{{secrets/schema_registry/credentials}}', 'mode', 'PERMISSIVE')) as payment_data"
  ],

  "refinery_select_exp_final": [
    "payment_data.payment_id",
    "payment_data.customer_id",
    "payment_data.amount",
    "payment_data.currency",
    "payment_data.status",
    "DATE(payment_data.transaction_timestamp) as payment_date",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp"
  ],

  "refinery_partition_columns": "payment_date"
}
```

---

## 🔐 Security Best Practices

### ✅ DO

- Store credentials in Databricks Secrets
- Use dedicated service accounts for Schema Registry
- Rotate credentials regularly (every 90 days)
- Use HTTPS for Schema Registry URL
- Enable audit logging

### ❌ DON'T

- Hardcode credentials in config files
- Share API keys across environments
- Use HTTP in production (unencrypted)
- Give write permissions to consumers
- Commit secrets to version control

---

## 🎓 Advanced Topics

### Schema Evolution Example

```json
{
  "refinery_select_exp": [
    "from_avro(value, 'events-value', map('url', 'https://schema-registry:8081', 'mode', 'PERMISSIVE')) as event"
  ],
  "refinery_select_exp_final": [
    "COALESCE(event.new_field, event.old_field) as unified_field",
    "event.other_field"
  ]
}
```

### Multi-Schema Topic Handling

```python
# When one topic has messages with different schemas
df.selectExpr(
    "CASE " +
    "  WHEN get_json_object(CAST(value AS STRING), '$.type') = 'order' " +
    "    THEN from_avro(value, 'orders-value', options) " +
    "  WHEN get_json_object(CAST(value AS STRING), '$.type') = 'payment' " +
    "    THEN from_avro(value, 'payments-value', options) " +
    "END as event"
)
```

---

## 📚 Additional Resources

- **Full Guide:** [Schema_Registry_Integration_Guide.md](Schema_Registry_Integration_Guide.md)
- **Kafka SSL Guide:** [Kafka_SSL_Ingestion_Onboarding_Guide.md](Kafka_SSL_Ingestion_Onboarding_Guide.md)
- **Databricks Docs:** https://docs.databricks.com/sql/language-manual/functions/from_avro.html
- **Confluent Docs:** https://docs.confluent.io/platform/current/schema-registry/

---

## 🆘 Getting Help

1. Check the [Troubleshooting](#-troubleshooting-quick-fixes) section
2. Review the [complete guide](Schema_Registry_Integration_Guide.md)
3. Test connection with curl commands
4. Contact Data Engineering team

---

**Version:** 1.0 | **Last Updated:** 2026-02-26
