# Kafka with Schema Registry Integration in dlt-meta

This guide explains how to use Kafka with Schema Registry (Avro/Protobuf) in dlt-meta for both inbound (source) and outbound (sink) data flows.

## Overview

The enhanced dlt-meta implementation supports:
- ✅ **Kafka Inbound**: Reading from Kafka topics with Avro/Protobuf deserialization via Schema Registry
- ✅ **Kafka Outbound**: Writing to Kafka topics with Avro/Protobuf serialization via Schema Registry
- ✅ **SSL Support**: Full SSL/TLS encryption for both Kafka brokers and Schema Registry
- ✅ **Secret Management**: Integration with Databricks Secrets for credential management

## Architecture

The implementation is based on the production-tested `platform_notebooks/landing_from_kafka_ng.py` pattern and uses:
- Direct Java API calls for Schema Registry integration
- `confluent.` prefix for Schema Registry SSL options (matching Confluent standards)
- Databricks Secrets for credential management

## Configuration Guide

### 1. Kafka Inbound (Source) - Avro

Example: [kafka_avro_source_example.json](kafka_avro_source_example.json)

```json
{
  "source_format": "kafka",
  "source_details": {
    "kafka.bootstrap.servers": "broker:9093",
    "subscribe": "topic-name",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.location": "/dbfs/FileStore/ssl/truststore.jks",
    "kafka.ssl.keystore.location": "/dbfs/FileStore/ssl/keystore.jks",
    "kafka.ssl.truststore.secrets.scope": "ssl_certs",
    "kafka.ssl.truststore.secrets.key": "truststore_password",
    "kafka.ssl.keystore.secrets.scope": "ssl_certs",
    "kafka.ssl.keystore.secrets.key": "keystore_password",
    "kafka.ssl.key.secrets.scope": "ssl_certs",
    "kafka.ssl.key.secrets.key": "key_password",
    "schema.registry.url": "https://schema-registry:8081",
    "schema.registry.subject": "topic-name-value",
    "data_format": "avro",
    "mode": "PERMISSIVE"
  },
  "landing_reader_options": {
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "100000",
    "failOnDataLoss": "false"
  }
}
```

**Key Configuration Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `schema.registry.url` | Yes | Schema Registry HTTPS URL |
| `schema.registry.subject` | Yes | Subject name (e.g., `topic-name-value`) |
| `data_format` | Yes | `avro` or `protobuf` |
| `mode` | No | `PERMISSIVE` (default) or `FAILFAST` |
| `kafka.ssl.truststore.location` | Yes* | Path to JKS truststore |
| `kafka.ssl.keystore.location` | Yes* | Path to JKS keystore |
| `kafka.ssl.*.secrets.scope` | Yes* | Databricks secret scope |
| `kafka.ssl.*.secrets.key` | Yes* | Databricks secret key |

*Required for SSL connections

### 2. Kafka Inbound (Source) - Protobuf

Example: [kafka_protobuf_source_example.json](kafka_protobuf_source_example.json)

```json
{
  "source_format": "kafka",
  "source_details": {
    "kafka.bootstrap.servers": "broker:9093",
    "subscribe": "topic-name",
    "schema.registry.url": "https://schema-registry:8081",
    "schema.registry.subject": "topic-name-value",
    "data_format": "protobuf",
    "mode": "PERMISSIVE"
  }
}
```

**Protobuf-specific notes:**
- Uses `from_protobuf` function internally
- Supports recursive fields (configurable depth)
- Schema Registry returns Protobuf descriptor

### 3. Kafka Outbound (Sink) - Avro

Example: [kafka_avro_sink_example.json](kafka_avro_sink_example.json)

```json
{
  "sinks": [
    {
      "name": "my_kafka_sink",
      "format": "kafka",
      "options": {
        "kafka.bootstrap.servers": "broker:9093",
        "topic": "output-topic",
        "kafka.security.protocol": "SSL",
        "kafka.ssl.truststore.location": "/dbfs/FileStore/ssl/truststore.jks",
        "kafka.ssl.keystore.location": "/dbfs/FileStore/ssl/keystore.jks",
        "kafka.ssl.truststore.password": "{{secrets/ssl_certs/truststore_password}}",
        "kafka.ssl.keystore.password": "{{secrets/ssl_certs/keystore_password}}",
        "kafka.ssl.key.password": "{{secrets/ssl_certs/key_password}}",
        "schema.registry.url": "https://schema-registry:8081",
        "schema.registry.subject": "output-topic-value",
        "data_format": "avro",
        "confluent.schema.registry.ssl.truststore.location": "/dbfs/FileStore/ssl/truststore.jks",
        "confluent.schema.registry.ssl.truststore.password": "{{secrets/ssl_certs/truststore_password}}",
        "confluent.schema.registry.ssl.keystore.location": "/dbfs/FileStore/ssl/keystore.jks",
        "confluent.schema.registry.ssl.keystore.password": "{{secrets/ssl_certs/keystore_password}}",
        "confluent.schema.registry.ssl.key.password": "{{secrets/ssl_certs/key_password}}"
      },
      "select_exp": [
        "order_id as key",
        "order_id",
        "customer_id",
        "amount"
      ],
      "where_clause": "status = 'COMPLETED'"
    }
  ]
}
```

**Sink Configuration Notes:**

- **`key` column is required**: Must be present in `select_exp` (e.g., `order_id as key`)
- **`value` serialization**: All columns except `key` are serialized into Avro/Protobuf
- **Secret interpolation**: Use `{{secrets/scope/key}}` syntax for runtime secret resolution
- **Schema Registry SSL**: Use `confluent.schema.registry.ssl.*` prefix (matches production pattern)

### 4. End-to-End Pipeline

Example: [kafka_e2e_pipeline_example.json](kafka_e2e_pipeline_example.json)

Shows complete data flow:
1. **Ingest** from Kafka topic with Avro deserialization
2. **Transform** data in refinery layer
3. **Export** to Kafka topic with Avro serialization

## SSL Certificate Management

### JKS Keystore/Truststore Setup

1. **Upload certificates to DBFS:**
   ```bash
   databricks fs cp truststore.jks dbfs:/FileStore/ssl/truststore.jks
   databricks fs cp keystore.jks dbfs:/FileStore/ssl/keystore.jks
   ```

2. **Store passwords in Databricks Secrets:**
   ```bash
   databricks secrets create-scope ssl_certs
   databricks secrets put-secret ssl_certs truststore_password
   databricks secrets put-secret ssl_certs keystore_password
   databricks secrets put-secret ssl_certs key_password
   ```

3. **Reference in configuration:**
   ```json
   {
     "kafka.ssl.truststore.location": "/dbfs/FileStore/ssl/truststore.jks",
     "kafka.ssl.truststore.secrets.scope": "ssl_certs",
     "kafka.ssl.truststore.secrets.key": "truststore_password"
   }
   ```

### PEM Certificate Alternative

If you have PEM certificates instead of JKS:

```json
{
  "kafka.ssl.ca.location": "/dbfs/FileStore/ssl/ca-cert.pem",
  "kafka.ssl.certificate.location": "/dbfs/FileStore/ssl/client-cert.pem",
  "kafka.ssl.key.location": "/dbfs/FileStore/ssl/client-key.pem"
}
```

## Schema Registry Subject Naming

Common patterns:
- **TopicNameStrategy** (default): `{topic}-value` or `{topic}-key`
- **RecordNameStrategy**: `{record.name}`
- **TopicRecordNameStrategy**: `{topic}-{record.name}`

Example for topic `payment-events`:
- Value subject: `payment-events-value`
- Key subject: `payment-events-key`

## Databricks Secrets Best Practices

1. **Use environment-specific scopes:**
   ```
   ssl_certs_nonprod
   ssl_certs_preprod
   ssl_certs_prod
   ```

2. **Store all credentials as secrets:**
   - Kafka SSL passwords
   - Schema Registry credentials
   - Never hardcode passwords in JSON

3. **Secret interpolation syntax:**
   ```json
   "password": "{{secrets/scope_name/secret_key}}"
   ```

## Data Formats

### Avro
- Binary format with schema evolution
- Compact storage
- Fast serialization/deserialization
- Best for: High-throughput streaming

### Protobuf
- Binary format with backward compatibility
- Language-agnostic
- Supports complex nested structures
- Best for: Cross-platform integrations

### JSON (Default)
- Human-readable
- No Schema Registry required
- Larger payload size
- Best for: Development/debugging

## Troubleshooting

### Issue: "from_avro not defined"
**Solution**: Ensure `spark-avro` package is installed on cluster:
- Cluster Libraries → Maven → `org.apache.spark:spark-avro_2.12:3.5.0`

### Issue: "Schema Registry connection timeout"
**Solution**: Check SSL certificates and network connectivity:
```python
import requests
response = requests.get(
    "https://schema-registry:8081/subjects",
    verify="/path/to/ca-cert.pem"
)
```

### Issue: "Subject not found"
**Solution**: Verify subject name matches Schema Registry:
```bash
curl -u user:pass https://schema-registry:8081/subjects
```

### Issue: "SSL handshake failed"
**Solution**: Verify JKS keystore/truststore passwords are correct in Databricks Secrets.

## Performance Tuning

### Kafka Reader Options
```json
{
  "maxOffsetsPerTrigger": "100000",
  "kafka.request.timeout.ms": "60000",
  "kafka.session.timeout.ms": "60000",
  "failOnDataLoss": "false"
}
```

### Schema Registry Caching
Schema Registry schemas are cached automatically. For high-throughput scenarios:
- Use latest stable schema versions
- Avoid frequent schema updates during streaming

## Migration from landing_from_kafka_ng.py

If migrating from the notebook approach:

| Notebook Pattern | dlt-meta Configuration |
|------------------|------------------------|
| `param_yaml["kafka_topic"]` | `source_details.subscribe` |
| `param_yaml["schema_registry_url"]` | `source_details.schema.registry.url` |
| `param_yaml["avro_value_schema"]` | `source_details.schema.registry.subject` |
| `param_yaml["data_format"]` | `source_details.data_format` |
| Environment variables | Databricks Secrets with scope/key |

## Example Use Cases

1. **Real-time Event Ingestion**: Ingest Avro events from Kafka → Land in Delta → Transform
2. **CDC Replication**: Capture change events from source → Enrich → Export to downstream Kafka
3. **Multi-hop Architecture**: Kafka → Bronze (landing) → Silver (refinery) → Kafka (gold export)
4. **Cross-environment Sync**: Nonprod Kafka → Transform → Prod Kafka

## Support

For issues or questions:
- Check logs: Look for "Schema Registry" log messages
- Review [platform_notebooks/landing_from_kafka_ng.py](../../../../platform_notebooks/notebook/landing_from_kafka_ng.py) for reference implementation
- Contact Data Platform team via Jira: https://jira.livenation.com/servicedesk/customer/portal/324/group/1242

## References

- [Databricks Kafka Documentation](https://docs.databricks.com/structured-streaming/kafka.html)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Protocol Buffers Documentation](https://developers.google.com/protocol-buffers)
