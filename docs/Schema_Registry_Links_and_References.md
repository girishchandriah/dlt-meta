# Schema Registry - Official Documentation Links

## 🔗 Official Documentation URLs

### Databricks Documentation

#### 1. **from_avro() Function**
**URL:** https://docs.databricks.com/sql/language-manual/functions/from_avro.html

**Key Features:**
- Deserializes binary Avro data into structured format
- Supports Schema Registry integration (Databricks Runtime 11.3+)
- Options: `mode`, `url`, authentication parameters

**Basic Syntax:**
```sql
from_avro(avroBin, schemaSubject, options)
```

**With Schema Registry:**
```sql
from_avro(
  value,
  'topic-name-value',
  map(
    'url', 'https://schema-registry:8081',
    'basic.auth.user.info', 'API_KEY:API_SECRET',
    'mode', 'PERMISSIVE'
  )
)
```

---

#### 2. **to_avro() Function**
**URL:** https://docs.databricks.com/sql/language-manual/functions/to_avro.html

**Purpose:** Serializes data to Avro binary format

---

#### 3. **Kafka Integration Guide**
**URL:** https://docs.databricks.com/structured-streaming/kafka.html

**Covers:**
- Reading/writing Kafka with Structured Streaming
- SSL/TLS configuration
- Authentication (SASL_SSL, PLAIN, SCRAM, AWS MSK IAM)
- Performance tuning

---

#### 4. **Structured Streaming + Kafka**
**URL:** https://docs.databricks.com/structured-streaming/index.html

**Topics:**
- Streaming concepts
- Watermarking
- Output modes
- Checkpointing

---

### Apache Spark Documentation

#### 5. **Avro Data Source**
**URL:** https://spark.apache.org/docs/latest/sql-data-sources-avro.html

**Content:**
- `from_avro()` and `to_avro()` basics
- Reading/writing Avro files
- Available options: `mode`, `datetimeRebaseMode`, `positionalFieldMatching`

**Note:** Official Spark docs focus on Avro files, not Schema Registry integration

---

### Confluent Documentation

#### 6. **Confluent Schema Registry Documentation**
**URL:** https://docs.confluent.io/platform/current/schema-registry/index.html

**Essential Topics:**
- Schema Registry overview
- REST API reference
- Schema evolution and compatibility
- Security configuration

---

#### 7. **Schema Registry REST API**
**URL:** https://docs.confluent.io/platform/current/schema-registry/develop/api.html

**Key Endpoints:**
- `GET /subjects` - List all subjects
- `GET /subjects/{subject}/versions` - Get schema versions
- `GET /schemas/ids/{id}` - Get schema by ID
- `POST /subjects/{subject}/versions` - Register new schema
- `POST /compatibility/subjects/{subject}/versions/latest` - Test compatibility

**Example:**
```bash
# List all subjects
curl -X GET https://schema-registry:8081/subjects \
  -u API_KEY:API_SECRET

# Get latest schema for subject
curl -X GET https://schema-registry:8081/subjects/payment-events-value/versions/latest \
  -u API_KEY:API_SECRET
```

---

#### 8. **Confluent Cloud Schema Registry**
**URL:** https://docs.confluent.io/cloud/current/sr/index.html

**Covers:**
- Confluent Cloud Schema Registry setup
- API key generation
- Authentication
- Pricing and limits

---

#### 9. **Schema Registry Security**
**URL:** https://docs.confluent.io/platform/current/schema-registry/security/index.html

**Security Options:**
- SSL/TLS encryption
- Authentication (Basic Auth, SASL, OAuth)
- Authorization with ACLs
- Encryption at rest

---

#### 10. **Kafka Serializers**
**URL:** https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html

**Topics:**
- AvroSerializer/AvroDeserializer
- JSON Schema serialization
- Protobuf serialization
- Wire format specification

---

### Community Resources

#### 11. **Databricks Community - Schema Registry**
**URL:** https://community.databricks.com/

**Search terms:** "Confluent Schema Registry", "from_avro Kafka"

---

#### 12. **Stack Overflow - Spark Avro**
**URL:** https://stackoverflow.com/questions/tagged/apache-spark-avro

**Common Questions:**
- Schema Registry authentication
- from_avro troubleshooting
- Performance optimization

---

## 📚 Code Examples and Tutorials

### Example 1: Basic from_avro with Schema Registry

```python
from pyspark.sql.functions import col

# Read from Kafka
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9093") \
  .option("subscribe", "my-topic") \
  .load()

# Deserialize Avro using Schema Registry
schema_registry_url = "https://schema-registry:8081"
schema_registry_creds = dbutils.secrets.get("schema_registry", "credentials")

decoded_df = kafka_df.selectExpr(
  "topic",
  "partition",
  "offset",
  "timestamp",
  f"""
  from_avro(
    value,
    'my-topic-value',
    map(
      'url', '{schema_registry_url}',
      'basic.auth.credentials.source', 'USER_INFO',
      'basic.auth.user.info', '{schema_registry_creds}',
      'mode', 'PERMISSIVE'
    )
  ) as data
  """
)

# Extract fields
result_df = decoded_df.select(
  "data.field1",
  "data.field2",
  "data.field3",
  "topic",
  "partition",
  "offset"
)
```

### Example 2: SQL Function in DLT-META Transformation

**Transformation YAML:**
```yaml
target_table: my_table
sql_query: |
  SELECT
    data.id,
    data.name,
    data.timestamp
  FROM (
    SELECT
      from_avro(
        value,
        'my-topic-value',
        map(
          'url', 'https://schema-registry:8081',
          'basic.auth.user.info', '{{secrets/schema_registry/credentials}}'
        )
      ) as data
    FROM LIVE.kafka_raw_table
  )
```

### Example 3: Testing Schema Registry Connection

```python
# Test Schema Registry connectivity
import requests
from requests.auth import HTTPBasicAuth

api_key = dbutils.secrets.get('schema_registry', 'api_key')
api_secret = dbutils.secrets.get('schema_registry', 'api_secret')

# List subjects
response = requests.get(
    'https://schema-registry:8081/subjects',
    auth=HTTPBasicAuth(api_key, api_secret)
)

if response.status_code == 200:
    print("✅ Connected successfully!")
    print(f"Subjects: {response.json()}")
else:
    print(f"❌ Connection failed: {response.status_code}")
    print(response.text)

# Get specific schema
response = requests.get(
    'https://schema-registry:8081/subjects/my-topic-value/versions/latest',
    auth=HTTPBasicAuth(api_key, api_secret)
)

if response.status_code == 200:
    schema_info = response.json()
    print(f"Schema ID: {schema_info['id']}")
    print(f"Version: {schema_info['version']}")
    print(f"Schema: {schema_info['schema']}")
```

---

## 🔧 from_avro() Options Reference

### Complete Options List

| Option | Type | Default | Description | Example |
|--------|------|---------|-------------|---------|
| `url` | String | None | Schema Registry URL | `'https://schema-registry:8081'` |
| `basic.auth.credentials.source` | String | None | Auth source | `'USER_INFO'` |
| `basic.auth.user.info` | String | None | API credentials | `'API_KEY:API_SECRET'` |
| `mode` | String | `FAILFAST` | Error handling | `'PERMISSIVE'`, `'DROPMALFORMED'`, `'FAILFAST'` |
| `ssl.truststore.location` | String | None | Truststore path (JKS) | `'/path/to/truststore.jks'` |
| `ssl.truststore.password` | String | None | Truststore password | `'password123'` |
| `ssl.keystore.location` | String | None | Keystore for mTLS | `'/path/to/keystore.jks'` |
| `ssl.keystore.password` | String | None | Keystore password | `'password123'` |
| `bearer.auth.token` | String | None | OAuth bearer token | `'eyJ0eXAi...'` |

### Mode Options Explained

**PERMISSIVE (Recommended):**
- Sets corrupted/incompatible fields to `NULL`
- Continues processing
- Best for production with schema evolution

**DROPMALFORMED:**
- Drops entire row if any field is incompatible
- Use when data quality is critical

**FAILFAST:**
- Throws exception on first incompatibility
- Use for testing/validation

---

## 🌐 Schema Registry Endpoints Quick Reference

### List All Subjects
```bash
curl -X GET https://schema-registry:8081/subjects \
  -u $API_KEY:$API_SECRET
```

**Response:**
```json
["topic1-value", "topic1-key", "topic2-value"]
```

### Get Latest Schema Version
```bash
curl -X GET https://schema-registry:8081/subjects/topic-value/versions/latest \
  -u $API_KEY:$API_SECRET
```

**Response:**
```json
{
  "subject": "topic-value",
  "version": 3,
  "id": 123,
  "schema": "{\"type\":\"record\",\"name\":\"Event\",...}"
}
```

### Get All Versions for Subject
```bash
curl -X GET https://schema-registry:8081/subjects/topic-value/versions \
  -u $API_KEY:$API_SECRET
```

**Response:**
```json
[1, 2, 3, 4]
```

### Get Schema by ID
```bash
curl -X GET https://schema-registry:8081/schemas/ids/123 \
  -u $API_KEY:$API_SECRET
```

**Response:**
```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Event\",...}"
}
```

### Test Schema Compatibility
```bash
curl -X POST https://schema-registry:8081/compatibility/subjects/topic-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u $API_KEY:$API_SECRET \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[...]}"
  }'
```

**Response:**
```json
{
  "is_compatible": true
}
```

### Register New Schema
```bash
curl -X POST https://schema-registry:8081/subjects/topic-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u $API_KEY:$API_SECRET \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[...]}"
  }'
```

**Response:**
```json
{
  "id": 124
}
```

---

## 🔍 Additional Resources

### Books
- **"Kafka: The Definitive Guide"** by Neha Narkhede - Chapter on Schema Registry
- **"Streaming Systems"** by Tyler Akidau - Schema management patterns

### GitHub Repositories
- **Confluent Schema Registry:** https://github.com/confluentinc/schema-registry
- **Apache Avro:** https://github.com/apache/avro
- **Spark Avro:** https://github.com/databricks/spark-avro

### Video Tutorials
- **Confluent YouTube Channel:** https://www.youtube.com/c/Confluent
  - Search: "Schema Registry Tutorial"
- **Databricks YouTube Channel:** https://www.youtube.com/c/Databricks
  - Search: "Kafka Streaming"

### Confluent Cloud Console
- **URL:** https://confluent.cloud/
- **Features:**
  - Schema Registry UI
  - Subject browser
  - Schema evolution viewer
  - Compatibility settings

---

## 📞 Support Channels

### Confluent Support
- **Community Forum:** https://forum.confluent.io/
- **Cloud Support:** https://support.confluent.io/
- **Professional Services:** https://www.confluent.io/services/

### Databricks Support
- **Community:** https://community.databricks.com/
- **Support Portal:** https://help.databricks.com/
- **Documentation Feedback:** https://docs.databricks.com/feedback.html

---

## 🎓 Training and Certification

### Confluent Training
- **Schema Registry Fundamentals:** https://www.confluent.io/training/
- **Apache Kafka Developer Certification**

### Databricks Training
- **Structured Streaming Course:** https://academy.databricks.com/
- **Delta Lake and Streaming**

---

**Last Updated:** February 26, 2026
**Maintained By:** Platform Data Engineering Team
