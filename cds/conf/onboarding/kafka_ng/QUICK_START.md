# Quick Start: Kafka Schema Registry Onboarding

This is a quick reference for onboarding Kafka dataflows with Schema Registry in dlt-meta.

## 🚀 Quick Onboarding (5 Steps)

### 1️⃣ Upload SSL Certificates

```bash
# Upload to Unity Catalog Volumes
databricks fs cp truststore.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/conf/certs/truststore.jks

databricks fs cp keystore.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/conf/certs/keystore.jks
```

### 2️⃣ Configure Secrets

```bash
databricks secrets put-secret tm_cds_platform kafka_ng_nonprod_truststore_password
databricks secrets put-secret tm_cds_platform kafka_ng_nonprod_keystore_password
```

### 3️⃣ Create Onboarding JSON

Copy and customize [kafka_protobuf_source_example.json](kafka_protobuf_source_example.json):

```json
{
  "data_flow_id": "YOUR_ID",
  "source_format": "kafka",
  "source_details": {
    "kafka.bootstrap.servers": "YOUR_BROKER:9093",
    "subscribe": "YOUR_TOPIC",
    "schema.registry.url": "https://YOUR_SCHEMA_REGISTRY:8081",
    "schema.registry.subject": "YOUR_TOPIC-value",
    "data_format": "protobuf",
    "kafka.ssl.truststore.location": "/Volumes/.../truststore.jks",
    "kafka.ssl.keystore.location": "/Volumes/.../keystore.jks",
    "kafka.ssl.truststore.secrets.scope": "tm_cds_platform",
    "kafka.ssl.truststore.secrets.key": "kafka_ng_nonprod_truststore_password",
    "kafka.ssl.keystore.secrets.scope": "tm_cds_platform",
    "kafka.ssl.keystore.secrets.key": "kafka_ng_nonprod_keystore_password"
  },
  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "kafka_YOUR_TABLE_NAME"
}
```

### 4️⃣ Run Onboarding

```bash
cd /path/to/dlt-meta
python cds/conf/onboarding/kafka_ng/onboard_kafka_example.py your_onboarding.json
```

### 5️⃣ Verify & Deploy

Check metadata table:
```sql
SELECT * FROM dataservices_nonprod.dlt_meta_nonprod.landing_dataflowspec
WHERE data_flow_id = 'YOUR_ID';
```

## 📋 Common Configurations

### Avro with SSL
```json
{
  "data_format": "avro",
  "schema.registry.subject": "topic-name-value"
}
```

### Protobuf with SSL
```json
{
  "data_format": "protobuf",
  "schema.registry.subject": "topic-name-value"
}
```

### JSON (No Schema Registry)
```json
{
  "data_format": "json"
}
```

## 🔧 Common CLI Patterns

### Onboard Landing Only
```python
OnboardCommand(
    onboarding_file_path="my_kafka.json",
    onboarding_files_dir_path="cds/conf/onboarding/kafka_ng/",
    onboard_layer="landing",
    env="nonprod",
    import_author="your.email@example.com",
    version="1.0",
    dlt_meta_schema="dlt_meta_nonprod",
    uc_enabled=True,
    uc_catalog_name="dataservices_nonprod"
)
```

### Onboard Landing + Refinery
```python
OnboardCommand(
    onboard_layer="landing_refinery",  # Both layers
    # ... other parameters same as above
    landing_dataflowspec_table="landing_dataflowspec",
    refinery_dataflowspec_table="refinery_dataflowspec"
)
```

### Deploy Pipeline After Onboarding
```python
DeployCommand(
    layer="landing",
    pipeline_name="kafka_my_topic_pipeline",
    dlt_target_schema="landing_nonprod",
    onboard_landing_group="kafka_ingest",
    dlt_meta_landing_schema="dlt_meta_nonprod",
    dataflowspec_landing_table="landing_dataflowspec",
    uc_catalog_name="dataservices_nonprod",
    uc_enabled=True,
    serverless=True
)
```

## 🐛 Quick Troubleshooting

| Issue | Solution |
|-------|----------|
| File not found | Check path: `ls -la your_file.json` |
| SSL handshake failed | Verify cert paths and secret names |
| Schema Registry timeout | Check network and SSL config |
| Subject not found | Verify subject name in Schema Registry |

## 📚 Full Documentation

- **Complete Guide**: [ONBOARDING_CLI_GUIDE.md](ONBOARDING_CLI_GUIDE.md)
- **Kafka Schema Registry**: [README_KAFKA_SCHEMA_REGISTRY.md](README_KAFKA_SCHEMA_REGISTRY.md)
- **Examples**: See `*.json` files in this directory

## 💡 Tips

1. ✅ Always test in `nonprod` first
2. ✅ Use Unity Catalog (`uc_enabled=True`)
3. ✅ Store passwords in Databricks Secrets
4. ✅ Use `data_source_date_part` for partitioning
5. ✅ Set `maxOffsetsPerTrigger` to control batch size

## 🆘 Support

- **Jira**: https://jira.livenation.com/servicedesk/customer/portal/324/group/1242
- **Reference Implementation**: `platform_notebooks/notebook/landing_from_kafka_ng.py`
