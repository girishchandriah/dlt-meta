# DLT-META CLI Onboarding Guide for Kafka with Schema Registry

This guide shows how to onboard Kafka data flows with Schema Registry using the dlt-meta CLI.

## Prerequisites

1. **Databricks Workspace** with appropriate permissions
2. **SSL Certificates** uploaded to DBFS or UC Volumes
3. **Databricks Secrets** configured for passwords
4. **Onboarding JSON file** prepared (see examples in this directory)

## CLI Onboarding Command Structure

The dlt-meta CLI uses Python SDK to onboard dataflows programmatically:

```python
from databricks.sdk import WorkspaceClient
from src.cli import DLTMeta, OnboardCommand

ws = WorkspaceClient()
dltmeta = DLTMeta(ws)

onboard_cmd = OnboardCommand(
    onboarding_file_path="path/to/onboarding.json",
    onboarding_files_dir_path="path/to/directory/",
    onboard_layer="landing",  # or "refinery", "treasury", "landing_refinery", "landing_refinery_treasury"
    env="nonprod",
    import_author="your.email@example.com",
    version="1.0",
    dlt_meta_schema="dlt_meta_nonprod",
    uc_enabled=True,
    uc_catalog_name="dataservices_nonprod",
    overwrite=True
)

dltmeta.onboard(onboard_cmd)
```

## Step-by-Step Onboarding

### Step 1: Prepare Your SSL Certificates

```bash
# Upload to UC Volumes (recommended)
databricks fs cp truststore.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/conf/certs/truststore.jks

databricks fs cp keystore.jks \
  dbfs:/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/conf/certs/keystore.jks

# Or upload to DBFS (legacy)
databricks fs cp truststore.jks dbfs:/FileStore/ssl/truststore.jks
databricks fs cp keystore.jks dbfs:/FileStore/ssl/keystore.jks
```

### Step 2: Configure Databricks Secrets

```bash
# Create secret scope (if not exists)
databricks secrets create-scope tm_cds_platform

# Add SSL passwords
databricks secrets put-secret tm_cds_platform kafka_ng_nonprod_truststore_password
databricks secrets put-secret tm_cds_platform kafka_ng_nonprod_keystore_password
```

### Step 3: Create Your Onboarding JSON File

Use one of the example files as a template:

**For Avro:** [kafka_avro_source_example.json](kafka_avro_source_example.json)
**For Protobuf:** [kafka_protobuf_source_example.json](kafka_protobuf_source_example.json)

Example: `my_kafka_onboarding.json`
```json
{
  "version": "v1",
  "data_flow_id": "3004",
  "data_flow_group": "my_kafka_ingest",
  "source_system": "kafka_nonprod",
  "source_format": "kafka",

  "source_details": {
    "kafka.bootstrap.servers": "br-lb.prd2428.dev.use1.kafka.nonprod-tmaws.io:9093",
    "subscribe": "my-topic-name",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.location": "/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/conf/certs/truststore.jks",
    "kafka.ssl.keystore.location": "/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/conf/certs/keystore.jks",
    "kafka.ssl.truststore.secrets.scope": "tm_cds_platform",
    "kafka.ssl.truststore.secrets.key": "kafka_ng_nonprod_truststore_password",
    "kafka.ssl.keystore.secrets.scope": "tm_cds_platform",
    "kafka.ssl.keystore.secrets.key": "kafka_ng_nonprod_keystore_password",
    "kafka.ssl.key.secrets.scope": "tm_cds_platform",
    "kafka.ssl.key.secrets.key": "kafka_ng_nonprod_keystore_password",
    "schema.registry.url": "https://sr-lb.prd2428.dev.use1.kafka.nonprod-tmaws.io:8081",
    "schema.registry.subject": "my-topic-name-value",
    "data_format": "avro",
    "mode": "PERMISSIVE"
  },

  "landing_reader_options": {
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "100000",
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "60000",
    "failOnDataLoss": "false"
  },

  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "kafka_my_topic_name",
  "landing_table_comment": "Kafka Avro ingestion for my-topic-name",
  "landing_partition_columns": "date"
}
```

### Step 4: Run Onboarding Script

Create a Python script `onboard_kafka.py`:

```python
#!/usr/bin/env python3
"""
Onboard Kafka dataflow with Schema Registry to dlt-meta.
"""
import os
from databricks.sdk import WorkspaceClient
from src.cli import DLTMeta, OnboardCommand

# Configuration
ONBOARDING_FILE = "cds/conf/onboarding/kafka_ng/my_kafka_onboarding.json"
ONBOARDING_DIR = "cds/conf/onboarding/kafka_ng/"
LAYER = "landing"
ENV = "nonprod"
IMPORT_AUTHOR = os.getenv("USER", "data-platform@example.com")

# Initialize Databricks workspace client
ws = WorkspaceClient()
dltmeta = DLTMeta(ws)

# Create onboarding command
onboard_cmd = OnboardCommand(
    # Required parameters
    onboarding_file_path=ONBOARDING_FILE,
    onboarding_files_dir_path=ONBOARDING_DIR,
    onboard_layer=LAYER,
    env=ENV,
    import_author=IMPORT_AUTHOR,
    version="1.0",
    dlt_meta_schema="dlt_meta_nonprod",

    # Unity Catalog configuration
    uc_enabled=True,
    uc_catalog_name="dataservices_nonprod",

    # Dataflowspec tables
    landing_dataflowspec_table="landing_dataflowspec",
    refinery_dataflowspec_table="refinery_dataflowspec",
    treasury_dataflowspec_table="treasury_dataflowspec",

    # Settings
    overwrite=True,
    update_paths=True
)

# Execute onboarding
print(f"Onboarding Kafka dataflow from: {ONBOARDING_FILE}")
dltmeta.onboard(onboard_cmd)
print("✅ Onboarding job submitted successfully!")
```

Run the script:
```bash
cd /path/to/dlt-meta
python onboard_kafka.py
```

### Step 5: Monitor Onboarding Job

The CLI will create and launch a Databricks job. You'll see output like:
```
Job created successfully. job_id=123456, url=https://your-workspace.databricks.com/jobs/123456
```

Monitor the job in the Databricks UI.

## Onboarding Parameters Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `onboarding_file_path` | Path to onboarding JSON file | `"cds/conf/onboarding/kafka_ng/my_onboarding.json"` |
| `onboarding_files_dir_path` | Directory containing all config files | `"cds/conf/onboarding/kafka_ng/"` |
| `onboard_layer` | Layer to onboard | `"landing"`, `"refinery"`, `"treasury"`, `"landing_refinery"`, `"landing_refinery_treasury"` |
| `env` | Environment | `"nonprod"`, `"preprod"`, `"prod"` |
| `import_author` | Email of person running onboarding | `"your.email@example.com"` |
| `version` | Version of dataflow | `"1.0"` |
| `dlt_meta_schema` | DLT-META metadata schema | `"dlt_meta_nonprod"` |

### Unity Catalog Parameters (Recommended)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `uc_enabled` | Enable Unity Catalog | `True` |
| `uc_catalog_name` | UC catalog name | `"dataservices_nonprod"` |
| `landing_dataflowspec_table` | Landing metadata table | `"landing_dataflowspec"` |
| `refinery_dataflowspec_table` | Refinery metadata table | `"refinery_dataflowspec"` |
| `treasury_dataflowspec_table` | Treasury metadata table | `"treasury_dataflowspec"` |

### DBFS Parameters (Legacy)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `uc_enabled` | Disable Unity Catalog | `False` |
| `dbfs_path` | DBFS path for metadata | `"/dbfs/dlt-meta"` |
| `landing_dataflowspec_path` | Landing metadata path | `"/dbfs/dlt-meta/landing_dataflowspec"` |
| `refinery_dataflowspec_path` | Refinery metadata path | `"/dbfs/dlt-meta/refinery_dataflowspec"` |

### Optional Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `overwrite` | Overwrite existing dataflows | `True` |
| `update_paths` | Update workspace paths | `True` |
| `serverless` | Use serverless compute | `True` |
| `cloud` | Cloud provider (if not serverless) | `"aws"` |
| `dbr_version` | DBR version (if not serverless) | `"13.3.x-scala2.12"` |

## Multi-Layer Onboarding

Onboard landing and refinery together:

```python
onboard_cmd = OnboardCommand(
    onboarding_file_path="my_e2e_onboarding.json",
    onboarding_files_dir_path="cds/conf/onboarding/kafka_ng/",
    onboard_layer="landing_refinery",  # Both layers
    env="nonprod",
    import_author="data-platform@example.com",
    version="1.0",
    dlt_meta_schema="dlt_meta_nonprod",
    uc_enabled=True,
    uc_catalog_name="dataservices_nonprod",
    landing_dataflowspec_table="landing_dataflowspec",
    refinery_dataflowspec_table="refinery_dataflowspec"
)
```

## Deployment After Onboarding

After onboarding, deploy the DLT pipeline:

```python
from src.cli import DeployCommand

deploy_cmd = DeployCommand(
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

dltmeta.deploy(deploy_cmd)
```

## Verification Steps

After onboarding:

1. **Check metadata tables:**
   ```sql
   SELECT * FROM dataservices_nonprod.dlt_meta_nonprod.landing_dataflowspec
   WHERE data_flow_group = 'my_kafka_ingest';
   ```

2. **Verify pipeline created:**
   - Go to Databricks Workflows → Delta Live Tables
   - Look for your pipeline

3. **Test run the pipeline:**
   - Click "Start" on your DLT pipeline
   - Monitor for successful ingestion

## Troubleshooting

### Issue: "Onboarding file not found"
**Solution**: Ensure file path is correct and file exists
```bash
ls -la cds/conf/onboarding/kafka_ng/my_kafka_onboarding.json
```

### Issue: "uc_catalog_name is required"
**Solution**: Set `uc_enabled=True` and provide `uc_catalog_name`

### Issue: "SSL handshake failed"
**Solution**:
- Verify certificate paths are correct
- Check secret scope and key names match
- Test certificate access:
```python
dbutils.secrets.get("tm_cds_platform", "kafka_ng_nonprod_truststore_password")
```

### Issue: "Schema Registry connection timeout"
**Solution**: Verify network connectivity and SSL certificates

## Example: Complete Onboarding Script

```python
#!/usr/bin/env python3
"""
Complete Kafka onboarding with Schema Registry.
"""
import os
import sys
from databricks.sdk import WorkspaceClient
from src.cli import DLTMeta, OnboardCommand, DeployCommand

def main():
    # Configuration
    ONBOARDING_FILE = sys.argv[1] if len(sys.argv) > 1 else "cds/conf/onboarding/kafka_ng/kafka_protobuf_source_example.json"

    ws = WorkspaceClient()
    dltmeta = DLTMeta(ws)

    # Step 1: Onboard
    print("=" * 60)
    print("STEP 1: Onboarding Kafka dataflow...")
    print("=" * 60)

    onboard_cmd = OnboardCommand(
        onboarding_file_path=ONBOARDING_FILE,
        onboarding_files_dir_path=os.path.dirname(ONBOARDING_FILE),
        onboard_layer="landing",
        env="nonprod",
        import_author=os.getenv("USER", "data-platform@example.com"),
        version="1.0",
        dlt_meta_schema="dlt_meta_nonprod",
        uc_enabled=True,
        uc_catalog_name="dataservices_nonprod",
        landing_dataflowspec_table="landing_dataflowspec",
        overwrite=True,
        update_paths=True
    )

    dltmeta.onboard(onboard_cmd)
    print("✅ Onboarding completed!")

    # Step 2: Deploy (optional)
    print("\n" + "=" * 60)
    print("STEP 2: Deploying DLT pipeline...")
    print("=" * 60)

    deploy_cmd = DeployCommand(
        layer="landing",
        pipeline_name="kafka_protobuf_pipeline_nonprod",
        dlt_target_schema="landing_nonprod",
        onboard_landing_group="kafka_protobuf_ingest",
        dlt_meta_landing_schema="dlt_meta_nonprod",
        dataflowspec_landing_table="landing_dataflowspec",
        uc_catalog_name="dataservices_nonprod",
        uc_enabled=True,
        serverless=True
    )

    dltmeta.deploy(deploy_cmd)
    print("✅ Pipeline deployed successfully!")

if __name__ == "__main__":
    main()
```

Save as `onboard_and_deploy_kafka.py` and run:
```bash
python onboard_and_deploy_kafka.py cds/conf/onboarding/kafka_ng/kafka_protobuf_source_example.json
```

## Best Practices

1. **Use Unity Catalog**: Always set `uc_enabled=True` for production systems
2. **Version Control**: Keep onboarding JSON files in git
3. **Environment Separation**: Use different catalogs/schemas for nonprod/preprod/prod
4. **Secret Management**: Never hardcode passwords in JSON files
5. **Testing**: Test in nonprod before promoting to production
6. **Documentation**: Add clear comments in onboarding files explaining configuration

## Next Steps

After successful onboarding:
1. Configure pipeline schedule (if needed)
2. Set up monitoring and alerts
3. Test end-to-end data flow
4. Document for operations team

## Support

- Jira: https://jira.livenation.com/servicedesk/customer/portal/324/group/1242
- Documentation: [README_KAFKA_SCHEMA_REGISTRY.md](README_KAFKA_SCHEMA_REGISTRY.md)
- Platform Team: Contact via Slack or Jira
