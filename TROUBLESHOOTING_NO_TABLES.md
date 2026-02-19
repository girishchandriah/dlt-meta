# Troubleshooting: NO_TABLES_IN_PIPELINE Error

## Error Message
```
com.databricks.pipelines.common.errors.DLTAnalysisException: [NO_TABLES_IN_PIPELINE]
Pipelines are expected to have at least one table defined but no tables were found in your pipeline.
```

## Root Cause

DLT-META is a **metadata-driven framework** that dynamically generates DLT tables based on **dataflow specifications** stored in Delta tables. The error occurs when:

1. **Dataflowspec tables don't exist or are empty**
2. **Required Spark configuration parameters are missing or incorrect**
3. **The group/dataflowIds filter excludes all rows**

## Understanding DLT-META Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Onboarding Process                                         │
│  ├─ Reads: conf/onboarding.json                             │
│  └─ Creates: landing_dataflowspec & refinery_dataflowspec   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  DLT Pipeline Execution                                     │
│  ├─ Reads dataflowspecs from Delta tables                   │
│  ├─ Filters by group or dataflowIds                         │
│  └─ Dynamically creates @dlt.table decorators               │
└─────────────────────────────────────────────────────────────┘
```

## Solution Steps

### Step 1: Verify Required Configuration

Your DLT pipeline **must** have these configuration parameters set:

#### For Landing Layer Only:
```json
{
  "layer": "landing",
  "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec",
  "landing.group": "your_group_name",
  "dlt_meta_whl": "path/to/dlt_meta_wheel.whl"
}
```

#### For Landing + Refinery Layers:
```json
{
  "layer": "landing_refinery",
  "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec",
  "landing.group": "your_group_name",
  "refinery.dataflowspecTable": "catalog.schema.refinery_dataflowspec",
  "refinery.group": "your_group_name",
  "dlt_meta_whl": "path/to/dlt_meta_wheel.whl"
}
```

**Where to set these:**
- In Databricks UI: Pipeline Settings → Configuration → Add configuration pairs
- Programmatically: When creating pipeline with `ws.pipelines.create(configuration={...})`

### Step 2: Run the Debug Script

Use the provided `debug_dataflowspecs.py` script to check if your dataflowspec tables exist and have data:

1. Edit the variables at the top:
   ```python
   catalog_name = "your_catalog"
   schema_name = "your_schema"
   landing_table = "landing_dataflowspec"
   refinery_table = "refinery_dataflowspec"
   group_name = "your_group"
   ```

2. Run in a Databricks notebook

3. Review the output to identify issues

### Step 3: Run Onboarding (If Tables Are Empty/Missing)

If your dataflowspec tables don't exist or are empty, you need to run the **onboarding process** first:

#### Option A: Using the Lakehouse App UI

1. Start the lakehouse app: `python lakehouse_app/app.py`
2. Navigate to the Onboarding page
3. Fill in the onboarding form with:
   - Unity Catalog details
   - Schema names (landing, refinery, dlt_meta)
   - Onboarding file path (e.g., `demo/conf/onboarding.json`)
4. Click "Onboard"

#### Option B: Using CLI

```bash
python src/cli.py '{
  "command": "onboard",
  "unity_catalog_enabled": "1",
  "unity_catalog_name": "your_catalog",
  "onboarding_file_path": "demo/conf/onboarding.json",
  "dlt_meta_schema": "your_dlt_meta_schema",
  "landing_schema": "your_landing_schema",
  "refinery_schema": "your_refinery_schema",
  "dlt_meta_layer": "landing_refinery",
  "landing_table": "landing_dataflowspec",
  "refinery_table": "refinery_dataflowspec",
  "overwrite": "1",
  "version": "v1"
}'
```

#### Option C: Using Python Wheel Entry Point

```bash
databricks bundle run setup_dlt_meta_pipeline_spec -- \
  --onboard_layer=landing_refinery \
  --database=catalog.schema \
  --onboarding_file_path=/path/to/onboarding.json \
  --landing_dataflowspec_table=landing_dataflowspec \
  --refinery_dataflowspec_table=refinery_dataflowspec \
  --overwrite=True
```

### Step 4: Verify Onboarding Was Successful

After onboarding, verify the tables were created:

```sql
-- Check landing dataflowspec
SELECT dataFlowId, dataFlowGroup, sourceFormat, targetDetails
FROM catalog.schema.landing_dataflowspec;

-- Check refinery dataflowspec
SELECT dataFlowId, dataFlowGroup, sourceDetails, targetDetails
FROM catalog.schema.refinery_dataflowspec;
```

**Expected result:** Both tables should have at least one row for your group.

### Step 5: Update Pipeline Configuration

Make sure your DLT pipeline configuration matches the actual table names and groups:

1. Go to Databricks UI → Workflows → Delta Live Tables
2. Find your pipeline → Settings
3. Check the **Configuration** section
4. Verify these keys exist and have correct values:
   - `layer`
   - `landing.dataflowspecTable`
   - `landing.group`
   - `refinery.dataflowspecTable` (if using refinery)
   - `refinery.group` (if using refinery)
   - `dlt_meta_whl`

### Step 6: Restart the Pipeline

After fixing the configuration:
1. Stop the pipeline if running
2. Start a new update
3. Monitor the logs for any configuration-related errors

## Common Issues and Solutions

### Issue 1: Group Name Mismatch
**Symptom:** Tables exist but pipeline finds no rows

**Solution:** Check the actual group names in your dataflowspec tables:
```sql
SELECT DISTINCT dataFlowGroup FROM catalog.schema.landing_dataflowspec;
```
Update your pipeline configuration to use the correct group name.

### Issue 2: Wrong Table Name in Configuration
**Symptom:** Error reading dataflowspec table

**Solution:** Verify table names are fully qualified:
```json
{
  "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec"
}
```
Not just `landing_dataflowspec`.

### Issue 3: Onboarding File Path Issues
**Symptom:** Onboarding fails or creates incorrect specs

**Solution:**
- Use absolute paths or paths relative to PYTHONPATH
- Check onboarding JSON file format matches examples in `demo/conf/`
- Validate JSON syntax

### Issue 4: Unity Catalog Permissions
**Symptom:** Can't read/write dataflowspec tables

**Solution:**
- Ensure pipeline run-as user has SELECT on dataflowspec tables
- Ensure pipeline run-as user has CREATE TABLE on target schemas

## Example: Complete Working Configuration

Here's a complete example that works:

**1. Onboarding Configuration (`conf/my_onboarding.json`):**
```json
{
  "dataFlowId": "1001",
  "dataFlowGroup": "my_group",
  "sourceFormat": "cloudFiles",
  "sourceDetails": {
    "path": "/mnt/data/input/",
    "format": "json"
  },
  "targetDetails": {
    "catalog": "my_catalog",
    "database": "landing_layer",
    "table": "my_table"
  },
  "tableProperties": {},
  "partitionColumns": ["date"],
  "version": "v1"
}
```

**2. Run Onboarding:**
```bash
python src/cli.py '{
  "command": "onboard",
  "unity_catalog_enabled": "1",
  "unity_catalog_name": "my_catalog",
  "onboarding_file_path": "conf/my_onboarding.json",
  "dlt_meta_schema": "dlt_meta_specs",
  "landing_schema": "landing_layer",
  "refinery_schema": "refinery_layer",
  "dlt_meta_layer": "landing_refinery",
  "landing_table": "landing_dataflowspec",
  "refinery_table": "refinery_dataflowspec",
  "overwrite": "1",
  "version": "v1"
}'
```

**3. DLT Pipeline Configuration:**
```json
{
  "layer": "landing_refinery",
  "landing.dataflowspecTable": "my_catalog.dlt_meta_specs.landing_dataflowspec",
  "landing.group": "my_group",
  "refinery.dataflowspecTable": "my_catalog.dlt_meta_specs.refinery_dataflowspec",
  "refinery.group": "my_group",
  "dlt_meta_whl": "dbfs:/path/to/dlt_meta_cds-0.0.10-py3-none-any.whl"
}
```

**4. Pipeline Notebook (`init_dlt_meta_pipeline.py`):**
```python
# Cell 1: Install wheel
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl
dbutils.library.restartPython()

# Cell 2: Run pipeline
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

## Additional Resources

- [DLT-META Documentation](./docs/)
- [Onboarding Examples](./demo/conf/)
- [Integration Tests](./integration_tests/) - Show complete working examples
- [init_dlt_meta_pipeline_correct.py](./init_dlt_meta_pipeline_correct.py) - Correct pipeline initialization

## Still Having Issues?

If you're still experiencing problems:

1. Check the DLT pipeline logs for the specific error
2. Run the debug script and share the output
3. Verify your onboarding JSON file follows the correct schema
4. Check Unity Catalog permissions
5. Try running one of the demo examples first to verify your setup

## Quick Start: Running a Demo

To verify your setup works, try running a demo:

```bash
# Run the cloudfiles demo
python demo/launch_af_cloudfiles_demo.py \
  --uc_catalog_name your_catalog \
  --profile DEFAULT
```

This will:
1. Create sample dataflowspec tables
2. Create a DLT pipeline with correct configuration
3. Run the pipeline end-to-end

If the demo works, compare its configuration to your pipeline configuration.
