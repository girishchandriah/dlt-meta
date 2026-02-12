# Databricks notebook source
# DBTITLE 1,Debug DLT-META - Test Dataflowspec Loading
"""
Use this notebook to debug DLT-META by testing components independently.
Run this in Databricks to troubleshoot pipeline issues.
"""

# COMMAND ----------
# Install dlt-meta wheel
# dlt_meta_whl = "/Volumes/catalog/schema/volume/wheels/dlt_meta_cds-0.0.10-py3-none-any.whl"
# %pip install $dlt_meta_whl
# dbutils.library.restartPython()

# COMMAND ----------
# DBTITLE 1,Step 1: Verify Dataflowspec Tables Exist
# Replace with your actual catalog/schema
CATALOG = "your_catalog"
SCHEMA = "your_schema"
LANDING_TABLE = f"{CATALOG}.{SCHEMA}.landing_dataflowspec_table"
REFINERY_TABLE = f"{CATALOG}.{SCHEMA}.refinery_dataflowspec_table"

print(f"Checking if dataflowspec tables exist...")
print(f"Landing table: {LANDING_TABLE}")
print(f"Refinery table: {REFINERY_TABLE}")

try:
    landing_df = spark.read.table(LANDING_TABLE)
    landing_count = landing_df.count()
    print(f"✅ Landing dataflowspec table exists with {landing_count} rows")

    if landing_count > 0:
        print("\nSample landing dataflowspec:")
        landing_df.select("dataFlowId", "dataFlowGroup", "sourceFormat", "targetFormat").show(5, truncate=False)
    else:
        print("⚠️  Landing dataflowspec table is EMPTY - no tables will be created!")

except Exception as e:
    print(f"❌ Error reading landing table: {e}")

try:
    refinery_df = spark.read.table(REFINERY_TABLE)
    refinery_count = refinery_df.count()
    print(f"✅ Refinery dataflowspec table exists with {refinery_count} rows")

    if refinery_count > 0:
        print("\nSample refinery dataflowspec:")
        refinery_df.select("dataFlowId", "dataFlowGroup", "sourceFormat", "targetFormat").show(5, truncate=False)
    else:
        print("⚠️  Refinery dataflowspec table is EMPTY - no tables will be created!")

except Exception as e:
    print(f"❌ Error reading refinery table: {e}")

# COMMAND ----------
# DBTITLE 1,Step 2: Test Loading Dataflowspec with DLT-META Code
from src.dataflow_spec import DataflowSpecUtils

# Set required Spark configs (same as your DLT pipeline settings)
spark.conf.set("landing.dataflowspecTable", LANDING_TABLE)
spark.conf.set("refinery.dataflowspecTable", REFINERY_TABLE)

# Optionally filter by group or dataflow IDs
# spark.conf.set("landing.group", "your_group_name")
# spark.conf.set("landing.dataflowIds", "'dataflow1','dataflow2'")

print("Testing DataflowSpecUtils.get_landing_dataflow_spec()...")
try:
    landing_specs = DataflowSpecUtils.get_landing_dataflow_spec(spark)
    print(f"✅ Loaded {len(landing_specs)} landing dataflow specs")

    if len(landing_specs) == 0:
        print("❌ NO DATAFLOW SPECS LOADED - This will cause NO_TABLES_IN_PIPELINE error!")
    else:
        print("\nFirst dataflow spec:")
        first_spec = landing_specs[0]
        print(f"  dataFlowId: {first_spec.dataFlowId}")
        print(f"  dataFlowGroup: {first_spec.dataFlowGroup}")
        print(f"  sourceFormat: {first_spec.sourceFormat}")
        print(f"  targetFormat: {first_spec.targetFormat}")
        print(f"  sourceDetails: {first_spec.sourceDetails}")

except Exception as e:
    print(f"❌ Error loading dataflow specs: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------
# DBTITLE 1,Step 3: Test Source Data Access
print("Testing if source data is accessible...")

# Get the first dataflow spec
if len(landing_specs) > 0:
    spec = landing_specs[0]
    source_details = dict(spec.sourceDetails)
    source_path = source_details.get("path")
    source_format = spec.sourceFormat

    print(f"Source path: {source_path}")
    print(f"Source format: {source_format}")

    try:
        # Try to read the source data
        test_df = spark.read.format(source_format).load(source_path)
        row_count = test_df.count()
        print(f"✅ Successfully read source data: {row_count} rows")
        print("\nSchema:")
        test_df.printSchema()
        print("\nSample data:")
        test_df.show(5)
    except Exception as e:
        print(f"❌ Error reading source data: {e}")
else:
    print("⚠️  No dataflow specs to test")

# COMMAND ----------
# DBTITLE 1,Step 4: Check DLT Pipeline Configuration
print("Expected DLT Pipeline Spark Configuration:")
print("=" * 60)
print(f"layer: landing  # or refinery, landing_refinery, etc.")
print(f"landing.dataflowspecTable: {LANDING_TABLE}")
print(f"refinery.dataflowspecTable: {REFINERY_TABLE}")
print(f"dlt_meta_whl: /Volumes/.../wheels/dlt_meta_cds-*.whl")
print("=" * 60)
print("\n✅ Make sure these are set in your DLT pipeline's Configuration tab!")
