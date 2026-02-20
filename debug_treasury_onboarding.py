"""Debug script to troubleshoot treasury onboarding."""
import json
from pyspark.sql import SparkSession

# Initialize Spark (adjust if needed for your environment)
spark = SparkSession.builder.appName("Debug Treasury Onboarding").getOrCreate()

# Configuration - UPDATE THESE VALUES
env = "nonprod"
catalog = "dataservices_nonprod"
schema = "dlt_meta_dataflowspecs_cds"
onboarding_file = "/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/cds/conf/onboarding/host_sales_ord_tran_0204/onboarding_sales_ord_tran.json"

print("=" * 80)
print("TREASURY ONBOARDING DEBUG SCRIPT")
print("=" * 80)

# Step 1: Read onboarding file
print("\n1. Reading onboarding file...")
onboarding_df = spark.read.option("multiline", "true").json(onboarding_file)
print(f"   Total flows in onboarding file: {onboarding_df.count()}")

# Step 2: Filter flows with treasury layer
print("\n2. Checking flows with treasury configuration...")
treasury_flows = onboarding_df.filter(f"treasury_database_{env} is not null")
print(f"   Flows with treasury_database_{env}: {treasury_flows.count()}")
treasury_flows.select("data_flow_id", "data_flow_group",
                      f"treasury_database_{env}", "treasury_table",
                      f"treasury_transformation_json_{env}").show(truncate=False)

# Step 3: Check transformation JSON files
print("\n3. Checking transformation JSON/YAML files...")
transformation_files = treasury_flows.select(f"treasury_transformation_json_{env}").distinct().collect()

for row in transformation_files:
    trans_file = row[f"treasury_transformation_json_{env}"]
    if trans_file:
        print(f"\n   File: {trans_file}")
        try:
            # Try reading as JSON
            trans_df = spark.read.option("multiline", "true").json(trans_file)
            print(f"   ✓ Successfully read as JSON")
            print(f"   Columns: {trans_df.columns}")
            print(f"   Row count: {trans_df.count()}")

            # Check for required fields
            if "target_table" in trans_df.columns:
                target_tables = trans_df.select("target_table").distinct().collect()
                print(f"   target_table values: {[r['target_table'] for r in target_tables]}")
            else:
                print(f"   ⚠ WARNING: 'target_table' field is MISSING!")

            if "sql_query" in trans_df.columns:
                print(f"   ✓ Has sql_query field")
            else:
                print(f"   ⚠ WARNING: 'sql_query' field is MISSING!")

            trans_df.show(truncate=False)
        except Exception as e:
            print(f"   ✗ Error reading file: {e}")

# Step 4: Check existing treasury dataflowspec table
print("\n4. Checking treasury dataflowspec table...")
try:
    treasury_table = f"{catalog}.{schema}.treasury_dataflowspec"
    treasury_df = spark.read.table(treasury_table)
    print(f"   Table: {treasury_table}")
    print(f"   Row count: {treasury_df.count()}")

    if treasury_df.count() > 0:
        print("\n   Sample rows:")
        treasury_df.select("dataFlowId", "dataFlowGroup", "sourceDetails",
                          "targetDetails").show(truncate=False)
    else:
        print("   ⚠ TABLE IS EMPTY")
        print("\n   Expected:")
        print("   - Flow 204: From refinery table to treasury")
        print("   - Flow 204-intermediate: From refinery table to treasury")
        print("   - Flow 204-batch-update: From refinery table (order events) to treasury")
except Exception as e:
    print(f"   ✗ Error reading table: {e}")

# Step 5: Check if refinery tables exist (treasury source)
print("\n5. Checking source tables for treasury flows...")
source_checks = [
    ("refinery_nonprod.pfocusst_dsn_st_trans_dl_dlt", "Flow 204 source"),
    ("refinery_nonprod.pfocusst_dsn_st_trans_dl1_dlt", "Flow 204-intermediate source"),
    ("refinery_nonprod.pfocusst_mdb_st_order_events_0208_dl", "Flow 204-batch-update source")
]

for table, description in source_checks:
    try:
        full_table = f"{catalog}.{table}"
        df = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}")
        count = df.collect()[0]['cnt']
        print(f"   ✓ {description}: {full_table} ({count} rows)")
    except Exception as e:
        print(f"   ✗ {description}: {full_table} - {str(e)[:100]}")

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
print("\nNext steps:")
print("1. If transformation files are missing 'target_table', they need to be updated")
print("2. If row count is 0, check logs for errors during onboarding")
print("3. Compare transformation file format with refinery transformations that work")
