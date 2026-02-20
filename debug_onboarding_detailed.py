"""Detailed debug script to troubleshoot onboarding at each step."""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
import json

# Initialize Spark
spark = SparkSession.builder.appName("Debug Onboarding Detailed").getOrCreate()

# Configuration - UPDATE THESE
env = "nonprod"
onboarding_file = "/Volumes/dataservices_nonprod/dlt_meta_dataflowspecs_cds/dlt_meta_files/dltmeta_conf/cds/conf/onboarding/host_sales_ord_tran_0204/onboarding_sales_ord_tran.json"

print("=" * 100)
print("DETAILED ONBOARDING DEBUG")
print("=" * 100)

# Step 1: Read and validate onboarding file
print("\n[STEP 1] Reading onboarding file...")
try:
    onboarding_df = spark.read.option("multiline", "true").json(onboarding_file)
    total_flows = onboarding_df.count()
    print(f"✓ Successfully read onboarding file")
    print(f"  Total flows: {total_flows}")
    onboarding_df.select("data_flow_id", "data_flow_group").show(truncate=False)
except Exception as e:
    print(f"✗ Error reading onboarding file: {e}")
    exit(1)

# Step 2: Check flows with refinery layer
print("\n[STEP 2] Checking flows with REFINERY configuration...")
refinery_flows = onboarding_df.filter(f"refinery_database_{env} is not null")
refinery_count = refinery_flows.count()
print(f"  Flows with refinery_database_{env}: {refinery_count}")

if refinery_count == 0:
    print("  ⚠ WARNING: No flows have refinery configuration!")
else:
    print("  Flow details:")
    refinery_flows.select(
        "data_flow_id",
        "data_flow_group",
        f"refinery_database_{env}",
        "refinery_table",
        f"refinery_transformation_json_{env}"
    ).show(truncate=False)

# Step 3: Check flows with treasury layer
print("\n[STEP 3] Checking flows with TREASURY configuration...")
treasury_flows = onboarding_df.filter(f"treasury_database_{env} is not null")
treasury_count = treasury_flows.count()
print(f"  Flows with treasury_database_{env}: {treasury_count}")

if treasury_count == 0:
    print("  ⚠ WARNING: No flows have treasury configuration!")
else:
    print("  Flow details:")
    treasury_flows.select(
        "data_flow_id",
        "data_flow_group",
        f"treasury_database_{env}",
        "treasury_table",
        f"treasury_transformation_json_{env}"
    ).show(truncate=False)

# Step 4: Test reading transformation files for REFINERY
print("\n[STEP 4] Testing REFINERY transformation files...")
if refinery_count > 0:
    trans_schema = StructType([
        StructField("sql_query", StringType(), True),
        StructField("target_partition_cols", ArrayType(StringType(), True), True),
        StructField("target_table", StringType(), True),
    ])

    refinery_trans_files = refinery_flows.select(f"refinery_transformation_json_{env}").distinct().collect()

    for row in refinery_trans_files:
        trans_file = row[f"refinery_transformation_json_{env}"]
        if trans_file:
            print(f"\n  File: {trans_file}")
            try:
                trans_df = spark.read.option("multiline", "true").schema(trans_schema).json(trans_file)
                count = trans_df.count()
                print(f"  ✓ Read successfully: {count} rows")

                if count > 0:
                    print("  Transformation details:")
                    trans_df.select("target_table").show(truncate=False)

                    # Check if target_table matches any refinery_table
                    target_tables = [r['target_table'] for r in trans_df.select("target_table").collect()]
                    refinery_tables = [r['refinery_table'] for r in refinery_flows.select("refinery_table").distinct().collect()]

                    print(f"  Target tables in transformations: {target_tables}")
                    print(f"  Refinery tables in onboarding: {refinery_tables}")

                    matches = set(target_tables) & set(refinery_tables)
                    if matches:
                        print(f"  ✓ Matching tables found: {matches}")
                    else:
                        print(f"  ✗ WARNING: No matching tables! Join will produce 0 rows!")
                else:
                    print(f"  ✗ WARNING: Transformation file is empty!")

            except Exception as e:
                print(f"  ✗ Error reading transformation file: {e}")

# Step 5: Test reading transformation files for TREASURY
print("\n[STEP 5] Testing TREASURY transformation files...")
if treasury_count > 0:
    trans_schema = StructType([
        StructField("sql_query", StringType(), True),
        StructField("target_partition_cols", ArrayType(StringType(), True), True),
        StructField("target_table", StringType(), True),
    ])

    treasury_trans_files = treasury_flows.select(f"treasury_transformation_json_{env}").distinct().collect()

    for row in treasury_trans_files:
        trans_file = row[f"treasury_transformation_json_{env}"]
        if trans_file:
            print(f"\n  File: {trans_file}")
            try:
                trans_df = spark.read.option("multiline", "true").schema(trans_schema).json(trans_file)
                count = trans_df.count()
                print(f"  ✓ Read successfully: {count} rows")

                if count > 0:
                    print("  Transformation details:")
                    trans_df.select("target_table").show(truncate=False)

                    # Check if target_table matches any treasury_table
                    target_tables = [r['target_table'] for r in trans_df.select("target_table").collect()]
                    treasury_tables = [r['treasury_table'] for r in treasury_flows.select("treasury_table").distinct().collect()]

                    print(f"  Target tables in transformations: {target_tables}")
                    print(f"  Treasury tables in onboarding: {treasury_tables}")

                    matches = set(target_tables) & set(treasury_tables)
                    if matches:
                        print(f"  ✓ Matching tables found: {matches}")
                    else:
                        print(f"  ✗ WARNING: No matching tables! Join will produce 0 rows!")
                else:
                    print(f"  ✗ WARNING: Transformation file is empty!")

            except Exception as e:
                print(f"  ✗ Error reading transformation file: {e}")

# Step 6: Simulate the join for REFINERY
print("\n[STEP 6] Simulating REFINERY dataflowspec creation...")
if refinery_count > 0:
    try:
        # Create base refinery dataflow spec (simplified)
        refinery_rows = refinery_flows.collect()
        print(f"  Base flows to process: {len(refinery_rows)}")

        for rf_row in refinery_rows:
            flow_id = rf_row['data_flow_id']
            refinery_table = rf_row['refinery_table']
            trans_file = rf_row[f'refinery_transformation_json_{env}']

            print(f"\n  Processing flow {flow_id}:")
            print(f"    Refinery table: {refinery_table}")
            print(f"    Transformation file: {trans_file}")

            if trans_file:
                try:
                    trans_df = spark.read.option("multiline", "true").schema(trans_schema).json(trans_file)
                    trans_count = trans_df.count()
                    print(f"    Transformation rows: {trans_count}")

                    if trans_count > 0:
                        # Check if target_table matches
                        target_table = trans_df.first()['target_table']
                        print(f"    Target table from transformation: {target_table}")

                        if target_table == refinery_table:
                            print(f"    ✓ Match found! This flow should be onboarded.")
                        else:
                            print(f"    ✗ No match! Expected '{refinery_table}' but got '{target_table}'")
                except Exception as e:
                    print(f"    ✗ Error: {e}")
            else:
                print(f"    ✗ No transformation file specified")

    except Exception as e:
        print(f"  ✗ Error during simulation: {e}")

# Step 7: Simulate the join for TREASURY
print("\n[STEP 7] Simulating TREASURY dataflowspec creation...")
if treasury_count > 0:
    try:
        treasury_rows = treasury_flows.collect()
        print(f"  Base flows to process: {len(treasury_rows)}")

        for tr_row in treasury_rows:
            flow_id = tr_row['data_flow_id']
            treasury_table = tr_row['treasury_table']
            trans_file = tr_row[f'treasury_transformation_json_{env}']

            print(f"\n  Processing flow {flow_id}:")
            print(f"    Treasury table: {treasury_table}")
            print(f"    Transformation file: {trans_file}")

            if trans_file:
                try:
                    trans_df = spark.read.option("multiline", "true").schema(trans_schema).json(trans_file)
                    trans_count = trans_df.count()
                    print(f"    Transformation rows: {trans_count}")

                    if trans_count > 0:
                        target_table = trans_df.first()['target_table']
                        print(f"    Target table from transformation: {target_table}")

                        if target_table == treasury_table:
                            print(f"    ✓ Match found! This flow should be onboarded.")
                        else:
                            print(f"    ✗ No match! Expected '{treasury_table}' but got '{target_table}'")
                except Exception as e:
                    print(f"    ✗ Error: {e}")
            else:
                print(f"    ✗ No transformation file specified")

    except Exception as e:
        print(f"  ✗ Error during simulation: {e}")

print("\n" + "=" * 100)
print("DEBUG COMPLETE")
print("=" * 100)
print("\nDiagnosis:")
print("1. If transformation files can't be read → File format or path issue")
print("2. If target_table doesn't match refinery_table/treasury_table → Mismatch in configuration")
print("3. If matches found but onboarding still fails → Check actual onboarding logs for errors")
