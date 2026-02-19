# ============================================================================
# DIAGNOSE: Why Landing Tables Are Not Being Created
# ============================================================================
# Run this in a Databricks notebook to find the root cause
# ============================================================================

import json

catalog = "privacy_nonprod"
dlt_meta_schema = "dlt_meta_dataflowspecs"
landing_table = "landing_dataflowspec"
group_name = "A1"

print("=" * 80)
print("DIAGNOSING LANDING LAYER ISSUE")
print("=" * 80)

# ============================================================================
# CHECK 1: What are the landing dataflowspecs?
# ============================================================================
print("\n" + "=" * 80)
print("CHECK 1: Landing Dataflowspecs for Group 'A1'")
print("=" * 80)

landing_df = spark.read.table(f"{catalog}.{dlt_meta_schema}.{landing_table}")
landing_specs = landing_df.filter(f"dataFlowGroup = '{group_name}'")

print(f"\nFound {landing_specs.count()} landing dataflowspec(s)")

if landing_specs.count() == 0:
    print("❌ PROBLEM: No landing specs found for group 'A1'")
    print("FIX: Re-run onboarding with correct group name")
else:
    print("\n📋 Landing Dataflowspecs:")
    landing_specs.select(
        "dataFlowId",
        "dataFlowGroup",
        "sourceFormat",
        "sourceDetails",
        "targetDetails"
    ).show(truncate=False)

    # ============================================================================
    # CHECK 2: Verify target details point to correct schema
    # ============================================================================
    print("\n" + "=" * 80)
    print("CHECK 2: Verify Target Schema Configuration")
    print("=" * 80)

    for row in landing_specs.collect():
        target_details = row.targetDetails
        source_details = row.sourceDetails

        # Parse if string
        if isinstance(target_details, str):
            target_details = json.loads(target_details)
        if isinstance(source_details, str):
            source_details = json.loads(source_details)

        target_catalog = target_details.get('catalog', catalog)
        target_database = target_details.get('database', '')
        target_table = target_details.get('table', '')

        print(f"\nDataFlow ID: {row.dataFlowId}")
        print(f"  Target: {target_catalog}.{target_database}.{target_table}")

        # Check if target database is correct
        if target_database == "dltmeta_landing":
            print(f"  ✓ Target database is correct: {target_database}")
        else:
            print(f"  ⚠️  WARNING: Target database is '{target_database}', expected 'dltmeta_landing'")
            print(f"     FIX: Update onboarding.json 'landing_database_prod' field")

    # ============================================================================
    # CHECK 3: Verify source data paths exist and have data
    # ============================================================================
    print("\n" + "=" * 80)
    print("CHECK 3: Verify Source Data Paths")
    print("=" * 80)

    for row in landing_specs.collect():
        source_details = row.sourceDetails

        # Parse if string
        if isinstance(source_details, str):
            source_details = json.loads(source_details)

        source_format = row.sourceFormat

        # Check cloudFiles paths
        if source_format == "cloudFiles":
            if "source_path" in source_details:
                source_path = source_details["source_path"]
            elif "path" in source_details:
                source_path = source_details["path"]
            else:
                source_path = None
                print(f"\n❌ DataFlow {row.dataFlowId}: No source path found!")
                continue

            print(f"\nDataFlow ID: {row.dataFlowId}")
            print(f"  Source Format: {source_format}")
            print(f"  Source Path: {source_path}")

            # Try to list the path
            try:
                files = dbutils.fs.ls(source_path)
                file_count = len(files)
                print(f"  ✓ Path exists with {file_count} file(s)/folder(s)")

                if file_count == 0:
                    print(f"  ⚠️  WARNING: Path is EMPTY - no data to process!")
                else:
                    # Show first few files
                    print(f"  📁 Contents (first 5):")
                    for f in files[:5]:
                        print(f"     - {f.name} ({f.size} bytes)")

            except Exception as e:
                print(f"  ❌ Path DOES NOT EXIST or is not accessible!")
                print(f"     Error: {e}")
                print(f"     FIX: Create the path or update onboarding.json with correct path")

        elif source_format == "delta":
            source_catalog = source_details.get('catalog', catalog)
            source_database = source_details.get('database', '')
            source_table = source_details.get('table', '')
            source_full = f"{source_catalog}.{source_database}.{source_table}"

            print(f"\nDataFlow ID: {row.dataFlowId}")
            print(f"  Source Format: {source_format}")
            print(f"  Source Table: {source_full}")

            try:
                source_df = spark.read.table(source_full)
                count = source_df.count()
                print(f"  ✓ Source table exists with {count} rows")

                if count == 0:
                    print(f"  ⚠️  WARNING: Source table is EMPTY!")
            except Exception as e:
                print(f"  ❌ Source table DOES NOT EXIST!")
                print(f"     Error: {e}")

# ============================================================================
# CHECK 4: Verify the target schema exists
# ============================================================================
print("\n" + "=" * 80)
print("CHECK 4: Verify Target Schema Exists")
print("=" * 80)

try:
    spark.sql(f"DESCRIBE SCHEMA {catalog}.dltmeta_landing")
    print(f"✓ Target schema exists: {catalog}.dltmeta_landing")
except Exception as e:
    print(f"❌ Target schema DOES NOT EXIST: {catalog}.dltmeta_landing")
    print(f"   Error: {e}")
    print(f"   FIX: Run setup_schemas.sql to create the schema")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print("""
Common Issues and Fixes:

1. ❌ Source paths don't exist:
   - Create the directories: dbutils.fs.mkdirs("path")
   - Add sample data files
   - OR update onboarding.json with correct paths and re-onboard

2. ❌ Source paths are empty:
   - Add data files to the source paths
   - Verify file format matches reader options (CSV, JSON, etc.)

3. ❌ Target schema doesn't exist:
   - Run setup_schemas.sql to create schemas

4. ❌ Wrong target database in targetDetails:
   - Update onboarding.json 'landing_database_prod' field
   - Re-run onboarding

5. ⚠️  If everything looks correct but landing tables still aren't created:
   - Check the DLT pipeline event log for specific errors
   - Try running a "landing" only pipeline first (not "landing_refinery")
   - Set "layer": "landing" in pipeline config to isolate the issue

Next Steps:
1. Fix any issues identified above
2. If source paths are missing, copy demo data:
   dbutils.fs.cp("/databricks-datasets/retail-org/customers",
                 "/Volumes/privacy_nonprod/.../customers",
                 recurse=True)
3. Re-run your pipeline
""")
