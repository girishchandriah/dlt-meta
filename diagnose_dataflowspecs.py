# ============================================================================
# DIAGNOSTIC: Check dataflowspec configuration for issues
# ============================================================================
# Run this in a Databricks notebook to diagnose configuration problems
# ============================================================================

# UPDATE THESE WITH YOUR ACTUAL VALUES
catalog = "privacy_nonprod"
dlt_meta_schema = "dlt_meta_dataflowspecs"  # Schema where dataflowspec tables are stored
landing_table = "landing_dataflowspec"
refinery_table = "refinery_dataflowspec"
group_name = "A1"

print("=" * 80)
print("DIAGNOSTIC: DLT-META DATAFLOWSPEC CONFIGURATION CHECK")
print("=" * 80)

# ============================================================================
# CHECK 1: Verify schemas exist
# ============================================================================
print("\n" + "=" * 80)
print("CHECK 1: Verify Required Schemas Exist")
print("=" * 80)

required_schemas = ["dltmeta_landing", "dltmeta_refinery", dlt_meta_schema]
for schema_name in required_schemas:
    try:
        spark.sql(f"DESCRIBE SCHEMA {catalog}.{schema_name}")
        print(f"✓ Schema exists: {catalog}.{schema_name}")
    except Exception as e:
        print(f"✗ Schema MISSING: {catalog}.{schema_name}")
        print(f"  Error: {e}")
        print(f"  FIX: Run setup_schemas.sql to create missing schemas")

# ============================================================================
# CHECK 2: Verify dataflowspec tables exist and have data
# ============================================================================
print("\n" + "=" * 80)
print("CHECK 2: Verify Dataflowspec Tables")
print("=" * 80)

# Check landing dataflowspec
print(f"\n📋 Landing Dataflowspec: {catalog}.{dlt_meta_schema}.{landing_table}")
try:
    landing_df = spark.read.table(f"{catalog}.{dlt_meta_schema}.{landing_table}")
    count = landing_df.count()
    print(f"  ✓ Table exists with {count} rows")

    if count > 0:
        # Filter by group
        group_df = landing_df.filter(f"dataFlowGroup = '{group_name}'")
        group_count = group_df.count()
        print(f"  ✓ Found {group_count} dataflows for group '{group_name}'")

        if group_count > 0:
            print(f"\n  📊 Landing Dataflows in group '{group_name}':")
            group_df.select(
                "dataFlowId",
                "dataFlowGroup",
                "sourceFormat",
                "sourceDetails",
                "targetDetails"
            ).show(truncate=False)
        else:
            print(f"  ✗ No dataflows found for group '{group_name}'")
            print("\n  Available groups:")
            landing_df.select("dataFlowGroup").distinct().show(truncate=False)
    else:
        print("  ✗ Table is EMPTY!")
        print("  FIX: Run onboarding process first")

except Exception as e:
    print(f"  ✗ Error: {e}")
    print(f"  FIX: Run onboarding to create this table")

# Check refinery dataflowspec
print(f"\n📋 Refinery Dataflowspec: {catalog}.{dlt_meta_schema}.{refinery_table}")
try:
    refinery_df = spark.read.table(f"{catalog}.{dlt_meta_schema}.{refinery_table}")
    count = refinery_df.count()
    print(f"  ✓ Table exists with {count} rows")

    if count > 0:
        # Filter by group
        group_df = refinery_df.filter(f"dataFlowGroup = '{group_name}'")
        group_count = group_df.count()
        print(f"  ✓ Found {group_count} dataflows for group '{group_name}'")

        if group_count > 0:
            print(f"\n  📊 Refinery Dataflows in group '{group_name}':")
            group_df.select(
                "dataFlowId",
                "dataFlowGroup",
                "sourceDetails",
                "targetDetails"
            ).show(truncate=False)

            # CHECK 3: Verify refinery sources point to landing tables
            print("\n  🔍 Checking refinery source configuration...")
            for row in group_df.collect():
                source_details = row.sourceDetails
                target_details = row.targetDetails

                # Parse source details (may be string or dict)
                if isinstance(source_details, str):
                    import json
                    source_details = json.loads(source_details)

                source_table = f"{source_details.get('catalog', catalog)}.{source_details['database']}.{source_details['table']}"
                target_table = f"{target_details.get('catalog', catalog)}.{target_details['database']}.{target_details['table']}"

                print(f"\n  Dataflow {row.dataFlowId}:")
                print(f"    Source: {source_table}")
                print(f"    Target: {target_table}")

                # Check if source is in landing schema
                if "dltmeta_landing" in source_table or "landing" in source_table:
                    print(f"    ✓ Source correctly points to landing layer")
                else:
                    print(f"    ⚠️  WARNING: Source doesn't appear to be in landing layer")
        else:
            print(f"  ✗ No dataflows found for group '{group_name}'")
            print("\n  Available groups:")
            refinery_df.select("dataFlowGroup").distinct().show(truncate=False)
    else:
        print("  ✗ Table is EMPTY!")
        print("  FIX: Run onboarding process first")

except Exception as e:
    print(f"  ✗ Error: {e}")
    print(f"  FIX: Run onboarding to create this table")

# ============================================================================
# CHECK 4: Verify target schemas can be written to
# ============================================================================
print("\n" + "=" * 80)
print("CHECK 4: Verify Write Permissions")
print("=" * 80)

test_table_name = f"{catalog}.dltmeta_landing.dlt_meta_permission_test"
try:
    # Try to create a test table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {test_table_name} (
            test_col STRING
        ) USING DELTA
    """)
    print(f"✓ Can create tables in {catalog}.dltmeta_landing")

    # Clean up
    spark.sql(f"DROP TABLE IF EXISTS {test_table_name}")
    print(f"✓ Can drop tables in {catalog}.dltmeta_landing")

except Exception as e:
    print(f"✗ Permission issue in {catalog}.dltmeta_landing")
    print(f"  Error: {e}")
    print(f"  FIX: Grant CREATE TABLE permission on schema")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY & RECOMMENDATIONS")
print("=" * 80)

print("""
If you see any ✗ errors above:

1. Missing schemas:
   - Run setup_schemas.sql to create schemas

2. Empty dataflowspec tables:
   - Go to Lakehouse App → Onboarding tab
   - Fill in the onboarding form
   - Click "Onboard"

3. Wrong group name:
   - Update your pipeline configuration to use the correct group
   - Or update onboarding.json and re-run onboarding

4. Permission issues:
   - Contact your Databricks admin
   - Request CREATE TABLE permission on schemas

5. Refinery sources don't point to landing:
   - Check your onboarding.json file
   - Verify refinery sources point to landing tables
   - Re-run onboarding with corrected configuration

Your pipeline configuration should be:
{
  "layer": "landing_refinery",
  "landing.group": "A1",
  "refinery.group": "A1",
  "landing.dataflowspecTable": "privacy_nonprod.dlt_meta_dataflowspecs.landing_dataflowspec",
  "refinery.dataflowspecTable": "privacy_nonprod.dlt_meta_dataflowspecs.refinery_dataflowspec",
  "dlt_meta_whl": "/path/to/wheel.whl"
}
""")
