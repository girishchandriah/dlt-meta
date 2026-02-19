# Debug script to check dataflowspec tables
# Run this in a Databricks notebook to diagnose the issue

# Replace these with your actual values
catalog_name = "your_catalog"
schema_name = "your_schema"
landing_table = "landing_dataflowspec"
refinery_table = "refinery_dataflowspec"
group_name = "your_group"  # The group you're trying to run

# Check if landing dataflowspec table exists and has data
print("=" * 80)
print("CHECKING LANDING DATAFLOWSPEC TABLE")
print("=" * 80)

try:
    landing_df = spark.read.table(f"{catalog_name}.{schema_name}.{landing_table}")
    count = landing_df.count()
    print(f"✓ Table exists: {catalog_name}.{schema_name}.{landing_table}")
    print(f"✓ Total rows: {count}")

    if count > 0:
        print(f"\n📋 Columns: {landing_df.columns}")
        print(f"\n📊 Sample data (first 5 rows):")
        landing_df.select("dataFlowId", "dataFlowGroup", "sourceFormat", "targetDetails").show(5, truncate=False)

        # Check for the specific group
        group_df = landing_df.filter(f"dataFlowGroup = '{group_name}'")
        group_count = group_df.count()
        print(f"\n🎯 Rows with dataFlowGroup='{group_name}': {group_count}")

        if group_count == 0:
            print(f"\n⚠️  WARNING: No rows found for group '{group_name}'")
            print("\nAvailable groups:")
            landing_df.select("dataFlowGroup").distinct().show(truncate=False)
    else:
        print("\n❌ Table is EMPTY - no dataflowspecs found!")
        print("You need to run the onboarding process first.")

except Exception as e:
    print(f"❌ Error accessing table: {e}")
    print("\nPossible issues:")
    print("1. Table doesn't exist - run onboarding first")
    print("2. Incorrect catalog/schema/table name")
    print("3. Permission issues")

# Check if refinery dataflowspec table exists and has data
print("\n" + "=" * 80)
print("CHECKING REFINERY DATAFLOWSPEC TABLE")
print("=" * 80)

try:
    refinery_df = spark.read.table(f"{catalog_name}.{schema_name}.{refinery_table}")
    count = refinery_df.count()
    print(f"✓ Table exists: {catalog_name}.{schema_name}.{refinery_table}")
    print(f"✓ Total rows: {count}")

    if count > 0:
        print(f"\n📋 Columns: {refinery_df.columns}")
        print(f"\n📊 Sample data (first 5 rows):")
        refinery_df.select("dataFlowId", "dataFlowGroup", "sourceDetails", "targetDetails").show(5, truncate=False)

        # Check for the specific group
        group_df = refinery_df.filter(f"dataFlowGroup = '{group_name}'")
        group_count = group_df.count()
        print(f"\n🎯 Rows with dataFlowGroup='{group_name}': {group_count}")

        if group_count == 0:
            print(f"\n⚠️  WARNING: No rows found for group '{group_name}'")
            print("\nAvailable groups:")
            refinery_df.select("dataFlowGroup").distinct().show(truncate=False)
    else:
        print("\n❌ Table is EMPTY - no dataflowspecs found!")
        print("You need to run the onboarding process first.")

except Exception as e:
    print(f"❌ Error accessing table: {e}")
    print("\nPossible issues:")
    print("1. Table doesn't exist - run onboarding first")
    print("2. Incorrect catalog/schema/table name")
    print("3. Permission issues")

print("\n" + "=" * 80)
print("NEXT STEPS")
print("=" * 80)
print("If tables are empty or don't exist, you need to run the onboarding process.")
print("See: demo/conf/onboarding.json for configuration examples")
print("\nTo onboard dataflowspecs, use the dlt-meta CLI:")
print("  python src/cli.py '{\"command\": \"onboard\", ...}'")
