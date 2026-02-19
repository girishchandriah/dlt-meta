# Quick script to check what dataFlowGroup values exist in your dataflowspec tables
# Run this in a Databricks notebook

# UPDATE THESE WITH YOUR ACTUAL VALUES
catalog_name = "privacy_nonprod"
schema_name = "your_dlt_meta_schema"  # Update this!
landing_table = "landing_dataflowspec"
refinery_table = "refinery_dataflowspec"

print("=" * 80)
print("CHECKING ACTUAL GROUP NAMES IN YOUR TABLES")
print("=" * 80)

# Check landing groups
print("\n📋 LANDING GROUPS:")
try:
    landing_groups = spark.read.table(f"{catalog_name}.{schema_name}.{landing_table}") \
        .select("dataFlowGroup") \
        .distinct() \
        .orderBy("dataFlowGroup")

    landing_groups.show(truncate=False)

    group_list = [row.dataFlowGroup for row in landing_groups.collect()]
    print(f"\n✓ Found {len(group_list)} group(s): {group_list}")

except Exception as e:
    print(f"❌ Error: {e}")

# Check refinery groups
print("\n📋 REFINERY GROUPS:")
try:
    refinery_groups = spark.read.table(f"{catalog_name}.{schema_name}.{refinery_table}") \
        .select("dataFlowGroup") \
        .distinct() \
        .orderBy("dataFlowGroup")

    refinery_groups.show(truncate=False)

    group_list = [row.dataFlowGroup for row in refinery_groups.collect()]
    print(f"\n✓ Found {len(group_list)} group(s): {group_list}")

except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 80)
print("WHAT TO DO NEXT")
print("=" * 80)
print("Update your DLT pipeline configuration to use one of the groups shown above.")
print("\nExample pipeline configuration:")
print('{')
print('  "layer": "landing_refinery",')
print(f'  "landing.group": "{group_list[0] if group_list else "YOUR_GROUP"}",')
print(f'  "refinery.group": "{group_list[0] if group_list else "YOUR_GROUP"}",')
print('  "landing.dataflowspecTable": "catalog.schema.landing_dataflowspec",')
print('  "refinery.dataflowspecTable": "catalog.schema.refinery_dataflowspec"')
print('}')
