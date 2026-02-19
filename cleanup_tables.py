# Script to drop all DLT-managed tables so you can start fresh
# Run this in a Databricks notebook ONLY if you want to delete all data

catalog = "privacy_nonprod"
landing_schema = "dltmeta_landing"
refinery_schema = "dltmeta_refinery"

# Tables from your onboarding.json
tables_to_drop = {
    "landing": [
        "customers",
        "customers_quarantine",
        "transactions",
        "transactions_quarantine",
        "products",
        "products_quarantine",
        "stores",
        "stores_quarantine"
    ],
    "refinery": [
        "customers",
        "transactions",
        "products",
        "stores"
    ]
}

print("=" * 80)
print("⚠️  WARNING: This will DELETE all data in the specified tables!")
print("=" * 80)
print(f"\nCatalog: {catalog}")
print(f"Landing Schema: {landing_schema}")
print(f"Refinery Schema: {refinery_schema}")
print("\nTables to drop:")
print(f"  Landing: {len(tables_to_drop['landing'])} tables")
print(f"  Refinery: {len(tables_to_drop['refinery'])} tables")

# Uncomment the lines below to actually drop the tables
# WARNING: This will delete all data!

# Drop landing tables
# for table in tables_to_drop['landing']:
#     try:
#         spark.sql(f"DROP TABLE IF EXISTS {catalog}.{landing_schema}.{table}")
#         print(f"✓ Dropped {catalog}.{landing_schema}.{table}")
#     except Exception as e:
#         print(f"✗ Error dropping {catalog}.{landing_schema}.{table}: {e}")

# Drop refinery tables
# for table in tables_to_drop['refinery']:
#     try:
#         spark.sql(f"DROP TABLE IF EXISTS {catalog}.{refinery_schema}.{table}")
#         print(f"✓ Dropped {catalog}.{refinery_schema}.{table}")
#     except Exception as e:
#         print(f"✗ Error dropping {catalog}.{refinery_schema}.{table}: {e}")

print("\n" + "=" * 80)
print("TO RUN THIS SCRIPT:")
print("=" * 80)
print("1. Review the catalog, schema, and table names above")
print("2. UNCOMMENT the drop table code blocks (lines with #)")
print("3. Run this script in a Databricks notebook")
print("4. Re-deploy your DLT pipeline")
