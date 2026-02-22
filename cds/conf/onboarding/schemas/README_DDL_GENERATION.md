# DDL Schema File Generation Guide

## Overview

As part of the automatic table pre-creation feature, you need to provide DDL schema files for refinery and treasury tables. This guide explains how to generate these files.

## DDL File Format

DDL files use Spark DDL syntax with comma-separated column definitions:

```
column_name: data_type, column_name: data_type, ...
```

**Example:**
```
sales_ord_id: string, event_id: string, sales_ord_tran_id: string, sales_ord_tran_dt: date, host_sys_cd: string, host_vax_acct_num: string, host_acct_create_dt: date
```

## Method 1: Generate from Existing Tables (Recommended)

If your refinery and treasury tables already exist in any environment (dev, qa, nonprod), you can generate DDL files from them.

### Using the Helper Script

1. **In Databricks Notebook**, run:

```python
%run /Workspace/path/to/cds/conf/onboarding/schemas/generate_ddl_from_table

# Generate refinery DDL
generate_ddl_file(
    spark,
    "dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_dl_dlt",
    "/dbfs/cds/conf/onboarding/schemas/refinery_sales_ord_tran.ddl"
)

# Generate treasury DDL
generate_ddl_file(
    spark,
    "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt",
    "/dbfs/cds/conf/onboarding/schemas/treasury_sales_ord_tran.ddl"
)
```

2. **Verify the generated files:**

```bash
%fs head dbfs:/cds/conf/onboarding/schemas/refinery_sales_ord_tran.ddl
%fs head dbfs:/cds/conf/onboarding/schemas/treasury_sales_ord_tran.ddl
```

### Manual SQL Query Method

If you prefer manual approach:

```python
# In Databricks notebook
df = spark.table("catalog.database.table_name")
schema_fields = []
for field in df.schema.fields:
    schema_fields.append(f"{field.name}: {field.dataType.simpleString()}")
ddl = ", ".join(schema_fields)
print(ddl)

# Copy the output and save to a .ddl file
```

## Method 2: Create from SQL Transformations

If tables don't exist yet, analyze your SQL transformations to determine output schema:

1. **Review transformation YAML** (e.g., `transformations_sales_ord_step2_nonprod.yaml`)
2. **Identify output columns** from the SELECT statement
3. **Determine data types** based on:
   - Source column types
   - SQL functions used (CAST, CONCAT, etc.)
   - Transformation logic

4. **Write DDL file** with identified columns

**Example from Step 2 transformation:**

```sql
-- If your SQL has:
SELECT
    sales_ord_id,
    event_id,
    CAST(sales_ord_tran_dt AS DATE) as sales_ord_tran_dt,
    ...
```

**Create DDL:**
```
sales_ord_id: string, event_id: string, sales_ord_tran_dt: date, ...
```

## Method 3: Run Pipeline Once and Extract Schema

1. **Run your DLT pipeline** once manually (will fail on table pre-creation but DLT will create tables)
2. **Extract schema** from created tables using Method 1
3. **Save DDL files**
4. **Re-run pipeline** (will now pre-create tables successfully)

## Common Data Types Mapping

| Source Type | DDL Type |
|-------------|----------|
| VARCHAR, CHAR | string |
| INT, INTEGER | int |
| BIGINT | bigint |
| DECIMAL(p,s) | decimal(p,s) |
| DATE | date |
| TIMESTAMP | timestamp |
| DOUBLE, FLOAT | double |
| BOOLEAN | boolean |

## File Naming Convention

- **Refinery**: `refinery_{table_name}.ddl`
- **Treasury**: `treasury_{table_name}.ddl`

Examples:
- `refinery_sales_ord_tran.ddl`
- `treasury_sales_ord_tran.ddl`

## Validation

After creating DDL files, validate them:

```python
from pyspark.sql import types as T

# Read and parse DDL
ddl_content = spark.read.text("path/to/schema.ddl", wholetext=True).collect()[0]["value"]
schema = T._parse_datatype_string(ddl_content)

# Print schema
print(f"Fields: {len(schema.fields)}")
for field in schema.fields:
    print(f"  {field.name}: {field.dataType.simpleString()}")
```

## Troubleshooting

### Error: "Failed to read DDL file"
- **Cause**: File path incorrect or file doesn't exist
- **Solution**: Check path is accessible from Databricks (use DBFS paths)

### Error: "Failed to parse DDL schema"
- **Cause**: Invalid DDL syntax
- **Solution**: Ensure format is `column: type, column: type` (comma-separated)

### Error: "Schema path not provided"
- **Cause**: Missing schema path in onboarding configuration
- **Solution**: Add `refinery_schema_path` and `treasury_schema_path` to your onboarding JSON

## Next Steps

After generating DDL files:

1. ✓ Create DDL files for all refinery tables
2. ✓ Create DDL files for all treasury tables
3. ✓ Update onboarding configuration with schema paths
4. ✓ Run pipeline - tables will be pre-created automatically!

## Questions?

See the main documentation or check the generated example DDL files in this directory.
