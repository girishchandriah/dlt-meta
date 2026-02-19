# Schema Definition in DLT-META

## Two Approaches for Defining Landing Table Schema

### ❌ WRONG: Using `landing_source_schema_map` (Inline JSON)
```json
{
  "landing_source_schema_map": {
    "column1": "string",
    "column2": "int",
    "column3": "date"
  }
}
```
**This field does not exist in dlt-meta!**

### ✅ CORRECT: Using `source_schema_path` (External DDL File)
```json
{
  "source_details": {
    "source_database": "my_database",
    "source_table": "my_table",
    "source_path_prod": "s3://bucket/path",
    "source_schema_path": "/Volumes/catalog/schema/config/schemas/my_table.ddl"
  }
}
```

## DDL File Format

### Syntax
```
column_name: data_type, column_name: data_type, ...
```

### Supported Data Types
- **String types**: `string`
- **Numeric types**: `int`, `bigint`, `decimal(p,s)`, `double`, `float`
- **Date/Time types**: `date`, `timestamp`
- **Boolean**: `boolean`
- **Complex types**: `array<type>`, `struct<field:type,...>`, `map<keyType,valueType>`

### Example DDL Files

#### Simple Schema
**File**: `customers.ddl`
```
customer_id: int, first_name: string, last_name: string, email: string, dob: date
```

#### Schema with Decimal and Timestamp
**File**: `transactions.ddl`
```
transaction_id: bigint, customer_id: int, transaction_date: date, amount: decimal(18,4), created_timestamp: timestamp
```

#### Schema with CDC Columns
**File**: `orders_cdc.ddl`
```
Op: string, dmsTimestamp: timestamp, order_id: int, customer_id: int, order_date: date, total_amount: decimal(18,2), status: string
```

#### Complex Schema (Our Sales Order Transaction)
**File**: `dsn_sales_ord_tran.ddl`
```
system: string, vaxacct: string, accountdate: string, vaxevid: string, transnum: string, transdate: string, transopcode: string, printdate: string, printopcode: string, voiddate: string, voidopcode: string, printflag: string, voidflag: string, tickets: string, seattypes: string, totaldollars: string, ticketvalue: string, servicedollars: string, facilityfee: string, exchangefee: string, exmopid: string, exmopname: string, firstqualifier: string, seatlocation: string, acmode: string, pricelevelid: string, pricelevelname: string, tax: string, zonecharge: string, servicetax: string, servicetax2: string, tkt_transfer_sent_flg: string, tkt_transfer_rcvd_flg: string, tkt_transfer_surrender_flg: string, resale_status_nm: string, vcode_used: string, mimic_flag: string
```

## File Location Best Practices

### Option 1: Unity Catalog Volumes (Recommended)
```json
"source_schema_path": "/Volumes/{catalog}/{schema}/dlt_meta_conf/schemas/{table}.ddl"
```

**Example**:
```json
"source_schema_path": "/Volumes/dataservices_prod/treasury_teradata_base/dlt_meta_conf/schemas/dsn_sales_ord_tran.ddl"
```

### Option 2: DBFS (Legacy)
```json
"source_schema_path": "/dbfs/dlt-meta/schemas/{table}.ddl"
```

**Example**:
```json
"source_schema_path": "/dbfs/dlt-meta/schemas/dsn_sales_ord_tran.ddl"
```

### Option 3: Workspace Files
```json
"source_schema_path": "/Workspace/Users/{email}/dlt-meta/schemas/{table}.ddl"
```

## Creating DDL Files

### From Existing Data
If you have sample data files, use Spark to infer schema:

```python
# Read sample file
df = spark.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", "|") \
    .option("inferSchema", "true") \
    .load("s3://bucket/path/sample_file.csv")

# Generate DDL string
schema_ddl = ", ".join([f"{field.name}: {field.dataType.simpleString()}" for field in df.schema.fields])
print(schema_ddl)
```

### From Table Schema
If landing table already exists:

```python
# Get table schema
table_schema = spark.table("catalog.schema.table_name").schema

# Generate DDL string
schema_ddl = ", ".join([f"{field.name}: {field.dataType.simpleString()}" for field in table_schema.fields])
print(schema_ddl)
```

### Manually (Your Original YAML)
From your YAML's `target_table_schema` section:

```yaml
target_table_schema:
  system string,
  vaxacct string,
  accountdate string,
  ...
```

Convert to DDL format:
```
system: string, vaxacct: string, accountdate: string, ...
```

## Why Use DDL Files vs Inline Schema?

### Advantages of DDL Files
✅ **Reusability**: One schema file can be referenced by multiple data flows
✅ **Maintainability**: Update schema in one place
✅ **Version Control**: Easier to track schema changes in Git
✅ **Readability**: Cleaner onboarding JSON files
✅ **Consistency**: Same format across all dlt-meta projects

### When Schema is Optional
If your source has `header: true` and you're using Auto Loader with schema inference:

```json
"landing_reader_options": {
  "cloudFiles.format": "csv",
  "cloudFiles.inferColumnTypes": "true",
  "header": "true"
}
```

**You can omit `source_schema_path`** - dlt-meta will auto-infer the schema.

### When Schema is Required
For sources **without headers** (like your pipe-delimited files):

```json
"landing_reader_options": {
  "cloudFiles.format": "csv",
  "header": "false",
  "delimiter": "|"
}
```

**You MUST provide `source_schema_path`** - otherwise Auto Loader won't know column names.

## Environment-Specific DDL Files

If schema differs by environment (rare), use environment suffixes:

```json
"source_details": {
  "source_schema_path_nonprod": "/Volumes/dataservices_nonprod/.../schema_nonprod.ddl",
  "source_schema_path_preprod": "/Volumes/dataservices_preprod/.../schema_preprod.ddl",
  "source_schema_path_prod": "/Volumes/dataservices_prod/.../schema.ddl"
}
```

**Note**: Usually schema is the same across environments, so a single `source_schema_path` is sufficient.

## Uploading DDL Files to Unity Catalog Volumes

### Step 1: Create Directory in Volume
```sql
-- In Databricks SQL or notebook
CREATE VOLUME IF NOT EXISTS dataservices_prod.treasury_teradata_base.dlt_meta_conf;
```

### Step 2: Upload via Databricks UI
1. Navigate to **Catalog Explorer**
2. Browse to your volume: `dataservices_prod` → `treasury_teradata_base` → `dlt_meta_conf`
3. Click **Upload**
4. Create folder `schemas` if needed
5. Upload your `.ddl` file

### Step 3: Upload via CLI (Alternative)
```bash
databricks fs cp schemas/dsn_sales_ord_tran.ddl \
  dbfs:/Volumes/dataservices_prod/treasury_teradata_base/dlt_meta_conf/schemas/dsn_sales_ord_tran.ddl
```

### Step 4: Verify
```python
# In notebook
display(dbutils.fs.ls("/Volumes/dataservices_prod/treasury_teradata_base/dlt_meta_conf/schemas"))
```

## Common Issues

### Issue 1: Schema Path Not Found
**Error**: `Path does not exist: /Volumes/...`

**Solutions**:
1. Verify volume exists: `SHOW VOLUMES IN {catalog}.{schema}`
2. Check file upload completed successfully
3. Ensure path uses `/Volumes/` prefix (not `dbfs:/Volumes/`)

### Issue 2: Schema Mismatch
**Error**: `Column 'xyz' not found in input data`

**Solutions**:
1. Verify column order in DDL matches CSV column order (when `header: false`)
2. Check delimiter is correct (e.g., `|` vs `,`)
3. Ensure all columns from source file are included in DDL

### Issue 3: Data Type Conversion Errors
**Error**: `Cannot cast string to date`

**Solutions**:
1. Use `string` type in landing DDL for date columns
2. Apply date conversions in refinery transformation SQL
3. This matches your original YAML approach (all columns as strings in landing)

## Best Practices

1. **Use descriptive file names**: `{source_system}_{table_name}.ddl`
   - Example: `jb_edw_dsn_sales_ord_tran.ddl`

2. **Keep landing schema simple**: Use `string` for most columns, cast later in refinery
   - Avoids parsing errors during ingestion
   - Handles malformed data gracefully

3. **Document schema changes**: Use Git commit messages for DDL file changes

4. **Test with sample data**: Verify schema matches actual source files

5. **Use consistent formatting**: One line, comma-separated (easier to read in single line)

## Example: Full Onboarding Configuration

```json
{
  "data_flow_id": "204",
  "data_flow_group": "sales_ord",
  "source_system": "jb_edw",
  "source_format": "cloudFiles",
  "source_details": {
    "source_database": "hostfile",
    "source_table": "dsn_sales_ord_tran",
    "source_path_prod": "s3a://bucket/path/file-ingest",
    "source_schema_path": "/Volumes/dataservices_prod/treasury_teradata_base/dlt_meta_conf/schemas/dsn_sales_ord_tran.ddl"
  },
  "landing_catalog_prod": "dataservices_prod",
  "landing_database_prod": "treasury_teradata_base",
  "landing_table": "pfocusst_ff_src_hst_dsn_trans",
  "landing_reader_options": {
    "cloudFiles.format": "csv",
    "header": "false",
    "delimiter": "|",
    "cloudFiles.rescuedDataColumn": "_rescued_data"
  }
}
```

## Summary

| Field | Location | Purpose |
|-------|----------|---------|
| `source_schema_path` | Inside `source_details` | Points to DDL file with column definitions |
| `landing_source_schema_map` | ❌ Does not exist | Don't use this! |
| DDL file format | External `.ddl` file | `column: type, column: type, ...` |
| Recommended location | Unity Catalog Volumes | `/Volumes/{catalog}/{schema}/dlt_meta_conf/schemas/{table}.ddl` |
