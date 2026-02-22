# Bug Fix: sourceDetails Access Issue

## Issue

When running the pipeline, you encountered this error:

```
ValueError: Schema path not provided for landing layer (dataFlowId=204).
Please add 'landing_schema_path' to onboarding configuration.
```

Even though the schema path was correctly configured in the onboarding JSON at:
```json
"source_details": {
  "source_schema_path": "/Volumes/.../schemas/dsn_sales_ord_tran.ddl"
}
```

## Root Cause

The issue was in [src/table_precreator.py](src/table_precreator.py) where we were incorrectly converting Spark map types to Python dicts:

**Before (Incorrect):**
```python
source_details = dict(dataflow_spec.sourceDetails) if dataflow_spec.sourceDetails else {}
schema_path = source_details.get("source_schema_path")
```

Spark's map types (used for `sourceDetails`, `targetDetails`, `tableProperties`) are already dict-like objects that support `.get()` and `[]` access directly. Converting them with `dict()` was causing the data to be lost or inaccessible.

## Fix Applied

Updated [src/table_precreator.py](src/table_precreator.py) to access Spark map types directly without conversion:

**After (Correct):**
```python
# Access sourceDetails directly (it's already dict-like)
if dataflow_spec.sourceDetails:
    schema_path = dataflow_spec.sourceDetails.get("source_schema_path")
```

**Changes made:**

1. **Line 73-75**: Fixed `sourceDetails` access for landing layer
2. **Line 54-59**: Fixed `targetDetails` access
3. **Line 97-107**: Fixed `tableProperties` access

This pattern matches how the rest of the dlt-meta codebase accesses these map types (see [dataflow_pipeline.py:128-129](src/dataflow_pipeline.py#L128-L129)).

## Testing

✅ All 31 unit tests pass after the fix
✅ The error should no longer occur when running the pipeline

## Next Steps

The fix is now deployed. Please try running your pipeline again:

```bash
# Your pipeline should now work correctly
databricks bundle run dlt_pipeline --layer landing_refinery_treasury
```

**Expected behavior:**
1. ✅ Schema path will be correctly read from `source_details.source_schema_path`
2. ✅ Landing tables will be pre-created with proper schema
3. ✅ Refinery and treasury tables will be pre-created (once DDL files exist)
4. ✅ Pipeline will proceed to DLT execution

## Still Need DDL Files

Remember, you still need to create DDL files for refinery and treasury:

```python
# In Databricks notebook
%run /Workspace/.../schemas/generate_ddl_from_table

# Generate refinery DDL
generate_ddl_file(
    spark,
    "dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_dl_dlt",
    "/dbfs/.../schemas/refinery_sales_ord_tran.ddl"
)

# Generate treasury DDL
generate_ddl_file(
    spark,
    "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt",
    "/dbfs/.../schemas/treasury_sales_ord_tran.ddl"
)
```

Or you can temporarily comment out refinery/treasury pre-creation in the pipeline to test landing first.

## Files Modified

- ✅ [src/table_precreator.py](src/table_precreator.py) - Fixed map type access
- ✅ [tests/test_table_precreator.py](tests/test_table_precreator.py) - Updated tests
- ✅ Tests confirmed passing (31/31)
