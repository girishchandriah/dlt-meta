# Troubleshooting: KeyError: 'database'

## The Error

```python
KeyError: 'database'
File "src/dataflow_pipeline.py", line 657, in read_refinery
    source_database = source_details["database"]
```

## Root Cause

When using `refinery_apply_changes_from_snapshot` **without a landing layer**, dlt-meta reads directly from `source_details`, but it expects **different field names** than what's used for landing layers.

## The Issue

### ❌ What You Had (Causes KeyError)

```json
{
  "source_details": {
    "snapshot_format": "delta",
    "source_catalog": "_member_db_catalog",   // ❌ Wrong
    "source_database": "member",              // ❌ Wrong - causes KeyError
    "source_table": "CUSTOMERS"               // ❌ Wrong
  },
  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  }
}
```

### ✅ What You Need (Fixed)

```json
{
  "source_details": {
    "snapshot_format": "delta",
    "catalog": "_member_db_catalog",    // ✅ No "source_" prefix
    "database": "member",               // ✅ No "source_" prefix
    "table": "CUSTOMERS"                // ✅ No "source_" prefix
  },
  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  }
}
```

## Why This Happens

### Field Naming Rules in dlt-meta

dlt-meta uses **different field names** depending on the layer:

| Layer | Field Names in source_details |
|-------|-------------------------------|
| **Landing Layer** | `source_catalog`, `source_database`, `source_table` |
| **Refinery Layer** (reading from source_details) | `catalog`, `database`, `table` |
| **Treasury Layer** (reading from source_details) | `catalog`, `database`, `table` |

### Two Valid Approaches

#### Approach 1: Single-Layer (Refinery Only) - FIXED ✅

When you **skip the landing layer** and go directly to refinery:

```json
{
  "source_format": "snapshot",
  "source_details": {
    "snapshot_format": "delta",
    "catalog": "_member_db_catalog",    // No prefix
    "database": "member",
    "table": "CUSTOMERS"
  },
  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_nonprod",
  "refinery_table": "memberdb_customers",
  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  }
}
```

**How it works:**
```
Oracle Federated Table
       ↓
  source_details (catalog, database, table)
       ↓
  Refinery Layer (SCD Type 1)
       ↓
  dataservices_nonprod.refinery_nonprod.memberdb_customers
```

#### Approach 2: Two-Layer (Landing + Refinery)

When you want **both landing and refinery layers**:

```json
{
  "source_format": "snapshot",
  "source_details": {
    "snapshot_format": "delta",
    "source_catalog": "_member_db_catalog",    // WITH prefix for landing
    "source_database": "member",
    "source_table": "CUSTOMERS"
  },
  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "memberdb_customers_raw",
  "landing_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  },
  "refinery_catalog_nonprod": "dataservices_nonprod",
  "refinery_database_nonprod": "refinery_nonprod",
  "refinery_table": "memberdb_customers",
  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "2"
  }
}
```

**How it works:**
```
Oracle Federated Table
       ↓
  source_details (source_catalog, source_database, source_table)
       ↓
  Landing Layer (SCD Type 1 - raw data)
       ↓
  dataservices_nonprod.landing_nonprod.memberdb_customers_raw
       ↓
  Refinery Layer (SCD Type 2 - with history)
       ↓
  dataservices_nonprod.refinery_nonprod.memberdb_customers
```

## Code Explanation

### Where the Error Occurs

In `src/dataflow_pipeline.py`, line 657 in the `read_refinery` method:

```python
def read_refinery(self, view_name: str = None, view_name_quarantine: str = None):
    """Read data for refinery layer."""
    refinery_dataflow_spec: RefineryDataflowSpec = self.dataflowSpec
    source_details = self._get_source_details()  # Gets from config
    reader_config_opts = self._get_reader_config_options()

    # THIS IS WHERE THE ERROR HAPPENS:
    source_database = source_details["database"]  # KeyError if "database" doesn't exist
    source_table = source_details["table"]
```

### How source_details Gets Populated

In `src/onboard_dataflowspec.py`, lines 1713-1765:

```python
# For refinery/treasury layer, determine source
source_details = {}

# Priority 1: Read from refinery layer (if exists)
if f"refinery_database_{env}" in onboarding_row:
    source_details = {
        "database": onboarding_row[f"refinery_database_{env}"],
        "table": onboarding_row["refinery_table"]
    }
    # ... catalog logic ...

# Priority 2: Read from landing layer (if exists)
elif f"landing_database_{env}" in onboarding_row:
    source_details = {
        "database": onboarding_row[f"landing_database_{env}"],
        "table": onboarding_row["landing_table"]
    }
    # ... catalog logic ...

# Priority 3: Read from source_details (direct read, no landing/refinery source)
elif "source_details" in onboarding_row:
    source_details_file = onboarding_row["source_details"]

    # IMPORTANT: Expects "database", NOT "source_database"
    if f"source_database_{env}" in source_details_file:
        source_details["database"] = source_details_file[f"source_database_{env}"]
    elif f"database_{env}" in source_details_file:
        source_details["database"] = source_details_file[f"database_{env}"]
    # NO FALLBACK TO JUST "source_database"!
```

**Key insight:** When reading directly from `source_details` for refinery/treasury layers, the code looks for:
1. `source_database_{env}` (e.g., `source_database_nonprod`)
2. `database_{env}` (e.g., `database_nonprod`)
3. **It does NOT fallback to just `source_database`**

## Solution Summary

### Option 1: Use Simple Field Names (Recommended for Single-Layer) ✅

```json
{
  "source_details": {
    "catalog": "...",
    "database": "...",
    "table": "..."
  }
}
```

**When to use:**
- Going directly to refinery layer (no landing)
- Simpler configuration
- Fewer tables to manage

### Option 2: Add Landing Layer

```json
{
  "source_details": {
    "source_catalog": "...",
    "source_database": "...",
    "source_table": "..."
  },
  "landing_catalog_nonprod": "...",
  "landing_database_nonprod": "...",
  "landing_table": "..."
}
```

**When to use:**
- Need both raw data (landing) and refined data (refinery)
- Want different SCD types for each layer
- Better separation of concerns

### Option 3: Use Environment-Specific Names

```json
{
  "source_details": {
    "source_catalog_nonprod": "_member_db_catalog",
    "source_database_nonprod": "member",
    "source_table": "CUSTOMERS"
  }
}
```

**When to use:**
- Different source tables per environment
- More complex environment-specific configuration

## Quick Fix Checklist

If you get `KeyError: 'database'`:

- [ ] Check if you have `landing_*` fields in your config
  - If YES: You're using 2-layer approach, this should work
  - If NO: You're using 1-layer approach, continue...

- [ ] Check your `source_details` field names:
  - [ ] Change `source_catalog` → `catalog`
  - [ ] Change `source_database` → `database`
  - [ ] Change `source_table` → `table`

- [ ] OR use environment-specific names:
  - [ ] Change `source_database` → `source_database_nonprod`
  - [ ] Change `source_catalog` → `source_catalog_nonprod`
  - Keep `source_table` as is

## Verification

After making the fix, verify your configuration:

```bash
# Check configuration syntax
cat your_config.json | python -m json.tool

# Deploy the pipeline
databricks labs dlt-meta deploy \
  --onboarding_file_path your_config.json \
  --env nonprod \
  --layer refinery

# Run the pipeline
databricks pipelines start --pipeline-id <your-pipeline-id>
```

## Common Mistakes

### Mistake 1: Mixing Field Name Styles

❌ **Wrong:**
```json
{
  "source_details": {
    "catalog": "_member_db_catalog",      // Without prefix
    "source_database": "member",           // WITH prefix - inconsistent!
    "table": "CUSTOMERS"
  }
}
```

✅ **Correct:**
```json
{
  "source_details": {
    "catalog": "_member_db_catalog",
    "database": "member",                  // All without prefix
    "table": "CUSTOMERS"
  }
}
```

### Mistake 2: Using Landing Field Names for Refinery

❌ **Wrong:**
```json
{
  "source_details": {
    "source_catalog": "_member_db_catalog",
    "source_database": "member",
    "source_table": "CUSTOMERS"
  },
  "refinery_apply_changes_from_snapshot": {  // Refinery expects different names!
    "keys": ["CUSTOMER_ID"]
  }
  // Missing landing_* fields!
}
```

✅ **Correct (Option A - Simple names):**
```json
{
  "source_details": {
    "catalog": "_member_db_catalog",
    "database": "member",
    "table": "CUSTOMERS"
  },
  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"]
  }
}
```

✅ **Correct (Option B - Add landing layer):**
```json
{
  "source_details": {
    "source_catalog": "_member_db_catalog",
    "source_database": "member",
    "source_table": "CUSTOMERS"
  },
  "landing_catalog_nonprod": "dataservices_nonprod",
  "landing_database_nonprod": "landing_nonprod",
  "landing_table": "memberdb_customers_raw",
  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"]
  }
}
```

## Additional Resources

- [dlt-meta Documentation](https://databrickslabs.github.io/dlt-meta/)
- [Snapshot CDC Guide](./FEDERATED_ORACLE_CDC_GUIDE.md)
- [Example Configurations](../../tests/resources/)
