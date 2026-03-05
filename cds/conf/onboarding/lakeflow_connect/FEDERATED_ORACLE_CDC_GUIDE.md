# Federated Oracle Database CDC with Complex SQL Queries

## Problem Statement

You have a complex SQL query with:
- Multiple JOINs (MEMBERS, CUSTOMERS, COUNTRY tables)
- UNION ALL combining two queries
- Incremental filtering using `LAST_UPDATE >= date - 24 hours`
- Oracle-specific hints (`/*+ use_nl */`)

**This is NOT a snapshot** - it's already doing **incremental CDC**.

## Why Snapshot CDC Doesn't Work Here

`applyChangesFromSnapshot` in dlt-meta expects:
- ✅ Full table dumps (all records, no date filters)
- ✅ Periodic snapshots to compare

Your SQL query:
- ❌ Filters by date (only last 24 hours)
- ❌ Returns incremental changes only
- ✅ **Already doing CDC** via `LAST_UPDATE` column

## Solution: 3 Approaches

---

## ✅ Option 1: Federated View + Traditional CDC (RECOMMENDED)

This is the cleanest approach for production use.

### Step 1: Create View in Lakehouse Federation

In Databricks SQL, create a federated view that wraps your complex query:

```sql
-- Create the view in your Oracle federated connection
CREATE OR REPLACE VIEW my_oracle_connection.member.vw_member_customer_combined AS
SELECT
    m.FIRST_NAME,
    m.MIDDLE_NAME,
    m.LAST_NAME,
    c.UC_EMAIL_ADDRESS,
    m.CUSTOMER_ID,
    m.CREATED,
    m.CREATE_FROM_DOMAIN_ID,
    m.HOME_DOMAIN_ID,
    m.ACTIVE,
    m.GENDER,
    m.LAST_UPDATE,
    co.COUNTRY_ID,
    co.ABBREV as COUNTRY_ABBREV,
    m.PHONE_NUMBER as MBR_PHN_NUM,
    m.PHONE_NUMBER_ADDED as MBR_PHN_TS,
    m.LOCKED as MBR_LOCKED_FLG,
    m.LOCKED_BY as MBR_LOCKED_BY,
    m.LOCKED_REASON as MBR_LOCKED_REASON,
    CAST(CURRENT_TIMESTAMP() as TIMESTAMP) as _EXTRACTION_TIMESTAMP
FROM
    MEMBER.MEMBERS m
LEFT OUTER JOIN
    MEMBER.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    MEMBER.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID
WHERE
    m.LAST_UPDATE >= CURRENT_TIMESTAMP() - INTERVAL '24' HOUR
    OR COALESCE(c.LAST_UPDATE, TO_TIMESTAMP('1970-01-01 00:00:00')) >= CURRENT_TIMESTAMP() - INTERVAL '24' HOUR

UNION ALL

SELECT
    m.FIRST_NAME,
    m.MIDDLE_NAME,
    m.LAST_NAME,
    c.UC_EMAIL_ADDRESS,
    m.CUSTOMER_ID,
    m.CREATED,
    m.CREATE_FROM_DOMAIN_ID,
    m.HOME_DOMAIN_ID,
    m.ACTIVE,
    m.GENDER,
    m.LAST_UPDATE,
    co.COUNTRY_ID,
    co.ABBREV as COUNTRY_ABBREV,
    m.PHONE_NUMBER as MBR_PHN_NUM,
    m.PHONE_NUMBER_ADDED as MBR_PHN_TS,
    m.LOCKED as MBR_LOCKED_FLG,
    m.LOCKED_BY as MBR_LOCKED_BY,
    m.LOCKED_REASON as MBR_LOCKED_REASON,
    CAST(CURRENT_TIMESTAMP() as TIMESTAMP) as _EXTRACTION_TIMESTAMP
FROM
    MEMBER.MEMBERS m
JOIN
    CRMAPPS.TD_CUSTOMER_BUMP bump
    ON m.CUSTOMER_ID = bump.CUSTOMER_ID
LEFT OUTER JOIN
    MEMBER.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    MEMBER.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID;
```

**Note:** Oracle hints (`/*+ use_nl */`) are typically not needed in Databricks federated views as Databricks optimizes the query execution.

### Step 2: Configure dlt-meta

Use the configuration in `lakeflow_member_incremental.json`:

```json
{
  "data_flow_id": "6002",
  "data_flow_group": "federated_oracle_members",
  "source_system": "oracle_member_db",
  "source_format": "delta",

  "source_details": {
    "source_catalog": "my_oracle_connection",
    "source_database": "member",
    "source_table": "vw_member_customer_combined"
  },

  "landing_catalog_prod": "main",
  "landing_database_prod": "landing",
  "landing_table": "member_customer_data",

  "landing_table_properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableChangeDataFeed": "true"
  },

  "refinery_catalog_prod": "main",
  "refinery_database_prod": "refinery",
  "refinery_table": "member_customer_data",

  "refinery_cdc_apply_changes": {
    "keys": ["CUSTOMER_ID"],
    "sequence_by": "LAST_UPDATE",
    "scd_type": "2",
    "except_column_list": ["_EXTRACTION_TIMESTAMP"]
  }
}
```

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  Oracle Database                                            │
│  - MEMBERS, CUSTOMERS, COUNTRY tables                       │
│  - Complex joins + UNION ALL                                │
│  - Incremental: LAST_UPDATE >= now() - 24 hours             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Federated View: my_oracle_connection.member.vw_...         │
│  - Executes your SQL on Oracle side                         │
│  - Returns only changed records                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  DLT-META Pipeline (reads from view)                        │
│  - Landing: Streams changes into Delta table                │
│  - Refinery: Applies CDC with SCD Type 2                    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Refinery Table: main.refinery.member_customer_data         │
│  - Maintains history (__START_AT, __END_AT)                 │
│  - Tracks changes based on CUSTOMER_ID + LAST_UPDATE        │
└─────────────────────────────────────────────────────────────┘
```

**Advantages:**
- ✅ Clean separation: query logic in view, CDC in dlt-meta
- ✅ Reusable view for other analytics
- ✅ Oracle optimizer handles joins efficiently
- ✅ Easy to test and debug

---

## ⚠️ Option 2: Use Full Snapshot CDC (If Possible)

If you can remove the date filter and get **full table snapshots**, use `applyChangesFromSnapshot`.

### When to Use This
- Small to medium tables (< 10M rows)
- Can afford reading full table each run
- Want automatic delete detection

### Configuration

```json
{
  "data_flow_id": "6003",
  "data_flow_group": "federated_oracle_members_snapshot",
  "source_system": "oracle_member_db",
  "source_format": "snapshot",

  "source_details": {
    "snapshot_format": "delta",
    "source_catalog": "my_oracle_connection",
    "source_database": "member",
    "source_table": "vw_member_customer_full_snapshot"
  },

  "landing_catalog_prod": "main",
  "landing_database_prod": "landing",
  "landing_table": "member_customer_snapshot",

  "landing_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "1"
  },

  "refinery_catalog_prod": "main",
  "refinery_database_prod": "refinery",
  "refinery_table": "member_customer_data",

  "refinery_apply_changes_from_snapshot": {
    "keys": ["CUSTOMER_ID"],
    "scd_type": "2",
    "track_history_except_column_list": ["_EXTRACTION_TIMESTAMP"]
  }
}
```

### View Without Date Filter

```sql
CREATE OR REPLACE VIEW my_oracle_connection.member.vw_member_customer_full_snapshot AS
SELECT
    m.FIRST_NAME,
    m.MIDDLE_NAME,
    m.LAST_NAME,
    c.UC_EMAIL_ADDRESS,
    m.CUSTOMER_ID,
    m.CREATED,
    m.CREATE_FROM_DOMAIN_ID,
    m.HOME_DOMAIN_ID,
    m.ACTIVE,
    m.GENDER,
    m.LAST_UPDATE,
    co.COUNTRY_ID,
    co.ABBREV as COUNTRY_ABBREV,
    m.PHONE_NUMBER as MBR_PHN_NUM,
    m.PHONE_NUMBER_ADDED as MBR_PHN_TS,
    m.LOCKED as MBR_LOCKED_FLG,
    m.LOCKED_BY as MBR_LOCKED_BY,
    m.LOCKED_REASON as MBR_LOCKED_REASON
FROM
    MEMBER.MEMBERS m
LEFT OUTER JOIN
    MEMBER.CUSTOMERS c
    ON m.CUSTOMER_ID = c.CUSTOMER_ID
    AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
LEFT OUTER JOIN
    MEMBER.COUNTRY co
    ON m.COUNTRY_ID = co.COUNTRY_ID;
-- NO WHERE CLAUSE - returns ALL records
```

**Disadvantages:**
- ❌ Reads full table every run (slower, more expensive)
- ❌ Doesn't leverage your incremental LAST_UPDATE logic
- ❌ Second part of UNION (bump table) not included

---

## 🔧 Option 3: Custom Notebook (Maximum Flexibility)

For very complex scenarios where views aren't sufficient.

### When to Use This
- Need parameterized queries (e.g., `{LAST_UPDATE}` variable)
- Complex conditional logic
- Multiple staging steps before CDC

### Implementation

```python
# custom_member_ingestion.py
import dlt
from pyspark.sql import functions as F
from datetime import datetime, timedelta

@dlt.table(
    name="member_customer_raw",
    comment="Raw member and customer data from Oracle",
    table_properties={
        "quality": "bronze"
    }
)
def ingest_member_customer():
    """
    Read from Oracle federated connection with custom SQL.
    """

    # Calculate lookback timestamp
    lookback_hours = 24
    last_update = (datetime.now() - timedelta(hours=lookback_hours)).strftime('%Y-%m-%d %H:%M:%S')

    # Build the SQL query with parameter substitution
    query = f"""
    SELECT
        m.FIRST_NAME,
        m.MIDDLE_NAME,
        m.LAST_NAME,
        c.UC_EMAIL_ADDRESS,
        m.CUSTOMER_ID,
        m.CREATED,
        m.CREATE_FROM_DOMAIN_ID,
        m.HOME_DOMAIN_ID,
        m.ACTIVE,
        m.GENDER,
        m.LAST_UPDATE,
        co.COUNTRY_ID,
        co.ABBREV as COUNTRY_ABBREV,
        m.PHONE_NUMBER as MBR_PHN_NUM,
        m.PHONE_NUMBER_ADDED as MBR_PHN_TS,
        m.LOCKED as MBR_LOCKED_FLG,
        m.LOCKED_BY as MBR_LOCKED_BY,
        m.LOCKED_REASON as MBR_LOCKED_REASON
    FROM
        MEMBER.MEMBERS m
    LEFT OUTER JOIN
        MEMBER.CUSTOMERS c
        ON m.CUSTOMER_ID = c.CUSTOMER_ID
        AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
    LEFT OUTER JOIN
        MEMBER.COUNTRY co
        ON m.COUNTRY_ID = co.COUNTRY_ID
    WHERE
        m.LAST_UPDATE >= TO_TIMESTAMP('{last_update}', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '24' HOUR
        OR COALESCE(c.LAST_UPDATE, TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
           >= TO_TIMESTAMP('{last_update}', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '24' HOUR

    UNION ALL

    SELECT
        m.FIRST_NAME,
        m.MIDDLE_NAME,
        m.LAST_NAME,
        c.UC_EMAIL_ADDRESS,
        m.CUSTOMER_ID,
        m.CREATED,
        m.CREATE_FROM_DOMAIN_ID,
        m.HOME_DOMAIN_ID,
        m.ACTIVE,
        m.GENDER,
        m.LAST_UPDATE,
        co.COUNTRY_ID,
        co.ABBREV as COUNTRY_ABBREV,
        m.PHONE_NUMBER as MBR_PHN_NUM,
        m.PHONE_NUMBER_ADDED as MBR_PHN_TS,
        m.LOCKED as MBR_LOCKED_FLG,
        m.LOCKED_BY as MBR_LOCKED_BY,
        m.LOCKED_REASON as MBR_LOCKED_REASON
    FROM
        MEMBER.MEMBERS m
    JOIN
        CRMAPPS.TD_CUSTOMER_BUMP bump
        ON m.CUSTOMER_ID = bump.CUSTOMER_ID
    LEFT OUTER JOIN
        MEMBER.CUSTOMERS c
        ON m.CUSTOMER_ID = c.CUSTOMER_ID
        AND c.UC_EMAIL_ADDRESS NOT LIKE 'TMACCOUNTLESSCHECKOUT%'
    LEFT OUTER JOIN
        MEMBER.COUNTRY co
        ON m.COUNTRY_ID = co.COUNTRY_ID
    """

    # Read from federated connection
    df = spark.read \
        .format("databricks_catalog") \
        .option("catalog", "my_oracle_connection") \
        .option("query", query) \
        .load()

    return df.withColumn("_INGESTION_TIMESTAMP", F.current_timestamp())


@dlt.table(
    name="member_customer_refined",
    comment="Refined member and customer data with CDC",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_customer_id", "CUSTOMER_ID IS NOT NULL")
def refine_member_customer():
    """
    Apply CDC transformations.
    """
    return dlt.read_stream("member_customer_raw")


# Apply SCD Type 2 CDC
dlt.create_streaming_table(
    name="member_customer_history",
    comment="Historical member and customer data with SCD Type 2"
)

dlt.apply_changes(
    target="member_customer_history",
    source="member_customer_refined",
    keys=["CUSTOMER_ID"],
    sequence_by="LAST_UPDATE",
    stored_as_scd_type=2,
    except_column_list=["_INGESTION_TIMESTAMP"]
)
```

**Advantages:**
- ✅ Full control over query execution
- ✅ Can parameterize queries
- ✅ Custom business logic

**Disadvantages:**
- ❌ More code to maintain
- ❌ Less standardized than dlt-meta approach

---

## Comparison Matrix

| Aspect | Option 1: Federated View | Option 2: Full Snapshot | Option 3: Custom Notebook |
|--------|-------------------------|------------------------|---------------------------|
| **Complexity** | Low | Low | High |
| **Flexibility** | Medium | Low | High |
| **Performance** | ⚡ Best (incremental) | 🐌 Slower (full scans) | ⚡ Best (incremental) |
| **Maintainability** | ✅ High | ✅ High | ⚠️ Medium |
| **Uses dlt-meta** | ✅ Yes | ✅ Yes | ❌ No |
| **Handles Complex SQL** | ✅ Yes | ⚠️ Without date filter | ✅ Yes |
| **Parameterization** | ❌ No | ❌ No | ✅ Yes |
| **Delete Detection** | ❌ Manual | ✅ Automatic | ❌ Manual |

---

## Recommended Approach

**For your use case, use Option 1: Federated View + Traditional CDC**

### Implementation Steps

1. **Create the federated view in Databricks:**

```sql
CREATE OR REPLACE VIEW my_oracle_connection.member.vw_member_customer_combined AS
-- [Your SQL query here, adjusted for Databricks SQL syntax]
```

2. **Configure dlt-meta:**

```bash
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_incremental.json \
  --env prod \
  --layer landing_refinery
```

3. **Schedule the pipeline** to run every hour (or based on your SLA)

4. **Monitor CDC operations:**

```sql
-- Check for recent changes
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    __START_AT,
    __END_AT,
    CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as STATUS
FROM main.refinery.member_customer_data
WHERE CUSTOMER_ID = 12345
ORDER BY __START_AT DESC;
```

---

## Testing Your Configuration

### 1. Test the Federated View

```sql
-- Verify view returns data
SELECT COUNT(*) as total_records
FROM my_oracle_connection.member.vw_member_customer_combined;

-- Check sample data
SELECT *
FROM my_oracle_connection.member.vw_member_customer_combined
LIMIT 10;
```

### 2. Test dlt-meta Pipeline

```bash
# Deploy
databricks labs dlt-meta deploy \
  --onboarding_file_path cds/conf/onboarding/lakeflow_connect/lakeflow_member_incremental.json \
  --env prod

# Run
databricks pipelines start --pipeline-id <pipeline-id>
```

### 3. Verify CDC

```sql
-- Landing table: should have latest data
SELECT COUNT(*) FROM main.landing.member_customer_data;

-- Refinery table: should have history
SELECT
    COUNT(*) as total_versions,
    COUNT(DISTINCT CUSTOMER_ID) as unique_customers
FROM main.refinery.member_customer_data;
```

---

## Troubleshooting

### Issue: View doesn't return data

**Solution:** Check federated connection and table permissions

```sql
-- Test connection
SELECT * FROM my_oracle_connection.member.members LIMIT 1;
```

### Issue: CDC not detecting changes

**Solution:** Verify `LAST_UPDATE` column is updating

```sql
-- Check LAST_UPDATE values
SELECT
    MIN(LAST_UPDATE) as oldest_update,
    MAX(LAST_UPDATE) as newest_update,
    COUNT(*) as total_records
FROM my_oracle_connection.member.vw_member_customer_combined;
```

### Issue: Performance problems

**Solution:** Add indexes to Oracle tables on join keys

```sql
-- In Oracle (requires DBA)
CREATE INDEX idx_members_customer_id ON MEMBER.MEMBERS(CUSTOMER_ID);
CREATE INDEX idx_members_last_update ON MEMBER.MEMBERS(LAST_UPDATE);
```

---

## Summary

✅ **Use Federated View approach** for your complex SQL query

✅ **Use `refinery_cdc_apply_changes`** (not `applyChangesFromSnapshot`)

✅ **Your query is already incremental** - leverage that with traditional CDC

❌ **Don't use `applyChangesFromSnapshot`** - it's for full snapshots only
