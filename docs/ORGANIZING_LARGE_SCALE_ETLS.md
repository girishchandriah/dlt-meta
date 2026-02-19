# Organizing Large-Scale ETLs with DLT-META

## Overview

When managing hundreds of ETL pipelines, proper organization is critical for:
- **Maintainability** - Easy to find and update configurations
- **Scalability** - Can add new ETLs without complexity
- **Team Collaboration** - Multiple teams can work independently
- **Performance** - Efficient pipeline execution and resource utilization
- **Troubleshooting** - Quick issue isolation and debugging

## Grouping Strategy

### 1. By Business Domain

Group ETLs by business function or data domain:

```
dataFlowGroup Examples:
- finance_gl           (General Ledger)
- finance_ap           (Accounts Payable)
- sales_crm            (CRM data)
- sales_orders         (Order data)
- marketing_campaigns  (Campaign data)
- hr_employee          (Employee data)
- supply_chain_inventory
- supply_chain_logistics
```

**Advantages:**
- Aligns with business organization
- Clear ownership by business teams
- Natural data domain boundaries
- Easy to understand for business users

**When to use:**
- Multi-source systems feeding one domain
- Business-aligned data products
- Cross-functional analytics

### 2. By Source System

Group ETLs by the originating system:

```
dataFlowGroup Examples:
- sap_erp             (SAP ERP tables)
- salesforce_crm      (Salesforce objects)
- mysql_customers_db  (MySQL customer database)
- oracle_financial    (Oracle Financial)
- kafka_clickstream   (Kafka event streams)
- s3_external_vendor  (External vendor data)
- api_rest_integration
```

**Advantages:**
- Isolates source system dependencies
- Easier to manage source system changes
- Natural for system migrations
- Clear SLA boundaries per source

**When to use:**
- Single source system per group
- Source system has distinct SLA requirements
- Need to isolate source system failures
- Different teams own different source systems

### 3. By Data Criticality/SLA

Group by business criticality and processing requirements:

```
dataFlowGroup Examples:
- critical_realtime    (< 5 min SLA, 24/7)
- standard_hourly      (1 hour SLA, business hours)
- batch_daily          (Daily batch, overnight)
- adhoc_analytics      (No SLA, on-demand)
```

**Advantages:**
- Optimizes resource allocation
- Clear SLA management
- Easier to prioritize issues
- Can use different cluster sizes

**When to use:**
- Distinct SLA requirements
- Different processing windows
- Resource optimization needed
- Cost management important

### 4. Hybrid Approach (Recommended)

Combine strategies using hierarchical naming:

```
dataFlowGroup Examples:
- finance_sap_critical
- sales_salesforce_hourly
- marketing_s3_daily
- hr_workday_batch
```

**Format:** `{domain}_{source}_{sla}` or `{domain}_{source}`

**Advantages:**
- Maximum flexibility
- Clear multi-dimensional organization
- Supports complex requirements
- Scales to hundreds of ETLs

## Directory Structure

### Recommended Layout

```
dlt-meta/
├── conf/
│   ├── onboarding/
│   │   ├── finance/
│   │   │   ├── sap_general_ledger.json
│   │   │   ├── sap_accounts_payable.json
│   │   │   └── oracle_financial.json
│   │   ├── sales/
│   │   │   ├── salesforce_opportunities.json
│   │   │   ├── salesforce_accounts.json
│   │   │   └── mysql_orders.json
│   │   ├── marketing/
│   │   │   ├── adobe_campaigns.json
│   │   │   └── google_analytics.json
│   │   └── hr/
│   │       └── workday_employees.json
│   │
│   ├── dqe/                    # Data Quality Expectations
│   │   ├── finance/
│   │   │   ├── general_ledger_dqe.json
│   │   │   └── accounts_payable_dqe.json
│   │   ├── sales/
│   │   │   ├── opportunities_dqe.json
│   │   │   └── orders_dqe.json
│   │   └── shared/             # Reusable DQE templates
│   │       ├── email_validation.json
│   │       └── phone_validation.json
│   │
│   ├── transformations/         # SQL transformations for refinery
│   │   ├── finance/
│   │   │   ├── gl_enrichment.json
│   │   │   └── ap_aggregation.json
│   │   ├── sales/
│   │   │   ├── order_enrichment.json
│   │   │   └── customer_360.json
│   │   └── shared/              # Reusable transformation logic
│   │       └── common_filters.sql
│   │
│   └── schemas/                 # DDL files for schema definition
│       ├── finance/
│       │   ├── general_ledger.ddl
│       │   └── accounts_payable.ddl
│       └── sales/
│           ├── opportunities.ddl
│           └── orders.ddl
│
├── pipelines/                   # Pipeline definitions by group
│   ├── finance_sap_pipeline.yml
│   ├── sales_salesforce_pipeline.yml
│   └── marketing_adobe_pipeline.yml
│
└── docs/
    ├── domain_ownership.md      # Who owns what
    ├── pipeline_runbook.md      # Operational procedures
    └── data_dictionary.md       # Business definitions
```

## Onboarding File Organization

### Option 1: One Large File Per Domain (Small-Medium Scale)

**File:** `conf/onboarding/finance/sap_all_tables.json`

```json
[
  {
    "data_flow_id": "1001",
    "data_flow_group": "finance_sap",
    "source_system": "sap",
    ...
  },
  {
    "data_flow_id": "1002",
    "data_flow_group": "finance_sap",
    "source_system": "sap",
    ...
  }
]
```

**Pros:**
- Simple to manage
- Single onboarding operation per domain
- Easy to see all ETLs in one place

**Cons:**
- Large files hard to edit
- Git merge conflicts with multiple contributors
- All-or-nothing onboarding

**Use when:** < 20 tables per domain

### Option 2: One File Per Table/ETL (Large Scale - Recommended)

**Files:**
```
conf/onboarding/finance/
├── gl_journal_entries.json
├── gl_accounts.json
├── ap_invoices.json
└── ap_vendors.json
```

Each file contains configuration for ONE table:

```json
[
  {
    "data_flow_id": "1001",
    "data_flow_group": "finance_sap",
    "source_system": "sap",
    "source_table": "journal_entries",
    ...
  }
]
```

**Pros:**
- Easy to maintain individual ETLs
- No merge conflicts
- Incremental onboarding
- Clear ownership per file

**Cons:**
- More files to manage
- Need automation to onboard multiple files

**Use when:** > 20 tables per domain or multiple contributors

### Option 3: Hybrid - Logical Groupings

Group related tables together:

```
conf/onboarding/finance/
├── sap_gl_tables.json          # 5-10 related GL tables
├── sap_ap_tables.json          # 5-10 related AP tables
└── sap_ar_tables.json          # 5-10 related AR tables
```

**Pros:**
- Balance between granularity and manageability
- Logical groupings for related ETLs
- Reasonable file sizes

**Use when:** 50-200 tables total

## DataFlowID Numbering Convention

Use meaningful ID ranges to indicate domain/source:

```
ID Ranges by Domain:
1000-1999: Finance
  1000-1099: SAP
  1100-1199: Oracle Financial
  1200-1299: Concur Expense

2000-2999: Sales
  2000-2099: Salesforce
  2100-2199: MySQL Orders DB
  2200-2299: Shopify

3000-3999: Marketing
  3000-3099: Adobe Campaign
  3100-3199: Google Analytics
  3200-3299: HubSpot

4000-4999: HR
  4000-4099: Workday
  4100-4199: ADP Payroll

5000-5999: Supply Chain
  5000-5099: SAP SCM
  5100-5199: Oracle Inventory
```

**Advantages:**
- IDs are self-documenting
- Easy to filter by domain
- Clear ownership
- Room for growth

## DQE Organization

### Shared DQE Templates

Create reusable DQE templates for common validations:

**File:** `conf/dqe/shared/email_validation.json`
```json
{
  "expect_all": {
    "valid_email": "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'"
  }
}
```

**File:** `conf/dqe/shared/phone_validation.json`
```json
{
  "expect_all": {
    "valid_phone": "phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) >= 10"
  }
}
```

### Domain-Specific DQE

**File:** `conf/dqe/finance/general_ledger_dqe.json`
```json
{
  "expect_all": {
    "valid_account_id": "account_id IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount != 0",
    "valid_currency": "currency_code IN ('USD', 'EUR', 'GBP')",
    "balanced_entry": "debit_amount + credit_amount = 0"
  },
  "expect_all_or_drop": {
    "future_date_check": "posting_date <= CURRENT_DATE()"
  },
  "expect_or_quarantine": {
    "suspicious_amount": "ABS(amount) <= 1000000"
  }
}
```

### DQE Naming Convention

```
{table_name}_{layer}_dqe.json

Examples:
- customers_landing_dqe.json      # Landing layer DQE
- customers_refinery_dqe.json     # Refinery layer DQE
- orders_landing_dqe.json
- orders_refinery_dqe.json
```

## SQL Transformations Organization

### Simple Transformations (Inline)

For simple SELECT statements, include directly in onboarding:

```json
{
  "data_flow_id": "2001",
  "data_flow_group": "sales_salesforce",
  "refinery_select_exp": ["*", "UPPER(customer_name) as customer_name_upper"],
  "refinery_where_clause": "status != 'DELETED'"
}
```

### Complex Transformations (Separate Files)

For complex SQL with JOINs, CTEs, create separate transformation files:

**File:** `conf/transformations/sales/customer_360.json`
```json
{
  "transformation_id": "customer_360_transform",
  "sql_query": "
    SELECT
      c.customer_id,
      c.customer_name,
      c.email,
      o.total_orders,
      o.total_revenue,
      s.last_support_ticket_date,
      CASE
        WHEN o.total_revenue > 10000 THEN 'VIP'
        WHEN o.total_revenue > 1000 THEN 'Standard'
        ELSE 'Basic'
      END as customer_tier
    FROM source_customers c
    LEFT JOIN (
      SELECT
        customer_id,
        COUNT(*) as total_orders,
        SUM(order_amount) as total_revenue
      FROM source_orders
      GROUP BY customer_id
    ) o ON c.customer_id = o.customer_id
    LEFT JOIN (
      SELECT
        customer_id,
        MAX(ticket_date) as last_support_ticket_date
      FROM source_support_tickets
      GROUP BY customer_id
    ) s ON c.customer_id = s.customer_id
  "
}
```

**Reference in onboarding:**
```json
{
  "data_flow_id": "2001",
  "data_flow_group": "sales_salesforce",
  "refinery_transformation_json_prod": "/path/to/conf/transformations/sales/customer_360.json"
}
```

### Transformation Libraries

Create reusable SQL functions/macros:

**File:** `conf/transformations/shared/common_functions.sql`
```sql
-- Standardize phone numbers
CREATE OR REPLACE FUNCTION standardize_phone(phone STRING)
RETURNS STRING
RETURN REGEXP_REPLACE(phone, '[^0-9]', '');

-- Calculate age from birthdate
CREATE OR REPLACE FUNCTION calculate_age(birth_date DATE)
RETURNS INT
RETURN DATEDIFF(CURRENT_DATE(), birth_date) / 365;
```

## Pipeline Organization Strategies

### Strategy 1: One Pipeline Per Group (Recommended for Most Cases)

Create separate pipelines for each `dataFlowGroup`:

```
Pipelines:
- finance_sap_pipeline          → group: "finance_sap"
- sales_salesforce_pipeline     → group: "sales_salesforce"
- marketing_adobe_pipeline      → group: "marketing_adobe"
```

**Configuration Example:**
```json
{
  "name": "finance_sap_pipeline",
  "configuration": {
    "layer": "landing_refinery",
    "landing.group": "finance_sap",
    "refinery.group": "finance_sap",
    ...
  }
}
```

**Advantages:**
- Clear isolation between domains
- Independent execution schedules
- Failures don't affect other domains
- Easy to scale resources per domain
- Clear ownership

**Disadvantages:**
- More pipelines to manage
- Higher infrastructure overhead
- Potential duplication

**Use when:**
- > 50 tables total
- Different SLAs per domain
- Different teams own different domains
- Need failure isolation

### Strategy 2: One Pipeline, Multiple Groups (Cost Optimization)

Use a single pipeline that processes multiple groups:

```
Pipeline: enterprise_data_pipeline
Groups: ["finance_sap", "sales_salesforce", "marketing_adobe"]
```

**Configuration:**
```json
{
  "name": "enterprise_data_pipeline",
  "configuration": {
    "layer": "landing_refinery",
    "landing.dataflowIds": "1001,1002,1003,2001,2002,3001,3002",
    ...
  }
}
```

**Advantages:**
- Lower infrastructure cost
- Unified monitoring
- Simpler deployment

**Disadvantages:**
- Single point of failure
- Complex resource tuning
- Difficult to assign ownership
- Harder to troubleshoot

**Use when:**
- < 50 tables total
- All ETLs have similar SLAs
- Cost is primary concern
- Single team owns all ETLs

### Strategy 3: Layered Pipelines (Advanced)

Separate pipelines by layer:

```
Pipelines:
- landing_all_sources     → layer: "landing"
- refinery_all_domains    → layer: "refinery"
- treasury_analytics      → layer: "treasury"
```

**Advantages:**
- Optimize resources per layer
- Clear layer boundaries
- Can use different cluster sizes
- Landing runs 24/7, refinery batch

**Disadvantages:**
- Complex dependencies
- Requires good monitoring
- Harder to debug end-to-end

**Use when:**
- Very large scale (100s of tables)
- Different resource needs per layer
- Landing is real-time, refinery is batch
- Need maximum optimization

## Onboarding Workflow for Large Scale

### Initial Setup

1. **Create directory structure:**
```bash
mkdir -p conf/onboarding/{finance,sales,marketing,hr}
mkdir -p conf/dqe/{finance,sales,marketing,hr,shared}
mkdir -p conf/transformations/{finance,sales,marketing,hr,shared}
mkdir -p conf/schemas/{finance,sales,marketing,hr}
```

2. **Create master tracking spreadsheet:**
   - Excel/Google Sheet with all ETLs
   - Columns: dataFlowId, dataFlowGroup, sourceSystem, sourceTable, owner, priority, status
   - Use this to generate onboarding JSONs

3. **Set up version control:**
```bash
git init
git add conf/
git commit -m "Initial ETL configuration structure"
```

### Incremental Onboarding Process

#### Phase 1: Start with Critical ETLs (Week 1-2)

1. Identify 10-20 most critical tables
2. Create onboarding configs for `critical` group
3. Test pipeline end-to-end
4. Fix issues before scaling

```bash
# Onboard critical group only
python src/cli.py onboard \
  --onboarding_file conf/onboarding/finance/sap_critical.json \
  --dlt_meta_layer landing_refinery \
  --overwrite True
```

#### Phase 2: Domain by Domain (Week 3-8)

5. Onboard one domain at a time
6. Finance → Sales → Marketing → HR
7. Validate each domain before moving to next

```bash
# Onboard finance domain
python src/cli.py onboard \
  --onboarding_file conf/onboarding/finance/ \
  --dlt_meta_layer landing_refinery

# Test finance pipeline
# Then move to sales
python src/cli.py onboard \
  --onboarding_file conf/onboarding/sales/ \
  --dlt_meta_layer landing_refinery
```

#### Phase 3: Scale to All ETLs (Week 9+)

8. Onboard remaining domains
9. Optimize and tune
10. Set up monitoring

### Automated Onboarding from Metadata

For 100s of tables, generate onboarding configs from metadata:

```python
# generate_onboarding_configs.py
import json
import pandas as pd

# Read metadata from spreadsheet/database
metadata = pd.read_csv('etl_metadata.csv')

for _, row in metadata.iterrows():
    onboarding_config = {
        "data_flow_id": row['dataFlowId'],
        "data_flow_group": f"{row['domain']}_{row['source_system']}",
        "source_system": row['source_system'],
        "source_format": row['source_format'],
        "source_details": {
            "source_database": row['source_database'],
            "source_table": row['source_table'],
            "source_path_prod": row['source_path']
        },
        "landing_catalog_prod": row['landing_catalog'],
        "landing_database_prod": row['landing_database'],
        "landing_table": row['source_table'],
        # ... other fields
    }

    # Write to appropriate domain folder
    domain = row['domain']
    filename = f"conf/onboarding/{domain}/{row['source_table']}.json"
    with open(filename, 'w') as f:
        json.dump([onboarding_config], f, indent=2)
```

## Monitoring and Observability

### Pipeline Monitoring Dashboard

Track metrics per group:

```sql
-- Example: Pipeline success rate by group
SELECT
  configuration['landing.group'] as data_flow_group,
  COUNT(*) as total_runs,
  SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as successful_runs,
  SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
  AVG(execution_duration_ms) / 1000 / 60 as avg_duration_minutes
FROM system.lakeflow.pipeline_updates
WHERE pipeline_id IN (...)
  AND update_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY configuration['landing.group']
ORDER BY failed_runs DESC
```

### Data Quality Monitoring

```sql
-- Track DQE failures by group
SELECT
  dataFlowGroup,
  dataFlowId,
  COUNT(*) as dqe_failures
FROM dltmeta_landing_quarantine.*
WHERE ingestion_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY dataFlowGroup, dataFlowId
ORDER BY dqe_failures DESC
```

## Best Practices Summary

### ✅ Do's

1. **Use meaningful group names** - Domain + Source + SLA
2. **Organize files by domain** - Clear directory structure
3. **One onboarding file per table** (for large scale)
4. **Use ID ranges by domain** - Self-documenting
5. **Create reusable DQE templates** - DRY principle
6. **Separate complex transformations** - External files
7. **Version control everything** - Git for all configs
8. **Document ownership** - Who owns what domain
9. **Incremental onboarding** - Start small, scale up
10. **Monitor per group** - Group-level metrics

### ❌ Don'ts

1. **Don't use generic group names** - "group1", "test", "prod"
2. **Don't put all ETLs in one group** - Hard to manage
3. **Don't mix unrelated ETLs** - Keep logical groupings
4. **Don't inline complex SQL** - Use transformation files
5. **Don't onboard everything at once** - Incremental approach
6. **Don't skip documentation** - Document as you build
7. **Don't ignore monitoring** - Set up from day 1
8. **Don't hardcode environments** - Use _prod, _nonprod suffixes
9. **Don't forget backups** - Version control + backups
10. **Don't skip testing** - Test each group before scaling

## Example: Real-World Setup for 200 Tables

```
Organization: 5 Domains, 200 Tables Total

Groups:
- finance_sap (50 tables)
- sales_salesforce (60 tables)
- marketing_adobe (40 tables)
- hr_workday (30 tables)
- supply_sap_scm (20 tables)

Directory Structure:
conf/onboarding/
├── finance/          # 50 JSON files, one per table
├── sales/            # 60 JSON files
├── marketing/        # 40 JSON files
├── hr/               # 30 JSON files
└── supply_chain/     # 20 JSON files

Pipelines (5 total):
- finance_sap_pipeline
- sales_salesforce_pipeline
- marketing_adobe_pipeline
- hr_workday_pipeline
- supply_sap_scm_pipeline

Onboarding Phases:
- Week 1: finance (10 critical tables)
- Week 2: finance (remaining 40 tables)
- Week 3: sales (20 critical tables)
- Week 4: sales (remaining 40 tables)
- Week 5-6: marketing, hr, supply chain

Result:
- Clear ownership per domain
- Independent pipeline execution
- Manageable configuration files
- Scalable to 500+ tables
```

## Tools and Automation

### 1. Config Generator Script

Create a script to generate onboarding configs from metadata:

```bash
python tools/generate_onboarding.py \
  --metadata etl_metadata.csv \
  --output conf/onboarding/ \
  --domain finance
```

### 2. Bulk Onboarding Script

Onboard multiple domains at once:

```bash
python tools/bulk_onboard.py \
  --domains finance,sales,marketing \
  --environment prod \
  --overwrite False
```

### 3. Validation Script

Validate all configs before onboarding:

```bash
python tools/validate_configs.py \
  --directory conf/onboarding/ \
  --check-paths True \
  --check-schemas True
```

### 4. Pipeline Generator

Auto-generate pipeline definitions:

```bash
python tools/generate_pipelines.py \
  --config conf/pipeline_templates.yml \
  --output pipelines/
```

## Conclusion

For 100s of ETLs:

1. **Organize by domain/source** - Clear logical groupings
2. **One file per table** - Easier to maintain
3. **Separate pipelines per group** - Better isolation
4. **Incremental onboarding** - Don't rush
5. **Automate config generation** - Don't do it manually
6. **Monitor per group** - Group-level observability
7. **Document everything** - Future you will thank you

Start small, validate the approach with 10-20 tables, then scale systematically.
