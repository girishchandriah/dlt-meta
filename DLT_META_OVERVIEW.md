# DLT-META Overview

## What is DLT-META?

**DLT-META** is a metadata-driven framework developed by Databricks Labs that automates the creation and management of data pipelines using [Databricks Lakeflow Declarative Pipelines](https://www.databricks.com/product/data-engineering/lakeflow-declarative-pipelines) (formerly Delta Live Tables - DLT). It enables data engineers to build scalable, production-grade data pipelines through configuration rather than extensive custom code.

### Core Philosophy

Instead of writing individual pipeline code for each data source, DLT-META uses a **single generic pipeline** that reads metadata specifications (Dataflowspecs) and automatically generates the appropriate data processing workflows. This metadata-driven approach dramatically reduces development time and maintenance overhead while ensuring consistency across pipelines.

---

## Key Features

### Metadata-Driven Architecture
- **Dataflowspecs**: JSON/YAML configuration files that define source-to-target mappings
- **Declarative Configuration**: Specify what you want, not how to build it
- **Single Pipeline Pattern**: One generic pipeline handles multiple data flows

### Comprehensive Data Source Support
- **Cloud Storage**: Autoloader for CSV, JSON, Parquet, Avro files
- **Streaming**: Kafka, Azure Event Hub
- **Batch**: Delta tables, database snapshots
- **Change Data Capture (CDC)**: Automated CDC processing with multiple strategies

### Medallion Architecture Layers
- **Landing Layer** (formerly Bronze): Raw data ingestion with schema enforcement
- **Refinery Layer** (formerly Silver): Cleaned, conformed, and deduplicated data
- **Treasury Layer** (formerly Gold): Business-level aggregates and analytics-ready datasets

### Built-In Data Quality
- Declarative data quality expectations at all layers
- Automatic quarantine tables for invalid records
- Configurable validation rules per table

### Advanced Capabilities
- **SQL Transformations**: Full SQL support with JOINs, aggregations, and CTEs
- **Custom Transformations**: Bring-your-own Python transformation functions
- **Liquid Clustering**: Optimized data layout for query performance
- **Pipeline Chaining**: Combine layers (landing_refinery, refinery_treasury, landing_refinery_treasury)
- **Data Sinks**: Export to external Delta tables, Kafka topics
- **Unity Catalog Integration**: Full UC support with catalog/schema namespacing

### Developer Experience
- **CLI Tools**: `databricks labs dlt-meta` for onboarding and deployment
- **Lakehouse App UI**: Web-based interface for pipeline management
- **Databricks Asset Bundles**: Infrastructure-as-code deployment support

---

## Architecture

### High-Level Process Flow

```
┌─────────────────┐
│  Dataflowspec   │  (JSON/YAML Configuration)
│   (Metadata)    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│   DLT-META Generic Pipeline Engine      │
│                                          │
│  ┌──────────────────────────────────┐   │
│  │  1. Read Metadata                │   │
│  │  2. Apply Readers (Autoloader,   │   │
│  │     Kafka, Delta, EventHub)      │   │
│  │  3. Apply Data Quality Rules     │   │
│  │  4. Apply CDC Logic              │   │
│  │  5. Apply Transformations        │   │
│  │  6. Build DLT Graph              │   │
│  └──────────────────────────────────┘   │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│   Lakeflow Declarative Pipeline         │
│   (Delta Live Tables)                   │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│      Lakehouse Storage                  │
│   Landing → Refinery → Treasury         │
└─────────────────────────────────────────┘
```

### Core Components

#### 1. **Metadata Interface** (`onboard_dataflowspec.py`)
- Captures input/output metadata in onboarding files
- Defines data quality rules
- Specifies transformation logic
- Manages pipeline configurations

#### 2. **Pipeline Readers** (`pipeline_readers.py`)
- **Autoloader**: Cloud files (CSV, JSON, Parquet, Avro)
- **Kafka**: Streaming data from Kafka topics
- **EventHub**: Azure Event Hub streams
- **Delta**: Delta table snapshots
- **Snapshot**: Database snapshot files

#### 3. **Pipeline Engine** (`dataflow_pipeline.py`)
- Orchestrates the entire pipeline workflow
- Dynamically builds DLT graph based on metadata
- Applies data quality expectations
- Handles CDC operations (`apply_changes`, `apply_changes_from_snapshot`)
- Manages pipeline state and dependencies

#### 4. **Dataflow Specifications** (`dataflow_spec.py`)
- Parses and validates Dataflowspec configurations
- Manages source and target metadata
- Handles layer-specific configurations
- Validates configuration completeness

#### 5. **Pipeline Writers** (`pipeline_writers.py`)
- Writes to external sinks (Delta, Kafka)
- Manages data export operations

#### 6. **CLI Interface** (`cli.py`)
- Command-line tools for onboarding and deployment
- Interactive prompts for configuration
- Integration with Databricks workspace

---

## Medallion Architecture Layers

### Landing Layer (Raw Ingestion)
**Purpose**: Ingest raw data exactly as it arrives from source systems

**Characteristics**:
- Schema enforcement with DDL files
- Data quality validation at ingestion
- Quarantine tables for invalid records
- File metadata tracking (source file name, timestamp)
- Support for CDC patterns (`apply_changes`, `apply_changes_from_snapshot`)
- Append-only or CDC merge modes

**Configuration Example**:
```json
{
  "landing_catalog_prod": "privacy_nonprod",
  "landing_database_prod": "dltmeta_landing",
  "landing_table": "customers",
  "landing_reader_options": {
    "cloudFiles.format": "csv",
    "cloudFiles.rescuedDataColumn": "_rescued_data",
    "header": "true"
  },
  "landing_data_quality_expectations_json_prod": "/path/to/dqe.json",
  "landing_quarantine_table": "customers_quarantine"
}
```

### Refinery Layer (Cleaned & Conformed)
**Purpose**: Clean, deduplicate, and conform data to business standards

**Characteristics**:
- CDC apply changes for slowly changing dimensions (SCD Type 1, 2)
- SQL transformations with JOIN support
- Data quality checks with quarantine
- Custom transformation functions
- Liquid clustering for performance

**Configuration Example**:
```json
{
  "refinery_catalog_prod": "privacy_nonprod",
  "refinery_database_prod": "dltmeta_refinery",
  "refinery_table": "customers",
  "refinery_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "dmsTimestamp",
    "scd_type": "2",
    "apply_as_deletes": "Op = 'D'",
    "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
  },
  "refinery_transformation_json_prod": "/path/to/transformations.json"
}
```

### Treasury Layer (Business Aggregates)
**Purpose**: Create business-level aggregates and analytics-ready datasets

**Characteristics**:
- Complex SQL with JOINs, aggregations, window functions
- Multi-table queries and CTEs
- Business logic enforcement
- Optimized for analytical queries

**Configuration Example**:
```json
{
  "treasury_catalog_prod": "privacy_nonprod",
  "treasury_database_prod": "dltmeta_treasury",
  "treasury_table": "customer_transactions_summary",
  "treasury_transformation_json_prod": "/path/to/treasury_transformations.json"
}
```

---

## Key Concepts

### Dataflowspec (Data Flow Specification)
A JSON or YAML file that defines a single data flow from source to target. Each Dataflowspec contains:
- **data_flow_id**: Unique identifier for the data flow
- **data_flow_group**: Logical grouping for related flows
- **source_system**: Source system name
- **source_format**: Input format (cloudFiles, kafka, eventhub, delta)
- **source_details**: Source-specific configuration
- **Layer configurations**: Landing, Refinery, Treasury settings
- **Data quality rules**: Expectations and quarantine configurations
- **CDC settings**: Change data capture configurations

### Data Quality Expectations (DQE)
Declarative rules that validate data quality at any layer:
```json
{
  "expectations": [
    {
      "name": "valid_email",
      "constraint": "email IS NOT NULL AND email LIKE '%@%'",
      "action": "quarantine"
    },
    {
      "name": "positive_amount",
      "constraint": "amount > 0",
      "action": "fail"
    }
  ]
}
```

**Actions**:
- `fail`: Stop pipeline on violation
- `drop`: Drop violating records
- `quarantine`: Move violating records to quarantine table

### CDC (Change Data Capture) Patterns

#### 1. **apply_changes** (Streaming CDC)
For continuous streaming CDC from Kafka, EventHub, or Autoloader:
```json
"refinery_cdc_apply_changes": {
  "keys": ["customer_id"],
  "sequence_by": "update_timestamp",
  "scd_type": "2",
  "apply_as_deletes": "operation = 'DELETE'",
  "except_column_list": ["operation", "_metadata"]
}
```

#### 2. **apply_changes_from_snapshot** (Snapshot CDC)
For batch CDC from periodic database snapshots:
```json
"landing_cdc_apply_changes": {
  "keys": ["order_id"],
  "sequence_by": "snapshot_timestamp",
  "scd_type": "1",
  "track_history_column_list": ["status", "amount"]
}
```

### Transformation Patterns

#### SQL Transformations
Define transformations in a separate JSON file:
```json
{
  "transformations": [
    {
      "table": "customer_orders",
      "query": """
        SELECT
          c.customer_id,
          c.customer_name,
          COUNT(o.order_id) as total_orders,
          SUM(o.order_amount) as total_spent
        FROM LIVE.customers c
        LEFT JOIN LIVE.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
      """
    }
  ]
}
```

#### Custom Python Transformations
Bring your own transformation functions:
```python
def my_custom_transform(df):
    """Apply custom business logic"""
    df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    df = df.filter(col("status") == "active")
    return df

invoke_dlt_pipeline(
    spark=spark,
    layer="landing_refinery",
    landing_custom_transform_func=my_custom_transform
)
```

### Pipeline Chaining
Combine multiple layers in a single DLT pipeline:
- `layer=landing_refinery`: Ingest and refine in one pipeline
- `layer=refinery_treasury`: Refine and aggregate in one pipeline
- `layer=landing_refinery_treasury`: All three layers in one pipeline

**Benefits**:
- Reduced latency (no intermediate storage reads)
- Simplified orchestration
- Cost optimization

---

## Use Cases

### 1. **Bulk Data Onboarding**
Onboard hundreds of tables from multiple source systems with minimal code:
```bash
# Create onboarding configurations
databricks labs dlt-meta onboard
# Deploy pipelines
databricks labs dlt-meta deploy
```

### 2. **Real-Time Streaming Pipelines**
Process streaming data from Kafka or Event Hub with automatic CDC:
- Landing: Ingest raw events
- Refinery: Apply changes to maintain current state
- Treasury: Real-time aggregations

### 3. **Data Lake Migration**
Migrate from traditional ETL to modern lakehouse architecture:
- Define legacy table mappings in Dataflowspecs
- Automatically generate DLT pipelines
- Maintain data lineage and quality

### 4. **Multi-Tenant Data Platforms**
Build scalable multi-tenant data platforms:
- Group-based pipeline organization
- Catalog/schema isolation per tenant
- Centralized governance and monitoring

### 5. **Regulated Industries (Healthcare, Finance)**
Meet compliance requirements with built-in features:
- Complete data lineage
- Audit trail of all changes
- Data quality validation and quarantine
- SCD Type 2 for historical tracking

---

## Getting Started

### Prerequisites
- Python 3.8+
- Databricks CLI v0.213+
- Databricks workspace access

### Installation

1. **Install Databricks CLI**:
```bash
# macOS
brew install databricks

# Windows
pip install databricks-cli

# Authenticate
databricks auth login --host <WORKSPACE_HOST>
```

2. **Install DLT-META**:
```bash
databricks labs install dlt-meta
```

### Quick Start

1. **Clone the repository** (optional, for demo files):
```bash
git clone https://github.com/databrickslabs/dlt-meta.git
cd dlt-meta
```

2. **Create virtual environment**:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install "PyYAML>=6.0" setuptools databricks-sdk
```

3. **Onboard data sources**:
```bash
databricks labs dlt-meta onboard
```

This command:
- Prompts for onboarding configuration
- Uploads code and data to Databricks workspace
- Creates an onboarding job
- Opens the job URL in your browser

4. **Deploy DLT pipeline**:
```bash
databricks labs dlt-meta deploy
```

This command:
- Prompts for pipeline configuration (layer, group, etc.)
- Creates and launches DLT pipeline
- Opens the pipeline URL in your browser

---

## Project Structure

```
dlt-meta/
├── src/                          # Core source code
│   ├── dataflow_pipeline.py      # Pipeline orchestration engine
│   ├── dataflow_spec.py          # Dataflowspec parsing and validation
│   ├── pipeline_readers.py       # Data source readers
│   ├── pipeline_writers.py       # Data sink writers
│   ├── onboard_dataflowspec.py   # Onboarding workflow
│   ├── cli.py                    # Command-line interface
│   └── config.py                 # Configuration management
├── demo/                         # Demo configurations and data
│   ├── conf/                     # Configuration files
│   │   ├── onboarding.json       # Sample Dataflowspecs
│   │   ├── dqe/                  # Data quality expectations
│   │   └── refinery_transformations.json
│   ├── notebooks/                # Demo notebooks
│   └── resources/                # Sample data and DDL files
├── examples/                     # Additional examples
│   ├── onboarding.template       # Dataflowspec template
│   └── dqe/                      # DQE examples
├── integration_tests/            # Integration test suite
├── lakehouse_app/               # Lakehouse App UI
├── docs/                        # Documentation
│   ├── content/                 # Hugo documentation content
│   │   ├── getting_started/     # Getting started guides
│   │   ├── demo/                # Demo documentation
│   │   └── faq/                 # Frequently asked questions
│   ├── CDC_APPLY_CHANGES_FROM_SNAPSHOT.md
│   ├── CDC_METHODS_COMPARISON.md
│   ├── ONBOARDING_FILE_REFERENCE.md
│   └── ONBOARDING_QUICK_REFERENCE.md
├── tests/                       # Unit tests
├── setup.py                     # Package setup
├── README.md                    # Main documentation
└── CHANGELOG.md                 # Release notes
```

---

## Configuration Files

### Onboarding Configuration (`onboarding.json`)
Defines data flows from source to target with all metadata:
- Source system details
- Reader options (format, headers, partitioning)
- Landing layer configuration
- Refinery layer configuration (CDC, transformations)
- Treasury layer configuration
- Data quality expectations paths
- Quarantine table settings

**Location**: `demo/conf/onboarding.json`

### Transformation Configuration
Defines SQL transformations for Refinery and Treasury layers:
- Table name
- SQL query with JOINs, aggregations
- Dependencies on other tables

**Location**: `demo/conf/refinery_transformations.json`

### Data Quality Expectations (DQE)
Defines validation rules per table:
- Expectation name
- SQL constraint
- Action (fail, drop, quarantine)

**Location**: `demo/conf/dqe/*.json`

---

## Advanced Features

### Liquid Clustering
Optimize table layout for query performance:
```json
"landing_cluster_by": ["customer_id", "order_date"],
"refinery_cluster_by": ["customer_id"]
```

### File Metadata Tracking
Capture source file information in landing layer:
```json
"landing_reader_options": {
  "cloudFiles.format": "csv",
  "cloudFiles.metadata.file.path": "true",
  "cloudFiles.metadata.file.modificationTime": "true"
}
```

### Data Sinks
Export data to external systems:
```json
"sink": {
  "type": "delta",
  "path": "/external/delta/table"
}
```

or

```json
"sink": {
  "type": "kafka",
  "bootstrap_servers": "kafka.example.com:9092",
  "topic": "output_topic"
}
```

### Databricks Asset Bundles (DAB)
Deploy pipelines as infrastructure-as-code:
```yaml
# databricks.yml
resources:
  pipelines:
    dlt_meta_pipeline:
      name: "DLT-META Pipeline"
      libraries:
        - notebook:
            path: ./notebooks/dlt_meta_pipeline.py
      configuration:
        layer: "landing_refinery"
        group: "A1"
```

---

## Best Practices

### 1. **Organize Dataflows by Group**
Use `data_flow_group` to logically organize related data flows:
- Group A1: Customer domain
- Group B1: Order domain
- Group C1: Product domain

### 2. **Start with Landing + Refinery**
Begin with `layer=landing_refinery` before adding Treasury:
- Validate data ingestion
- Confirm data quality rules
- Test CDC logic
- Add Treasury layer for business aggregates

### 3. **Use Quarantine Tables Generously**
Capture invalid records rather than dropping them:
- Enables root cause analysis
- Provides visibility into data quality issues
- Prevents data loss

### 4. **Leverage Liquid Clustering**
Define clustering keys based on query patterns:
- Landing: Partition columns, timestamp columns
- Refinery: Primary keys, frequently filtered columns
- Treasury: Dimension keys used in JOINs

### 5. **Version Control Dataflowspecs**
Treat Dataflowspecs as code:
- Store in Git repositories
- Use pull requests for changes
- Maintain environment-specific configurations

### 6. **Monitor Pipeline Health**
Use DLT pipeline metrics and logs:
- Monitor data quality expectation violations
- Track quarantine table growth
- Alert on pipeline failures

---

## Troubleshooting

### Pipeline Conflicts
**Issue**: Multiple pipelines writing to same tables

**Resolution**: See `TROUBLESHOOTING_PIPELINE_CONFLICT.md`

### No Tables Created
**Issue**: Pipeline runs but doesn't create tables

**Resolution**: See `TROUBLESHOOTING_NO_TABLES.md`

### Schema Evolution Issues
**Issue**: Schema changes cause pipeline failures

**Resolution**:
- Update DDL files in source_schema_path
- Use `cloudFiles.inferColumnTypes` for automatic detection
- Enable schema evolution in DLT pipeline settings

### Data Quality Violations
**Issue**: Excessive quarantine records

**Resolution**:
- Review DQE rules for correctness
- Check source data quality
- Adjust expectations based on business requirements

---

## Resources

### Documentation
- [Official Documentation](https://databrickslabs.github.io/dlt-meta/)
- [Getting Started Guide](https://databrickslabs.github.io/dlt-meta/getting_started)
- [FAQ](https://databrickslabs.github.io/dlt-meta/faq)
- [Onboarding File Reference](docs/ONBOARDING_FILE_REFERENCE.md)
- [Onboarding Quick Reference](docs/ONBOARDING_QUICK_REFERENCE.md)
- [CDC Methods Comparison](docs/CDC_METHODS_COMPARISON.md)

### Examples
- [GitHub Examples](https://github.com/databrickslabs/dlt-meta/tree/main/examples)
- [Demo Notebooks](demo/notebooks/)
- [Sample Configurations](demo/conf/)

### Support
- [GitHub Issues](https://github.com/databrickslabs/dlt-meta/issues)
- [Databricks Community](https://community.databricks.com/)

### Release Information
- [Changelog](CHANGELOG.md)
- [PyPI Package](https://pypi.org/project/dlt-meta/)

---

## License

DLT-META is released under the Databricks License and provided AS-IS by Databricks Labs. This is not a formally supported Databricks product and is provided for exploration purposes only.

---

## Version Information

**Current Version**: 0.0.10
**Python Requirement**: >=3.8
**Package Name**: dlt_meta_cds

---

## Recent Updates (v0.0.10)

### Terminology Update
- **Bronze** → **Landing** (raw ingestion layer)
- **Silver** → **Refinery** (cleaned and conformed data layer)
- **Gold** → **Treasury** (business-level aggregates layer)
- Backward compatibility maintained for legacy parameter names

### New Features
- Treasury (Gold) layer support with SQL transformations
- Enhanced SQL transformations with JOINs and aggregations
- Pipeline chaining options (landing_refinery, refinery_treasury, landing_refinery_treasury)
- Quarantine support in Refinery layer
- Lakehouse App UI for onboarding/deployment
- Data sinks (Delta, Kafka) support
- Liquid clustering support

### Breaking Changes
- DPM mode flag removed (must migrate to default publishing mode)
- `invoke_dlt_pipeline` argument changes (layer-specific prefixes required)

---

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request with detailed description
4. Ensure tests pass and code follows style guidelines

---

## Summary

**DLT-META** transforms how data teams build and maintain pipelines by:
- **Reducing development time** from weeks to hours through metadata-driven automation
- **Ensuring consistency** with standardized patterns and configurations
- **Improving quality** with built-in data validation and quarantine capabilities
- **Scaling effortlessly** from dozens to hundreds of data flows
- **Simplifying maintenance** with centralized configuration management

Whether you're building a new data platform or modernizing existing pipelines, DLT-META provides the foundation for scalable, production-grade data engineering on Databricks.
