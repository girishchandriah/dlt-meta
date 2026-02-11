---
title: "Metadata Preparation"
date: 2021-08-04T14:25:26-04:00
weight: 6
draft: false
---


### Directory structure
```
conf/
    onboarding.json
    refinery_transformations.json
    treasury_transformations.json
    dqe/
        landing_data_quality_expectations.json
```

1. Create [onboarding.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/onboarding.template)
2. Create [refinery_transformations.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/refinery_transformations.json) with SQL queries
3. Create [treasury_transformations.json](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/treasury_transformations.json) with SQL queries (optional for gold layer)
4. Create data quality rules json's for each entity e.g. [Data Quality Rules](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/dqe/)

The `onboarding.json` file contains links to transformation JSON files and data quality expectation files.

### onboarding.json File structure: Examples( [Autoloader](https://github.com/databrickslabs/dlt-meta/blob/main/examples/cloudfiles-onboarding.template), [Eventhub](https://github.com/databrickslabs/dlt-meta/blob/main/examples/eventhub-onboarding.template), [Kafka](https://github.com/databrickslabs/dlt-meta/blob/main/examples/kafka-onboarding.template) )
`env` is your environment placeholder e.g `dev`, `prod`, `stag`
| Field | Description |
| :-----------: | :----------- |
| data_flow_id | This is unique identifier for pipeline |
| data_flow_group | This is group identifier for launching multiple pipelines under single Lakeflow Declarative Pipeline |
| source_format | Source format e.g `cloudFiles`, `eventhub`, `kafka`, `delta`, `snapshot` |
| source_details | This map Type captures all source details for cloudfiles = `source_schema_path`, `source_path_{env}`, `source_catalog`, `source_database`, `source_metadata` For eventhub= `source_schema_path` , `eventhub.accessKeyName`, `eventhub.accessKeySecretName`, `eventhub.name` , `eventhub.secretsScopeName` , `kafka.sasl.mechanism`, `kafka.security.protocol`, `eventhub.namespace`, `eventhub.port`. For Source schema file spark DDL schema format parsing is supported <br> In case of custom schema format then write schema parsing function `landing_schema_mapper(schema_file_path, spark):Schema` and provide to `OnboardDataflowspec` initialization <br> e.g `onboardDataFlowSpecs = OnboardDataflowspec(spark, dict_obj,landing_schema_mapper).onboardDataFlowSpecs()`.<br> For cloudFiles option _metadata columns addtiion there is `source_metadata` tag with attributes: `include_autoloader_metadata_column` flag (`True` or `False` value) will add _metadata column to target landing dataframe, `autoloader_metadata_col_name` if this provided then will be used to rename _metadata to this value otherwise default is `source_metadata`,`select_metadata_cols:{key:value}` will be used to extract columns from _metadata. key is target dataframe column name and value is expression used to add column from _metadata column. <br> for snapshot= `snapshot_format`, `source_path_{env}` |
| landing_catalog_{env} | Unity catalog name |         
| landing_database_{env} | Delta lake landing database name. |
| landing_table | Delta lake landing table name |
| landing_table_comment | landing table comment |
| landing_reader_options | Reader options which can be provided to spark reader <br> e.g multiline=true,header=true in json format |
| landing_parition_columns | landing table partition cols list |
| landing_cluster_by | landing tables cluster by cols list |
| landing_cdc_apply_changes | landing cdc apply changes Json |
| landing_apply_changes_from_snapshot | landing apply changes from snapshot Json e.g. Mandatory fields: keys=["userId"], scd_type=`1` or `2` optional fields: track_history_column_list=`[col1]`, track_history_except_column_list=`[col2]` | 
| landing_table_path_{env} | landing table storage path.|
| landing_table_properties | Lakeflow Declarative Pipeline table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| landing_sink | Lakeflow Declarative Pipeline Sink API properties: e.g Delta: `{"name": "landing_sink","format": "delta","options": {"tableName": "my_catalog.my_schema.my_table"}}`, Kafka:`{"name": "landing_sink","format": "kafka","options": { "kafka.bootstrap.servers": "host:port","subscribe": "my_topic"}}` |
| landing_data_quality_expectations_json | landing table data quality expectations |
| landing_catalog_quarantine_{env} | Unity catalog name | 
| landing_database_quarantine_{env} | landing database for quarantine data which fails expectations. |
| landing_quarantine_table	| landing Table for quarantine data which fails expectations |
| landing_quarantine_table_comment | landing quarantine table comment |
| landing_quarantine_table_path_{env} | landing database for quarantine data which fails expectations. |
| landing_quarantine_table_partitions | landing quarantine tables partition cols |
| landing_quarantine_table_cluster_by | landing quarantine tables cluster cols |
| landing_quarantine_table_properties | Lakeflow Declarative Pipeline table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false" }` |
| landing_append_flows | landing table append flows json. e.g.`"landing_append_flows":[{"name":"customer_landing_flow", "create_streaming_table": false,"source_format": "cloudFiles", "source_details": {"source_database": "APP","source_table":"CUSTOMERS", "source_path_dev": "tests/resources/data/customers", "source_schema_path": "tests/resources/schema/customer_schema.ddl"},"reader_options": {"cloudFiles.format": "json","cloudFiles.inferColumnTypes": "true","cloudFiles.rescuedDataColumn": "_rescued_data"},"once": true}]` |
| refinery_catalog_{env} | Unit Catalog name. |
| refinery_database_{env} | refinery database name. |
| refinery_table | refinery table name |
| refinery_table_comment | refinery table comments |
| refinery_partition_columns | refinery table partition columns list |
| refinery_cluster_by | refinery tables cluster by cols list |
| refinery_cdc_apply_changes | refinery cdc apply changes Json |
| refinery_table_path_{env} | refinery table storage path. |
| refinery_table_properties | Lakeflow Declarative Pipeline table properties map. e.g. `{"pipelines.autoOptimize.managed": "false" , "pipelines.autoOptimize.zOrderCols": "year,month", "pipelines.reset.allowed": "false"}` |
| refinery_sink | Lakeflow Declarative Pipeline Sink API properties: e.g Delta:`{"name": "refinery_sink","format": "delta","options": {"tableName": "my_catalog.my_schema.my_table"}}`, Kafka:`{"name": "refinery_sink","format": "kafka","options": { "kafka.bootstrap.servers": "host:port","subscribe": "my_topic"}}`|
| refinery_transformation_json | refinery table sql transformation json path |
| refinery_data_quality_expectations_json_{env} | refinery table data quality expectations json file path
| refinery_append_flows | refinery table append flows json. e.g.`"refinery_append_flows":[{"name":"customer_landing_flow", 
| refinery_apply_changes_from_snapshot | refinery apply changes from snapshot Json e.g. Mandatory fields: keys=["userId"], scd_type=`1` or `2` optional fields: track_history_column_list=`[col1]`, track_history_except_column_list=`[col2]`|



### Data Quality Rules File Structure([Examples](https://github.com/databrickslabs/dlt-meta/tree/main/examples/dqe))
| Field | Description |
| :-----------: | :----------- |
| expect | Specify multiple data quality sql for each field when records that fail validation should be included in the target dataset| 
| expect_or_fail  | Specify multiple data quality sql for each field when records that fail validation should halt pipeline execution |
| expect_or_drop  | Specify multiple data quality sql for each field when records that fail validation should be dropped from the target dataset |
| expect_or_quarantine  | Specify multiple data quality sql for each field when records that fails validation will be dropped from main table and inserted into quarantine table specified in dataflowspec (only applicable for landing layer) |


### Refinery/Treasury Transformation File Structure([Example](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/refinery_transformations.json))
| Field | Description |
| :-----------: | :----------- |
| target_table | Specify target table name : Type String |
| target_partition_cols  | Specify partition columns : Type Array |
| sql_query | Specify full SQL SELECT query including FROM and WHERE clauses : Type String. Supports JOINs, GROUP BY, aggregations, etc. Use temp view name `source_{dataFlowId}` to reference the primary source table. |

**Example Refinery Transformation**:
```json
{
  "target_table": "customers_clean",
  "sql_query": "SELECT customer_id, UPPER(first_name) as first_name, email FROM source_201 WHERE email IS NOT NULL"
}
```

**Example Treasury Transformation with JOIN**:
```json
{
  "target_table": "customer_order_summary",
  "sql_query": "SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_revenue FROM refinery_customers c LEFT JOIN refinery_orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.name"
}
```
