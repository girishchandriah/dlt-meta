# Changelog

## [Unreleased]
### ⚠️ Major Changes - Terminology Update
- **Medallion Architecture Layer Naming**: Updated terminology to align with modern lakehouse best practices
  - Bronze → **Landing** (raw ingestion layer)
  - Silver → **Refinery** (cleaned and conformed data layer)
  - Gold → **Treasury** (business-level aggregates layer)
- **Backward Compatibility**: All existing parameter names (bronze_, silver_) are still supported for backward compatibility
- **New Parameter Names**: Recommended to use new parameter names (landing_, refinery_, treasury_) for new implementations

### Added
- **Treasury (Gold) Layer Support**: Full support for treasury layer with SQL transformations and JOINs
- **Enhanced SQL Transformations**: Refinery and treasury layers now support full SQL queries including:
  - Complex SELECT statements with JOINs
  - GROUP BY and aggregations
  - WHERE clauses and filtering
  - Multi-table queries and CTEs
- **Extended Layer Support**: All major features now support all three layers:
  - Data Quality Expectations: Landing, Refinery, Treasury
  - Custom Transformations: Landing, Refinery, Treasury
  - Liquid Clustering: Landing, Refinery, Treasury
  - Sinks (delta, kafka): Landing, Refinery, Treasury
  - Quarantine Tables: Landing, Refinery
- **Pipeline Chaining Options**: New layer combination options:
  - `layer=landing_refinery` - Combines landing and refinery layers
  - `layer=refinery_treasury` - Combines refinery and treasury layers
  - `layer=landing_refinery_treasury` - Combines all three layers
- **New Demo Configurations**:
  - refinery_transformations.json - Demonstrates SQL transformations for refinery layer
  - treasury_transformations.json - Demonstrates SQL JOINs and aggregations for treasury layer

### Documentation Updates
- Updated all documentation to reflect new terminology
- Added comprehensive examples for SQL transformations with JOINs
- Updated CLI and manual guides with new parameter names
- Expanded FAQ with information about all three layers
- Updated demo documentation to use new terminology

### Migration Notes
- **No Breaking Changes**: Existing configurations with bronze/silver parameters will continue to work
- **Recommended Updates**: For clarity and future compatibility, update configurations to use new terminology
- **API Parameters**: Both old and new parameter names are accepted:
  - Old: bronze_dataflowspec_table, silver_dataflowspec_table, onboard_layer="bronze_silver"
  - New: landing_dataflowspec_table, refinery_dataflowspec_table, onboard_layer="landing_refinery"

## [v0.0.10]
### ⚠️ Breaking Changes
- **DPM Mode Flag Removal from v0.0.9**: DLT-META v0.0.9 pipelines using DPM mode flag must be migrated to the default publishing mode before upgrading. This change is metadata-only and doesn't impact existing datasets, but is irreversible.
- **invoke_dlt_pipeline Argument Changes**: Method arguments now require layer-specific prefixes (bronze_ or silver_) to support apply_changes_from_snapshot in both layers. This affects existing pipeline configurations using the previous argument naming.

### Migration Guide
1. **DPM Mode Migration**:
   - Before upgrading to v0.0.10, update pipeline JSON settings as per Databricks documentation [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode)
   - This is a one-way migration - ensure all stakeholders are informed
   - Verify pipeline functionality in test environment first

2. **invoke_dlt_pipeline Updates**:
   - Method signature changed to support layer-specific functions:
     ```python
     invoke_dlt_pipeline(
         spark,
         layer,
         bronze_custom_transform_func=None,    # Previously: custom_transform_func
         silver_custom_transform_func=None,    # New in v0.0.10
         bronze_next_snapshot_and_version=None,  # Previously: next_snapshot_and_version
         silver_next_snapshot_and_version=None   # New in v0.0.10
     )
     ```
   - Layer-specific functions allow different transformations for bronze and silver layers
   - Existing code using single custom_transform_func should move to bronze_custom_transform_func
   - Existing code using next_snapshot_and_version should move to bronze_next_snapshot_and_version
   - Review and update all pipeline configurations using this method

### Added
- Added apply_changes_from_snapshot support in silver layer [PR](https://github.com/databrickslabs/dlt-meta/pull/187)
- Added UI using databricks lakehouse app for onboarding/deploy commands [PR](https://github.com/databrickslabs/dlt-meta/pull/168)
- Added support for non-Delta as sinks(delta, kafka) [PR](https://github.com/databrickslabs/dlt-meta/pull/157)
- Added quarantine support in silver layer for data quality rules [PR](https://github.com/databrickslabs/dlt-meta/pull/191)
- Added support for table comments, column comments, and cluster_by [PR](https://github.com/databrickslabs/dlt-meta/pull/91)
- Added catalog support for sourceDetails and targetDetails [PR](https://github.com/databrickslabs/dlt-meta/issues/173)
- Added DBDemos for dlt-meta [PR](https://github.com/databrickslabs/dlt-meta/issues/183)
- Added YAML support for onboarding [PR](https://github.com/databrickslabs/dlt-meta/issues/184)
- Fixed issue cluster by not working with bronze append only table [PR](https://github.com/databrickslabs/dlt-meta/issues/197)
- Fixed issue view name containing period when using DPM [PR](https://github.com/databrickslabs/dlt-meta/issues/169)
- Fixed issue CLI onboarding overwrite option always set to True [PR](https://github.com/databrickslabs/dlt-meta/issues/163)
- Fixed issue Silver DLT not creating based on passed database [PR](https://github.com/databrickslabs/dlt-meta/issues/160)
- Fixed issue PyPI download stats display [PR](https://github.com/databrickslabs/dlt-meta/issues/200)
- Fixed issue Silver Data Quality not working [PR](https://github.com/databrickslabs/dlt-meta/issues/156)
- Fixed issue Removed DPM flag check inside dataflowpipeline [PR](https://github.com/databrickslabs/dlt-meta/issues/177)
- Fixed issue Updated dlt-meta demos into Delta Live Tables Notebook github [PR](https://github.com/databrickslabs/dlt-meta/issues/158)
- Fixed issue Adding multiple col support for auto_cdc api [PR](https://github.com/databrickslabs/dlt-meta/pull/224)
- Fixed issue Added support for custom transformations for Kafka/Delta [PR](https://github.com/databrickslabs/dlt-meta/pull/228)


## [v.0.0.9] 
- Added  apply_changes_from_snapshot api support in bronze layer: [PR](https://github.com/databrickslabs/dlt-meta/pull/124)
- Added dlt append_flow api support for silver layer: [PR](https://github.com/databrickslabs/dlt-meta/pull/63)
- Added support for file metadata columns for autoloader: [PR](https://github.com/databrickslabs/dlt-meta/pull/56)
- Added support for Bring your own custom transformation: [Issue](https://github.com/databrickslabs/dlt-meta/issues/68)
- Added support to Unify PyPI releases with GitHub OIDC: [PR](https://github.com/databrickslabs/dlt-meta/pull/62)
- Added demo for append_flow and file_metadata options: [PR](https://github.com/databrickslabs/dlt-meta/issues/74)
- Added Demo for silver fanout architecture: [PR](https://github.com/databrickslabs/dlt-meta/pull/83)
- Added  hugo-theme-relearn themee: [PR](https://github.com/databrickslabs/dlt-meta/pull/132)
- Added unit tests to showcase silver layer fanout examples: [PR](https://github.com/databrickslabs/dlt-meta/pull/67)
- Added liquid cluster support: [PR](https://github.com/databrickslabs/dlt-meta/pull/136)
- Added support for UC Volume + Serverless support for CLI, Integration tests and Demos: [PR](https://github.com/databrickslabs/dlt-meta/pull/105)
- Added Chaining bronze/silver pipelines into single DLT: [PR](https://github.com/databrickslabs/dlt-meta/pull/130)
- Fixed issue for No such file or directory: '/demo' :[PR](https://github.com/databrickslabs/dlt-meta/issues/59)
- Fixed issue DLT-META CLI onboard command issue for Azure: databricks.sdk.errors.platform.ResourceAlreadyExists :[PR](https://github.com/databrickslabs/dlt-meta/issues/51)
- Fixed issue Changed dbfs.create to mkdirs for CLI: [PR](https://github.com/databrickslabs/dlt-meta/pull/53)
- Fixed issue DLT-META CLI should use pypi lib instead of whl : [PR](https://github.com/databrickslabs/dlt-meta/pull/79)
- Fixed issue Onboarding with multiple partition columns errors out: [PR](https://github.com/databrickslabs/dlt-meta/pull/134)

## [v.0.0.7] 
- Added dlt-meta cli documentation and readme with browser support: [PR](https://github.com/databrickslabs/dlt-meta/pull/45)

## [v.0.0.6] 
- migrate to create streaming table api from create streaming live table: [PR](https://github.com/databrickslabs/dlt-meta/pull/39)

## [v.0.0.5] 
- Enabled Unity Catalog support: [PR](https://github.com/databrickslabs/dlt-meta/pull/28)
- Added databricks labs cli: [PR](https://github.com/databrickslabs/dlt-meta/pull/28)

## [v0.0.4] - 2023-10-09
### Added
- Functionality to introduce an new option for event hub configuration. Namely a source_details option 'eventhub.accessKeySecretName' to properly construct the eh_shared_key_value properly. Without this option, there were errors while connecting to the event hub service (linked to [issue-13 - java.lang.RuntimeException: non-nullable field authBytes was serialized as null #13](https://github.com/databrickslabs/dlt-meta/issues/13))

## [v0.0.3] - 2023-06-07
### Fixed
-  infer datatypes from sequence_by to __START_AT, __END_AT for apply changes API
### Changed
-   setup.py for version
### Removed
-   Git release tag from github actions

## [v0.0.2] - 2023-05-11
### Added
- Table properties support for bronze, quarantine and silver tables using create_streaming_live_table api call
- Support for track history column using apply_changes api
- Support for delta as source
- Validation for bronze/silver onboarding
### Fixed
- Input schema parsing issue in onboarding
### Modified
-  Readme and docs to include above features

## [v0.0.1] - 2023-03-22
### Added

- Initial public release version.