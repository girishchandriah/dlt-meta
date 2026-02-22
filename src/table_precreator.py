"""Table Pre-Creation Utility for DLT-Meta.

This module provides automatic table pre-creation functionality to ensure
target tables exist before DLT pipeline execution.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import types as T

from .dataflow_spec import LandingDataflowSpec, RefineryDataflowSpec, TreasuryDataflowSpec

logger = logging.getLogger("dlt-meta")


class TablePreCreator:
    """Utility class for automatic table pre-creation in DLT pipelines.

    This class handles checking table existence and creating empty Delta tables
    with proper schema, partitioning, and properties before DLT pipeline execution.
    """

    def __init__(self, spark: SparkSession, uc_enabled: bool):
        """Initialize TablePreCreator.

        Args:
            spark: Spark session
            uc_enabled: Whether Unity Catalog is enabled
        """
        self.spark = spark
        self.uc_enabled = uc_enabled
        self.logger = logging.getLogger("dlt-meta")

    def ensure_table_exists(self, dataflow_spec, layer_type: str) -> None:
        """Ensure target table exists, create if not.

        This is the main entry point for table pre-creation. It checks if the target
        table exists and creates it with the proper schema if it doesn't.

        Args:
            dataflow_spec: Landing, Refinery, or Treasury dataflow spec
            layer_type: Layer type ("landing", "refinery", or "treasury")

        Raises:
            ValueError: If schema path not provided or target details missing
            RuntimeError: If table creation fails
        """
        # Skip pre-creation for tables with CDC apply changes configured
        # DLT manages these tables automatically
        if hasattr(dataflow_spec, 'cdcApplyChanges') and dataflow_spec.cdcApplyChanges:
            self.logger.info(
                f"Skipping pre-creation for {layer_type} table (dataFlowId={dataflow_spec.dataFlowId}) "
                f"because CDC apply changes is configured. DLT will manage this table."
            )
            return

        # Extract target details (targetDetails is already dict-like)
        if not dataflow_spec.targetDetails:
            raise ValueError(f"Missing targetDetails for {layer_type} (dataFlowId={dataflow_spec.dataFlowId})")

        catalog = dataflow_spec.targetDetails.get('catalog')
        database = dataflow_spec.targetDetails.get('database')
        table = dataflow_spec.targetDetails.get('table')

        if not database or not table:
            raise ValueError(
                f"Missing database or table in targetDetails for {layer_type} "
                f"(dataFlowId={dataflow_spec.dataFlowId})"
            )

        full_name = f"{catalog}.{database}.{table}" if catalog else f"{database}.{table}"

        # Check if table exists
        if self._table_exists(catalog, database, table):
            self.logger.info(f"Table already exists: {full_name}")
            return

        # Get schema path based on layer
        schema_path = None
        if layer_type == "landing":
            # First try landingSchemaPath (new approach)
            schema_path = dataflow_spec.landingSchemaPath

            # Fallback to sourceDetails for backward compatibility
            if not schema_path and dataflow_spec.sourceDetails:
                self.logger.info("landingSchemaPath not found, falling back to sourceDetails")
                try:
                    # Try bracket notation first (works for Spark Row types)
                    schema_path = dataflow_spec.sourceDetails["source_schema_path"]
                except (KeyError, TypeError):
                    try:
                        # Fallback to .get() method
                        schema_path = dataflow_spec.sourceDetails.get("source_schema_path")
                    except Exception:
                        # Last resort: convert to dict and access
                        source_dict = dict(dataflow_spec.sourceDetails) if dataflow_spec.sourceDetails else {}
                        schema_path = source_dict.get("source_schema_path")
        elif layer_type == "refinery":
            schema_path = dataflow_spec.refinerySchemaPath
        elif layer_type == "treasury":
            schema_path = dataflow_spec.treasurySchemaPath
        else:
            raise ValueError(f"Invalid layer_type: {layer_type}")

        if not schema_path:
            raise ValueError(
                f"Schema path not provided for {layer_type} layer "
                f"(dataFlowId={dataflow_spec.dataFlowId}). "
                f"Please add '{layer_type}_schema_path' to onboarding configuration."
            )

        # Load schema from DDL file
        schema = self._get_schema_from_ddl(schema_path)

        # Extract table properties (already dict-like)
        partition_cols = (
            dataflow_spec.partitionColumns
            if hasattr(dataflow_spec, 'partitionColumns') and dataflow_spec.partitionColumns
            else None
        )
        table_properties = (
            dataflow_spec.tableProperties
            if hasattr(dataflow_spec, 'tableProperties') and dataflow_spec.tableProperties
            else {}
        )
        cluster_by = (
            dataflow_spec.clusterBy
            if hasattr(dataflow_spec, 'clusterBy') and dataflow_spec.clusterBy
            else None
        )

        # Create table
        self._create_empty_table(
            catalog, database, table, schema,
            partition_cols, table_properties, cluster_by
        )

    def _table_exists(self, catalog: Optional[str], database: str, table: str) -> bool:
        """Check if table exists in Unity Catalog or Hive metastore.

        Args:
            catalog: Catalog name (optional, for Unity Catalog)
            database: Database/schema name
            table: Table name

        Returns:
            True if table exists, False otherwise
        """
        try:
            if catalog and self.uc_enabled:
                # Unity Catalog: use full three-part name
                full_table_name = f"{catalog}.{database}.{table}"
                return self.spark.catalog.tableExists(full_table_name)
            else:
                # Legacy Hive metastore: use two-part name
                return self.spark.catalog.tableExists(database, table)
        except Exception as e:
            self.logger.debug(f"Table existence check failed: {e}")
            return False

    def _get_schema_from_ddl(self, ddl_file_path: str) -> StructType:
        """Load and parse DDL file to Spark StructType.

        This method reuses the logic from onboard_dataflowspec.__get_landing_schema()
        to maintain consistency with existing schema loading.

        DDL file format: "column_name: data_type, column_name: data_type, ..."
        Example: "id: int, name: string, created_date: date"

        Args:
            ddl_file_path: Path to DDL file

        Returns:
            Spark StructType schema

        Raises:
            ValueError: If DDL file cannot be read or parsed
        """
        if not ddl_file_path:
            raise ValueError("Schema path is required but not provided")

        # Read DDL file
        try:
            ddl_schema_str = self.spark.read.text(
                paths=ddl_file_path, wholetext=True
            ).collect()[0]["value"]
            self.logger.debug(f"Loaded DDL from {ddl_file_path}: {ddl_schema_str[:100]}...")
        except Exception as e:
            raise ValueError(f"Failed to read DDL file '{ddl_file_path}': {e}")

        # Parse DDL string to StructType
        try:
            spark_schema = T._parse_datatype_string(ddl_schema_str)
            self.logger.info(f"Parsed schema with {len(spark_schema.fields)} fields from {ddl_file_path}")
            return spark_schema
        except Exception as e:
            raise ValueError(
                f"Failed to parse DDL schema from '{ddl_file_path}': {e}. "
                f"Ensure DDL format is: column_name: data_type, column_name: data_type, ..."
            )

    def _create_empty_table(
        self,
        catalog: Optional[str],
        database: str,
        table: str,
        schema: StructType,
        partition_cols: Optional[list],
        table_properties: dict,
        cluster_by: Optional[list]
    ) -> None:
        """Create empty Delta table with schema and properties.

        Args:
            catalog: Catalog name (optional, for Unity Catalog)
            database: Database/schema name
            table: Table name
            schema: Spark StructType schema
            partition_cols: List of partition column names (optional)
            table_properties: Dict of table properties (optional)
            cluster_by: List of clustering column names (optional)

        Raises:
            RuntimeError: If table creation fails
        """
        full_table_name = f"{catalog}.{database}.{table}" if catalog else f"{database}.{table}"

        self.logger.info(f"Creating table using DataFrame API: {full_table_name}")

        try:
            # Create empty DataFrame with the schema
            empty_df = self.spark.createDataFrame([], schema)

            # Start building the write operation
            writer = empty_df.write.format("delta").mode("append")

            # Add table properties (options must be set before saveAsTable)
            if table_properties:
                for key, value in table_properties.items():
                    writer = writer.option(key, value)

            # Add partitioning
            if partition_cols:
                # Filter out empty strings from partition columns
                valid_partition_cols = [col for col in partition_cols if col and col.strip()]
                if valid_partition_cols:
                    self.logger.info(f"Partitioning by: {valid_partition_cols}")
                    writer = writer.partitionBy(*valid_partition_cols)

            # Add clustering (Databricks-specific)
            if cluster_by:
                # Filter out empty strings from cluster columns
                valid_cluster_cols = [col for col in cluster_by if col and col.strip()]
                if valid_cluster_cols:
                    self.logger.info(f"Clustering by: {valid_cluster_cols}")
                    # Use clusterBy option for Delta tables
                    cluster_cols_str = ",".join(valid_cluster_cols)
                    writer = writer.option("clusterBy", cluster_cols_str)

            # Save as table
            writer.saveAsTable(full_table_name)
            self.logger.info(f"Successfully created table: {full_table_name}")

        except Exception as e:
            raise RuntimeError(
                f"Failed to create table '{full_table_name}': {e}. "
                f"Check permissions and table configuration."
            )
