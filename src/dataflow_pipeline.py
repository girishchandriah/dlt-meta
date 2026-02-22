"""DataflowPipeline provide generic code using dataflowspec."""
import json
import logging
from typing import Callable, Optional
import ast
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, struct
from pyspark.sql.types import StructType, StructField
from src.dataflow_spec import LandingDataflowSpec, RefineryDataflowSpec, TreasuryDataflowSpec, DataflowSpecUtils
from src.pipeline_writers import AppendFlowWriter, DLTSinkWriter
from src.__about__ import __version__
from src.pipeline_readers import PipelineReaders
from src.table_precreator import TablePreCreator

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class DataflowPipeline:
    """This class uses dataflowSpec to launch Lakeflow Declarative Pipelines.

    Raises:
        Exception: "Dataflow not supported!"

    Returns:
        [type]: [description]
    """

    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine=None,
                 custom_transform_func: Optional[Callable] = None,
                 next_snapshot_and_version: Optional[Callable] = None):
        """Initialize Constructor."""
        logger.info(
            f"""dataflowSpec={dataflow_spec} ,
                view_name={view_name},
                view_name_quarantine={view_name_quarantine}"""
        )
        if isinstance(dataflow_spec, LandingDataflowSpec) or isinstance(dataflow_spec, RefineryDataflowSpec) or isinstance(dataflow_spec, TreasuryDataflowSpec):
            self.__initialize_dataflow_pipeline(
                spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func, next_snapshot_and_version
            )
        else:
            raise Exception("Dataflow not supported!")

    # Type-safe helper methods for dictionary access
    def _safe_dict_access(self, dict_obj, key, default=None):
        """Safely access dictionary-like objects with proper type casting."""
        if dict_obj is None:
            return default
        dict_data = dict(dict_obj) if hasattr(dict_obj, '__iter__') else dict_obj
        return dict_data.get(key, default)

    def _safe_dict_get_item(self, dict_obj, key):
        """Safely get item from dictionary-like objects with proper type casting."""
        if dict_obj is None:
            raise KeyError(f"Dictionary is None, cannot access key: {key}")
        dict_data = dict(dict_obj) if hasattr(dict_obj, '__iter__') else dict_obj
        return dict_data[key]

    def _get_dict_as_dict(self, dict_obj):
        """Convert map-type objects to proper dictionaries."""
        if dict_obj is None:
            return {}
        return dict(dict_obj) if hasattr(dict_obj, '__iter__') else dict_obj

    def _get_source_details(self):
        """Get source details as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.sourceDetails)

    def _get_target_details(self):
        """Get target details as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.targetDetails)

    def _get_quarantine_target_details(self):
        """Get quarantine target details as a proper dictionary."""
        if hasattr(self.dataflowSpec, 'quarantineTargetDetails'):
            return self._get_dict_as_dict(self.dataflowSpec.quarantineTargetDetails)
        return {}

    def _get_reader_config_options(self):
        """Get reader config options as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.readerConfigOptions)

    def _get_table_properties(self):
        """Get table properties as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.tableProperties)

    def __initialize_dataflow_pipeline(
        self, spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func: Callable,
        next_snapshot_and_version: Callable
    ):
        """Initialize dataflow pipeline state."""
        self.spark = spark
        uc_enabled_str = spark.conf.get("spark.databricks.unityCatalog.enabled", "False")
        spark.conf.set("databrickslab.dlt-meta.version", f"{__version__}")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        self.custom_transform_func = custom_transform_func
        if dataflow_spec.cdcApplyChanges:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        # appendFlows only exists for Landing and Refinery layers
        if hasattr(dataflow_spec, 'appendFlows') and dataflow_spec.appendFlows:
            self.appendFlows = DataflowSpecUtils.get_append_flows(dataflow_spec.appendFlows)
        else:
            self.appendFlows = None
        # applyChangesFromSnapshot only exists for Landing and Refinery layers
        if hasattr(dataflow_spec, 'applyChangesFromSnapshot') and dataflow_spec.applyChangesFromSnapshot:
            self.applyChangesFromSnapshot = DataflowSpecUtils.get_apply_changes_from_snapshot(
                self.dataflowSpec.applyChangesFromSnapshot
            )
        if isinstance(dataflow_spec, LandingDataflowSpec):
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        elif isinstance(dataflow_spec, RefineryDataflowSpec) or isinstance(dataflow_spec, TreasuryDataflowSpec):
            self.schema_json = None
        self.next_snapshot_and_version = None
        self.next_snapshot_and_version = next_snapshot_and_version
        self.next_snapshot_and_version_from_source_view = False
        if self.dataflowSpec.sourceDetails and self.dataflowSpec.sourceDetails.get("snapshot_format", None):
            self.snapshot_source_format = self.dataflowSpec.sourceDetails["snapshot_format"]
        else:
            self.snapshot_source_format = None
        self.refinery_schema = None

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def is_create_view(self):
        """Determine if a view should be created based on source details and snapshot configuration.

        Returns:
            bool: True if a view should be created, False otherwise.
        """
        # if sourceDetails is provided and snapshot_format is delta, then create a view
        # if next_snapshot_and_version is provided, then do not create a view
        # otherwise create a view
        if (self.dataflowSpec.sourceDetails and self.dataflowSpec.sourceDetails.get("snapshot_format") == "delta"):
            self.next_snapshot_and_version_from_source_view = True
            return True
        elif self.next_snapshot_and_version:
            return False
        return True

    def read(self):
        """Read DLT."""
        logger.info("In read function")
        if isinstance(self.dataflowSpec, LandingDataflowSpec) and self.is_create_view():
            dlt.view(
                self.read_landing,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, RefineryDataflowSpec) and self.is_create_view():
            dlt.view(
                self.read_refinery,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, TreasuryDataflowSpec) and self.is_create_view():
            dlt.view(
                self.read_treasury,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        else:
            if not self.next_snapshot_and_version:
                raise Exception("Dataflow read not supported for {}".format(type(self.dataflowSpec)))
        if self.appendFlows:
            self.read_append_flows()

    def read_append_flows(self):
        if hasattr(self.dataflowSpec, 'appendFlows') and self.dataflowSpec.appendFlows:
            append_flows_schema_map = getattr(self.dataflowSpec, 'appendFlowsSchemas', None)
            for append_flow in self.appendFlows:
                flow_schema = None
                if append_flows_schema_map:
                    flow_schema = append_flows_schema_map.get(append_flow.name)
                pipeline_reader = PipelineReaders(
                    self.spark,
                    append_flow.source_format,
                    append_flow.source_details,
                    append_flow.reader_options,
                    json.loads(flow_schema) if flow_schema else None
                )
                if append_flow.source_format == "cloudFiles":
                    dlt.view(pipeline_reader.read_dlt_cloud_files,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
                elif append_flow.source_format == "delta":
                    dlt.view(pipeline_reader.read_dlt_delta,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
                elif append_flow.source_format == "eventhub" or append_flow.source_format == "kafka":
                    dlt.view(pipeline_reader.read_kafka,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
        else:
            raise Exception(f"Append Flows not found for dataflowSpec={self.dataflowSpec}")

    def write(self):
        """Write DLT."""
        if self.dataflowSpec.sinks:
            dlt_sinks = DataflowSpecUtils.get_sinks(self.dataflowSpec.sinks, self.spark)
            for dlt_sink in dlt_sinks:
                DLTSinkWriter(dlt_sink, self.view_name).write_to_sink()
        if isinstance(self.dataflowSpec, LandingDataflowSpec):
            self.write_landing()
        elif isinstance(self.dataflowSpec, RefineryDataflowSpec):
            self.write_refinery()
        elif isinstance(self.dataflowSpec, TreasuryDataflowSpec):
            self.write_treasury()
        else:
            raise Exception(f"Dataflow write not supported for type= {type(self.dataflowSpec)}")

    def _get_target_table_info(self):
        """Extract target table information from dataflow spec."""
        target_details = self._get_target_details()
        target_path = None if self.uc_enabled else target_details.get("path")
        target_cl = target_details.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = target_details['database']
        target_table_name = target_details['table']
        target_table = f"{target_cl_name}{target_db_name}.{target_table_name}"
        return target_path, target_table, target_table_name

    def _get_table_comment(self, target_table, layer_name="landing"):
        """Generate appropriate comment for the table."""
        target_details = self._get_target_details()
        if 'comment' in target_details:
            return target_details.get('comment')
        return f"{layer_name} dlt table{target_table}"

    def _write_standard_table(self, layer_name="landing"):
        """Write standard DLT table for landing, refinery, or treasury layer."""
        target_path, target_table, target_table_name = self._get_target_table_info()
        comment = self._get_table_comment(target_table, layer_name)
        dlt.table(
            self.write_to_delta,
            name=f"{target_table}",
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            table_properties=self.dataflowSpec.tableProperties,
            path=target_path,
            comment=comment,
        )

    def _write_treasury_batch_cdc(self):
        """Write treasury batch data - for CDC with batch sources, write to intermediate table.

        Treasury batch CDC limitation: DLT's create_auto_cdc_flow requires streaming sources.
        For batch treasury with CDC config, this writes to an intermediate table per flow.
        User should manually create final merge logic or use a post-processing step.
        """
        target_path, target_table, target_table_name = self._get_target_table_info()

        # For batch CDC, write to intermediate table with flow ID suffix
        # Final table merge must be handled separately by user
        data_flow_id = str(self.dataflowSpec.dataFlowId).replace('-', '_').replace('.', '_')
        intermediate_table_name = f"{target_table_name}_{data_flow_id}"

        target_details = self._get_target_details()
        target_cl = target_details.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = target_details['database']
        intermediate_table = f"{target_cl_name}{target_db_name}.{intermediate_table_name}"

        comment = f"Treasury intermediate table for batch CDC flow {self.dataflowSpec.dataFlowId}. " \
                  f"Merge into {target_table} using separate DLT table or post-processing."

        logger.warning(
            f"Treasury batch CDC limitation: Writing to intermediate table {intermediate_table}. "
            f"Create a separate DLT table to merge intermediate tables into {target_table_name}."
        )

        dlt.table(
            self.write_to_delta,
            name=intermediate_table,
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            table_properties=self.dataflowSpec.tableProperties,
            path=target_path if target_path else None,
            comment=comment,
        )

    def write_layer_table(self):
        """Write Landing, Refinery, or Treasury tables using unified logic."""
        is_landing = isinstance(self.dataflowSpec, LandingDataflowSpec)
        is_refinery = isinstance(self.dataflowSpec, RefineryDataflowSpec)
        is_treasury = isinstance(self.dataflowSpec, TreasuryDataflowSpec)

        layer_name = "landing" if is_landing else ("refinery" if is_refinery else "treasury")

        # Handle special cases first
        if is_landing:
            landing_spec = self.dataflowSpec
            # Handle snapshot format for landing
            if landing_spec.sourceFormat and landing_spec.sourceFormat.lower() == "snapshot":
                if self.next_snapshot_and_version:
                    self.apply_changes_from_snapshot()
                else:
                    raise Exception("Snapshot reader function not provided!")
                self._handle_append_flows()
                return
            # Handle data quality expectations for landing
            if landing_spec.dataQualityExpectations:
                self.write_layer_with_dqe()
                self._handle_append_flows()
                return
        elif is_refinery:
            # Handle apply changes from snapshot for refinery
            refinery_spec = self.dataflowSpec
            if refinery_spec.applyChangesFromSnapshot:
                self.apply_changes_from_snapshot()
                self._handle_append_flows()
                return
            # Handle data quality expectations for refinery
            if refinery_spec.dataQualityExpectations:
                self.write_layer_with_dqe()
                self._handle_append_flows()
                return
        elif is_treasury:
            # Handle data quality expectations for treasury
            treasury_spec = self.dataflowSpec
            if treasury_spec.dataQualityExpectations:
                self.write_layer_with_dqe()
                return

        # Handle CDC apply changes
        if self.dataflowSpec.cdcApplyChanges and not self.dataflowSpec.dataQualityExpectations:
            if is_treasury:
                # Treasury batch CDC: write to staging table, DLT will merge via views
                # Each flow writes to a unique staging table, then merged into final table
                self._write_treasury_batch_cdc()
            else:
                # Landing/Refinery: use streaming CDC
                self.cdc_apply_changes()
        else:
            # Write standard table
            self._write_standard_table(layer_name)
        # Handle append flows (for landing and refinery)
        if not is_treasury:
            self._handle_append_flows()

    def _handle_append_flows(self):
        """Handle append flows if they exist."""
        if hasattr(self.dataflowSpec, 'appendFlows') and self.dataflowSpec.appendFlows:
            self.write_append_flows()

    def write_landing(self):
        """Write Landing tables."""
        self.write_layer_table()

    def write_refinery(self):
        """Write refinery tables."""
        self.write_layer_table()

    def write_treasury(self):
        """Write treasury tables."""
        self.write_layer_table()

    def read_landing(self) -> DataFrame:
        """Read Landing Table."""
        logger.info("In read_landing func")
        pipeline_reader = PipelineReaders(
            self.spark,
            self.dataflowSpec.sourceFormat,
            self.dataflowSpec.sourceDetails,
            self.dataflowSpec.readerConfigOptions,
            self.schema_json
        )
        landing_dataflow_spec: LandingDataflowSpec = self.dataflowSpec
        input_df = None
        if landing_dataflow_spec.sourceFormat == "cloudFiles":
            input_df = pipeline_reader.read_dlt_cloud_files()
        elif landing_dataflow_spec.sourceFormat == "delta" or landing_dataflow_spec.sourceFormat == "snapshot":
            input_df = pipeline_reader.read_dlt_delta()
        elif landing_dataflow_spec.sourceFormat == "eventhub" or landing_dataflow_spec.sourceFormat == "kafka":
            input_df = pipeline_reader.read_kafka()
        else:
            raise Exception(f"{landing_dataflow_spec.sourceFormat} source format not supported")
        return self.apply_custom_transform_fun(input_df)

    def apply_custom_transform_fun(self, input_df):
        if self.custom_transform_func:
            input_df = self.custom_transform_func(input_df, self.dataflowSpec)
        return input_df

    def replace_sql_placeholders(self, sql_query: str) -> str:
        """Replace placeholders in SQL query with actual catalog and database names.

        Supports placeholders like:
        - {landing_catalog}, {landing_database}
        - {refinery_catalog}, {refinery_database}
        - {treasury_catalog}, {treasury_database}

        Tries to get values from:
        1. DataflowSpec (current and source layers)
        2. Spark configuration (fallback for cross-layer references)

        Args:
            sql_query: SQL query with placeholders

        Returns:
            SQL query with placeholders replaced
        """
        if not sql_query or not sql_query.strip():
            return sql_query

        # Get environment from spark config (e.g., 'nonprod', 'prod', 'preprod')
        env = self.spark.conf.get("env", "prod")
        logger.info(f"Replacing SQL placeholders for environment: {env}")

        replacements = {}

        # Helper function to safely get catalog/database with fallback to spark conf
        def get_catalog_database(layer_name, details_dict=None):
            """Get catalog and database for a layer from dict or spark conf."""
            catalog = None
            database = None

            if details_dict:
                catalog = details_dict.get('catalog', '')
                database = details_dict.get('database', '')

            # Fallback to spark conf if not found in dict
            if not catalog:
                catalog = self.spark.conf.get(f"{layer_name}.catalog", None)
            if not database:
                database = self.spark.conf.get(f"{layer_name}.database", None)

            # Final fallback: use environment-based naming convention for cross-layer references
            # This handles cases where refinery SQL references treasury tables
            if not catalog or not database:
                # Try to infer from current dataflowspec's naming pattern
                current_target = self.dataflowSpec.targetDetails if hasattr(self.dataflowSpec, 'targetDetails') else {}
                if current_target:
                    current_catalog = current_target.get('catalog', '')
                    current_database = current_target.get('database', '')

                    # For nonprod/preprod/prod environments, apply common naming patterns
                    if 'nonprod' in str(current_catalog).lower() or 'nonprod' in str(current_database).lower():
                        if not catalog and layer_name == 'treasury':
                            catalog = 'dataservices_nonprod'
                        if not database and layer_name == 'treasury':
                            database = 'treasury_teradata_base_nonprod'
                    elif 'preprod' in str(current_catalog).lower() or 'preprod' in str(current_database).lower():
                        if not catalog and layer_name == 'treasury':
                            catalog = 'dataservices_preprod'
                        if not database and layer_name == 'treasury':
                            database = 'treasury_teradata_base_preprod'
                    elif 'prod' in str(current_catalog).lower() or 'prod' in str(current_database).lower():
                        if not catalog and layer_name == 'treasury':
                            catalog = 'dataservices_treasury'
                        if not database and layer_name == 'treasury':
                            database = 'treasury_teradata_base'

            return catalog, database

        # Get landing catalog/database
        if isinstance(self.dataflowSpec, LandingDataflowSpec):
            target_details = dict(self.dataflowSpec.targetDetails) if self.dataflowSpec.targetDetails else {}
            landing_catalog, landing_database = get_catalog_database('landing', target_details)
        elif isinstance(self.dataflowSpec, RefineryDataflowSpec):
            # For refinery, landing is the source
            source_details = self._get_source_details()
            landing_catalog, landing_database = get_catalog_database('landing', source_details)
        else:
            # Fallback for other layers
            landing_catalog, landing_database = get_catalog_database('landing')

        if landing_catalog:
            replacements['{landing_catalog}'] = landing_catalog
        if landing_database:
            replacements['{landing_database}'] = landing_database

        # Get refinery catalog/database
        if isinstance(self.dataflowSpec, RefineryDataflowSpec):
            target_details = dict(self.dataflowSpec.targetDetails) if self.dataflowSpec.targetDetails else {}
            refinery_catalog, refinery_database = get_catalog_database('refinery', target_details)
        elif isinstance(self.dataflowSpec, TreasuryDataflowSpec):
            # For treasury, refinery is the source
            source_details = self._get_source_details()
            refinery_catalog, refinery_database = get_catalog_database('refinery', source_details)
        else:
            # Fallback for other layers
            refinery_catalog, refinery_database = get_catalog_database('refinery')

        if refinery_catalog:
            replacements['{refinery_catalog}'] = refinery_catalog
        if refinery_database:
            replacements['{refinery_database}'] = refinery_database

        # Get treasury catalog/database
        if isinstance(self.dataflowSpec, TreasuryDataflowSpec):
            target_details = dict(self.dataflowSpec.targetDetails) if self.dataflowSpec.targetDetails else {}
            treasury_catalog, treasury_database = get_catalog_database('treasury', target_details)
        else:
            # Try to get from spark conf (for cross-layer references)
            treasury_catalog, treasury_database = get_catalog_database('treasury')

        if treasury_catalog:
            replacements['{treasury_catalog}'] = treasury_catalog
        if treasury_database:
            replacements['{treasury_database}'] = treasury_database

        # Perform replacements
        replaced_query = sql_query
        for placeholder, value in replacements.items():
            if placeholder in replaced_query:
                replaced_query = replaced_query.replace(placeholder, value)
                logger.info(f"Replaced {placeholder} with {value}")

        # Log if there are still unreplaced placeholders
        import re
        remaining_placeholders = re.findall(r'\{[^}]+\}', replaced_query)
        if remaining_placeholders:
            logger.warning(f"Unreplaced placeholders found in SQL: {remaining_placeholders}")
            logger.warning(f"Available replacements were: {list(replacements.keys())}")

        return replaced_query

    def execute_sql_transformation(self, source_df: DataFrame, sql_query: str) -> DataFrame:
        """Execute full SQL query with support for JOINs.

        Args:
            source_df: Source DataFrame
            sql_query: Full SELECT query (e.g., "SELECT * FROM table WHERE ...")

        Returns:
            Transformed DataFrame
        """
        if not sql_query or not sql_query.strip():
            return source_df

        # Replace placeholders in SQL query before execution
        sql_query = self.replace_sql_placeholders(sql_query)

        # Create temp view for source table
        source_view_name = f"source_{self.dataflowSpec.dataFlowId}"
        source_df.createOrReplaceTempView(source_view_name)

        try:
            # Execute SQL query
            result_df = self.spark.sql(sql_query)
            return result_df
        except Exception as e:
            logger.error(f"SQL transformation failed for dataFlowId={self.dataflowSpec.dataFlowId}: {str(e)}")
            logger.error(f"SQL Query: {sql_query}")
            raise
        finally:
            # Clean up temp view
            try:
                self.spark.catalog.dropTempView(source_view_name)
            except:
                pass  # Ignore if view doesn't exist

    def get_refinery_schema(self):
        """Get Refinery table Schema."""
        refinery_dataflow_spec: RefineryDataflowSpec = self.dataflowSpec
        source_details = self._get_source_details()
        source_cl = source_details.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = source_details["database"]
        source_table = source_details["table"]
        sql_query = refinery_dataflow_spec.sqlQuery

        # Read source table
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_cl_name}{source_database}.{source_table}"
        ) if self.uc_enabled else self.spark.readStream.load(
            path=source_details.get("path"),
            format="delta"
        )

        # Apply SQL transformation if provided
        if sql_query and sql_query.strip():
            raw_delta_table_stream = self.execute_sql_transformation(raw_delta_table_stream, sql_query)

        return raw_delta_table_stream.schema

    def read_refinery(self) -> DataFrame:
        """Read Refinery tables with SQL transformations."""
        refinery_dataflow_spec: RefineryDataflowSpec = self.dataflowSpec
        source_details = self._get_source_details()
        reader_config_opts = self._get_reader_config_options()

        # Read from landing layer
        source_cl = source_details.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = source_details["database"]
        source_table = source_details["table"]

        if reader_config_opts:
            if refinery_dataflow_spec.sourceFormat == "snapshot":
                landing_df = self.spark.read.options(**reader_config_opts).table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.read.options(**reader_config_opts).load(
                    path=source_details.get("path"), format="delta"
                )
            else:
                landing_df = self.spark.readStream.options(**reader_config_opts).table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.readStream.options(**reader_config_opts).load(
                    path=source_details.get("path"), format="delta"
                )
        else:
            if refinery_dataflow_spec.sourceFormat == "snapshot":
                landing_df = self.spark.read.table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.read.load(
                    path=source_details.get("path"), format="delta"
                )
            else:
                landing_df = self.spark.readStream.table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.readStream.load(
                    path=source_details.get("path"), format="delta"
                )

        # Apply SQL transformation
        sql_query = refinery_dataflow_spec.sqlQuery
        if sql_query and sql_query.strip():
            refinery_df = self.execute_sql_transformation(landing_df, sql_query)
        else:
            refinery_df = landing_df

        # Apply custom transform if provided
        refinery_df = self.apply_custom_transform_fun(refinery_df)
        return refinery_df

    def read_treasury(self) -> DataFrame:
        """Read Treasury tables with SQL transformations (batch only)."""
        treasury_dataflow_spec: TreasuryDataflowSpec = self.dataflowSpec
        source_details = self._get_source_details()
        reader_config_opts = self._get_reader_config_options()

        # Read from refinery layer (batch mode only)
        source_cl = source_details.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = source_details["database"]
        source_table = source_details["table"]

        if reader_config_opts:
            refinery_df = self.spark.read.options(**reader_config_opts).table(
                f"{source_cl_name}{source_database}.{source_table}"
            ) if self.uc_enabled else self.spark.read.options(**reader_config_opts).load(
                path=source_details.get("path"), format="delta"
            )
        else:
            refinery_df = self.spark.read.table(
                f"{source_cl_name}{source_database}.{source_table}"
            ) if self.uc_enabled else self.spark.read.load(
                path=source_details.get("path"), format="delta"
            )

        # Apply SQL transformation
        sql_query = treasury_dataflow_spec.sqlQuery
        if sql_query and sql_query.strip():
            treasury_df = self.execute_sql_transformation(refinery_df, sql_query)
        else:
            treasury_df = refinery_df

        # Apply custom transform if provided
        treasury_df = self.apply_custom_transform_fun(treasury_df)
        return treasury_df

    def write_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(self.view_name)

    def apply_changes_from_snapshot(self):
        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]
        self.create_streaming_table(None, target_path)
        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']
        target_table = (
            f"{target_cl_name}{target_db_name}.{target_table_name}"
        )
        source = (
            (lambda latest_snapshot_version: self.next_snapshot_and_version(
                latest_snapshot_version, self.dataflowSpec
            ))
            if self.next_snapshot_and_version and not self.next_snapshot_and_version_from_source_view
            else self.view_name
        )

        dlt.create_auto_cdc_from_snapshot_flow(
            target=target_table,
            source=source,
            keys=self.applyChangesFromSnapshot.keys,
            stored_as_scd_type=self.applyChangesFromSnapshot.scd_type,
            track_history_column_list=self.applyChangesFromSnapshot.track_history_column_list,
            track_history_except_column_list=self.applyChangesFromSnapshot.track_history_except_column_list,
        )

    def write_layer_with_dqe(self):
        """Write Landing, Refinery, or Treasury table with data quality expectations."""
        is_landing = isinstance(self.dataflowSpec, LandingDataflowSpec)
        data_quality_expectations_json = json.loads(self.dataflowSpec.dataQualityExpectations)

        dlt_table_with_expectation = None
        expect_or_quarantine_dict = None
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()
        # Both landing and refinery layers support quarantine tables
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        if self.dataflowSpec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path, target_table, target_table_name = self._get_target_table_info()
            layer_name = "landing" if is_landing else ("refinery" if isinstance(self.dataflowSpec, RefineryDataflowSpec) else "treasury")
            target_comment = self._get_table_comment(target_table, layer_name)
            # Create base table with expectations
            if expect_all_dict:
                dlt_table_with_expectation = dlt.expect_all(expect_all_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{target_table_name}",
                        table_properties=self.dataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                        cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                        path=target_path,
                        comment=target_comment,
                    )
                )
            if expect_all_or_fail_dict:
                if expect_all_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{target_table_name}",
                            table_properties=self.dataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                            path=target_path,
                            comment=target_comment,
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt_table_with_expectation)
            if expect_all_or_drop_dict:
                if expect_all_dict is None and expect_all_or_fail_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{target_table_name}",
                            table_properties=self.dataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                            path=target_path,
                            comment=target_comment,
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt_table_with_expectation)
            # Handle quarantine table (landing and refinery layers)
        if expect_or_quarantine_dict:
            q_partition_cols = None
            q_cluster_by = None
            quarantine_target_details = self._get_quarantine_target_details()
            if quarantine_target_details.get("partition_columns"):
                q_partition_cols = [quarantine_target_details["partition_columns"]]

            if quarantine_target_details.get("cluster_by"):
                # Parse cluster_by if it's a string representation of a list
                cluster_by_value = quarantine_target_details['cluster_by']
                if isinstance(cluster_by_value, str) and cluster_by_value.strip().startswith(('[', "[")):
                    # Handle string representations like "['id', 'email']" or '["id", "email"]'
                    try:
                        parsed_cluster_by = ast.literal_eval(cluster_by_value)
                        if isinstance(parsed_cluster_by, list):
                            cluster_by_value = parsed_cluster_by
                    except (ValueError, SyntaxError):
                        # If parsing fails, keep as string and let get_partition_cols handle it
                        quarantine_table_name = quarantine_target_details.get('table', '')
                        msg = f"Invalid cluster_by {cluster_by_value} for {quarantine_table_name}"
                        logger.error(msg)
                q_cluster_by = DataflowSpecUtils.get_partition_cols(cluster_by_value)

            quarantine_path = None if self.uc_enabled else quarantine_target_details.get("path")
            quarantine_cl = quarantine_target_details.get('catalog', None)
            quarantine_cl_name = f"{quarantine_cl}." if quarantine_cl is not None else ''
            quarantine_db = quarantine_target_details.get('database', '')
            quarantine_table_name = quarantine_target_details.get('table', '')

            # Check if quarantine_table_name is not empty (handles both None and empty string)
            if not quarantine_table_name or quarantine_table_name.strip() == '':
                logger.warning("Quarantine table name is empty or None. Skipping quarantine table creation.")
                return

            quarantine_table = (
                f"{quarantine_cl_name}{quarantine_db}.{quarantine_table_name}"
            )
            is_landing = isinstance(self.dataflowSpec, LandingDataflowSpec)
            layer_name = "landing" if is_landing else ("refinery" if isinstance(self.dataflowSpec, RefineryDataflowSpec) else "treasury")
            quarantine_comment = (
                quarantine_target_details.get('comment')
                if 'comment' in quarantine_target_details
                else f"{layer_name} dlt quarantine table {quarantine_table}"
            )

            dlt.expect_all_or_drop(expect_or_quarantine_dict)(
                dlt.table(
                    self.write_to_delta,
                    name=f"{quarantine_table_name}",
                    table_properties=getattr(self.dataflowSpec, 'quarantineTableProperties', None),
                    partition_cols=q_partition_cols,
                    cluster_by=q_cluster_by,
                    path=quarantine_path,
                    comment=quarantine_comment,
                )
            )

    def write_append_flows(self):
        """Creates an append flow for the target specified in the dataflowSpec.

        This method creates a streaming table with the given schema and target path.
        It then appends the flow to the table using the specified parameters.

        Args:
            None

        Returns:
            None
        """
        if self.appendFlows is None:
            return
        for append_flow in self.appendFlows:
            struct_schema = None
            if self.schema_json:
                struct_schema = (
                    StructType.fromJson(self.schema_json)
                    if isinstance(self.dataflowSpec, LandingDataflowSpec)
                    else self.refinery_schema
                )
            target_details = self._get_target_details()
            append_flow_writer = AppendFlowWriter(
                self.spark, append_flow,
                target_details['table'],
                struct_schema,
                self.dataflowSpec.tableProperties,
                self.dataflowSpec.partitionColumns,
                self.dataflowSpec.clusterBy
            )
            append_flow_writer.write_flow()

    def cdc_apply_changes(self):
        """CDC Apply Changes against dataflowspec."""
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        struct_schema = None
        if self.schema_json:
            struct_schema = self.modify_schema_for_cdc_changes(cdc_apply_changes)

        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]

        # For landing/refinery streaming mode, create streaming table first
        # For treasury batch mode, skip table creation - use regular table writes instead
        is_treasury = isinstance(self.dataflowSpec, TreasuryDataflowSpec)
        if not is_treasury:
            self.create_streaming_table(struct_schema, target_path)

        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)

        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']

        target_table = (
            f"{target_cl_name}{target_db_name}.{target_table_name}"
        )

        # Handle comma-separated sequence columns using struct
        sequence_by = cdc_apply_changes.sequence_by
        if ',' in sequence_by:
            sequence_cols = [col.strip() for col in sequence_by.split(',')]
            sequence_by = struct(*sequence_cols)  # Use struct() from pyspark.sql.functions

        # Auto-generate unique flow name if not provided (needed when multiple flows target same table)
        flow_name = cdc_apply_changes.flow_name
        if not flow_name:
            data_flow_id = str(self.dataflowSpec.dataFlowId).replace('-', '_').replace('.', '_')
            flow_name = f"{target_table_name}_{data_flow_id}_flow"

        dlt.create_auto_cdc_flow(
            target=target_table,
            source=self.view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list,
            flow_name=flow_name,
            once=cdc_apply_changes.once,
            ignore_null_updates_column_list=cdc_apply_changes.ignore_null_updates_column_list,
            ignore_null_updates_except_column_list=cdc_apply_changes.ignore_null_updates_except_column_list
        )

    def modify_schema_for_cdc_changes(self, cdc_apply_changes):
        if isinstance(self.dataflowSpec, LandingDataflowSpec) and self.schema_json is None:
            return None
        if isinstance(self.dataflowSpec, RefineryDataflowSpec) and self.refinery_schema is None:
            return None

        struct_schema = None
        if isinstance(self.dataflowSpec, LandingDataflowSpec) and self.schema_json is not None:
            struct_schema = StructType.fromJson(self.schema_json)
        elif isinstance(self.dataflowSpec, RefineryDataflowSpec):
            struct_schema = self.refinery_schema

        if struct_schema is None:
            return None

        sequenced_by_data_type = None

        if cdc_apply_changes.except_column_list:
            modified_schema = StructType([])
            if struct_schema:
                for field in struct_schema.fields:
                    if field.name not in cdc_apply_changes.except_column_list:
                        modified_schema.add(field)
                    # For SCD Type 2, get data type of first sequence column
                    sequence_by = cdc_apply_changes.sequence_by.strip()
                    if ',' not in sequence_by:
                        # Single column sequence
                        if field.name == sequence_by:
                            sequenced_by_data_type = field.dataType
                    else:
                        # Multiple column sequence - use first column's type
                        first_sequence_col = sequence_by.split(',')[0].strip()
                        if field.name == first_sequence_col:
                            sequenced_by_data_type = field.dataType
                struct_schema = modified_schema
            else:
                raise Exception(f"Schema is None for {self.dataflowSpec} for cdc_apply_changes! ")

        if struct_schema and cdc_apply_changes.scd_type == "2" and sequenced_by_data_type is not None:
            struct_schema.add(StructField("__START_AT", sequenced_by_data_type))
            struct_schema.add(StructField("__END_AT", sequenced_by_data_type))
        return struct_schema

    def create_streaming_table(self, struct_schema, target_path=None):
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()

        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']

        target_table = (
            f"{target_cl_name}{target_db_name}.{target_table_name}"
        )
        dlt.create_streaming_table(
            name=target_table,
            table_properties=self.dataflowSpec.tableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            path=target_path,
            schema=struct_schema,
            expect_all=expect_all_dict,
            expect_all_or_drop=expect_all_or_drop_dict,
            expect_all_or_fail=expect_all_or_fail_dict,
        )

    def get_dq_expectations(self):
        """
        Retrieves the data quality expectations for the table.

        Returns:
            A tuple containing three dictionaries:
            - expect_all_dict: A dictionary containing the 'expect_all' data quality expectations.
            - expect_all_or_drop_dict: A dictionary containing the 'expect_all_or_drop' data quality expectations.
            - expect_all_or_fail_dict: A dictionary containing the 'expect_all_or_fail' data quality expectations.
        """
        expect_all_dict = None
        expect_all_or_drop_dict = None
        expect_all_or_fail_dict = None
        if self.table_has_expectations():
            data_quality_expectations_json = json.loads(self.dataflowSpec.dataQualityExpectations)
            if "expect_all" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect_all"]
            if "expect" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect"]
            if "expect_all_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_all_or_drop"]
            if "expect_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
            if "expect_all_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_all_or_fail"]
            if "expect_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        return expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict

    def run_dlt(self):
        """Run DLT."""
        logger.info("in run_dlt function")
        self.read()
        self.write()

    @staticmethod
    def invoke_dlt_pipeline(spark,
                            layer,
                            landing_custom_transform_func: Callable = None,
                            refinery_custom_transform_func: Callable = None,
                            treasury_custom_transform_func: Callable = None,
                            landing_next_snapshot_and_version: Callable = None,
                            refinery_next_snapshot_and_version: Callable = None):
        """Invoke dlt pipeline will launch dlt with given dataflowspec.

        Args:
            spark: SparkSession
            layer: Layer name (landing, refinery, treasury, landing_refinery, landing_refinery_treasury, etc.)
            landing_custom_transform_func: Custom transform function for landing layer
            refinery_custom_transform_func: Custom transform function for refinery layer
            treasury_custom_transform_func: Custom transform function for treasury layer
            landing_next_snapshot_and_version: Snapshot version function for landing layer
            refinery_next_snapshot_and_version: Snapshot version function for refinery layer
        """

        dataflowspec_list = None
        if "landing" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_landing_dataflow_spec(spark)
            # Pre-create landing tables
            uc_enabled = spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true"
            table_creator = TablePreCreator(spark, uc_enabled)
            for spec in dataflowspec_list:
                table_creator.ensure_table_exists(spec, "landing")
            DataflowPipeline._launch_dlt_flow(
                spark, "landing", dataflowspec_list, landing_custom_transform_func, landing_next_snapshot_and_version
            )
        elif "refinery" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_refinery_dataflow_spec(spark)
            # Pre-create refinery tables
            uc_enabled = spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true"
            table_creator = TablePreCreator(spark, uc_enabled)
            for spec in dataflowspec_list:
                table_creator.ensure_table_exists(spec, "refinery")
            DataflowPipeline._launch_dlt_flow(
                spark, "refinery", dataflowspec_list, refinery_custom_transform_func, refinery_next_snapshot_and_version
            )
        elif "treasury" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_treasury_dataflow_spec(spark)
            # Pre-create treasury tables
            uc_enabled = spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true"
            table_creator = TablePreCreator(spark, uc_enabled)
            for spec in dataflowspec_list:
                table_creator.ensure_table_exists(spec, "treasury")
            DataflowPipeline._launch_dlt_flow(
                spark, "treasury", dataflowspec_list, treasury_custom_transform_func, None
            )
        elif "landing_refinery" == layer.lower():
            landing_dataflowspec_list = DataflowSpecUtils.get_landing_dataflow_spec(spark)
            # Pre-create landing tables
            uc_enabled = spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true"
            table_creator = TablePreCreator(spark, uc_enabled)
            for spec in landing_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "landing")
            DataflowPipeline._launch_dlt_flow(
                spark, "landing", landing_dataflowspec_list, landing_custom_transform_func,
                landing_next_snapshot_and_version
            )
            refinery_dataflowspec_list = DataflowSpecUtils.get_refinery_dataflow_spec(spark)
            # Pre-create refinery tables
            for spec in refinery_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "refinery")
            DataflowPipeline._launch_dlt_flow(
                spark, "refinery", refinery_dataflowspec_list, refinery_custom_transform_func,
                refinery_next_snapshot_and_version
            )
        elif "refinery_treasury" == layer.lower():
            refinery_dataflowspec_list = DataflowSpecUtils.get_refinery_dataflow_spec(spark)
            # Pre-create refinery tables
            uc_enabled = spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true"
            table_creator = TablePreCreator(spark, uc_enabled)
            for spec in refinery_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "refinery")
            DataflowPipeline._launch_dlt_flow(
                spark, "refinery", refinery_dataflowspec_list, refinery_custom_transform_func,
                refinery_next_snapshot_and_version
            )
            treasury_dataflowspec_list = DataflowSpecUtils.get_treasury_dataflow_spec(spark)
            # Pre-create treasury tables
            for spec in treasury_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "treasury")
            DataflowPipeline._launch_dlt_flow(
                spark, "treasury", treasury_dataflowspec_list, treasury_custom_transform_func, None
            )
        elif "landing_refinery_treasury" == layer.lower():
            landing_dataflowspec_list = DataflowSpecUtils.get_landing_dataflow_spec(spark)
            # Pre-create landing tables
            uc_enabled = spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower() == "true"
            table_creator = TablePreCreator(spark, uc_enabled)
            for spec in landing_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "landing")
            DataflowPipeline._launch_dlt_flow(
                spark, "landing", landing_dataflowspec_list, landing_custom_transform_func,
                landing_next_snapshot_and_version
            )
            refinery_dataflowspec_list = DataflowSpecUtils.get_refinery_dataflow_spec(spark)
            # Pre-create refinery tables
            for spec in refinery_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "refinery")
            DataflowPipeline._launch_dlt_flow(
                spark, "refinery", refinery_dataflowspec_list, refinery_custom_transform_func,
                refinery_next_snapshot_and_version
            )
            treasury_dataflowspec_list = DataflowSpecUtils.get_treasury_dataflow_spec(spark)
            # Pre-create treasury tables
            for spec in treasury_dataflowspec_list:
                table_creator.ensure_table_exists(spec, "treasury")
            DataflowPipeline._launch_dlt_flow(
                spark, "treasury", treasury_dataflowspec_list, treasury_custom_transform_func, None
            )

    @staticmethod
    def _launch_dlt_flow(
        spark, layer, dataflowspec_list, custom_transform_func=None, next_snapshot_and_version: Callable = None
    ):
        for dataflowSpec in dataflowspec_list:
            logger.info("Printing Dataflow Spec")
            logger.info(dataflowSpec)
            quarantine_input_view_name = None
            if hasattr(dataflowSpec, 'quarantineTargetDetails') and dataflowSpec.quarantineTargetDetails is not None \
                    and dataflowSpec.quarantineTargetDetails != {}:

                qrt_cl = dataflowSpec.quarantineTargetDetails.get('catalog', None)
                qrt_cl_str = f"{qrt_cl}_" if qrt_cl is not None else ''
                qrt_db = dataflowSpec.quarantineTargetDetails['database'].replace('.', '_')
                qrt_table = dataflowSpec.quarantineTargetDetails['table']
                # Include dataFlowId to make quarantine view name unique
                qrt_data_flow_id = str(dataflowSpec.dataFlowId).replace('-', '_').replace('.', '_')
                quarantine_input_view_name = (
                    f"{qrt_cl_str}{qrt_db}_{qrt_table}_{qrt_data_flow_id}"
                    f"_{layer}_quarantine_inputview"
                )
                quarantine_input_view_name = quarantine_input_view_name.replace(".", "").lower()
            else:
                logger.info("quarantine_input_view_name set to None")
            # Skip dataflowSpecs with None database (flows without this layer)
            if dataflowSpec.targetDetails.get('database') is None:
                logger.info(f"Skipping {layer} layer for dataFlowId={dataflowSpec.dataFlowId} (no database configured)")
                continue

            target_cl = dataflowSpec.targetDetails.get('catalog', None)
            target_cl_str = f"{target_cl}_" if target_cl is not None else ''
            target_db = dataflowSpec.targetDetails['database'].replace('.', '_')
            target_table = dataflowSpec.targetDetails['table']
            # Include dataFlowId to make view name unique when multiple flows target the same table
            data_flow_id = str(dataflowSpec.dataFlowId).replace('-', '_').replace('.', '_')
            target_view_name = f"{target_cl_str}{target_db}_{target_table}_{data_flow_id}_{layer}_inputview"
            target_view_name = target_view_name.replace(".", "").lower()
            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                target_view_name,
                quarantine_input_view_name,
                custom_transform_func,
                next_snapshot_and_version
            )
            dlt_data_flow.run_dlt()

    # Additional optimization methods for common patterns
    def _build_table_name(self, catalog, database, table):
        """Build a fully qualified table name."""
        catalog_prefix = f"{catalog}." if catalog else ''
        return f"{catalog_prefix}{database}.{table}"

    def _get_source_table_info(self):
        """Extract source table information."""
        source_details = self._get_source_details()
        catalog = source_details.get('catalog', None)
        database = source_details["database"]
        table = source_details["table"]
        return self._build_table_name(catalog, database, table), source_details

    def _get_target_table_name(self):
        """Get the fully qualified target table name."""
        target_details = self._get_target_details()
        catalog = target_details.get('catalog', None)
        database = target_details['database']
        table = target_details['table']
        return self._build_table_name(catalog, database, table)

    def _create_dataframe_reader(self, is_streaming=True, reader_options=None):
        """Create a DataFrame reader with common configuration."""
        if reader_options is None:
            reader_options = {}
        if is_streaming:
            reader = self.spark.readStream
        else:
            reader = self.spark.read
        if reader_options:
            reader = reader.options(**reader_options)
        return reader

    def _read_from_source(self, source_format, is_streaming=True):
        """Generic method to read from different source formats."""
        source_table_name, source_details = self._get_source_table_info()
        reader_options = self._get_reader_config_options()
        reader = self._create_dataframe_reader(is_streaming, reader_options)
        if source_format == "snapshot" or not is_streaming:
            if self.uc_enabled:
                return reader.table(source_table_name)
            else:
                return reader.load(path=source_details.get("path"), format="delta")
        else:
            if self.uc_enabled:
                return reader.table(source_table_name)
            else:
                return reader.load(path=source_details.get("path"), format="delta")

    def _apply_transformations(self, df, select_exp=None, where_clause=None):
        """Apply common transformations (select and where) to a DataFrame."""
        if select_exp:
            df = df.selectExpr(*select_exp)
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for clause in where_clause:
                    df = df.where(clause)
        return df
