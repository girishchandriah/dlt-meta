
"""
This module contains classes for writing data to Lakeflow Declarative Pipelines  and other sinks.

Classes:
    AppendFlowWriter: A class for writing append flows to Lakeflow Declarative Pipelines.
    DLTSinkWriter: A class for writing data to various sinks using Lakeflow Declarative Pipelines.

"""
from src.dataflow_spec import DataflowSpecUtils, DLTSink
import dlt
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.protobuf.functions import to_protobuf
from pyspark.sql.functions import col, struct
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column
import logging

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class AppendFlowWriter:
    """Append Flow Writer class."""

    def __init__(self, spark, append_flow, target, struct_schema, table_properties=None,
                 partition_cols=None, cluster_by=None):
        """Init."""
        self.spark = spark
        self.target = target
        self.append_flow = append_flow
        self.struct_schema = struct_schema
        self.table_properties = table_properties
        self.partition_cols = partition_cols
        self.cluster_by = cluster_by

    def read_af_view(self):
        """Write to Delta."""
        return dlt.read_stream(f"{self.append_flow.name}_view")

    def write_flow(self):
        """Write Append Flow."""
        if self.append_flow.create_streaming_table:
            dlt.create_streaming_table(
                name=self.target,
                table_properties=self.table_properties,
                partition_cols=DataflowSpecUtils.get_partition_cols(self.partition_cols),
                cluster_by=DataflowSpecUtils.get_partition_cols(self.cluster_by),
                schema=self.struct_schema,
                expect_all=None,
                expect_all_or_drop=None,
                expect_all_or_fail=None,
            )
        comment = (
            self.append_flow.comment
            if self.append_flow.comment
            else f"append_flow={self.append_flow.name} for target={self.target}"
        )
        spark_conf = self.append_flow.spark_conf if self.append_flow.spark_conf else {}
        dlt.append_flow(
            name=self.append_flow.name,
            target=self.target,
            comment=comment,
            spark_conf=spark_conf,
            once=self.append_flow.once
        )(self.read_af_view)


class DLTSinkWriter:
    """DLT Sink Writer class."""

    def __init__(self, dlt_sink: DLTSink, source_view_name):
        """Init."""
        self.dlt_sink = dlt_sink
        self.source_view_name = source_view_name

    def read_input_view(self):
        """Write to Sink."""
        input_df = dlt.read_stream(self.source_view_name)
        if self.dlt_sink.select_exp:
            input_df = input_df.selectExpr(*self.dlt_sink.select_exp)
        if self.dlt_sink.where_clause:
            input_df = input_df.where(self.dlt_sink.where_clause)
        return input_df

    @staticmethod
    def to_avro_with_schema_registry(data, subject, registry_url, options={}, schema_registry_options={}):
        """
        Custom to_avro that supports Schema Registry with SSL.
        Based on platform_notebooks/landing_from_kafka_ng.py implementation.

        Args:
            data: Column containing struct data to serialize
            subject: Schema Registry subject name
            registry_url: Schema Registry URL
            options: Additional Avro serialization options
            schema_registry_options: Schema Registry SSL configuration options

        Returns:
            Column: Serialized Avro binary data
        """
        combined_options = {**options, **schema_registry_options}

        sc = SparkContext._active_spark_context
        jc = sc._jvm.org.apache.spark.sql.avro.functions.to_avro(
            _to_java_column(data), subject, registry_url, combined_options or {}
        )
        return Column(jc)

    def _get_schema_registry_options(self, sink_options):
        """Build Schema Registry SSL options from sink options."""
        schema_registry_opts = {}

        # Add SSL options if provided
        ssl_truststore = sink_options.get("confluent.schema.registry.ssl.truststore.location")
        ssl_keystore = sink_options.get("confluent.schema.registry.ssl.keystore.location")

        if ssl_truststore and ssl_keystore:
            schema_registry_opts.update({
                "confluent.schema.registry.ssl.truststore.location": ssl_truststore,
                "confluent.schema.registry.ssl.truststore.password": sink_options.get("confluent.schema.registry.ssl.truststore.password"),
                "confluent.schema.registry.ssl.keystore.location": ssl_keystore,
                "confluent.schema.registry.ssl.keystore.password": sink_options.get("confluent.schema.registry.ssl.keystore.password"),
                "confluent.schema.registry.ssl.key.password": sink_options.get("confluent.schema.registry.ssl.key.password")
            })
            logger.info("Schema Registry SSL configuration added for sink")

        return schema_registry_opts

    def read_input_view_for_kafka(self):
        """Prepare data for Kafka sink with Avro/Protobuf serialization."""
        logger.info(f"Reading input view for Kafka sink: {self.source_view_name}")
        input_df = dlt.read_stream(self.source_view_name)

        # Apply select and where clauses
        if self.dlt_sink.select_exp:
            input_df = input_df.selectExpr(*self.dlt_sink.select_exp)
        if self.dlt_sink.where_clause:
            input_df = input_df.where(self.dlt_sink.where_clause)

        # Get sink options
        sink_options = dict(self.dlt_sink.options)
        data_format = sink_options.get("data_format", "json")

        logger.info(f"Kafka sink data format: {data_format}")

        # Serialize based on format
        if data_format == "avro":
            # Avro serialization with Schema Registry
            schema_registry_url = sink_options.get("schema.registry.url")
            subject = sink_options.get("schema.registry.subject")

            if not schema_registry_url or not subject:
                raise Exception("schema.registry.url and schema.registry.subject required for Avro serialization")

            logger.info(f"Using Avro serialization with subject: {subject}")

            # Create struct of all columns except key
            value_columns = [c for c in input_df.columns if c != "key"]
            input_df = input_df.withColumn(
                "value",
                self.to_avro_with_schema_registry(
                    struct(*value_columns),
                    subject,
                    schema_registry_url,
                    {},
                    self._get_schema_registry_options(sink_options)
                )
            )
        elif data_format == "protobuf":
            # Protobuf serialization with Schema Registry
            schema_registry_address = sink_options.get("schema.registry.address") or sink_options.get("schema.registry.url")
            subject = sink_options.get("schema.registry.subject")

            if not schema_registry_address or not subject:
                raise Exception("schema.registry.address (or schema.registry.url) and schema.registry.subject required for Protobuf")

            logger.info(f"Using Protobuf serialization with subject: {subject}")

            protobuf_options = {
                "schema.registry.subject": subject,
                "schema.registry.address": schema_registry_address
            }
            protobuf_options.update(self._get_schema_registry_options(sink_options))

            value_columns = [c for c in input_df.columns if c != "key"]
            input_df = input_df.withColumn(
                "value",
                to_protobuf(struct(*value_columns), options=protobuf_options)
            )
        else:
            # JSON serialization (default)
            logger.info("Using JSON serialization (default)")
            if "value" not in input_df.columns:
                raise Exception("For JSON format, 'value' column must exist or be selected in select_exp")
            input_df = input_df.withColumn("value", col("value").cast("string"))

        # Ensure key column exists
        if "key" not in input_df.columns:
            raise Exception("Kafka sink requires a 'key' column. Add it in select_exp (e.g., 'order_id as key')")

        return input_df.select("key", "value")

    def write_to_kafka_sink(self):
        """Write to Kafka sink with Schema Registry support."""
        logger.info(f"Writing to Kafka sink: {self.dlt_sink.name}")

        # Get Kafka connection options
        sink_options_dict = dict(self.dlt_sink.options)

        # Extract Kafka-specific options (exclude schema registry and data format options)
        kafka_options = {
            k: v for k, v in sink_options_dict.items()
            if (k.startswith("kafka.") or k == "topic") and not k.startswith("kafka.ssl.key.secrets")
        }

        logger.info(f"Kafka options: {list(kafka_options.keys())}")

        dlt.create_sink(
            name=self.dlt_sink.name,
            format="kafka",
            options=kafka_options
        )

        dlt.append_flow(
            name=f"{self.dlt_sink.name}_flow",
            target=self.dlt_sink.name,
            comment=f"Kafka sink flow for {self.dlt_sink.name}"
        )(self.read_input_view_for_kafka)

    def write_to_sink(self):
        """Write to Sink - route to appropriate writer based on format."""
        logger.info(f"Writing to sink: {self.dlt_sink.name}, format: {self.dlt_sink.format}")

        if self.dlt_sink.format == "kafka":
            self.write_to_kafka_sink()
        else:
            # Original implementation for other sinks (delta, eventhub, etc.)
            dlt.create_sink(
                name=self.dlt_sink.name,
                format=self.dlt_sink.format,
                options=self.dlt_sink.options
            )
            dlt.append_flow(
                name=f"{self.dlt_sink.name}_flow",
                target=self.dlt_sink.name,
                comment=f"Sink flow for {self.dlt_sink.name}"
            )(self.read_input_view)
