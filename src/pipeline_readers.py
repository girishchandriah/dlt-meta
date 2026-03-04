"""PipelineReaders providers DLT readers functionality."""
import logging
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark import SparkContext
from pyspark.sql.column import Column
from pyspark.util import _print_missing_jar

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class PipelineReaders:
    """PipelineReader Class.

    Returns:
        _type_: _description_
    """
    def __init__(self, spark, source_format, source_details, reader_config_options, schema_json=None):
        """Init."""
        self.spark = spark
        self.source_format = source_format
        self.source_details = source_details
        self.reader_config_options = reader_config_options
        self.schema_json = schema_json

    def read_dlt_cloud_files(self) -> DataFrame:
        """Read dlt cloud files.

        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        input_df = None
        source_path = self.source_details["path"]
        if self.schema_json and self.source_format != "delta":
            schema = StructType.fromJson(self.schema_json)
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .schema(schema)
                .load(source_path)
            )
        else:
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .load(source_path)
            )
        if self.source_details and "source_metadata" in self.source_details.keys():
            input_df = PipelineReaders.add_cloudfiles_metadata(self.source_details, input_df)
        return input_df

    @staticmethod
    def add_cloudfiles_metadata(sourceDetails, input_df):
        source_metadata_json = json.loads(sourceDetails.get("source_metadata"))
        keys = source_metadata_json.keys()
        autoloader_metadata_column_flag = False
        source_metadata_col_name = "_metadata"
        input_df = input_df.selectExpr("*", f"{source_metadata_col_name}")
        if "select_metadata_cols" in source_metadata_json:
            select_metadata_cols = source_metadata_json["select_metadata_cols"]
            for select_metadata_col in select_metadata_cols:
                input_df = input_df.withColumn(select_metadata_col, col(select_metadata_cols[select_metadata_col]))
        if "include_autoloader_metadata_column" in keys:
            autoloader_metadata_column = source_metadata_json["include_autoloader_metadata_column"]
            autoloader_metadata_column_flag = True if autoloader_metadata_column.lower() == "true" else False
            if autoloader_metadata_column_flag and "autoloader_metadata_col_name" in source_metadata_json:
                custom_source_metadata_col_name = source_metadata_json["autoloader_metadata_col_name"]
                if custom_source_metadata_col_name != source_metadata_col_name:
                    input_df = input_df.withColumnRenamed(f"{source_metadata_col_name}",
                                                          f"{custom_source_metadata_col_name}")
            elif autoloader_metadata_column_flag and "autoloader_metadata_col_name" not in source_metadata_json:
                input_df = input_df.withColumnRenamed("_metadata", "source_metadata")
        else:
            input_df = input_df.drop(f"{source_metadata_col_name}")
        return input_df

    def read_dlt_delta(self) -> DataFrame:
        """Read dlt delta.

        Args:
            spark (_type_): _description_
            landing_dataflow_spec (_type_): _description_
        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")

        source_cl = self.source_details.get('source_catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        table_path = f"{source_cl_name}{self.source_details['source_database']}.{self.source_details['source_table']}"

        if self.source_format == "snapshot":
            reader = self.spark.read
        else:
            reader = self.spark.readStream

        if self.reader_config_options:
            return reader.options(**self.reader_config_options).table(table_path)
        else:
            return reader.table(table_path)

    def get_db_utils(self):
        """Get databricks utils using DBUtils package."""
        from pyspark.dbutils import DBUtils
        return DBUtils(self.spark)

    def read_kafka(self) -> DataFrame:
        """Read eventhub with dataflowspec and schema.

        Args:
            spark (_type_): _description_
            landing_dataflow_spec (_type_): _description_
            schema_json (_type_): _description_

        Returns:
            DataFrame: _description_
        """
        if self.source_format == "eventhub":
            kafka_options = self.get_eventhub_kafka_options()
        elif self.source_format == "kafka":
            kafka_options = self.get_kafka_options()
        raw_df = (
            self.spark
            .readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
            # add date, hour, and minute columns derived from eventhub enqueued timestamp
            .selectExpr("*", "to_date(timestamp) as date", "hour(timestamp) as hour", "minute(timestamp) as minute")
        )
        if self.schema_json:
            schema = StructType.fromJson(self.schema_json)
            return (
                raw_df.withColumn("parsed_records", from_json(col("value").cast("string"), schema))
            )
        else:
            return raw_df

    def get_eventhub_kafka_options(self):
        """Get eventhub options from dataflowspec."""
        dbutils = self.get_db_utils()
        eh_namespace = self.source_details.get("eventhub.namespace")
        eh_port = self.source_details.get("eventhub.port")
        eh_name = self.source_details.get("eventhub.name")
        eh_shared_key_name = self.source_details.get("eventhub.accessKeyName")
        secret_name = self.source_details.get("eventhub.accessKeySecretName")
        if not secret_name:
            # set default value if "eventhub.accessKeySecretName" is not specified
            secret_name = eh_shared_key_name
        secret_scope = self.source_details.get("eventhub.secretsScopeName")
        eh_shared_key_value = dbutils.secrets.get(secret_scope, secret_name)
        eh_shared_key_value = f"SharedAccessKeyName={eh_shared_key_name};SharedAccessKey={eh_shared_key_value}"
        eh_conn_str = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;{eh_shared_key_value}"
        eh_kafka_str = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        sasl_config = f"{eh_kafka_str} required username=\"$ConnectionString\" password=\"{eh_conn_str}\";"

        eh_conn_options = {
            "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:{eh_port}",
            "subscribe": eh_name,
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": sasl_config
        }
        kafka_options = {**eh_conn_options, **self.reader_config_options}
        return kafka_options

    def get_kafka_options(self):
        """Get kafka options from dataflowspec."""
        kafka_broker = self.source_details.get("kafka.bootstrap.servers", None)
        if not kafka_broker:
            kafka_source_servers_secrets_scope_key = self.source_details.get(
                "kafka_source_servers_secrets_scope_key",
                None
            )
            kafka_source_servers_secrets_scope_name = self.source_details.get(
                "kafka_source_servers_secrets_scope_name", None)
            if kafka_source_servers_secrets_scope_key and kafka_source_servers_secrets_scope_name:
                dbutils = self.get_db_utils()
                kafka_broker = dbutils.secrets.get(
                    kafka_source_servers_secrets_scope_name, kafka_source_servers_secrets_scope_key)
            else:
                raise Exception(
                    f"Kafka broker details not found for source_details={self.source_details}!"
                )
        topic = self.source_details.get("subscribe", None)
        if not topic:
            raise Exception(f"Kafka topic details not found for source_details={self.source_details}!")
        kafka_base_ops = {
            "kafka.bootstrap.servers": kafka_broker,
            "subscribe": self.source_details.get("subscribe")
        }
        ssl_truststore_location = self.source_details.get("kafka.ssl.truststore.location", None)
        ssl_keystore_location = self.source_details.get("kafka.ssl.keystore.location", None)
        if ssl_truststore_location and ssl_keystore_location:
            truststore_scope = self.source_details.get("kafka.ssl.truststore.secrets.scope", None)
            truststore_key = self.source_details.get("kafka.ssl.truststore.secrets.key", None)
            keystore_scope = self.source_details.get("kafka.ssl.keystore.secrets.scope", None)
            keystore_key = self.source_details.get("kafka.ssl.keystore.secrets.key", None)
            if (truststore_scope and truststore_key and keystore_scope and keystore_key):
                dbutils = self.get_db_utils()
                kafka_ssl_conn = {
                    "kafka.ssl.truststore.location": ssl_truststore_location,
                    "kafka.ssl.keystore.location": ssl_keystore_location,
                    "kafka.ssl.keystore.password": dbutils.secrets.get(keystore_scope, keystore_key),
                    "kafka.ssl.truststore.password": dbutils.secrets.get(truststore_scope, truststore_key)
                }
                kafka_options = {**kafka_base_ops, **kafka_ssl_conn, **self.reader_config_options}
            else:
                params = ["kafka.ssl.truststore.secrets.scope",
                          "kafka.ssl.truststore.secrets.key",
                          "kafka.ssl.keystore.secrets.scope",
                          "kafka.ssl.keystore.secrets.key"
                          ]
                raise Exception(f"Kafka ssl required params are: {params}! provided options are :{self.source_details}")
        else:
            kafka_options = {**kafka_base_ops, **self.reader_config_options}
        return kafka_options

    @staticmethod
    def from_avro_with_schema_registry(data, subject, registry_url, options={}, schema_registry_options={}):
        """
        Custom from_avro that supports Schema Registry with SSL.
        Based on platform_notebooks/landing_from_kafka_ng.py implementation.

        Args:
            data: Column containing Avro binary data
            subject: Schema Registry subject name
            registry_url: Schema Registry URL
            options: Additional Avro parsing options (e.g., {"mode": "PERMISSIVE"})
            schema_registry_options: Schema Registry SSL configuration options

        Returns:
            Column: Deserialized Avro data as struct
        """
        combined_options = {**options, **schema_registry_options}

        sc = SparkContext._active_spark_context
        try:
            jc = sc._jvm.org.apache.spark.sql.avro.functions.from_avro(
                data._jc, subject, registry_url, combined_options or {}
            )
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                _print_missing_jar("Avro", "avro", "avro", sc.version)
            raise
        return Column(jc)

    def read_kafka_with_schema_registry(self) -> DataFrame:
        """
        Read Kafka with Schema Registry support (Avro/Protobuf).
        Supports SSL for both Kafka and Schema Registry.

        Expected source_details configuration:
        {
            "kafka.bootstrap.servers": "broker:9093",
            "subscribe": "topic-name",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": "/path/to/truststore.jks",
            "kafka.ssl.keystore.location": "/path/to/keystore.jks",
            "kafka.ssl.truststore.secrets.scope": "ssl_certs",
            "kafka.ssl.truststore.secrets.key": "truststore_password",
            "kafka.ssl.keystore.secrets.scope": "ssl_certs",
            "kafka.ssl.keystore.secrets.key": "keystore_password",
            "kafka.ssl.key.secrets.scope": "ssl_certs",
            "kafka.ssl.key.secrets.key": "key_password",
            "schema.registry.url": "https://schema-registry:8081",
            "schema.registry.subject": "topic-name-value",
            "data_format": "avro" or "protobuf",
            "mode": "PERMISSIVE" (optional, defaults to PERMISSIVE)
        }

        Returns:
            DataFrame: Kafka DataFrame with parsed_records column containing deserialized data
        """
        logger.info("In read_kafka_with_schema_registry func")
        dbutils = self.get_db_utils()

        # Get Kafka connection options
        kafka_options = self.get_kafka_options()

        # Get Schema Registry details
        schema_registry_url = self.source_details.get("schema.registry.url")
        schema_registry_subject = self.source_details.get("schema.registry.subject")
        data_format = self.source_details.get("data_format", "avro")
        mode = self.source_details.get("mode", "PERMISSIVE")

        if not schema_registry_url or not schema_registry_subject:
            raise Exception(
                "schema.registry.url and schema.registry.subject are required for Schema Registry integration!"
            )

        logger.info(f"Schema Registry URL: {schema_registry_url}")
        logger.info(f"Schema Registry Subject: {schema_registry_subject}")
        logger.info(f"Data Format: {data_format}")

        # Build Schema Registry SSL options
        schema_registry_options = {
            "schema.registry.url": schema_registry_url
        }

        # Add SSL options for Schema Registry
        ssl_truststore_location = self.source_details.get("kafka.ssl.truststore.location")
        ssl_keystore_location = self.source_details.get("kafka.ssl.keystore.location")

        if ssl_truststore_location and ssl_keystore_location:
            truststore_scope = self.source_details.get("kafka.ssl.truststore.secrets.scope")
            truststore_key = self.source_details.get("kafka.ssl.truststore.secrets.key")
            keystore_scope = self.source_details.get("kafka.ssl.keystore.secrets.scope")
            keystore_key = self.source_details.get("kafka.ssl.keystore.secrets.key")
            key_password_scope = self.source_details.get("kafka.ssl.key.secrets.scope")
            key_password_key = self.source_details.get("kafka.ssl.key.secrets.key")

            if truststore_scope and truststore_key and keystore_scope and keystore_key:
                truststore_password = dbutils.secrets.get(truststore_scope, truststore_key)
                keystore_password = dbutils.secrets.get(keystore_scope, keystore_key)
                key_password = dbutils.secrets.get(key_password_scope, key_password_key) if key_password_scope and key_password_key else keystore_password

                schema_registry_options.update({
                    "confluent.schema.registry.ssl.truststore.location": ssl_truststore_location,
                    "confluent.schema.registry.ssl.truststore.password": truststore_password,
                    "confluent.schema.registry.ssl.keystore.location": ssl_keystore_location,
                    "confluent.schema.registry.ssl.keystore.password": keystore_password,
                    "confluent.schema.registry.ssl.key.password": key_password
                })
                logger.info("Schema Registry SSL configuration added")

        # Read from Kafka
        raw_df = (
            self.spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
            .selectExpr("*", "to_date(timestamp) as date", "hour(timestamp) as hour", "minute(timestamp) as minute")
        )

        # Deserialize based on data format
        if data_format == "protobuf":
            logger.info("Using Protobuf deserialization")
            # Protobuf deserialization
            protobuf_options = {
                "schema.registry.subject": schema_registry_subject,
                "schema.registry.address": schema_registry_url,
                "mode": mode
            }
            protobuf_options.update(schema_registry_options)

            return raw_df.withColumn(
                "parsed_records",
                from_protobuf(col("value"), options=protobuf_options)
            )
        elif data_format == "avro":
            logger.info("Using Avro deserialization")
            # Avro deserialization
            return raw_df.withColumn(
                "parsed_records",
                self.from_avro_with_schema_registry(
                    col("value"),
                    schema_registry_subject,
                    schema_registry_url,
                    {"mode": mode},
                    schema_registry_options
                )
            )
        else:
            raise Exception(
                f"Unsupported data_format: {data_format}. Supported formats: avro, protobuf"
            )
