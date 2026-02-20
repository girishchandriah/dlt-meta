"""OnboardDataflowSpec class provides landing/refinery/treasury onboarding features."""

import copy
import dataclasses
import json
import yaml
import logging
import ast

import pyspark.sql.types as T
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from src.dataflow_spec import LandingDataflowSpec, RefineryDataflowSpec, TreasuryDataflowSpec, DataflowSpecUtils
from src.metastore_ops import DeltaPipelinesInternalTableOps, DeltaPipelinesMetaStoreOps

logger = logging.getLogger("databricks.labs.dltmeta")
logger.setLevel(logging.INFO)


class OnboardDataflowspec:
    """OnboardDataflowSpec class provides landing/refinery/treasury onboarding features."""

    def __init__(self, spark, dict_obj, landing_schema_mapper=None, uc_enabled=False):
        """Onboard Dataflowspec Constructor."""
        self.spark = spark
        self.dict_obj = dict_obj
        self.landing_dict_obj = copy.deepcopy(dict_obj)
        self.refinery_dict_obj = copy.deepcopy(dict_obj)
        self.treasury_dict_obj = copy.deepcopy(dict_obj)
        self.uc_enabled = uc_enabled
        self.__initialize_paths(uc_enabled)
        self.landing_schema_mapper = landing_schema_mapper
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.onboard_file_type = None

    def __initialize_paths(self, uc_enabled):
        # Remove refinery and treasury params from landing dict
        if "refinery_dataflowspec_table" in self.landing_dict_obj:
            del self.landing_dict_obj["refinery_dataflowspec_table"]
        if "refinery_dataflowspec_path" in self.landing_dict_obj:
            del self.landing_dict_obj["refinery_dataflowspec_path"]
        if "treasury_dataflowspec_table" in self.landing_dict_obj:
            del self.landing_dict_obj["treasury_dataflowspec_table"]
        if "treasury_dataflowspec_path" in self.landing_dict_obj:
            del self.landing_dict_obj["treasury_dataflowspec_path"]

        # Remove landing and treasury params from refinery dict
        if "landing_dataflowspec_table" in self.refinery_dict_obj:
            del self.refinery_dict_obj["landing_dataflowspec_table"]
        if "landing_dataflowspec_path" in self.refinery_dict_obj:
            del self.refinery_dict_obj["landing_dataflowspec_path"]
        if "treasury_dataflowspec_table" in self.refinery_dict_obj:
            del self.refinery_dict_obj["treasury_dataflowspec_table"]
        if "treasury_dataflowspec_path" in self.refinery_dict_obj:
            del self.refinery_dict_obj["treasury_dataflowspec_path"]

        # Remove landing and refinery params from treasury dict
        if "landing_dataflowspec_table" in self.treasury_dict_obj:
            del self.treasury_dict_obj["landing_dataflowspec_table"]
        if "landing_dataflowspec_path" in self.treasury_dict_obj:
            del self.treasury_dict_obj["landing_dataflowspec_path"]
        if "refinery_dataflowspec_table" in self.treasury_dict_obj:
            del self.treasury_dict_obj["refinery_dataflowspec_table"]
        if "refinery_dataflowspec_path" in self.treasury_dict_obj:
            del self.treasury_dict_obj["refinery_dataflowspec_path"]

        if uc_enabled:
            if "landing_dataflowspec_path" in self.landing_dict_obj:
                del self.landing_dict_obj["landing_dataflowspec_path"]
            if "refinery_dataflowspec_path" in self.refinery_dict_obj:
                del self.refinery_dict_obj["refinery_dataflowspec_path"]
            if "treasury_dataflowspec_path" in self.treasury_dict_obj:
                del self.treasury_dict_obj["treasury_dataflowspec_path"]

    @staticmethod
    def __validate_dict_attributes(attributes, dict_obj):
        """Validate dict attributes method will validate dict attributes keys.

        Args:
            attributes ([type]): [description]
            dict_obj ([type]): [description]

        Raises:
            ValueError: [description]
        """
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(f"expected: {set(attributes)}, actual: {attributes_keys}")
            logger.info(
                "missing attributes : {}".format(
                    set(attributes).difference(attributes_keys)
                )
            )
            raise ValueError(
                f"missing attributes : {set(attributes).difference(attributes_keys)}"
            )

    def onboard_dataflow_specs(self):
        """
        Onboard_dataflow_specs method will onboard dataFlowSpecs for landing, refinery and treasury.

        This method takes in a SparkSession object and a dictionary object containing the following attributes:
        - onboarding_file_path: The path to the onboarding file.
        - database: The name of the database to onboard the dataflow specs to.
        - env: The environment to onboard the dataflow specs to.
        - landing_dataflowspec_table: The name of the landing dataflow specs table.
        - landing_dataflowspec_path: The path to the landing dataflow specs (if uc_enabled is False).
        - refinery_dataflowspec_table: The name of the refinery dataflow specs table.
        - refinery_dataflowspec_path: The path to the refinery dataflow specs (if uc_enabled is False).
        - treasury_dataflowspec_table: The name of the treasury dataflow specs table.
        - treasury_dataflowspec_path: The path to the treasury dataflow specs (if uc_enabled is False).
        - import_author: The author of the import.
        - version: The version of the import.
        - overwrite: Whether to overwrite existing dataflow specs or not.

        If the `uc_enabled` flag is set to True, the dictionary object must contain all the attributes listed above.
        If the `uc_enabled` flag is set to False, the dictionary object must contain all the attributes listed above
        including the path attributes.

        This method calls the `onboard_landing_dataflow_spec`, `onboard_refinery_dataflow_spec`, and
        `onboard_treasury_dataflow_spec` methods to onboard the landing, refinery, and treasury dataflow specs
        respectively.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "landing_dataflowspec_table",
            "refinery_dataflowspec_table",
            "treasury_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]

        if self.uc_enabled:
            if "landing_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["landing_dataflowspec_path"]
            if "refinery_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["refinery_dataflowspec_path"]
            if "treasury_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["treasury_dataflowspec_path"]
            self.__validate_dict_attributes(attributes, self.dict_obj)
        else:
            attributes.append("landing_dataflowspec_path")
            attributes.append("refinery_dataflowspec_path")
            attributes.append("treasury_dataflowspec_path")
            self.__validate_dict_attributes(attributes, self.dict_obj)
        self.onboard_landing_dataflow_spec()
        self.onboard_refinery_dataflow_spec()
        self.onboard_treasury_dataflow_spec()

    def register_landing_dataflow_spec_tables(self):
        """Register landing/refinery dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["landing_dataflowspec_table"],
            self.dict_obj["landing_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded landing table={self.dict_obj["database"]}.{self.dict_obj["landing_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["landing_dataflowspec_table"]}"""
        ).show()

    def register_refinery_dataflow_spec_tables(self):
        """Register refinery dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["refinery_dataflowspec_table"],
            self.dict_obj["refinery_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded refinery table={self.dict_obj["database"]}.{self.dict_obj["refinery_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["refinery_dataflowspec_table"]}"""
        ).show()

    def register_treasury_dataflow_spec_tables(self):
        """Register treasury dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["treasury_dataflowspec_table"],
            self.dict_obj["treasury_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded treasury table={self.dict_obj["database"]}.{self.dict_obj["treasury_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["treasury_dataflowspec_table"]}"""
        ).show()

    def onboard_refinery_dataflow_spec(self):
        """
        Onboard refinery dataflow spec.

        Args:
            onboarding_df (pyspark.sql.DataFrame): DataFrame containing the onboarding file data.
            dict_obj (dict): Dictionary containing the required attributes for onboarding refinery dataflow spec.
                Required attributes:
                    - onboarding_file_path (str): Path of the onboarding file.
                    - database (str): Name of the database.
                    - env (str): Environment name.
                    - refinery_dataflowspec_table (str): Name of the refinery dataflow spec table.
                    - refinery_dataflowspec_path (str): Path of the refinery dataflow spec file. if uc_enabled is False
                    - import_author (str): Name of the import author.
                    - version (str): Version of the dataflow spec.
                    - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "refinery_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.refinery_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("refinery_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )
        refinery_data_flow_spec_df = self.__get_refinery_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )
        columns = StructType(
            [
                StructField("sql_query", StringType(), True),
                StructField(
                    "target_partition_cols", ArrayType(StringType(), True), True
                ),
                StructField("target_table", StringType(), True),
            ]
        )

        emp_rdd = []
        env = dict_obj["env"]
        refinery_transformation_json_df = self.spark.createDataFrame(
            data=emp_rdd, schema=columns
        )
        refinery_transformation_json_file = onboarding_df.select(
            f"refinery_transformation_json_{env}"
        ).dropDuplicates()

        refinery_transformation_json_files = refinery_transformation_json_file.collect()
        for row in refinery_transformation_json_files:
            trans_file_path = row[f"refinery_transformation_json_{env}"]
            # Skip if transformation file path is None
            if not trans_file_path:
                continue
            # Check if file is YAML or JSON based on extension
            if trans_file_path.endswith(('.yaml', '.yml')):
                # Read YAML file and convert to JSON-like format
                yaml_content = self.spark.read.text(trans_file_path, wholetext=True).collect()[0]["value"]
                yaml_data = yaml.safe_load(yaml_content)
                # Create a single-row dataframe from the YAML data
                trans_data = [(
                    yaml_data.get('sql_query', ''),
                    yaml_data.get('target_partition_cols', []),
                    yaml_data.get('target_table', '')
                )]
                trans_df = self.spark.createDataFrame(trans_data, schema=columns)
                refinery_transformation_json_df = refinery_transformation_json_df.union(trans_df)
            else:
                # Read as JSON
                refinery_transformation_json_df = refinery_transformation_json_df.union(
                    self.spark.read.option("multiline", "true")
                    .schema(columns)
                    .json(trans_file_path)
                )

        logger.info(refinery_transformation_json_file)

        refinery_data_flow_spec_df = refinery_transformation_json_df.join(
            refinery_data_flow_spec_df,
            refinery_transformation_json_df.target_table
            == refinery_data_flow_spec_df.targetDetails["table"],
        )
        refinery_dataflow_spec_df = (
            refinery_data_flow_spec_df.drop("target_table")
            .drop("target_partition_cols")
            .withColumnRenamed("sql_query", "sqlQuery")
        )

        refinery_dataflow_spec_df = self.__add_audit_columns(
            refinery_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        refinery_fields = [field.name for field in dataclasses.fields(RefineryDataflowSpec)]
        refinery_dataflow_spec_df = refinery_dataflow_spec_df.select(refinery_fields)
        database = dict_obj["database"]
        table = dict_obj["refinery_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    refinery_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                refinery_dataflow_spec_df.write.mode("overwrite").format("delta").option(
                    "mergeSchema", "true"
                ).save(dict_obj["refinery_dataflowspec_path"])
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["refinery_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["refinery_dataflowspec_path"]
                )
            logger.info("In Merge block for refinery")
            self.deltaPipelinesInternalTableOps.merge(
                refinery_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_refinery_dataflow_spec_tables()

    def onboard_treasury_dataflow_spec(self):
        """
        Onboard treasury dataflow spec.

        Args:
            dict_obj (dict): Dictionary containing the required attributes for onboarding treasury dataflow spec.
                Required attributes:
                    - onboarding_file_path (str): Path of the onboarding file.
                    - database (str): Name of the database.
                    - env (str): Environment name.
                    - treasury_dataflowspec_table (str): Name of the treasury dataflow spec table.
                    - treasury_dataflowspec_path (str): Path of the treasury dataflow spec file. if uc_enabled is False
                    - import_author (str): Name of the import author.
                    - version (str): Version of the dataflow spec.
                    - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "treasury_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.treasury_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("treasury_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )
        treasury_data_flow_spec_df = self.__get_treasury_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )
        columns = StructType(
            [
                StructField("sql_query", StringType(), True),
                StructField(
                    "target_partition_cols", ArrayType(StringType(), True), True
                ),
                StructField("target_table", StringType(), True),
            ]
        )

        emp_rdd = []
        env = dict_obj["env"]
        treasury_transformation_json_df = self.spark.createDataFrame(
            data=emp_rdd, schema=columns
        )
        treasury_transformation_json_file = onboarding_df.select(
            f"treasury_transformation_json_{env}"
        ).dropDuplicates()

        treasury_transformation_json_files = treasury_transformation_json_file.collect()
        for row in treasury_transformation_json_files:
            trans_file_path = row[f"treasury_transformation_json_{env}"]
            # Skip if transformation file path is None
            if not trans_file_path:
                continue
            # Check if file is YAML or JSON based on extension
            if trans_file_path.endswith(('.yaml', '.yml')):
                    # Read YAML file and convert to JSON-like format
                    yaml_content = self.spark.read.text(trans_file_path, wholetext=True).collect()[0]["value"]
                    yaml_data = yaml.safe_load(yaml_content)
                    # Create a single-row dataframe from the YAML data
                    trans_data = [(
                        yaml_data.get('sql_query', ''),
                        yaml_data.get('target_partition_cols', []),
                        yaml_data.get('target_table', '')
                    )]
                    trans_df = self.spark.createDataFrame(trans_data, schema=columns)
                    treasury_transformation_json_df = treasury_transformation_json_df.union(trans_df)
                else:
                    # Read as JSON
                    treasury_transformation_json_df = treasury_transformation_json_df.union(
                        self.spark.read.option("multiline", "true")
                        .schema(columns)
                        .json(trans_file_path)
                    )

        logger.info(treasury_transformation_json_file)

        treasury_data_flow_spec_df = treasury_transformation_json_df.join(
            treasury_data_flow_spec_df,
            treasury_transformation_json_df.target_table
            == treasury_data_flow_spec_df.targetDetails["table"],
        )
        treasury_dataflow_spec_df = (
            treasury_data_flow_spec_df.drop("target_table")
            .drop("target_partition_cols")
            .withColumnRenamed("sql_query", "sqlQuery")
        )

        treasury_dataflow_spec_df = self.__add_audit_columns(
            treasury_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        treasury_fields = [field.name for field in dataclasses.fields(TreasuryDataflowSpec)]
        treasury_dataflow_spec_df = treasury_dataflow_spec_df.select(treasury_fields)
        database = dict_obj["database"]
        table = dict_obj["treasury_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    treasury_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                treasury_dataflow_spec_df.write.mode("overwrite").format("delta").option(
                    "mergeSchema", "true"
                ).save(dict_obj["treasury_dataflowspec_path"])
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["treasury_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["treasury_dataflowspec_path"]
                )
            logger.info("In Merge block for treasury")
            self.deltaPipelinesInternalTableOps.merge(
                treasury_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_treasury_dataflow_spec_tables()

    def onboard_landing_dataflow_spec(self):
        """
        Onboard landing dataflow spec.

        This function reads the onboarding file and creates landing dataflow spec. It adds audit columns to the dataframe
        If overwrite is True, it overwrites the table or file with the new dataframe. If overwrite is False,
        it merges the new dataframe with the existing dataframe.
        dict_obj (dict): Dictionary containing the required attributes for onboarding landing dataflow spec.
            Required attributes:
                - onboarding_file_path (str): Path of the onboarding file.
                - database (str): Name of the database.
                - env (str): Environment name.
                - landing_dataflowspec_table (str): Name of the landing dataflow spec table.
                - landing_dataflowspec_path (str): Path of the landing dataflow spec file. if uc_enabled is False
                - import_author (str): Name of the import author.
                - version (str): Version of the dataflow spec.
                - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.

        Args:
            None

        Returns:
            None
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "landing_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.landing_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("landing_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )

        landing_dataflow_spec_df = self.__get_landing_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )

        landing_dataflow_spec_df = self.__add_audit_columns(
            landing_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )
        landing_fields = [field.name for field in dataclasses.fields(LandingDataflowSpec)]
        landing_dataflow_spec_df = landing_dataflow_spec_df.select(landing_fields)
        database = dict_obj["database"]
        table = dict_obj["landing_dataflowspec_table"]
        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    landing_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                (
                    landing_dataflow_spec_df.write.mode("overwrite")
                    .format("delta")
                    .option("mergeSchema", "true")
                    .save(path=dict_obj["landing_dataflowspec_path"])
                )
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["landing_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["landing_dataflowspec_path"]
                )

            logger.info("In Merge block for landing")
            self.deltaPipelinesInternalTableOps.merge(
                landing_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_landing_dataflow_spec_tables()

    def __delete_none(self, _dict):
        """Delete None values recursively from all of the dictionaries"""
        filtered = {k: v for k, v in _dict.items() if v is not None}
        _dict.clear()
        _dict.update(filtered)
        return _dict

    def convert_yml_to_json(self, onboarding_file_path):
        """Get dataframe from YAML onboarding file.
        Args:
            onboarding_file_path (str): Path to YAML onboarding file
        Returns:
            DataFrame: Spark DataFrame containing onboarding data
        Raises:
            Exception: If duplicate data_flow_ids found
        """
        # Read YAML file as text
        with open(onboarding_file_path, 'r') as yaml_file:
            yaml_data = yaml.safe_load(yaml_file)

        json_data = json.dumps(yaml_data, indent=4)

        onboarding_file_path = onboarding_file_path.replace(".yml", "_yml.json")

        with open(onboarding_file_path, 'w') as json_file:
            json_file.write(json_data)
        return onboarding_file_path

    def __get_onboarding_file_dataframe(self, onboarding_file_path):
        onboarding_df = None
        if onboarding_file_path.lower().endswith((".yml", ".yaml")):
            onboarding_file_path = self.convert_yml_to_json(onboarding_file_path)
        if onboarding_file_path.lower().endswith(".json"):
            onboarding_df = self.spark.read.option("multiline", "true").json(
                onboarding_file_path
            )
            onboarding_df.show()
            self.onboard_file_type = "json"
            onboarding_df_dupes = (
                onboarding_df.groupBy("data_flow_id").count().filter("count > 1")
            )
            if len(onboarding_df_dupes.head(1)) > 0:
                onboarding_df_dupes.show()
                raise Exception("onboarding file have duplicated data_flow_ids! ")
        else:
            raise Exception(
                "Onboarding file format not supported! Please provide json file format"
            )
        return onboarding_df

    def __add_audit_columns(self, df, dict_obj):
        """Add_audit_columns method will add AuditColumns like version, dates, author.

        Args:
            df ([type]): [description]
            dict_obj ([type]): attributes = ["import_author", "version"]

        Returns:
            [type]: attributes = ["import_author", "version"]
        """
        attributes = ["import_author", "version"]
        self.__validate_dict_attributes(attributes, dict_obj)

        df = (
            df.withColumn("version", f.lit(dict_obj["version"]))
            .withColumn("createDate", f.current_timestamp())
            .withColumn("createdBy", f.lit(dict_obj["import_author"]))
            .withColumn("updateDate", f.current_timestamp())
            .withColumn("updatedBy", f.lit(dict_obj["import_author"]))
        )
        return df

    def __get_landing_schema(self, metadata_file):
        """Get schema from metadafile in json format.

        Args:
            metadata_file ([string]): metadata schema file path
        """
        ddlSchemaStr = self.spark.read.text(
            paths=metadata_file, wholetext=True
        ).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        logger.info(spark_schema)
        schema = json.dumps(spark_schema.jsonValue())
        return schema

    def __validate_mandatory_fields(self, onboarding_row, mandatory_fields):
        for field in mandatory_fields:
            # Allow None/null values for optional layers (e.g., flows without landing layer)
            # Only raise exception if field is missing or is an empty string (but not None)
            if field not in onboarding_row:
                raise Exception(f"Missing field={field} in onboarding_row")
            if onboarding_row[field] == "":
                raise Exception(f"Missing field={field} in onboarding_row (empty string)")

    def __get_landing_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get landing dataflow spec method will convert onboarding dataframe to landing Dataflowspec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "appendFlows",
            "appendFlowsSchemas",
            "sinks",
            "clusterBy"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("schema", StringType(), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField(
                    "quarantineTargetDetails",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField(
                    "quarantineTableProperties",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("sinks", StringType(), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
            ]
        )
        data = []
        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            "source_details",
            f"landing_database_{env}",
            "landing_table"
            # "landing_reader_options",
        ]  # , f"landing_table_path_{env}"
        for onboarding_row in onboarding_rows:
            # Skip flows without landing layer (null landing_database)
            landing_db_field = f"landing_database_{env}"
            if landing_db_field in onboarding_row.asDict() and onboarding_row[landing_db_field] is None:
                logger.info(f"Skipping landing layer for data_flow_id={onboarding_row['data_flow_id']} (no landing_database)")
                continue

            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"landing_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            landing_data_flow_spec_id = onboarding_row["data_flow_id"]
            landing_data_flow_spec_group = onboarding_row["data_flow_group"]
            if "source_format" not in onboarding_row:
                raise Exception(f"Source format not provided for row={onboarding_row}")

            source_format = onboarding_row["source_format"]
            if source_format.lower() not in [
                "cloudfiles",
                "eventhub",
                "kafka",
                "delta",
                "snapshot"
            ]:
                raise Exception(
                    f"Source format {source_format} not supported in DLT-META! row={onboarding_row}"
                )
            source_details, landing_reader_config_options, schema = (
                self.get_landing_source_details_reader_options_schema(
                    onboarding_row, env
                )
            )
            landing_target_format = "delta"
            landing_target_details = {
                "database": onboarding_row["landing_database_{}".format(env)],
                "table": onboarding_row["landing_table"],
            }
            landing_cl = (
                onboarding_row["landing_catalog_{}".format(env)]
                if "landing_catalog_{}".format(env) in onboarding_row
                else None
            )
            if "landing_table_comment" in onboarding_row:
                landing_target_details["comment"] = onboarding_row["landing_table_comment"]

            if landing_cl:
                landing_target_details["catalog"] = landing_cl
            if not self.uc_enabled:
                if f"landing_table_path_{env}" in onboarding_row:
                    landing_target_details["path"] = onboarding_row[f"landing_table_path_{env}"]
                else:
                    raise Exception(f"landing_table_path_{env} not provided in onboarding_row={onboarding_row}")
            landing_table_properties = {}
            if (
                "landing_table_properties" in onboarding_row
                and onboarding_row["landing_table_properties"]
            ):
                landing_table_properties = self.__delete_none(
                    onboarding_row["landing_table_properties"].asDict()
                )

            partition_columns = [""]
            if (
                "landing_partition_columns" in onboarding_row
                and onboarding_row["landing_partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row["landing_partition_columns"]:
                    partition_columns = onboarding_row["landing_partition_columns"].split(",")
                else:
                    partition_columns = [onboarding_row["landing_partition_columns"]]

            dlt_sinks = None
            if "landing_sinks" in onboarding_row and onboarding_row["landing_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "landing")
            cluster_by = self.__get_cluster_by_properties(onboarding_row, landing_table_properties,
                                                          "landing_cluster_by")

            cdc_apply_changes = None
            if (
                "landing_cdc_apply_changes" in onboarding_row
                and onboarding_row["landing_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "landing")
                cdc_apply_changes = json.dumps(
                    self.__delete_none(
                        onboarding_row["landing_cdc_apply_changes"].asDict()
                    )
                )
            apply_changes_from_snapshot = None
            if ("landing_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["landing_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "landing")
                apply_changes_from_snapshot = json.dumps(
                    self.__delete_none(onboarding_row["landing_apply_changes_from_snapshot"].asDict())
                )
            data_quality_expectations = None
            quarantine_target_details = {}
            quarantine_table_properties = {}
            if f"landing_data_quality_expectations_json_{env}" in onboarding_row:
                landing_data_quality_expectations_json = onboarding_row[
                    f"landing_data_quality_expectations_json_{env}"
                ]
                if landing_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        landing_data_quality_expectations_json
                    )
                    if onboarding_row["landing_quarantine_table"]:
                        quarantine_target_details, quarantine_table_properties = self.__get_quarantine_details(
                            env, "landing", onboarding_row
                        )

            append_flows, append_flows_schemas = self.get_append_flows_json(
                onboarding_row, "landing", env
            )
            landing_row = (
                landing_data_flow_spec_id,
                landing_data_flow_spec_group,
                source_format,
                source_details,
                landing_reader_config_options,
                landing_target_format,
                landing_target_details,
                landing_table_properties,
                schema,
                partition_columns,
                cdc_apply_changes,
                apply_changes_from_snapshot,
                data_quality_expectations,
                quarantine_target_details,
                quarantine_table_properties,
                append_flows,
                append_flows_schemas,
                dlt_sinks,
                cluster_by
            )
            data.append(landing_row)
            # logger.info(landing_parition_columns)

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)

        return data_flow_spec_rows_df

    def __parse_cluster_by_string(self, cluster_by_value, cluster_key):
        """Parse string representation of list into actual list."""

        if isinstance(cluster_by_value, list):
            return cluster_by_value

        if isinstance(cluster_by_value, str):
            # Try to parse string representation of a list
            try:
                parsed = ast.literal_eval(cluster_by_value)
                if isinstance(parsed, list):
                    return parsed
                else:
                    raise ValueError(f"Parsed value is not a list: {type(parsed).__name__}")
            except (ValueError, SyntaxError) as e:
                raise Exception(
                    f"Invalid {cluster_key}: Cannot parse string as list. "
                    f"Value: '{cluster_by_value}'. Error: {str(e)}"
                )

        raise Exception(
            f"Invalid {cluster_key}: Expected a list or string representation of list but got "
            f"{type(cluster_by_value).__name__}. Value: {cluster_by_value}"
        )

    def __get_cluster_by_properties(self, onboarding_row, table_properties, cluster_key):
        cluster_by = None
        if cluster_key in onboarding_row and onboarding_row[cluster_key]:
            if table_properties.get('pipelines.autoOptimize.zOrderCols') is not None:
                raise Exception(
                    f"Cannot support zOrder and cluster_by together at {cluster_key} "
                    f"for onboarding_row={onboarding_row}"
                )
            # Parse cluster_by value (handles both lists and string representations)
            cluster_by = self.__parse_cluster_by_string(onboarding_row[cluster_key], cluster_key)

            # Validate that each element in the list is a properly formatted string
            for i, column in enumerate(cluster_by):
                if not isinstance(column, str):
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} must be a string but got "
                        f"{type(column).__name__}. Value: {column}"
                    )

                # Check for common string formatting issues
                if column.strip() != column:
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} contains leading/trailing whitespace. "
                        f"Value: '{column}' (should be '{column.strip()}')"
                    )

                if not column.strip():
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} is empty or contains only whitespace. "
                        f"Value: '{column}'"
                    )

                # Check for unbalanced quotes or malformed strings
                if (column.count('"') % 2 != 0) or (column.count("'") % 2 != 0):
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} contains unbalanced quotes. "
                        f"Value: '{column}'"
                    )
            return cluster_by

    def __get_quarantine_details(self, env, layer, onboarding_row):
        quarantine_table_partition_columns = ""
        quarantine_target_details = {}
        quarantine_table_properties = {}
        quarantine_table_cluster_by = None
        if (
            f"{layer}_quarantine_table_partitions" in onboarding_row
            and onboarding_row[f"{layer}_quarantine_table_partitions"]
        ):
            # Split if this is a list separated by commas
            if "," in onboarding_row[f"{layer}_quarantine_table_partitions"]:
                quarantine_table_partition_columns = onboarding_row[f"{layer}_quarantine_table_partitions"].split(",")
            else:
                quarantine_table_partition_columns = onboarding_row[f"{layer}_quarantine_table_partitions"]
        if (
            f"{layer}_quarantine_table_properties" in onboarding_row
            and onboarding_row[f"{layer}_quarantine_table_properties"]
        ):
            quarantine_table_properties = self.__delete_none(
                onboarding_row[f"{layer}_quarantine_table_properties"].asDict()
            )

        quarantine_table_cluster_by = self.__get_cluster_by_properties(onboarding_row, quarantine_table_properties,
                                                                       f"{layer}_quarantine_table_cluster_by")
        if (
            f"{layer}_database_quarantine_{env}" in onboarding_row
            and onboarding_row[f"{layer}_database_quarantine_{env}"]
        ):
            quarantine_target_details = {"database": onboarding_row[f"{layer}_database_quarantine_{env}"],
                                         "table": onboarding_row[f"{layer}_quarantine_table"],
                                         "partition_columns": quarantine_table_partition_columns,
                                         "cluster_by": quarantine_table_cluster_by
                                         }
            quarantine_catalog = (
                onboarding_row[f"{layer}_catalog_quarantine_{env}"]
                if f"{layer}_catalog_quarantine_{env}" in onboarding_row
                else None
            )
            if quarantine_catalog:
                quarantine_target_details["catalog"] = quarantine_catalog
            if "{layer}_quarantine_table_comment" in onboarding_row:
                quarantine_target_details["comment"] = onboarding_row[f"{layer}_quarantine_table_comment"]
        if not self.uc_enabled and f"{layer}_quarantine_table_path_{env}" in onboarding_row:
            quarantine_target_details["path"] = onboarding_row[f"{layer}_quarantine_table_path_{env}"]

        return quarantine_target_details, quarantine_table_properties

    def get_append_flows_json(self, onboarding_row, layer, env):
        append_flows = None
        append_flows_schema = {}
        if (
            f"{layer}_append_flows" in onboarding_row
            and onboarding_row[f"{layer}_append_flows"]
        ):
            self.__validate_append_flow(onboarding_row, layer)
            json_append_flows = onboarding_row[f"{layer}_append_flows"]
            from pyspark.sql.types import Row

            af_list = []
            for json_append_flow in json_append_flows:
                json_append_flow = json_append_flow.asDict()
                append_flow_map = {}
                for key in json_append_flow.keys():
                    if isinstance(json_append_flow[key], Row):
                        fs = json_append_flow[key].__fields__
                        mp = {}
                        for ff in fs:
                            if f"source_path_{env}" == ff:
                                mp["path"] = json_append_flow[key][f"{ff}"]
                            elif "source_schema_path" == ff:
                                source_schema_path = json_append_flow[key][f"{ff}"]
                                if source_schema_path:
                                    schema = self.__get_landing_schema(
                                        source_schema_path
                                    )
                                    append_flows_schema[json_append_flow["name"]] = (
                                        schema
                                    )
                            else:
                                mp[f"{ff}"] = json_append_flow[key][f"{ff}"]
                        append_flow_map[key] = self.__delete_none(mp)
                    else:
                        append_flow_map[key] = json_append_flow[key]
                af_list.append(self.__delete_none(append_flow_map))
            append_flows = json.dumps(af_list)
        return append_flows, append_flows_schema

    def get_sink_details(self, onboarding_row, layer):
        sink_details_json = onboarding_row[f"{layer}_sinks"]
        sinks_json = self.get_validated_sinks_details(sink_details_json)
        return sinks_json

    def get_validated_sinks_details(self, sinks_details_json):
        sink_list = []
        for sink_details_json in sinks_details_json:
            sink = {}
            sink_details = sink_details_json.asDict()
            sink_details_keys = set(sink_details.keys())
            missing_sink_details_keys = set(DataflowSpecUtils.sink_mandatory_attributes).difference(sink_details_keys)
            if missing_sink_details_keys:
                raise Exception(f"Missing sink details keys: {missing_sink_details_keys}")
            if sink_details.get("name", None):
                sink["name"] = sink_details["name"].lower()
            if sink_details.get("format", None):
                sink_format_options = ["delta", "kafka", "eventhub"]
                if sink_details["format"].lower() not in sink_format_options:
                    raise Exception(f"Sink format {sink_details['format']} not supported in DLT-META!")
                sink["format"] = sink_details["format"].lower()
            if sink_details.get("options", None):
                options_dict = self.__delete_none(sink_details["options"].asDict())
                options_json = json.dumps(self.__delete_none(options_dict))
                sink["options"] = options_json
                delta_format_options = ["path", "tablename"]
                dlt_sink_options_keys = set(options_dict.keys())
                if sink["format"] == "delta":
                    if "path" in dlt_sink_options_keys or "tablename" in dlt_sink_options_keys:
                        logger.info("Validated delta sink options")
                    else:
                        raise Exception(f"Missing delta sink options: {delta_format_options}")
            sink["select_exp"] = sink_details.get("select_exp", None)
            sink["where_clause"] = sink_details.get("where_clause", None)
            sink_list.append(sink)
        sinks_json = json.dumps(sink_list)
        logger.info(f"Validated sinks details: {sinks_json}")
        return sinks_json

    def __validate_apply_changes(self, onboarding_row, layer):
        cdc_apply_changes = onboarding_row[f"{layer}_cdc_apply_changes"]
        json_cdc_apply_changes = self.__delete_none(cdc_apply_changes.asDict())
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(
            DataflowSpecUtils.cdc_applychanges_api_attributes
        ).difference(payload_keys)
        logger.info(
            f"""missing cdc payload keys:{missing_cdc_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
            payload_keys
        ):
            missing_mandatory_attr = set(
                DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes
            ) - set(payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_cdc_apply_changes = {missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_cdc_apply_changes atrributes
                {DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

    def __validate_apply_changes_from_snapshot(self, onboarding_row, layer):
        apply_changes_from_snapshot = onboarding_row[f"{layer}_apply_changes_from_snapshot"]
        json_apply_changes_from_snapshot = self.__delete_none(apply_changes_from_snapshot.asDict())
        logger.info(f"actual applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = (
            set(DataflowSpecUtils.apply_changes_from_snapshot_api_attributes).difference(payload_keys)
        )
        logger.info(
            f"""missing applyChangesFromSnapshot payload keys:{missing_apply_changes_from_snapshot_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_apply_changes_from_snapshot = {
                missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_apply_changes_from_snapshot atrributes
                 {DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

    def get_landing_source_details_reader_options_schema(self, onboarding_row, env):
        """Get landing source reader options.

        Args:
            onboarding_row ([type]): [description]

        Returns:
            [type]: [description]
        """
        source_details = {}
        landing_reader_config_options = {}
        schema = None
        source_format = onboarding_row["source_format"]
        landing_reader_options_json = (
            onboarding_row["landing_reader_options"]
            if "landing_reader_options" in onboarding_row
            else {}
        )
        if landing_reader_options_json:
            landing_reader_config_options = self.__delete_none(
                landing_reader_options_json.asDict()
            )
        source_details_json = onboarding_row["source_details"]
        if source_details_json:
            source_details_file = self.__delete_none(source_details_json.asDict())
            if (source_format.lower() == "cloudfiles"
                    or source_format.lower() == "delta"
                    or source_format.lower() == "snapshot"):
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                if f"source_catalog_{env}" in source_details_file:
                    source_details["catalog"] = source_details_file[f"source_catalog_{env}"]
                if "source_database" in source_details_file:
                    source_details["source_database"] = source_details_file[
                        "source_database"
                    ]
                if "source_table" in source_details_file:
                    source_details["source_table"] = source_details_file["source_table"]
                if "source_metadata" in source_details_file:
                    source_metadata_dict = self.__delete_none(
                        source_details_file["source_metadata"].asDict()
                    )
                    if "select_metadata_cols" in source_metadata_dict:
                        select_metadata_cols = self.__delete_none(
                            source_metadata_dict["select_metadata_cols"].asDict()
                        )
                        source_metadata_dict["select_metadata_cols"] = select_metadata_cols
                    source_details["source_metadata"] = json.dumps(
                        self.__delete_none(source_metadata_dict)
                    )
            if source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
            elif source_format.lower() == "eventhub" or source_format.lower() == "kafka":
                source_details = source_details_file
            elif source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                else:
                    raise Exception(f"source_path_{env} is missing in the source_details")
            if "source_schema_path" in source_details_file:
                source_schema_path = source_details_file["source_schema_path"]
                if source_schema_path:
                    if self.landing_schema_mapper is not None:
                        schema = self.landing_schema_mapper(
                            source_schema_path, self.spark
                        )
                    else:
                        schema = self.__get_landing_schema(source_schema_path)
                else:
                    logger.info(f"no input schema provided for row={onboarding_row}")
                logger.info("spark_schema={}".format(schema))

        return source_details, landing_reader_config_options, schema

    def __validate_append_flow(self, onboarding_row, layer):
        append_flows = onboarding_row[f"{layer}_append_flows"]
        for append_flow in append_flows:
            json_append_flow = append_flow.asDict()
            logger.info(f"actual appendFlow={json_append_flow}")
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = set(
                DataflowSpecUtils.append_flow_api_attributes_defaults
            ).difference(payload_keys)
            logger.info(
                f"""missing append flow payload keys:{missing_append_flow_payload_keys}
                    for onboarding row = {onboarding_row}"""
            )
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(
                payload_keys
            ):
                missing_mandatory_attr = set(
                    DataflowSpecUtils.append_flow_mandatory_attributes
                ) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(
                    f"""mandatory missing atrributes for {layer}_append_flow = {missing_mandatory_attr}
                    for onboarding row = {onboarding_row}"""
                )
            else:
                logger.info(
                    f"""all mandatory {layer}_append_flow atrributes
                    {DataflowSpecUtils.append_flow_mandatory_attributes} exists"""
                )

    def __get_data_quality_expecations(self, json_file_path):
        """Get Data Quality expections from json file.

        Args:
            json_file_path ([type]): [description]
        """
        json_string = None
        if json_file_path and json_file_path.endswith(".json"):
            expectations_df = self.spark.read.text(json_file_path, wholetext=True)
            expectations_arr = expectations_df.collect()
            if len(expectations_arr) == 1:
                json_string = expectations_df.collect()[0]["value"]
        return json_string

    def __get_refinery_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get refinery_dataflow_spec method transform onboarding dataframe to refinery dataflowSpec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "quarantineClusterBy",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "sinks"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("quarantineTargetDetails", MapType(StringType(), StringType(), True), True),
                StructField("quarantineTableProperties", MapType(StringType(), StringType(), True), True),
                StructField("quarantineClusterBy", ArrayType(StringType(), True), True),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("sinks", StringType(), True)
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"refinery_database_{env}",
            "refinery_table",
            f"refinery_transformation_json_{env}",
        ]  # f"refinery_table_path_{env}",

        for onboarding_row in onboarding_rows:
            # Skip flows without refinery layer (null refinery_database)
            refinery_db_field = f"refinery_database_{env}"
            if refinery_db_field in onboarding_row.asDict() and onboarding_row[refinery_db_field] is None:
                logger.info(f"Skipping refinery layer for data_flow_id={onboarding_row['data_flow_id']} (no refinery_database)")
                continue

            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"refinery_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            refinery_data_flow_spec_id = onboarding_row["data_flow_id"]
            refinery_data_flow_spec_group = onboarding_row["data_flow_group"]
            refinery_reader_config_options = {}

            refinery_target_format = "delta"

            landing_target_details = {
                "database": onboarding_row["landing_database_{}".format(env)],
                "table": onboarding_row["landing_table"],
            }
            landing_cl = (
                onboarding_row["landing_catalog_{}".format(env)]
                if "landing_catalog_{}".format(env) in onboarding_row
                else None
            )
            if landing_cl:
                landing_target_details["catalog"] = landing_cl
            refinery_target_details = {
                "database": onboarding_row["refinery_database_{}".format(env)],
                "table": onboarding_row["refinery_table"],
            }
            refinery_cl = (
                onboarding_row["refinery_catalog_{}".format(env)]
                if "refinery_catalog_{}".format(env) in onboarding_row
                else None
            )
            if "refinery_table_comment" in onboarding_row:
                refinery_target_details["comment"] = onboarding_row["refinery_table_comment"]
            if refinery_cl:
                refinery_target_details["catalog"] = refinery_cl
            if not self.uc_enabled:
                landing_target_details["path"] = onboarding_row[
                    f"landing_table_path_{env}"
                ]
                refinery_target_details["path"] = onboarding_row[
                    f"refinery_table_path_{env}"
                ]
            refinery_reader_options_json = (
                onboarding_row["refinery_reader_options"]
                if "refinery_reader_options" in onboarding_row
                else {}
            )
            if refinery_reader_options_json:
                refinery_reader_config_options = self.__delete_none(
                    refinery_reader_options_json.asDict()
                )
            refinery_table_properties = {}
            if (
                "refinery_table_properties" in onboarding_row
                and onboarding_row["refinery_table_properties"]
            ):
                refinery_table_properties = self.__delete_none(
                    onboarding_row["refinery_table_properties"].asDict()
                )

            refinery_partition_columns_var = [""]
            if (
                "refinery_partition_columns" in onboarding_row
                and onboarding_row["refinery_partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row["refinery_partition_columns"]:
                    refinery_partition_columns_var = onboarding_row["refinery_partition_columns"].split(",")
                else:
                    refinery_partition_columns_var = [onboarding_row["refinery_partition_columns"]]

            dlt_sinks = None
            if "refinery_sinks" in onboarding_row and onboarding_row["refinery_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "refinery")
            refinery_cluster_by = self.__get_cluster_by_properties(onboarding_row, refinery_table_properties,
                                                                 "refinery_cluster_by")

            refinery_cdc_apply_changes = None
            if (
                "refinery_cdc_apply_changes" in onboarding_row
                and onboarding_row["refinery_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "refinery")
                refinery_cdc_apply_changes_row = onboarding_row[
                    "refinery_cdc_apply_changes"
                ]
                if self.onboard_file_type == "json":
                    refinery_cdc_apply_changes = json.dumps(
                        self.__delete_none(refinery_cdc_apply_changes_row.asDict())
                    )
            data_quality_expectations = None
            refinery_quarantine_target_details = None
            refinery_quarantine_table_properties = None
            refinery_quarantine_cluster_by = None
            if f"refinery_data_quality_expectations_json_{env}" in onboarding_row:
                refinery_data_quality_expectations_json = onboarding_row[
                    f"refinery_data_quality_expectations_json_{env}"
                ]
                if refinery_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        refinery_data_quality_expectations_json
                    )
                refinery_quarantine_target_details, refinery_quarantine_table_properties = self.__get_quarantine_details(
                    env, "refinery", onboarding_row
                )
                refinery_quarantine_cluster_by = self.__get_cluster_by_properties(
                    onboarding_row,
                    refinery_quarantine_table_properties,
                    "refinery_quarantine_cluster_by"
                )
            append_flows, append_flow_schemas = self.get_append_flows_json(
                onboarding_row, layer="refinery", env=env
            )
            apply_changes_from_snapshot = None
            source_format = "delta"
            if ("refinery_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["refinery_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "refinery")
                apply_changes_from_snapshot = json.dumps(
                    self.__delete_none(onboarding_row["refinery_apply_changes_from_snapshot"].asDict())
                )
                source_format = "snapshot"
            refinery_row = (
                refinery_data_flow_spec_id,
                refinery_data_flow_spec_group,
                source_format,
                landing_target_details,
                refinery_reader_config_options,
                refinery_target_format,
                refinery_target_details,
                refinery_table_properties,
                refinery_partition_columns_var,
                refinery_cdc_apply_changes,
                apply_changes_from_snapshot,
                data_quality_expectations,
                refinery_quarantine_target_details,
                refinery_quarantine_table_properties,
                refinery_quarantine_cluster_by,
                append_flows,
                append_flow_schemas,
                refinery_cluster_by,
                dlt_sinks
            )
            data.append(refinery_row)
            logger.info(f"refinery_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df

    def __get_treasury_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get treasury_dataflow_spec method transform onboarding dataframe to treasury dataflowSpec dataframe.

        Args:
            onboarding_df: Onboarding dataframe
            env: Environment (nonprod/preprod/prod)

        Returns:
            DataFrame: Treasury dataflowspec dataframe
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "dataQualityExpectations",
            "clusterBy",
            "sinks"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("sinks", StringType(), True)
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"treasury_database_{env}",
            "treasury_table",
            f"treasury_transformation_json_{env}",
        ]

        for onboarding_row in onboarding_rows:
            # Skip flows without treasury layer (null treasury_database)
            treasury_db_field = f"treasury_database_{env}"
            if treasury_db_field in onboarding_row.asDict() and onboarding_row[treasury_db_field] is None:
                logger.info(f"Skipping treasury layer for data_flow_id={onboarding_row['data_flow_id']} (no treasury_database)")
                continue

            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"treasury_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)

            treasury_data_flow_spec_id = onboarding_row["data_flow_id"]
            treasury_data_flow_spec_group = onboarding_row["data_flow_group"]
            treasury_reader_config_options = {}

            treasury_target_format = "delta"

            # Determine source layer - could be refinery, landing, or another delta table
            # Priority: refinery > landing > source_details (for delta sources)
            source_details = {}
            if f"refinery_database_{env}" in onboarding_row and onboarding_row[f"refinery_database_{env}"] is not None:
                # Source is refinery layer
                source_details = {
                    "database": onboarding_row[f"refinery_database_{env}"],
                    "table": onboarding_row["refinery_table"],
                }
                refinery_cl = (
                    onboarding_row[f"refinery_catalog_{env}"]
                    if f"refinery_catalog_{env}" in onboarding_row
                    else None
                )
                if refinery_cl:
                    source_details["catalog"] = refinery_cl
                if not self.uc_enabled and f"refinery_table_path_{env}" in onboarding_row:
                    source_details["path"] = onboarding_row[f"refinery_table_path_{env}"]
            elif f"landing_database_{env}" in onboarding_row and onboarding_row[f"landing_database_{env}"] is not None:
                # Source is landing layer
                source_details = {
                    "database": onboarding_row[f"landing_database_{env}"],
                    "table": onboarding_row["landing_table"],
                }
                landing_cl = (
                    onboarding_row[f"landing_catalog_{env}"]
                    if f"landing_catalog_{env}" in onboarding_row
                    else None
                )
                if landing_cl:
                    source_details["catalog"] = landing_cl
                if not self.uc_enabled and f"landing_table_path_{env}" in onboarding_row:
                    source_details["path"] = onboarding_row[f"landing_table_path_{env}"]
            elif "source_details" in onboarding_row and onboarding_row["source_details"]:
                # Source is from source_details (for delta sources)
                source_details_file = self.__delete_none(onboarding_row["source_details"].asDict())
                if "catalog_{}".format(env) in source_details_file:
                    source_details["catalog"] = source_details_file[f"catalog_{env}"]
                if "database_{}".format(env) in source_details_file:
                    source_details["database"] = source_details_file[f"database_{env}"]
                if "table" in source_details_file:
                    source_details["table"] = source_details_file["table"]
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]

            treasury_target_details = {
                "database": onboarding_row[f"treasury_database_{env}"],
                "table": onboarding_row["treasury_table"],
            }
            treasury_cl = (
                onboarding_row[f"treasury_catalog_{env}"]
                if f"treasury_catalog_{env}" in onboarding_row
                else None
            )
            if "treasury_table_comment" in onboarding_row:
                treasury_target_details["comment"] = onboarding_row["treasury_table_comment"]
            if treasury_cl:
                treasury_target_details["catalog"] = treasury_cl
            if not self.uc_enabled and f"treasury_table_path_{env}" in onboarding_row:
                treasury_target_details["path"] = onboarding_row[f"treasury_table_path_{env}"]

            treasury_reader_options_json = (
                onboarding_row["treasury_reader_options"]
                if "treasury_reader_options" in onboarding_row
                else {}
            )
            if treasury_reader_options_json:
                treasury_reader_config_options = self.__delete_none(
                    treasury_reader_options_json.asDict()
                )

            treasury_table_properties = {}
            if (
                "treasury_table_properties" in onboarding_row
                and onboarding_row["treasury_table_properties"]
            ):
                treasury_table_properties = self.__delete_none(
                    onboarding_row["treasury_table_properties"].asDict()
                )

            treasury_partition_columns_var = [""]
            if (
                "treasury_partition_columns" in onboarding_row
                and onboarding_row["treasury_partition_columns"]
            ):
                if "," in onboarding_row["treasury_partition_columns"]:
                    treasury_partition_columns_var = onboarding_row["treasury_partition_columns"].split(",")
                else:
                    treasury_partition_columns_var = [onboarding_row["treasury_partition_columns"]]

            dlt_sinks = None
            if "treasury_sinks" in onboarding_row and onboarding_row["treasury_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "treasury")

            treasury_cluster_by = self.__get_cluster_by_properties(
                onboarding_row, treasury_table_properties, "treasury_cluster_by"
            )

            treasury_cdc_apply_changes = None
            if (
                "treasury_cdc_apply_changes" in onboarding_row
                and onboarding_row["treasury_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "treasury")
                treasury_cdc_apply_changes_row = onboarding_row["treasury_cdc_apply_changes"]
                if self.onboard_file_type == "json":
                    treasury_cdc_apply_changes = json.dumps(
                        self.__delete_none(treasury_cdc_apply_changes_row.asDict())
                    )

            data_quality_expectations = None
            if f"treasury_data_quality_expectations_json_{env}" in onboarding_row:
                treasury_data_quality_expectations_json = onboarding_row[
                    f"treasury_data_quality_expectations_json_{env}"
                ]
                if treasury_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        treasury_data_quality_expectations_json
                    )

            source_format = "delta"

            treasury_row = (
                treasury_data_flow_spec_id,
                treasury_data_flow_spec_group,
                source_format,
                source_details,
                treasury_reader_config_options,
                treasury_target_format,
                treasury_target_details,
                treasury_table_properties,
                treasury_partition_columns_var,
                treasury_cdc_apply_changes,
                data_quality_expectations,
                treasury_cluster_by,
                dlt_sinks
            )
            data.append(treasury_row)
            logger.info(f"treasury_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df
