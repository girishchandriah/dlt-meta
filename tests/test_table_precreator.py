"""Unit tests for TablePreCreator class."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from src.table_precreator import TablePreCreator
from src.dataflow_spec import LandingDataflowSpec, RefineryDataflowSpec, TreasuryDataflowSpec


class TestTablePreCreator:
    """Test suite for TablePreCreator class."""

    @pytest.fixture
    def spark_mock(self):
        """Create a mock Spark session."""
        spark = Mock()
        spark.conf.get.return_value = "true"
        return spark

    @pytest.fixture
    def table_creator(self, spark_mock):
        """Create a TablePreCreator instance with mocked Spark."""
        return TablePreCreator(spark_mock, uc_enabled=True)

    @pytest.fixture
    def sample_schema(self):
        """Create a sample StructType schema."""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("created_date", DateType(), True),
            StructField("amount", DecimalType(10, 2), True)
        ])

    def test_init(self, spark_mock):
        """Test TablePreCreator initialization."""
        creator = TablePreCreator(spark_mock, uc_enabled=True)
        assert creator.spark == spark_mock
        assert creator.uc_enabled is True
        assert creator.logger is not None

    def test_init_uc_disabled(self, spark_mock):
        """Test TablePreCreator initialization with Unity Catalog disabled."""
        creator = TablePreCreator(spark_mock, uc_enabled=False)
        assert creator.uc_enabled is False

    def test_table_exists_unity_catalog_true(self, table_creator, spark_mock):
        """Test table existence check for Unity Catalog when table exists."""
        spark_mock.catalog.tableExists.return_value = True

        result = table_creator._table_exists("test_catalog", "test_db", "test_table")

        assert result is True
        spark_mock.catalog.tableExists.assert_called_once_with("test_catalog.test_db.test_table")

    def test_table_exists_unity_catalog_false(self, table_creator, spark_mock):
        """Test table existence check for Unity Catalog when table doesn't exist."""
        spark_mock.catalog.tableExists.return_value = False

        result = table_creator._table_exists("test_catalog", "test_db", "test_table")

        assert result is False

    def test_table_exists_legacy(self, spark_mock):
        """Test table existence check for legacy Hive metastore."""
        creator = TablePreCreator(spark_mock, uc_enabled=False)
        spark_mock.catalog.tableExists.return_value = True

        result = creator._table_exists(None, "test_db", "test_table")

        assert result is True
        spark_mock.catalog.tableExists.assert_called_once_with("test_db", "test_table")

    def test_table_exists_exception_handling(self, table_creator, spark_mock):
        """Test table existence check handles exceptions gracefully."""
        spark_mock.catalog.tableExists.side_effect = Exception("Catalog error")

        result = table_creator._table_exists("test_catalog", "test_db", "test_table")

        assert result is False

    @patch('src.table_precreator.T._parse_datatype_string')
    def test_get_schema_from_ddl_valid(self, mock_parse, table_creator, spark_mock):
        """Test loading schema from valid DDL file."""
        # Mock DDL file content
        ddl_content = "id: int, name: string, created_date: date"
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=ddl_content)
        spark_mock.read.text.return_value.collect.return_value = [mock_row]

        # Mock the schema parsing
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("created_date", DateType(), True)
        ])
        mock_parse.return_value = expected_schema

        schema = table_creator._get_schema_from_ddl("/path/to/schema.ddl")

        assert isinstance(schema, StructType)
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "created_date"
        mock_parse.assert_called_once_with(ddl_content)

    def test_get_schema_from_ddl_empty_path(self, table_creator):
        """Test that empty schema path raises ValueError."""
        with pytest.raises(ValueError, match="Schema path is required"):
            table_creator._get_schema_from_ddl("")

    def test_get_schema_from_ddl_none_path(self, table_creator):
        """Test that None schema path raises ValueError."""
        with pytest.raises(ValueError, match="Schema path is required"):
            table_creator._get_schema_from_ddl(None)

    def test_get_schema_from_ddl_file_not_found(self, table_creator, spark_mock):
        """Test that missing DDL file raises ValueError."""
        spark_mock.read.text.return_value.collect.side_effect = Exception("File not found")

        with pytest.raises(ValueError, match="Failed to read DDL file"):
            table_creator._get_schema_from_ddl("/nonexistent/schema.ddl")

    def test_get_schema_from_ddl_invalid_syntax(self, table_creator, spark_mock):
        """Test that invalid DDL syntax raises ValueError."""
        # Mock invalid DDL content
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="invalid syntax here")
        spark_mock.read.text.return_value.collect.return_value = [mock_row]

        with pytest.raises(ValueError, match="Failed to parse DDL schema"):
            table_creator._get_schema_from_ddl("/path/to/invalid.ddl")

    def test_create_empty_table_basic(self, table_creator, spark_mock, sample_schema):
        """Test basic table creation without partitions or clustering."""
        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, None, {}, None
        )

        # Verify SQL was executed
        spark_mock.sql.assert_called_once()
        sql_call = spark_mock.sql.call_args[0][0]

        assert "CREATE TABLE IF NOT EXISTS test_catalog.test_db.test_table" in sql_call
        assert "`id` int" in sql_call
        assert "`name` string" in sql_call
        assert "`created_date` date" in sql_call
        assert "`amount` decimal(10,2)" in sql_call
        assert "USING DELTA" in sql_call

    def test_create_empty_table_with_partitions(self, table_creator, spark_mock, sample_schema):
        """Test table creation with partition columns."""
        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, ["created_date"], {}, None
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "PARTITIONED BY (`created_date`)" in sql_call

    def test_create_empty_table_with_multiple_partitions(self, table_creator, spark_mock, sample_schema):
        """Test table creation with multiple partition columns."""
        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, ["created_date", "name"], {}, None
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "PARTITIONED BY (`created_date`, `name`)" in sql_call

    def test_create_empty_table_with_clustering(self, table_creator, spark_mock, sample_schema):
        """Test table creation with cluster by columns."""
        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, None, {}, ["id", "name"]
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "CLUSTER BY (`id`, `name`)" in sql_call

    def test_create_empty_table_with_properties(self, table_creator, spark_mock, sample_schema):
        """Test table creation with table properties."""
        properties = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true"
        }

        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, None, properties, None
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "TBLPROPERTIES" in sql_call
        assert "'delta.enableChangeDataFeed' = 'true'" in sql_call
        assert "'delta.autoOptimize.optimizeWrite' = 'true'" in sql_call

    def test_create_empty_table_all_features(self, table_creator, spark_mock, sample_schema):
        """Test table creation with partitions, clustering, and properties."""
        properties = {"delta.enableChangeDataFeed": "true"}

        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, ["created_date"], properties, ["id"]
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_catalog.test_db.test_table" in sql_call
        assert "USING DELTA" in sql_call
        assert "PARTITIONED BY (`created_date`)" in sql_call
        assert "CLUSTER BY (`id`)" in sql_call
        assert "TBLPROPERTIES" in sql_call

    def test_create_empty_table_without_catalog(self, table_creator, spark_mock, sample_schema):
        """Test table creation without catalog (legacy mode)."""
        table_creator._create_empty_table(
            None, "test_db", "test_table",
            sample_schema, None, {}, None
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_db.test_table" in sql_call
        assert "test_catalog" not in sql_call

    def test_create_empty_table_filters_empty_partitions(self, table_creator, spark_mock, sample_schema):
        """Test that empty strings in partition columns are filtered out."""
        table_creator._create_empty_table(
            "test_catalog", "test_db", "test_table",
            sample_schema, ["created_date", "", "  "], {}, None
        )

        sql_call = spark_mock.sql.call_args[0][0]
        assert "PARTITIONED BY (`created_date`)" in sql_call
        # Should not have empty or whitespace-only columns

    def test_create_empty_table_creation_failure(self, table_creator, spark_mock, sample_schema):
        """Test that table creation failure raises RuntimeError."""
        spark_mock.sql.side_effect = Exception("Permission denied")

        with pytest.raises(RuntimeError, match="Failed to create table"):
            table_creator._create_empty_table(
                "test_catalog", "test_db", "test_table",
                sample_schema, None, {}, None
            )

    @patch('src.table_precreator.T._parse_datatype_string')
    def test_ensure_table_exists_landing_success(self, mock_parse, table_creator, spark_mock):
        """Test successful table pre-creation for landing layer."""
        # Create landing spec
        spec = Mock(spec=LandingDataflowSpec)
        spec.dataFlowId = "test_flow_001"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_table"
        }
        spec.sourceDetails = {
            "source_schema_path": "/path/to/schema.ddl"
        }
        spec.partitionColumns = ["date"]
        spec.tableProperties = {"delta.enableChangeDataFeed": "true"}
        spec.clusterBy = ["id"]

        # Mock table doesn't exist
        spark_mock.catalog.tableExists.return_value = False

        # Mock DDL file reading
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="id: int, name: string, date: date")
        spark_mock.read.text.return_value.collect.return_value = [mock_row]

        # Mock schema parsing
        mock_parse.return_value = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", DateType(), True)
        ])

        # Execute
        table_creator.ensure_table_exists(spec, "landing")

        # Verify table was created
        spark_mock.sql.assert_called_once()

    @patch('src.table_precreator.T._parse_datatype_string')
    def test_ensure_table_exists_refinery_success(self, mock_parse, table_creator, spark_mock):
        """Test successful table pre-creation for refinery layer."""
        spec = Mock(spec=RefineryDataflowSpec)
        spec.dataFlowId = "test_flow_002"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_refinery_table"
        }
        spec.refinerySchemaPath = "/path/to/refinery_schema.ddl"
        spec.partitionColumns = None
        spec.tableProperties = {}
        spec.clusterBy = None

        spark_mock.catalog.tableExists.return_value = False

        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="id: int, processed: boolean")
        spark_mock.read.text.return_value.collect.return_value = [mock_row]

        # Mock schema parsing
        mock_parse.return_value = StructType([
            StructField("id", IntegerType(), True),
            StructField("processed", StringType(), True)  # boolean -> string for simplicity
        ])

        table_creator.ensure_table_exists(spec, "refinery")

        spark_mock.sql.assert_called_once()

    @patch('src.table_precreator.T._parse_datatype_string')
    def test_ensure_table_exists_treasury_success(self, mock_parse, table_creator, spark_mock):
        """Test successful table pre-creation for treasury layer."""
        spec = Mock(spec=TreasuryDataflowSpec)
        spec.dataFlowId = "test_flow_003"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_treasury_table"
        }
        spec.treasurySchemaPath = "/path/to/treasury_schema.ddl"
        spec.partitionColumns = ["event_date"]
        spec.tableProperties = {"delta.enableChangeDataFeed": "true"}
        spec.clusterBy = ["event_id"]

        spark_mock.catalog.tableExists.return_value = False

        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="event_id: string, event_date: date, amount: decimal(10,2)")
        spark_mock.read.text.return_value.collect.return_value = [mock_row]

        # Mock schema parsing
        mock_parse.return_value = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_date", DateType(), True),
            StructField("amount", DecimalType(10, 2), True)
        ])

        table_creator.ensure_table_exists(spec, "treasury")

        spark_mock.sql.assert_called_once()

    def test_ensure_table_exists_table_already_exists(self, table_creator, spark_mock):
        """Test that existing table is not recreated."""
        spec = Mock(spec=LandingDataflowSpec)
        spec.dataFlowId = "test_flow_004"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "existing_table"
        }
        spec.sourceDetails = {"source_schema_path": "/path/to/schema.ddl"}

        # Mock table exists
        spark_mock.catalog.tableExists.return_value = True

        table_creator.ensure_table_exists(spec, "landing")

        # Verify no table creation attempt
        spark_mock.sql.assert_not_called()
        spark_mock.read.text.assert_not_called()

    def test_ensure_table_exists_missing_database(self, table_creator):
        """Test that missing database raises ValueError."""
        spec = Mock(spec=LandingDataflowSpec)
        spec.dataFlowId = "test_flow_005"
        spec.targetDetails = {"catalog": "test_catalog", "table": "test_table"}

        with pytest.raises(ValueError, match="Missing database or table"):
            table_creator.ensure_table_exists(spec, "landing")

    def test_ensure_table_exists_missing_table(self, table_creator):
        """Test that missing table name raises ValueError."""
        spec = Mock(spec=LandingDataflowSpec)
        spec.dataFlowId = "test_flow_006"
        spec.targetDetails = {"catalog": "test_catalog", "database": "test_db"}

        with pytest.raises(ValueError, match="Missing database or table"):
            table_creator.ensure_table_exists(spec, "landing")

    def test_ensure_table_exists_missing_landing_schema_path(self, table_creator, spark_mock):
        """Test that missing landing schema path raises ValueError."""
        spec = Mock(spec=LandingDataflowSpec)
        spec.dataFlowId = "test_flow_007"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_table"
        }
        spec.sourceDetails = {}  # No source_schema_path

        spark_mock.catalog.tableExists.return_value = False

        with pytest.raises(ValueError, match="Schema path not provided for landing layer"):
            table_creator.ensure_table_exists(spec, "landing")

    def test_ensure_table_exists_missing_refinery_schema_path(self, table_creator, spark_mock):
        """Test that missing refinery schema path raises ValueError."""
        spec = Mock(spec=RefineryDataflowSpec)
        spec.dataFlowId = "test_flow_008"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_table"
        }
        spec.refinerySchemaPath = None

        spark_mock.catalog.tableExists.return_value = False

        with pytest.raises(ValueError, match="Schema path not provided for refinery layer"):
            table_creator.ensure_table_exists(spec, "refinery")

    def test_ensure_table_exists_missing_treasury_schema_path(self, table_creator, spark_mock):
        """Test that missing treasury schema path raises ValueError."""
        spec = Mock(spec=TreasuryDataflowSpec)
        spec.dataFlowId = "test_flow_009"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_table"
        }
        spec.treasurySchemaPath = None

        spark_mock.catalog.tableExists.return_value = False

        with pytest.raises(ValueError, match="Schema path not provided for treasury layer"):
            table_creator.ensure_table_exists(spec, "treasury")

    def test_ensure_table_exists_invalid_layer_type(self, table_creator, spark_mock):
        """Test that invalid layer type raises ValueError."""
        spec = Mock()
        spec.dataFlowId = "test_flow_010"
        spec.targetDetails = {"catalog": "c", "database": "d", "table": "t"}

        # Make sure table doesn't exist so we proceed to layer type check
        spark_mock.catalog.tableExists.return_value = False

        with pytest.raises(ValueError, match="Invalid layer_type"):
            table_creator.ensure_table_exists(spec, "invalid_layer")

    @patch('src.table_precreator.T._parse_datatype_string')
    def test_ensure_table_exists_without_optional_attributes(self, mock_parse, table_creator, spark_mock):
        """Test table creation when spec doesn't have optional attributes."""
        spec = Mock(spec=LandingDataflowSpec)
        spec.dataFlowId = "test_flow_011"
        spec.targetDetails = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_table"
        }
        spec.sourceDetails = {"source_schema_path": "/path/to/schema.ddl"}
        # Spec doesn't have partitionColumns, tableProperties, clusterBy

        spark_mock.catalog.tableExists.return_value = False

        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value="id: int")
        spark_mock.read.text.return_value.collect.return_value = [mock_row]

        # Mock schema parsing
        mock_parse.return_value = StructType([
            StructField("id", IntegerType(), True)
        ])

        # Should not raise error
        table_creator.ensure_table_exists(spec, "landing")

        spark_mock.sql.assert_called_once()
