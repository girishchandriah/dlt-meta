"""Test OnboardDataflowSpec class."""
import copy
from tests.utils import DLTFrameworkTestCase
from src.onboard_dataflowspec import OnboardDataflowspec
from src.dataflow_spec import LandingDataflowSpec, RefineryDataflowSpec
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame


class OnboardDataflowspecTests(DLTFrameworkTestCase):
    """OnboardDataflowSpec Unit Test ."""

    def test_onboard_yml_landing_dataflow_spec(self):
        """Test onboarding landing dataflow spec from YAML file."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        onboarding_params_map["onboarding_file_path"] = "tests/resources/onboarding.yml"
        onboard_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboard_dfs.onboard_landing_dataflow_spec()
        # Verify the onboarded data
        landing_df = self.read_dataflowspec(
            onboarding_params_map["database"],
            onboarding_params_map["landing_dataflowspec_table"]
        )
        # Check number of records matches YAML file
        self.assertEqual(landing_df.count(), 3)  # Two dataflows in YAML

    def test_validate_params_for_onboardLandingDataflowSpec(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        for key in onboarding_params_map:
            test_onboarding_params_map = copy.deepcopy(onboarding_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_landing_dataflow_spec()

    def test_validate_params_for_onboardRefineryDataflowSpec_uc(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        onboard_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        print(onboard_dfs.landing_dict_obj)
        print(onboard_dfs.refinery_dict_obj)
        self.assertNotIn('refinery_dataflowspec_path', onboard_dfs.landing_dict_obj)
        self.assertNotIn('landing_dataflowspec_path', onboard_dfs.refinery_dict_obj)

    def test_validate_params_for_onboardRefineryDataflowSpec(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["landing_dataflowspec_table"]
        del onboarding_params_map["landing_dataflowspec_path"]

        for key in onboarding_params_map:
            test_onboarding_params_map = copy.deepcopy(onboarding_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_refinery_dataflow_spec()

    def test_validate_params_for_onboardDataFlowSpecs(self):
        """Test for onboardDataflowspec parameters."""
        for key in self.onboarding_landing_refinery_params_map:
            test_onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_dataflow_specs()

    def test_upgrade_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def read_dataflowspec(self, database, table):
        return self.spark.read.table(f"{database}.{table}")

    def test_onboardDataFlowSpecs_with_uc_enabled(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark,
                                                   self.onboarding_landing_refinery_params_uc_map,
                                                   uc_enabled=True)
        self.assertNotIn('landing_dataflowspec_path', onboardDataFlowSpecs.landing_dict_obj)
        self.assertNotIn('refinery_dataflowspec_path', onboardDataFlowSpecs.refinery_dict_obj)

    @patch.object(OnboardDataflowspec, 'onboard_landing_dataflow_spec', new_callable=MagicMock())
    @patch.object(OnboardDataflowspec, 'onboard_refinery_dataflow_spec', new_callable=MagicMock())
    def test_onboardDataFlowSpecs_validate_with_uc_enabled(self, mock_landing, mock_refinery):
        """Test for onboardDataflowspec."""
        mock_landing.return_value = None
        mock_refinery.return_value = None
        landing_refinery_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        del landing_refinery_params_map["uc_enabled"]
        OnboardDataflowspec(self.spark,
                            landing_refinery_params_map,
                            uc_enabled=True).onboard_dataflow_specs()
        assert mock_landing.called
        assert mock_refinery.called

    def test_onboardDataFlowSpecs_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)
        landing_df_rows = landing_dataflowSpec_df.collect()
        for landing_df_row in landing_df_rows:
            landing_row = LandingDataflowSpec(**landing_df_row.asDict())
            if landing_row.dataFlowId in ["100", "101"]:
                self.assertIsNone(landing_row.readerConfigOptions.get("cloudFiles.rescuedDataColumn"))
            if landing_row.dataFlowId == "103":
                self.assertEqual(landing_row.readerConfigOptions.get("maxOffsetsPerTrigger"), "60000")

    def test_onboardDataFlowSpecs_with_merge_uc(self):
        """Test for onboardDataflowspec with merge scenario."""
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        local_params["onboarding_file_path"] = self.onboarding_json_file
        del local_params["uc_enabled"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)
        landing_df_rows = landing_dataflowSpec_df.collect()
        for landing_df_row in landing_df_rows:
            landing_row = LandingDataflowSpec(**landing_df_row.asDict())
            if landing_row.dataFlowId in ["101", "102"]:
                self.assertIsNone(landing_row.readerConfigOptions.get("cloudFiles.rescuedDataColumn"))
            if landing_row.dataFlowId == "103":
                self.assertEqual(landing_row.readerConfigOptions.get("maxOffsetsPerTrigger"), "60000")

    def test_onboardDataflowSpec_with_multiple_partitions(self):
        """Test for onboardDataflowspec with multiple partitions for landing layer."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_multiple_partitions_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()

        # Assert landing DataflowSpec for multiple partition, and quarantine partition columns.
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_uc_map['database'],
            self.onboarding_landing_refinery_params_uc_map['landing_dataflowspec_table'])
        landing_df_rows = landing_dataflowSpec_df.collect()
        for landing_df_row in landing_df_rows:
            landing_row = LandingDataflowSpec(**landing_df_row.asDict())
            self.assertEqual(len(landing_row.partitionColumns), 2)
            quarantine_partitions = [
                col for col in landing_row.quarantineTargetDetails.get('partition_columns').strip('[]').split(',')
            ]
            self.assertEqual(len(quarantine_partitions), 2)
        # Assert refinery DataflowSpec for multiple partition columns.
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_df_rows = refinery_dataflowSpec_df.collect()
        for refinery_df_row in refinery_df_rows:
            refinery_row = RefineryDataflowSpec(**refinery_df_row.asDict())
            self.assertEqual(len(refinery_row.partitionColumns), 2)

    def test_onboardLandingDataflowSpec_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_landing_dataflow_spec()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)

    def test_getOnboardingFileDataframe_for_unsupported_file(self):
        """Test onboardingFiles not supported."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_unsupported_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_landing_dataflow_spec()

    def test_onboardRefineryDataflowSpec_positive(self):
        """Test refinerydataflowspec positive."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["landing_dataflowspec_table"]
        del onboarding_params_map["landing_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_refinery_dataflow_spec()
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_dataflow_ids_dup_onboard(self):
        """Test dataflow for duplicate ids."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_dups
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_landing_dataflow_spec()

    def test_validate_mandatory_fields_landing(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_missing_keys_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_landing_dataflow_spec()

    def test_validate_mandatory_fields_refinery(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        del onboarding_params_map["landing_dataflowspec_table"]
        del onboarding_params_map["landing_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_missing_keys_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_refinery_dataflow_spec()

    def test_onboardRefineryDataflowSpec_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        onboardDataFlowSpecs.onboard_refinery_dataflow_spec()
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_refinery_dataflow_spec()
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    @patch.object(DataFrame, "write", new_callable=MagicMock)
    def test_refinery_dataflow_spec_dataframe_withuc(self, mock_write):
        """Test for onboardDataflowspec with merge scenario."""
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None

        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        del onboarding_params_map["landing_dataflowspec_table"]
        del onboarding_params_map["landing_dataflowspec_path"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        print(onboarding_params_map)
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        o_dfs.onboard_refinery_dataflow_spec()
        # Assert
        database = onboarding_params_map["database"]
        table = onboarding_params_map["refinery_dataflowspec_table"]
        mock_write.format.assert_called_once_with("delta")
        mock_write.format.return_value.mode.assert_called_once_with("overwrite")
        mock_write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            f"{database}.{table}")

    @patch.object(DataFrame, "write", new_callable=MagicMock)
    def test_landing_dataflow_spec_dataframe_withuc(self, mock_write):
        """Test for onboardDataflowspec with merge scenario."""
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        del onboarding_params_map["landing_dataflowspec_path"]
        print(onboarding_params_map)
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        o_dfs.onboard_landing_dataflow_spec()
        # Assert
        database = onboarding_params_map["database"]
        table = onboarding_params_map["landing_dataflowspec_table"]
        mock_write.format.assert_called_once_with("delta")
        mock_write.format.return_value.mode.assert_called_once_with("overwrite")
        mock_write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            f"{database}.{table}")

    def test_landing_dataflow_spec_append_flow(self):
        """Test for onboardDataflowspec with appendflow scenario."""
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_append_flow_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        landing_dataflowSpec_df.show(truncate=False)
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_refinery_fanout_dataflow_spec_dataframe(self):
        """Test for onboardDataflowspec with fanout scenario."""
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        local_params["onboarding_file_path"] = self.onboarding_refinery_fanout_json_file
        del local_params["landing_dataflowspec_table"]
        del local_params["landing_dataflowspec_path"]
        local_params["overwrite"] = "False"
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_refinery_dataflow_spec()
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(refinery_dataflowSpec_df.count(), 4)

    def test_onboard_landing_refinery_with_v7(self):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v7_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        landing_dataflowSpec_df.show(truncate=False)
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_onboard_landing_create_sink(self):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["landing_dataflowspec_table"] = "landing_dataflowspec_sink"
        del local_params["refinery_dataflowspec_table"]
        del local_params["refinery_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_landing_dataflow_spec()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            "landing_dataflowspec_sink")
        landing_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 1)

    def test_refinery_landing_create_sink(self):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["refinery_dataflowspec_table"] = "refinery_dataflowspec_sink"
        del local_params["landing_dataflowspec_table"]
        del local_params["landing_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_refinery_dataflow_spec()
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            "refinery_dataflowspec_sink")
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(refinery_dataflowSpec_df.count(), 1)

    def test_onboard_landing_refinery_with_v8(self):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v8_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        landing_dataflowSpec_df.show(truncate=False)
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_onboard_landing_refinery_with_v9(self):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v9_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        landing_dataflowSpec_df.show(truncate=False)
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_onboard_landing_refinery_with_v10(self):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v10_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        landing_dataflowSpec_df.show(truncate=False)
        refinery_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        refinery_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 5)
        self.assertEqual(refinery_dataflowSpec_df.count(), 5)

    def test_onboard_apply_changes_from_snapshot_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        onboarding_params_map['env'] = 'it'
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_apply_changes_from_snapshot_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        onboardDataFlowSpecs.onboard_landing_dataflow_spec()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 2)

    def test_onboard_apply_changes_from_snapshot_negative(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        onboarding_params_map['env'] = 'it'
        del onboarding_params_map["refinery_dataflowspec_table"]
        del onboarding_params_map["refinery_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_apply_changes_from_snapshot_json__error_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_landing_dataflow_spec()

    def test_onboard_refinery_apply_changes_from_snapshot_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        onboarding_params_map['env'] = 'dev'
        onboarding_params_map["onboarding_file_path"] = self.onboarding_refinery_apply_changes_from_snapshot_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['landing_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        landing_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_landing_refinery_params_map['database'],
            self.onboarding_landing_refinery_params_map['refinery_dataflowspec_table'])
        self.assertEqual(landing_dataflowSpec_df.count(), 3)

    def test_get_quarantine_details_with_partitions_and_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "landing_quarantine_table_partitions": "partition_col",
            "landing_database_quarantine_it": "quarantine_db",
            "landing_quarantine_table": "quarantine_table",
            "landing_quarantine_table_path_it": "quarantine_path",
            "landing_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"property_key": "property_value"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "landing", onboarding_row)
        )
        self.assertEqual(quarantine_target_details["database"], "quarantine_db")
        self.assertEqual(quarantine_target_details["table"], "quarantine_table")
        self.assertEqual(quarantine_target_details["partition_columns"], "partition_col")
        self.assertEqual(quarantine_target_details["path"], "quarantine_path")
        self.assertEqual(quarantine_table_properties, {"property_key": "property_value"})

    def test_get_quarantine_details_without_partitions_and_properties(self):
        """Test get_quarantine_details without partitions and properties."""
        onboarding_row = {
            "landing_database_quarantine_it": "quarantine_db",
            "landing_quarantine_table": "quarantine_table",
            "landing_quarantine_table_path_it": "quarantine_path"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "landing", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details["path"], "quarantine_path")
        self.assertEqual(quarantine_table_properties, {})

    def test_get_quarantine_details_with_uc_enabled(self):
        """Test get_quarantine_details with UC enabled."""
        onboarding_row = {
            "landing_database_quarantine_it": "quarantine_db",
            "landing_quarantine_table": "quarantine_table",
            "landing_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"property_key": "property_value"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(
            self.spark, self.onboarding_landing_refinery_params_map, uc_enabled=True
        )
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "landing", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details["database"], "quarantine_db")
        self.assertEqual(quarantine_target_details["table"], "quarantine_table")
        self.assertNotIn("path", quarantine_target_details)
        self.assertEqual(quarantine_table_properties, {"property_key": "property_value"})

    def test_get_quarantine_details_with_cluster_by_and_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "landing_quarantine_table_cluster_by": ['col1', 'col2'],
            "landing_database_quarantine_it": "quarantine_db",
            "landing_quarantine_table": "quarantine_table",
            "landing_quarantine_table_path_it": "quarantine_path",
            "landing_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"key": "value"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "landing", onboarding_row)
        )
        self.assertEqual(quarantine_target_details["database"], "quarantine_db")
        self.assertEqual(quarantine_target_details["table"], "quarantine_table")
        self.assertEqual(quarantine_target_details["cluster_by"], ['col1', 'col2'])
        self.assertEqual(quarantine_target_details["path"], "quarantine_path")

    def test_set_quarantine_details_with_cluster_by_and_zOrder_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "landing_quarantine_table_cluster_by": ['col1', 'col2'],
            "landing_database_quarantine_it": "quarantine_db",
            "landing_quarantine_table": "quarantine_table",
            "landing_quarantine_table_path_it": "quarantine_path",
            "landing_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"pipelines.autoOptimize.zOrderCols": "col1,col2"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "landing", onboarding_row)
        print(str(context.exception))
        self.assertTrue(
            "Cannot support zOrder and cluster_by together at landing_quarantine_table_cluster_by"
            in str(context.exception))

    def test_set_landing_table_cluster_by_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "landing_cluster_by": ['col1', 'col2'],
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertEqual(cluster_by, ['col1', 'col2'])

    def test_set_landing_table_cluster_by_and_zOrder_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "landing_cluster_by": ['col1', 'col2'],
            "landing_table_properties": MagicMock(
                asDict=MagicMock(return_value={"pipelines.autoOptimize.zOrderCols": "col1,col2"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertTrue(
            "Cannot support zOrder and cluster_by together at landing_cluster_by" in str(context.exception))

    def test_set_refinery_table_cluster_by_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "refinery_cluster_by": ['col1', 'col2'],
            "refinery_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)
        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['refinery_table_properties'], "refinery_cluster_by")
        self.assertEqual(cluster_by, ['col1', 'col2'])

    def test_set_refinery_table_cluster_by_and_zOrder_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "refinery_cluster_by": ['col1', 'col2'],
            "refinery_table_properties": MagicMock(
                asDict=MagicMock(return_value={"pipelines.autoOptimize.zOrderCols": "col1,col2"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['refinery_table_properties'], "refinery_cluster_by")
        self.assertTrue(
            "Cannot support zOrder and cluster_by together at refinery_cluster_by" in str(context.exception))

    def test_cluster_by_validation_non_list(self):
        """Test cluster_by validation with non-list value that cannot be parsed."""
        onboarding_row = {
            "landing_cluster_by": "col1,col2",  # String that cannot be parsed as list
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("Cannot parse string as list", str(context.exception))

    def test_cluster_by_validation_non_string_element(self):
        """Test cluster_by validation with non-string element in list."""
        onboarding_row = {
            "landing_cluster_by": ["col1", 123, "col3"],  # Integer in list
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("Element at index 1 must be a string but got int", str(context.exception))

    def test_cluster_by_validation_whitespace_element(self):
        """Test cluster_by validation with whitespace in element."""
        onboarding_row = {
            "landing_cluster_by": ["col1", " col2 ", "col3"],  # Whitespace around col2
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("contains leading/trailing whitespace", str(context.exception))

    def test_cluster_by_validation_empty_element(self):
        """Test cluster_by validation with empty element."""
        onboarding_row = {
            "landing_cluster_by": ["col1", "", "col3"],  # Empty string
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("is empty or contains only whitespace", str(context.exception))

    def test_cluster_by_validation_unbalanced_quotes(self):
        """Test cluster_by validation with unbalanced quotes."""
        onboarding_row = {
            "landing_cluster_by": ["col1", "col2\"", "col3"],  # Unbalanced quote
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("contains unbalanced quotes", str(context.exception))

    def test_cluster_by_validation_valid_list(self):
        """Test cluster_by validation with valid list."""
        onboarding_row = {
            "refinery_cluster_by": ["id", "customer_id"],  # Valid list
            "refinery_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['refinery_table_properties'], "refinery_cluster_by")
        self.assertEqual(cluster_by, ["id", "customer_id"])

    def test_cluster_by_string_parsing_valid(self):
        """Test cluster_by parsing from valid string representation."""
        onboarding_row = {
            "landing_cluster_by": "['id', 'email']",  # String representation of list
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertEqual(cluster_by, ["id", "email"])

    def test_cluster_by_string_parsing_invalid_syntax(self):
        """Test cluster_by parsing with invalid string syntax."""
        onboarding_row = {
            "landing_cluster_by": "['id', 'email'",  # Missing closing bracket
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("Cannot parse string as list", str(context.exception))

    def test_cluster_by_string_parsing_not_list(self):
        """Test cluster_by parsing when string represents non-list."""
        onboarding_row = {
            "landing_cluster_by": "'id,email'",  # String that evaluates to string, not list
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertIn("Parsed value is not a list", str(context.exception))

    def test_cluster_by_string_parsing_double_quotes(self):
        """Test cluster_by parsing with double quotes."""
        onboarding_row = {
            "landing_cluster_by": '["id", "customer_id"]',  # Double quotes
            "landing_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_landing_refinery_params_map)

        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['landing_table_properties'], "landing_cluster_by")
        self.assertEqual(cluster_by, ["id", "customer_id"])
