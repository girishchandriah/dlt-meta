"""Test Main class."""
from tests.utils import DLTFrameworkTestCase
import sys
import copy
from src import __main__
from unittest.mock import MagicMock

spark = MagicMock()
OnboardDataflowspec = MagicMock()


class MainTests(DLTFrameworkTestCase):
    """Main Unit Test ."""

    def test_parse_args(self):
        """Parse arguments."""
        landing_param_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        landing_param_map["uc_enabled"] = "False"
        landing_param_map["onboard_layer"] = "landing"
        list = ["dummy_test"]
        for key in landing_param_map:
            list.append(f"--{key}={landing_param_map[key]}")
        sys.argv = list
        args = __main__.parse_args()
        print(args.__dict__.keys())
        print(landing_param_map.keys())
        self.assertTrue(args.__dict__.keys() == landing_param_map.keys())

    def test_main_landing(self):
        """Test landing onboarding."""
        landing_param_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        landing_param_map["onboard_layer"] = "landing"
        list = ["dummy_test"]
        for key in landing_param_map:
            list.append(f"--{key}={landing_param_map[key]}")
        sys.argv = list
        __main__.main()
        landing_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{landing_param_map['database']}.{landing_param_map['landing_dataflowspec_table']}")
        )
        self.assertEqual(landing_dataflowSpec_df.count(), 3)

    def test_main_refinery(self):
        """Test refinery onboarding."""
        refinery_param_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        refinery_param_map["onboard_layer"] = "refinery"
        list = ["dummy_test"]
        for key in refinery_param_map:
            list.append(f"--{key}={refinery_param_map[key]}")
        sys.argv = list
        __main__.main()
        refinery_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{refinery_param_map['database']}.{refinery_param_map['refinery_dataflowspec_table']}")
        )
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_main_landing_refinery(self):
        """Test landing and refinery onboarding."""
        param_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        param_map["onboard_layer"] = "landing_refinery"
        list = ["dummy_test"]
        for key in param_map:
            list.append(f"--{key}={param_map[key]}")
        sys.argv = list
        __main__.main()
        landing_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{param_map['database']}.{param_map['landing_dataflowspec_table']}")
        )
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        refinery_dataflowSpec_df = (self.spark.read.format("delta") .table(
            f"{param_map['database']}.{param_map['refinery_dataflowspec_table']}")
        )
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)

    def test_main_negative(self):
        """Test landing onboarding."""
        landing_param_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        list = ["dummy_test"]
        for key in landing_param_map:
            list.append(f"--{key}={landing_param_map[key]}")
        sys.argv = list
        with self.assertRaises(Exception):
            __main__.main()

    def test_main_landing_uc(self):
        """Test landing onboarding."""
        landing_param_map = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        landing_param_map["onboard_layer"] = "landing"
        list = ["dummy_test"]
        for key in landing_param_map:
            list.append(f"--{key}={landing_param_map[key]}")
        sys.argv = list
        __main__.main()
        landing_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{landing_param_map['database']}.{landing_param_map['landing_dataflowspec_table']}")
        )
        self.assertEqual(landing_dataflowSpec_df.count(), 3)

    def test_main_layer_missing(self):
        """Test landing and refinery onboarding."""
        param_map = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        list = ["dummy_test"]
        for key in param_map:
            list.append(f"--{key}={param_map[key]}")
        sys.argv = list
        with self.assertRaises(Exception):
            __main__.main()

    def test_main_landing_refinery_uc(self):
        """Test landing and refinery onboarding for uc."""
        OnboardDataflowspec.return_value = None
        spark_mock = MagicMock("SparkSession")
        spark.builder.appName("DLT-META_Onboarding_Task").getOrCreate().return_value = spark_mock
        param_map = copy.deepcopy(self.onboarding_landing_refinery_params_uc_map)
        param_map["onboard_layer"] = "landing_refinery"
        list = ["dummy_test"]
        for key in param_map:
            list.append(f"--{key}={param_map[key]}")
        sys.argv = list
        __main__.main()
        landing_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{param_map['database']}.{param_map['landing_dataflowspec_table']}")
        )
        self.assertEqual(landing_dataflowSpec_df.count(), 3)
        refinery_dataflowSpec_df = (self.spark.read.format("delta") .table(
            f"{param_map['database']}.{param_map['refinery_dataflowspec_table']}")
        )
        self.assertEqual(refinery_dataflowSpec_df.count(), 3)
        del param_map['onboard_layer']
        del param_map['uc_enabled']
        del param_map['landing_dataflowspec_path']
        del param_map['refinery_dataflowspec_path']
        OnboardDataflowspec.called_once_with(spark_mock, param_map, uc_enabled=True)

    def test_onboarding(self):
        mock_onboard_dataflowspec = OnboardDataflowspec
        mock_args = MagicMock()
        mock_args.onboard_layer = "landing_refinery"
        mock_args.uc_enabled = 'true'
        mock_args.__dict__ = {
            'onboard_layer': 'landing_refinery',
            'uc_enabled': 'true',
            'landing_dataflowspec_path': 'path/to/landing_dataflowspec',
            'refinery_dataflowspec_path': 'path/to/refinery_dataflowspec'
        }

        spark_mock = MagicMock("SparkSession")
        spark.builder.appName("DLT-META_Onboarding_Task").getOrCreate().return_value = spark_mock

        mock_onboard_obj = MagicMock()
        mock_onboard_dataflowspec.return_value = mock_onboard_obj

        # Act
        onboard_layer = mock_args.onboard_layer
        uc_enabled = True if mock_args.uc_enabled and mock_args.uc_enabled.lower() == "true" else False
        onboarding_args_dict = mock_args.__dict__
        del onboarding_args_dict['onboard_layer']
        del onboarding_args_dict['uc_enabled']
        if uc_enabled:
            if 'landing_dataflowspec_path' in onboarding_args_dict:
                del onboarding_args_dict['landing_dataflowspec_path']
            if 'refinery_dataflowspec_path' in onboarding_args_dict:
                del onboarding_args_dict['refinery_dataflowspec_path']
        onboard_obj = mock_onboard_dataflowspec(spark, onboarding_args_dict, uc_enabled=uc_enabled)

        if onboard_layer.lower() == "landing_refinery":
            onboard_obj.onboard_dataflow_specs()
        elif onboard_layer.lower() == "landing":
            onboard_obj.onboard_landing_dataflow_spec()
        elif onboard_layer.lower() == "refinery":
            onboard_obj.onboard_refinery_dataflow_spec()
        else:
            raise Exception("onboard_layer argument missing in commandline")

        # Assert
        mock_onboard_dataflowspec.assert_called_once_with(spark, {}, uc_enabled=True)
        mock_onboard_obj.onboard_dataflow_specs.assert_called_once()
