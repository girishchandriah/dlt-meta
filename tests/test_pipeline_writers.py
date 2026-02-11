import copy
from src.dataflow_pipeline import DataflowPipeline
from src.dataflow_spec import LandingDataflowSpec, DataflowSpecUtils
from src.onboard_dataflowspec import OnboardDataflowspec
from unittest.mock import MagicMock, patch
from src.pipeline_writers import AppendFlowWriter, DLTSinkWriter
from src.dataflow_spec import DLTSink
from tests.utils import DLTFrameworkTestCase


class TestAppendFlowWriter(DLTFrameworkTestCase):

    @patch('src.pipeline_writers.dlt.read_stream')
    def test_read_af_view(self, mock_read_stream):
        appendflow_writer = AppendFlowWriter(
            self.spark, MagicMock(), "test_target", "test_schema",
            {"property": "value"}, ["col1"], ["col2"]
        )
        appendflow_writer.read_af_view()
        mock_read_stream.assert_called_once()

    @patch('src.pipeline_writers.dlt.create_streaming_table')
    @patch('src.pipeline_writers.dlt.append_flow')
    def test_write_flow(self, mock_append_flow, mock_create_streaming_table):
        appendflow_writer = AppendFlowWriter(
            self.spark, MagicMock(), "test_target", "test_schema",
            {"property": "value"}, ["col1"], ["col2"]
        )
        appendflow_writer.write_flow()
        mock_create_streaming_table.assert_called_once()
        mock_append_flow.assert_called_once()


class TestDLTSinkWriter(DLTFrameworkTestCase):

    @patch('src.pipeline_writers.dlt.read_stream')
    def test_read_input_view(self, mock_read_stream):
        dlt_sink = DLTSink(
            name="test_sink",
            format="kafka",
            options={},
            select_exp=["col1", "col2"],
            where_clause="col1 > 0"
        )
        sink_writer = DLTSinkWriter(dlt_sink, "test_view")
        sink_writer.read_input_view()
        mock_read_stream.assert_called_once_with("test_view")

    @patch('src.pipeline_writers.dlt.create_sink')
    @patch('src.pipeline_writers.dlt.append_flow')
    def test_write_to_sink(self, mock_append_flow, mock_create_sink):
        dlt_sink = DLTSink(
            name="test_sink",
            format="kafka",
            options={},
            select_exp=["col1", "col2"],
            where_clause="col1 > 0"
        )
        sink_writer = DLTSinkWriter(dlt_sink, "test_view")
        sink_writer.write_to_sink()
        mock_create_sink.assert_called_once_with(name='test_sink', format='kafka', options={})
        mock_append_flow.assert_called_once()

    @patch('dlt.create_sink', new_callable=MagicMock)
    @patch('dlt.append_flow', new_callable=MagicMock)
    @patch('dlt.table', new_callable=MagicMock)
    def test_dataflowpipeline_landing_sink_write(self, mock_dlt_table, mock_append_flow, mock_create_sink):
        local_params = copy.deepcopy(self.onboarding_landing_refinery_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["landing_dataflowspec_table"] = "landing_dataflowspec_sink"
        del local_params["refinery_dataflowspec_table"]
        del local_params["refinery_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_landing_dataflow_spec()
        landing_dataflowSpec_df = self.spark.read.table(
            f"{self.onboarding_landing_refinery_params_map['database']}.landing_dataflowspec_sink")
        landing_dataflowSpec_df.show(truncate=False)
        self.assertEqual(landing_dataflowSpec_df.count(), 1)
        landing_dataflow_spec = DataflowSpecUtils._get_dataflow_spec(
            spark=self.spark,
            dataflow_spec_df=landing_dataflowSpec_df,
            layer="landing"
        ).collect()[0]
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{landing_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, LandingDataflowSpec(**landing_dataflow_spec.asDict()), view_name, None)
        pipeline.write()
        assert mock_create_sink.called_with(
            name="sink",
            target="sink",
            comment="sink dlt table sink"
        )
        assert mock_append_flow.called_with(
            name="sink",
            target="sink"
        )
        assert mock_dlt_table.called_with(
            pipeline.write_to_delta,
            name="sink",
            partition_cols=[],
            table_properties={},
            path=None,
            comment="sink dlt table sink"
        )
