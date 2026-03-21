
import uuid
import traceback
from databricks.sdk.service import jobs, compute
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


class DLTMETATrefineryFanoutDemo(DLTMETARunner):
    """
    Represents the DLT-META refinery Fanout Demo.

    This class is responsible for running the DLT-META refinery Fanout Demo, which includes setting up metadata tables,
    creating clusters, launching workflows, and more.

    Attributes:
    - args: The command-line arguments passed to the script.
    - ws: The Databricks workspace object.
    - base_dir: The base directory of the project.

    Methods:
    - run: Runs the DLT-META refinery Fanout Demo.
    - init_runner_conf: Initializes the runner configuration for running integration tests.
    - launch_workflow: Launches the workflow for the DLT-META refinery Fanout Demo.
    - create_sfo_workflow_spec: Creates the workflow for the DLT-META refinery Fanout Demo by defining the tasks
                                and their dependencies.
    """

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META refinery Fanout Demo.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.init_dltmeta_runner_conf(runner_conf)
            self.create_landing_refinery_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # finally:
        #     self.clean_up(runner_conf)

    def init_runner_conf(self) -> DLTMetaRunnerConf:
        """
        Initialize the runner configuration for running integration tests.

        Returns:
        -------
        DLTMetaRunnerConf
            The initialized runner configuration.
        """
        run_id = uuid.uuid4().hex
        runner_conf = DLTMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            landing_schema=f"dlt_meta_landing_demo_{run_id}",
            refinery_schema=f"dlt_meta_refinery_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_fout_demo/{run_id}",
            runners_full_local_path="demo/notebooks/refinery_fanout_runners",
            source="cloudfiles",
            # node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            # dbr_version=self.args.__dict__['dbr_version'],
            cloudfiles_template="demo/conf/onboarding_cars.template",
            onboarding_fanout_templates="demo/conf/onboarding_fanout_cars.template",
            onboarding_file_path="demo/conf/onboarding_cars.json",
            onboarding_fanout_file_path="demo/conf/onboarding_fanout_cars.json",
            env="demo"
        )
        runner_conf.uc_catalog_name = self.args['uc_catalog_name']
        runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_dlt_meta_fout_demo_{run_id}"
        return runner_conf

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_sfo_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)

    def create_sfo_workflow_spec(self, runner_conf: DLTMetaRunnerConf):
        """
        Creates the workflow for the DLT-META refinery Fanout Demo by defining the tasks and their dependencies.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.

        Returns:
        - created_job: The created job object.
        """
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]
        return self.ws.jobs.create(
            name=f"dlt-refinery-fanout-demo-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=[
                jobs.Task(
                    task_key="onboarding_job",
                    description="Sets up metadata tables for DLT-META",
                    environment_key="dl_meta_int_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta_cds",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "landing_refinery",
                            "database": f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}",
                            "onboarding_file_path":
                            f"{runner_conf.uc_volume_path}/{runner_conf.onboarding_file_path}",
                            "refinery_dataflowspec_table": "refinery_dataflowspec_cdc",
                            "landing_dataflowspec_table": "landing_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True"
                        },
                    ),
                ),
                jobs.Task(
                    task_key="onboard_refineryfanout_job",
                    description="Sets up metadata tables for DLT-META",
                    depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                    environment_key="dl_meta_int_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta_cds",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "refinery",
                            "database": f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}",
                            "onboarding_file_path":
                            f"{runner_conf.uc_volume_path}/{runner_conf.onboarding_fanout_file_path}",
                            "refinery_dataflowspec_table": "refinery_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "overwrite": "False",
                            "env": runner_conf.env,
                            "uc_enabled": "True"
                        },
                    ),
                ),
                jobs.Task(
                    task_key="landing_dlt",
                    depends_on=[jobs.TaskDependency(task_key="onboard_refineryfanout_job")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.landing_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="refinery_dlt",
                    depends_on=[jobs.TaskDependency(task_key="landing_dlt")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.refinery_pipeline_id
                    )
                )
            ]
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args['profile'])
    dltmeta_afam_demo_runner = DLTMETATrefineryFanoutDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
