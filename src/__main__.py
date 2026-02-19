"""Main entry point of the Python Wheel."""
import logging
import argparse
from src.onboard_dataflowspec import OnboardDataflowspec
from pyspark.sql import SparkSession

logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)

arguments = [
    "--onboard_layer",
    "--onboarding_file_path",
    "--database",
    "--env",
    "--landing_dataflowspec_table",
    "--landing_dataflowspec_path",
    "--refinery_dataflowspec_table",
    "--refinery_dataflowspec_path",
    "--treasury_dataflowspec_table",
    "--treasury_dataflowspec_path",
    "--import_author",
    "--version",
    "--overwrite",
    "--uc_enabled",
]


def parse_args():
    """Parse command line."""
    parser = argparse.ArgumentParser()
    for argument in arguments:
        parser.add_argument(argument)
    args = parser.parse_args()
    logger.info(f"Input arguments dict: {args}")
    return args


def main():
    """Whl file entry point."""
    args = parse_args()
    onboard_dataflowspecs(args)


def onboard_dataflowspecs(args):
    onboard_layer = args.onboard_layer
    uc_enabled = True if args.uc_enabled and args.uc_enabled.lower() == "true" else False
    onboarding_args_dict = args.__dict__
    del onboarding_args_dict['onboard_layer']
    del onboarding_args_dict['uc_enabled']
    if uc_enabled:
        if 'landing_dataflowspec_path' in onboarding_args_dict:
            del onboarding_args_dict['landing_dataflowspec_path']
        if 'refinery_dataflowspec_path' in onboarding_args_dict:
            del onboarding_args_dict['refinery_dataflowspec_path']
        if 'treasury_dataflowspec_path' in onboarding_args_dict:
            del onboarding_args_dict['treasury_dataflowspec_path']
    spark = SparkSession.builder.appName("DLT-META_Onboarding_Task").getOrCreate()
    onboard_obj = OnboardDataflowspec(spark, onboarding_args_dict, uc_enabled=uc_enabled)

    if onboard_layer.lower() in ["landing_refinery_treasury", "landing_refinery"]:
        onboard_obj.onboard_dataflow_specs()
    elif onboard_layer.lower() == "landing":
        onboard_obj.onboard_landing_dataflow_spec()
    elif onboard_layer.lower() == "refinery":
        onboard_obj.onboard_refinery_dataflow_spec()
    elif onboard_layer.lower() == "treasury":
        # Treasury onboarding is not yet fully implemented
        # For now, this will raise an error
        raise NotImplementedError("Treasury-only onboarding is not yet implemented. Use 'landing_refinery_treasury' to onboard all layers including treasury.")
    else:
        raise Exception("onboard_layer must be one of: landing, refinery, landing_refinery, treasury, landing_refinery_treasury")


if __name__ == "__main__":
    main()
