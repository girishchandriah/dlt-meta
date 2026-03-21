#!/usr/bin/env python3
"""
Example script to onboard Kafka dataflow with Schema Registry using dlt-meta CLI.

Usage:
    python onboard_kafka_example.py [onboarding_file.json]

Example:
    python onboard_kafka_example.py kafka_protobuf_source_example.json
"""
import os
import sys
from databricks.sdk import WorkspaceClient
from src.cli import DLTMeta, OnboardCommand

def main():
    # Get onboarding file from command line or use default
    if len(sys.argv) > 1:
        onboarding_file = sys.argv[1]
    else:
        onboarding_file = "kafka_protobuf_source_example.json"

    # Make path absolute if relative
    if not os.path.isabs(onboarding_file):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        onboarding_file = os.path.join(script_dir, onboarding_file)

    # Verify file exists
    if not os.path.exists(onboarding_file):
        print(f"❌ Error: Onboarding file not found: {onboarding_file}")
        sys.exit(1)

    print(f"📋 Onboarding file: {onboarding_file}")

    # Configuration
    ONBOARDING_DIR = os.path.dirname(onboarding_file)
    ENV = "nonprod"  # Change to "preprod" or "prod" as needed
    IMPORT_AUTHOR = os.getenv("USER", "data-platform@example.com")

    # Initialize Databricks workspace client
    print("🔧 Initializing Databricks workspace client...")
    ws = WorkspaceClient()
    dltmeta = DLTMeta(ws)

    # Create onboarding command
    print("📦 Creating onboarding command...")
    onboard_cmd = OnboardCommand(
        # File paths
        onboarding_file_path=onboarding_file,
        onboarding_files_dir_path=ONBOARDING_DIR,

        # Layer configuration
        onboard_layer="landing",  # Options: "landing", "refinery", "treasury", "landing_refinery"

        # Environment
        env=ENV,
        import_author=IMPORT_AUTHOR,
        version="1.0",

        # DLT-META schema
        dlt_meta_schema=f"dlt_meta_{ENV}",

        # Unity Catalog configuration (recommended)
        uc_enabled=True,
        uc_catalog_name=f"dataservices_{ENV}",

        # Dataflowspec tables
        landing_dataflowspec_table="landing_dataflowspec",
        refinery_dataflowspec_table="refinery_dataflowspec",
        treasury_dataflowspec_table="treasury_dataflowspec",

        # Settings
        overwrite=False,  # Merge with existing specs (recommended)
        serverless=True  # Use serverless compute
    )

    # Execute onboarding
    print("\n" + "=" * 60)
    print("🚀 Starting Kafka dataflow onboarding...")
    print("=" * 60)
    print(f"  Environment: {ENV}")
    print(f"  Catalog: dataservices_{ENV}")
    print(f"  Schema: dlt_meta_{ENV}")
    print(f"  Author: {IMPORT_AUTHOR}")
    print("=" * 60 + "\n")

    try:
        dltmeta.onboard(onboard_cmd)
        print("\n✅ Onboarding job submitted successfully!")
        print("\nCheck the Databricks workspace for job status:")
        print(f"  {ws.config.host}/jobs")
    except Exception as e:
        print(f"\n❌ Onboarding failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
