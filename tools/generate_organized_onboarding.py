#!/usr/bin/env python3
"""
Generate Organized Onboarding Configurations for Large-Scale ETLs

This script helps you generate DLT-META onboarding configurations at scale,
with proper organization by domain, source system, and table.

Usage:
    python tools/generate_organized_onboarding.py \
        --metadata etl_metadata.csv \
        --output conf/onboarding/ \
        --domain finance

CSV Format (etl_metadata.csv):
    domain,source_system,source_table,source_path,priority,owner
    finance,sap,journal_entries,/path/to/data,critical,john@example.com
    sales,salesforce,opportunities,/path/to/data,standard,jane@example.com
"""

import argparse
import json
import os
import pandas as pd
from pathlib import Path
from typing import Dict, List


class OnboardingConfigGenerator:
    """Generate organized onboarding configurations from metadata."""

    def __init__(self, metadata_file: str, output_dir: str):
        self.metadata_file = metadata_file
        self.output_dir = Path(output_dir)
        self.metadata_df = None

        # ID ranges by domain
        self.id_ranges = {
            'finance': (1000, 1999),
            'sales': (2000, 2999),
            'marketing': (3000, 3999),
            'hr': (4000, 4999),
            'supply_chain': (5000, 5999),
            'operations': (6000, 6999),
            'customer_service': (7000, 7999),
            'analytics': (8000, 8999),
        }

        # Track used IDs per domain
        self.next_id = {domain: start for domain, (start, _) in self.id_ranges.items()}

    def load_metadata(self):
        """Load ETL metadata from CSV file."""
        print(f"Loading metadata from {self.metadata_file}...")
        self.metadata_df = pd.read_csv(self.metadata_file)

        required_columns = ['domain', 'source_system', 'source_table', 'source_path']
        missing_columns = [col for col in required_columns if col not in self.metadata_df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        print(f"Loaded {len(self.metadata_df)} ETL configurations")

        # Display summary
        print("\nSummary by Domain:")
        print(self.metadata_df.groupby('domain').size())

    def get_next_id(self, domain: str) -> str:
        """Get next available ID for a domain."""
        if domain not in self.next_id:
            raise ValueError(f"Unknown domain: {domain}. Add to id_ranges.")

        current_id = self.next_id[domain]
        _, max_id = self.id_ranges[domain]

        if current_id > max_id:
            raise ValueError(f"Exceeded ID range for domain {domain}")

        self.next_id[domain] += 1
        return str(current_id)

    def generate_dataflow_group(self, domain: str, source_system: str) -> str:
        """Generate dataFlowGroup name."""
        # Clean and standardize names
        domain_clean = domain.lower().replace(' ', '_')
        source_clean = source_system.lower().replace(' ', '_')
        return f"{domain_clean}_{source_clean}"

    def generate_onboarding_config(self, row: pd.Series) -> Dict:
        """Generate onboarding configuration for a single table."""
        domain = row['domain']
        source_system = row['source_system']
        source_table = row['source_table']
        source_path = row['source_path']

        data_flow_id = self.get_next_id(domain)
        data_flow_group = self.generate_dataflow_group(domain, source_system)

        # Get optional fields with defaults
        source_format = row.get('source_format', 'cloudFiles')
        file_format = row.get('file_format', 'csv')
        catalog = row.get('catalog', 'privacy_nonprod')
        landing_schema = row.get('landing_schema', 'dltmeta_landing')
        refinery_schema = row.get('refinery_schema', 'dltmeta_refinery')

        # Build table name with prefix
        table_prefix = f"{domain.lower()}_{source_system.lower()}_"
        landing_table = f"{table_prefix}{source_table}"
        refinery_table = f"{table_prefix}{source_table}"

        config = {
            "data_flow_id": data_flow_id,
            "data_flow_group": data_flow_group,
            "source_system": source_system,
            "source_format": source_format,
            "source_details": {
                "source_database": row.get('source_database', source_system),
                "source_table": source_table,
                "source_path_prod": source_path
            },
            "landing_catalog_prod": catalog,
            "landing_database_prod": landing_schema,
            "landing_table": landing_table,
            "landing_table_comment": f"{source_table} landing table from {source_system}",
            "landing_reader_options": {
                "cloudFiles.format": file_format,
                "cloudFiles.rescuedDataColumn": "_rescued_data",
                "header": "true"
            },
            "refinery_catalog_prod": catalog,
            "refinery_database_prod": refinery_schema,
            "refinery_table": refinery_table,
            "refinery_table_comment": f"{source_table} refinery table from {source_system}"
        }

        # Add optional DQE path if specified
        if 'dqe_path' in row and pd.notna(row['dqe_path']):
            config["landing_data_quality_expectations_json_prod"] = row['dqe_path']

        # Add optional transformation path if specified
        if 'transformation_path' in row and pd.notna(row['transformation_path']):
            config["refinery_transformation_json_prod"] = row['transformation_path']

        # Add CDC configuration if specified
        if 'cdc_keys' in row and pd.notna(row['cdc_keys']):
            config["refinery_cdc_apply_changes"] = {
                "keys": row['cdc_keys'].split(','),
                "sequence_by": row.get('cdc_sequence_by', 'update_timestamp'),
                "scd_type": row.get('cdc_scd_type', '1')
            }

        return config

    def create_directory_structure(self):
        """Create organized directory structure."""
        domains = self.metadata_df['domain'].unique()

        for domain in domains:
            domain_clean = domain.lower().replace(' ', '_')

            # Create directories
            (self.output_dir / domain_clean).mkdir(parents=True, exist_ok=True)
            (self.output_dir.parent / 'dqe' / domain_clean).mkdir(parents=True, exist_ok=True)
            (self.output_dir.parent / 'transformations' / domain_clean).mkdir(parents=True, exist_ok=True)
            (self.output_dir.parent / 'schemas' / domain_clean).mkdir(parents=True, exist_ok=True)

        print(f"\nCreated directory structure under {self.output_dir.parent}")

    def generate_all_configs(self, domain_filter: str = None):
        """Generate all onboarding configurations."""
        # Filter by domain if specified
        df = self.metadata_df
        if domain_filter:
            df = df[df['domain'] == domain_filter]
            print(f"\nFiltering to domain: {domain_filter}")

        if len(df) == 0:
            print("No records to process!")
            return

        print(f"\nGenerating {len(df)} onboarding configurations...")

        for idx, row in df.iterrows():
            try:
                # Generate config
                config = self.generate_onboarding_config(row)

                # Determine output file path
                domain_clean = row['domain'].lower().replace(' ', '_')
                source_table_clean = row['source_table'].lower().replace(' ', '_')
                output_file = self.output_dir / domain_clean / f"{source_table_clean}.json"

                # Write config
                with open(output_file, 'w') as f:
                    json.dump([config], f, indent=2)

                print(f"✓ Generated: {output_file}")

            except Exception as e:
                print(f"✗ Error processing {row['source_table']}: {e}")

    def generate_summary_report(self):
        """Generate summary report of generated configs."""
        report_path = self.output_dir / "onboarding_summary.txt"

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("ONBOARDING CONFIGURATION SUMMARY\n")
            f.write("=" * 80 + "\n\n")

            # Summary by domain
            f.write("Tables by Domain:\n")
            f.write("-" * 40 + "\n")
            summary = self.metadata_df.groupby('domain').agg({
                'source_table': 'count',
                'source_system': lambda x: ', '.join(x.unique())
            })
            f.write(summary.to_string() + "\n\n")

            # Summary by group
            f.write("Tables by DataFlowGroup:\n")
            f.write("-" * 40 + "\n")
            self.metadata_df['data_flow_group'] = self.metadata_df.apply(
                lambda row: self.generate_dataflow_group(row['domain'], row['source_system']),
                axis=1
            )
            group_summary = self.metadata_df.groupby('data_flow_group')['source_table'].count()
            f.write(group_summary.to_string() + "\n\n")

            # ID ranges used
            f.write("ID Ranges Used:\n")
            f.write("-" * 40 + "\n")
            for domain, next_id in self.next_id.items():
                start, end = self.id_ranges[domain]
                if next_id > start:
                    f.write(f"{domain}: {start} - {next_id - 1} (used {next_id - start} IDs)\n")

        print(f"\nSummary report written to: {report_path}")

    def generate_pipeline_configs(self):
        """Generate recommended pipeline configurations."""
        pipelines_dir = self.output_dir.parent.parent / 'pipelines'
        pipelines_dir.mkdir(parents=True, exist_ok=True)

        # Group by dataFlowGroup
        self.metadata_df['data_flow_group'] = self.metadata_df.apply(
            lambda row: self.generate_dataflow_group(row['domain'], row['source_system']),
            axis=1
        )

        groups = self.metadata_df['data_flow_group'].unique()

        for group in groups:
            pipeline_config = {
                "name": f"{group}_pipeline",
                "configuration": {
                    "layer": "landing_refinery",
                    "landing.group": group,
                    "refinery.group": group,
                    "landing.dataflowspecTable": "privacy_nonprod.dlt_meta_dataflowspecs.landing_dataflowspec",
                    "refinery.dataflowspecTable": "privacy_nonprod.dlt_meta_dataflowspecs.refinery_dataflowspec",
                    "dlt_meta_whl": "/Volumes/privacy_nonprod/dlt_meta_dataflowspecs/dlt_meta_files/wheels/dlt_meta_cds-0.0.10-py3-none-any.whl"
                },
                "catalog": "privacy_nonprod",
                "serverless": True,
                "libraries": [
                    {
                        "notebook": {
                            "path": "/Users/your_user/dlt-meta/init_dlt_meta_pipeline.py"
                        }
                    }
                ]
            }

            output_file = pipelines_dir / f"{group}_pipeline.json"
            with open(output_file, 'w') as f:
                json.dump(pipeline_config, f, indent=2)

            print(f"✓ Generated pipeline config: {output_file}")


def create_sample_metadata_csv(output_path: str):
    """Create a sample metadata CSV file."""
    sample_data = {
        'domain': ['finance', 'finance', 'sales', 'sales', 'marketing'],
        'source_system': ['sap', 'sap', 'salesforce', 'salesforce', 'adobe'],
        'source_table': ['journal_entries', 'accounts', 'opportunities', 'contacts', 'campaigns'],
        'source_path': [
            '/Volumes/privacy_nonprod/dlt_meta_dataflowspecs/dlt_meta_files/finance/sap/journal_entries',
            '/Volumes/privacy_nonprod/dlt_meta_dataflowspecs/dlt_meta_files/finance/sap/accounts',
            '/Volumes/privacy_nonprod/dlt_meta_dataflowspecs/dlt_meta_files/sales/salesforce/opportunities',
            '/Volumes/privacy_nonprod/dlt_meta_dataflowspecs/dlt_meta_files/sales/salesforce/contacts',
            '/Volumes/privacy_nonprod/dlt_meta_dataflowspecs/dlt_meta_files/marketing/adobe/campaigns'
        ],
        'source_format': ['cloudFiles', 'cloudFiles', 'cloudFiles', 'cloudFiles', 'cloudFiles'],
        'file_format': ['csv', 'csv', 'csv', 'csv', 'json'],
        'catalog': ['privacy_nonprod'] * 5,
        'landing_schema': ['dltmeta_landing'] * 5,
        'refinery_schema': ['dltmeta_refinery'] * 5,
        'priority': ['critical', 'standard', 'critical', 'standard', 'standard'],
        'owner': ['john@example.com', 'john@example.com', 'jane@example.com', 'jane@example.com', 'bob@example.com']
    }

    df = pd.DataFrame(sample_data)
    df.to_csv(output_path, index=False)
    print(f"Created sample metadata file: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate organized onboarding configurations for DLT-META',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--metadata',
        required=True,
        help='Path to CSV file containing ETL metadata'
    )
    parser.add_argument(
        '--output',
        default='conf/onboarding/',
        help='Output directory for onboarding configs (default: conf/onboarding/)'
    )
    parser.add_argument(
        '--domain',
        help='Filter to specific domain (optional)'
    )
    parser.add_argument(
        '--create-sample',
        action='store_true',
        help='Create a sample metadata CSV file'
    )
    parser.add_argument(
        '--generate-pipelines',
        action='store_true',
        help='Also generate pipeline configuration files'
    )

    args = parser.parse_args()

    # Create sample metadata if requested
    if args.create_sample:
        create_sample_metadata_csv(args.metadata)
        print("\nEdit the sample CSV file and run again without --create-sample")
        return

    # Generate configurations
    generator = OnboardingConfigGenerator(args.metadata, args.output)

    try:
        generator.load_metadata()
        generator.create_directory_structure()
        generator.generate_all_configs(args.domain)
        generator.generate_summary_report()

        if args.generate_pipelines:
            generator.generate_pipeline_configs()

        print("\n" + "=" * 80)
        print("SUCCESS! Onboarding configurations generated.")
        print("=" * 80)
        print("\nNext steps:")
        print("1. Review generated configurations in:", args.output)
        print("2. Run onboarding:")
        print(f"   python src/cli.py onboard --onboarding_file {args.output}")
        print("3. Deploy pipelines")
        print("4. Start pipeline updates")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == '__main__':
    main()
