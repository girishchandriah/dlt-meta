"""Generate DDL files from existing Delta tables.

This script helps generate DDL schema files from existing tables in your Databricks workspace.
Use this to create schema files for refinery and treasury tables.

Usage:
    In Databricks notebook or Python environment with Spark:

    from generate_ddl_from_table import generate_ddl_file

    # Generate refinery schema
    generate_ddl_file(
        spark,
        "dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_dl_dlt",
        "refinery_sales_ord_tran.ddl"
    )

    # Generate treasury schema
    generate_ddl_file(
        spark,
        "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt",
        "treasury_sales_ord_tran.ddl"
    )
"""


def generate_ddl_file(spark, table_name, output_file_path):
    """Generate DDL file from existing table.

    Args:
        spark: SparkSession
        table_name: Full table name (catalog.database.table or database.table)
        output_file_path: Path to write DDL file (can be local or DBFS path)

    Returns:
        str: DDL string

    Example:
        >>> ddl = generate_ddl_file(
        ...     spark,
        ...     "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt",
        ...     "/dbfs/cds/conf/onboarding/schemas/treasury_sales_ord_tran.ddl"
        ... )
        >>> print(ddl)
        sales_ord_id: string, event_id: string, ...
    """
    try:
        # Read table schema
        df = spark.table(table_name)
        schema = df.schema

        # Generate DDL string
        schema_fields = []
        for field in schema.fields:
            field_name = field.name
            field_type = field.dataType.simpleString()
            schema_fields.append(f"{field_name}: {field_type}")

        ddl_string = ", ".join(schema_fields)

        # Write to file
        if output_file_path.startswith("/dbfs/"):
            # DBFS path - write directly
            with open(output_file_path, 'w') as f:
                f.write(ddl_string)
            print(f"✓ Generated DDL file: {output_file_path}")
        elif output_file_path.startswith("dbfs:/"):
            # DBFS path with dbfs:/ prefix - use dbutils
            dbutils.fs.put(output_file_path, ddl_string, overwrite=True)
            print(f"✓ Generated DDL file: {output_file_path}")
        else:
            # Local path
            with open(output_file_path, 'w') as f:
                f.write(ddl_string)
            print(f"✓ Generated DDL file: {output_file_path}")

        print(f"Table: {table_name}")
        print(f"Fields: {len(schema.fields)}")
        print(f"DDL preview: {ddl_string[:200]}...")

        return ddl_string

    except Exception as e:
        print(f"✗ Error generating DDL for {table_name}: {e}")
        raise


def generate_ddl_string_only(spark, table_name):
    """Generate DDL string without writing to file.

    Args:
        spark: SparkSession
        table_name: Full table name

    Returns:
        str: DDL string
    """
    df = spark.table(table_name)
    schema = df.schema
    schema_fields = []
    for field in schema.fields:
        schema_fields.append(f"{field.name}: {field.dataType.simpleString()}")
    return ", ".join(schema_fields)


if __name__ == "__main__":
    # Example usage in Databricks notebook
    print("=" * 80)
    print("DDL Generator for DLT-Meta")
    print("=" * 80)
    print()
    print("Run this in your Databricks notebook:")
    print()
    print("# For refinery table")
    print('generate_ddl_file(')
    print('    spark,')
    print('    "dataservices_nonprod.refinery_nonprod.pfocusst_dsn_st_trans_dl_dlt",')
    print('    "/dbfs/cds/conf/onboarding/schemas/refinery_sales_ord_tran.ddl"')
    print(')')
    print()
    print("# For treasury table")
    print('generate_ddl_file(')
    print('    spark,')
    print('    "dataservices_nonprod.treasury_teradata_base_nonprod.pfocusdb_sales_ord_tran_dlt",')
    print('    "/dbfs/cds/conf/onboarding/schemas/treasury_sales_ord_tran.ddl"')
    print(')')
