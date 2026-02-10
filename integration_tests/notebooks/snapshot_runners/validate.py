# Databricks notebook source
import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = eval(dbutils.widgets.get("uc_enabled"))
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
output_file_path = dbutils.widgets.get("output_file_path")
landing_schema = dbutils.widgets.get("landing_schema")
refinery_schema = dbutils.widgets.get("refinery_schema")
log_list = []

# Assumption is that to get to this notebook Landing and Refinery completed successfully
log_list.append("Completed Landing Eventhub Lakeflow Declarative Pipeline.")

UC_TABLES = {
    f"{uc_catalog_name}.{landing_schema}.products": 20,
    f"{uc_catalog_name}.{landing_schema}.stores": 2,
    f"{uc_catalog_name}.{refinery_schema}.products": 20,
    f"{uc_catalog_name}.{refinery_schema}.stores": 2
}

log_list.append("Validating Lakeflow Declarative Pipeline for Eventhub Landing Table Counts...")
for table, counts in UC_TABLES.items():
    query = spark.sql(f"SELECT count(*) as cnt FROM {table}")
    cnt = query.collect()[0].cnt

    log_list.append(f"Validating Counts for Table {table}.")
    try:
        assert int(cnt) >= counts
        log_list.append(f"Expected >= {counts} Actual: {cnt}. Passed!")
    except AssertionError:
        log_list.append(f"Expected > {counts} Actual: {cnt}. Failed!")

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
