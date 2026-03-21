# Databricks notebook source
# DBTITLE 1,Install DLT-META Wheel from Configuration
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Run DLT-META Pipeline
layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
