---
title: "DLT-META Manual"
date: 2021-08-04T14:25:26-04:00
weight: 8
draft: false
---

## OnboardJob 
### Option#1: using Databricks Python whl job
1. Go to your Databricks landing page and do one of the following:

2. In the sidebar, click Jobs Icon Workflows and click Create Job Button.

3. In the sidebar, click New Icon New and select Job from the menu.

4. In the task dialog box that appears on the Tasks tab, replace Add a name for your job… with your job name, for example, Python wheel example.

5. In Task name, enter a name for the task, for example, ```dlt_meta_onboarding_pythonwheel_task```.

6. In Type, select Python wheel.

7. In Package name, enter ```dlt_meta```.

8. In Entry point, enter ``run``. 

9. Click Add under Dependent Libraries. In the Add dependent library dialog, under Library Type, click PyPI. Enter Package = ```dlt-meta```

10. Click Add.

11. In Parameters, select keyword argument then select JSON. Past below json parameters with :
- Without Unity Cataglog
```json 
    {
        "onboard_layer": "landing_refinery",
        "database": "dlt_demo",
        "onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
        "refinery_dataflowspec_table": "refinery_dataflowspec_table",
        "refinery_dataflowspec_path": "dbfs:/onboarding_tables_cdc/refinery",
        "landing_dataflowspec_table": "landing_dataflowspec_table",
        "import_author": "Ravi",
        "version": "v1",
        "landing_dataflowspec_path": "dbfs:/onboarding_tables_cdc/landing",
        "onboard_layer": "landing_refinery",
        "uc_enabled": "False",
        "overwrite": "True",
        "env": "dev"
    } 
```
- with Unity catalog
```json
    {
        "onboard_layer": "landing_refinery",
        "database": "uc_name.dlt_demo",
        "onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
        "refinery_dataflowspec_table": "refinery_dataflowspec_table",
        "landing_dataflowspec_table": "landing_dataflowspec_table",
        "import_author": "Ravi",
        "version": "v1",
        "uc_enabled": "True",
        "overwrite": "True",
        "env": "dev"
    } 
```
- Note in database field you need to provide catalog name then schema name as `<<uc_name>>.<<schema>> `

Alternatly you can enter keyword arguments, click + Add and enter a key and value. Click + Add again to enter more arguments. 

12. Click Save task.

13. Run now

14. Make sure job runs successfully. Verify metadata in your dataflow spec tables entered in step: 11 e.g ```dlt_demo.landing_dataflowspec_table```, ```dlt_demo.refinery_dataflowspec_table```

**Note:** The framework also supports legacy parameter names (bronze_dataflowspec_table, silver_dataflowspec_table) for backward compatibility.

### Option#2: Databricks Notebook 
1. Copy below code to databricks notebook cells
```%pip install dlt-meta```
- without unity catalog
```python 
onboarding_params_map = {
		"database": "dlt_demo",
		"onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
		"landing_dataflowspec_table": "landing_dataflowspec_table",
		"landing_dataflowspec_path": "dbfs:/onboarding_tables_cdc/landing",
		"refinery_dataflowspec_table": "refinery_dataflowspec_table",
		"refinery_dataflowspec_path": "dbfs:/onboarding_tables_cdc/refinery",
		"overwrite": "True",
		"env": "dev",
		"version": "v1",
		"import_author": "Ravi"
		}

from src.onboard_dataflowspec import OnboardDataflowspec
OnboardDataflowspec(spark, onboarding_params_map).onboard_dataflow_specs()
```
- with unity catalog
```python
onboarding_params_map = {
		"database": "uc_name.dlt_demo",
		"onboarding_file_path": "dbfs:/dlt-meta/conf/onboarding.json",
		"landing_dataflowspec_table": "landing_dataflowspec_table",
		"refinery_dataflowspec_table": "refinery_dataflowspec_table",
		"overwrite": "True",
		"env": "dev",
		"version": "v1",
		"import_author": "Ravi"
		}

from src.onboard_dataflowspec import OnboardDataflowspec
OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()
```
2. Specify your onboarding config params in above ```onboarding_params_map```

3. Run notebook cells

## Lakeflow Declarative Pipeline: 

### Lakeflow Declarative Pipelines launch notebook

1. Go to your Databricks landing page and select Create a notebook, or click New Icon New in the sidebar and select Notebook. The Create Notebook dialog appears.

2. In the Create Notebook dialogue, give your notebook a name e.g ```dlt_meta_pipeline``` and select Python from the Default Language dropdown menu. You can leave Cluster set to the default value. The Lakeflow Declarative Pipelines runtime creates a cluster before it runs your pipeline.

3. Click Create.

4. You can add the [example dlt pipeline](https://github.com/databrickslabs/dlt-meta/blob/main/examples/dlt_meta_pipeline.ipynb) code or import iPython notebook as is.
    ```
        %pip install dlt-meta
    ```
    ```
        layer = spark.conf.get("layer", None)
        from src.dataflow_pipeline import DataflowPipeline
        DataflowPipeline.invoke_dlt_pipeline(spark, layer)
    ```
### Create Bronze Lakeflow Declarative Pipeline

1. Click Jobs Icon Workflows in the sidebar, click the Lakeflow Declarative Pipelines tab, and click Create Pipeline.

2. Give the pipeline a name e.g. DLT_META_LANDING and click File Picker Icon to select a notebook ```dlt_meta_pipeline``` created in step: ```Create a dlt launch notebook```.

3. Optionally enter a storage location for output data from the pipeline. The system uses a default location if you leave Storage location empty.

4. Select Triggered for Pipeline Mode.

5. Enter Configuration parameters e.g.
    ```
    "layer": "landing",
    "landing.dataflowspecTable": "dataflowspec table name",
    "landing.group": "enter group name from metadata e.g. G1",
    ```

6. Enter target schema where you want your landing tables to be created

7. Click Create.

8. Start pipeline: click the Start button in top panel. The system returns a message confirming that your pipeline is starting

### Create Refinery Lakeflow Declarative Pipelines

1. Click Jobs Icon Workflows in the sidebar, click the Lakeflow Declarative Pipelines tab, and click Create Pipeline.

2. Give the pipeline a name e.g. DLT_META_REFINERY and click File Picker Icon to select a notebook ```dlt_meta_pipeline``` created in step: ```Create a dlt launch notebook```.

3. Optionally enter a storage location for output data from the pipeline. The system uses a default location if you leave Storage location empty.

4. Select Triggered for Pipeline Mode.

5. Enter Configuration parameters e.g.
    ```
    "layer": "refinery",
    "refinery.dataflowspecTable": "dataflowspec table name",
    "refinery.group": "enter group name from metadata e.g. G1",
    ```

6. Enter target schema where you want your refinery tables to be created

### Create Treasury Lakeflow Declarative Pipelines (Gold Layer)

Follow the same steps as above for the Treasury layer:

1. Click Jobs Icon Workflows in the sidebar, click the Lakeflow Declarative Pipelines tab, and click Create Pipeline.

2. Give the pipeline a name e.g. DLT_META_TREASURY and select the ```dlt_meta_pipeline``` notebook.

3. Enter Configuration parameters e.g.
    ```
    "layer": "treasury",
    "treasury.dataflowspecTable": "dataflowspec table name",
    "treasury.group": "enter group name from metadata e.g. G1",
    ```

**Alternative: Combined Layer Pipelines**

You can also create pipelines that span multiple layers:
- `layer=landing_refinery` - Combines landing and refinery layers
- `layer=refinery_treasury` - Combines refinery and treasury layers
- `layer=landing_refinery_treasury` - Combines all three layers

7. Click Create.

8. Start pipeline: click the Start button on in top panel. The system returns a message confirming that your pipeline is starting 
