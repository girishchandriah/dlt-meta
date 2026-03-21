clean:
	rm -fr build .databricks dlt_meta_cds.egg-info

dev:
	python3 -m venv .databricks
	.databricks/bin/python -m pip install -e .