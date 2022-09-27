# Databricks notebook source
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner
from kedro.config import ConfigLoader
from kedro.framework.project import settings
import os
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# Prepare a data catalog
project_path = os.path.abspath(os.curdir)
conf_path = str(project_path + "/" + settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path, env="local")
conf_catalog = conf_loader.get("catalog*", "catalog*/**")

data_catalog = DataCatalog().from_config(conf_catalog)
data_catalog.load("amex_data")

# Prepare first node
def select_columns(data: DeltaTable) -> DataFrame:
    return data.toDF().select("customer_ID", "S_2", "D_39")


return_greeting_node = node(select_columns, inputs="amex_data", outputs="selected_data")

# Assemble nodes into a pipeline
greeting_pipeline = pipeline([return_greeting_node])

# Create a runner to run the pipeline
runner = SequentialRunner()

# Run the pipeline
print(runner.run(greeting_pipeline, data_catalog))

