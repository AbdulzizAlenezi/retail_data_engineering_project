# Databricks notebook source
from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from datetime import datetime
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder.appName('sparkapplication').getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Users/pdmentor.help@gmail.com/retail_de_project_scd/Bronze_ingestion_notebooks/project_config

# COMMAND ----------


print(config['bronze_path'])

# COMMAND ----------

try:
    raw_df = spark.read.option("header", True).option("mode","DROPMALFORMED").csv(config["bronze_path"])
    print(f"Successfully read data from {config['bronze_path']}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
