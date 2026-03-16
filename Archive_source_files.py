# Databricks notebook source
from datetime import datetime 
run_date = datetime.now()

# COMMAND ----------

input_file = 'retail_data_500.csv'
source = f'/Volumes/retail_de_project/default/bronze_ingest/{input_file}'
target = f'/Volumes/retail_de_project/default/bronze_ingest/archive/{input_file}_{run_date}/'
try:
    dbutils.fs.mv(source,target)
    print(f"File moved successfully.")
except Exception as e:
    print(f"ERROR: Failed to move file")
    print(f"Caused By: {str(e)}")

# COMMAND ----------

