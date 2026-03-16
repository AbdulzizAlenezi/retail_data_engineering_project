# Databricks notebook source
# MAGIC %run /Workspace/Users/pdmentor.help@gmail.com/retail_de_project_scd/KPI_transform/monthly_kpi_calculation

# COMMAND ----------

monthly_kpi.write.mode("append").format("delta").saveAsTable("retail_de_project.default.monthly_kpi")