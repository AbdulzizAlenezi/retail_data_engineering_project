# Databricks notebook source
# MAGIC %run /Workspace/Users/pdmentor.help@gmail.com/retail_de_project_scd/Gold_model_notebooks/gold_modelling

# COMMAND ----------

dim_customer.write.mode("overwrite").format("delta").saveAsTable("retail_de_project.default.dim_customer")
dim_product.write.mode("overwrite").format("delta").saveAsTable("retail_de_project.default.dim_product")
dim_store.write.mode("overwrite").format("delta").saveAsTable("retail_de_project.default.dim_store")
dim_date.write.mode("overwrite").format("delta").saveAsTable("retail_de_project.default.dim_date")
fact_sales.write.mode("overwrite").format("delta").saveAsTable("retail_de_project.default.fact_sales")