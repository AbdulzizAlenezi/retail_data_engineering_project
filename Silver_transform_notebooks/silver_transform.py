# Databricks notebook source
# MAGIC %run /Workspace/Users/pdmentor.help@gmail.com/retail_de_project_scd/Bronze_ingestion_notebooks/data_ingestion

# COMMAND ----------

# MAGIC %run /Workspace/Users/pdmentor.help@gmail.com/retail_de_project_scd/Bronze_ingestion_notebooks/project_config

# COMMAND ----------

display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

silver_df = raw_df.select(
    col("OrderID").cast("int"),
    coalesce(
        expr("try_to_date(OrderDate, 'yyyy-MM-dd')"),
        expr("try_to_date(OrderDate, 'dd-MM-yyyy')")
    ).alias("OrderDate"),
    col("CustomerID"),
    col("CustomerName"),
    col("ProductID"),
    col("ProductName"),
    col("Category"),
    col("Quantity").cast("int"),
    expr("try_cast(UnitPrice as double)").alias("UnitPrice"),
    expr("try_cast(TotalPrice as double)").alias("TotalPrice"),
    col("StoreID"),
    col("StoreLocation"),
    col("PaymentMethod")
)


silver_df = silver_df.withColumn("OrderDateStr", date_format("OrderDate", "dd-MM-yyyy"))


silver_path = config['silver_path']
today_date = datetime.now().strftime("%Y-%m-%d")
silver_path_date = f"{silver_path}/{today_date}/"

silver_df.write.mode("append").parquet(silver_path_date)

print(f"Silver data successfully written to: {silver_path_date}")

# COMMAND ----------

silver_df = silver_df.dropDuplicates(["OrderID"])

# COMMAND ----------

silver_df = silver_df.filter(
    (col("OrderID").isNotNull()) &
    (col("OrderDate").isNotNull()) &
    (col("CustomerID").isNotNull()) &
    (col("ProductID").isNotNull()) &
    (col("Quantity").isNotNull()) &
    (col("Quantity") > 0) &
    (col("UnitPrice").isNotNull()) &
    (col("TotalPrice").isNotNull())
)


# COMMAND ----------

silver_path = config['silver_path']
print(silver_path)
today_date = datetime.now().strftime("%Y-%m-%d")
silver_path_date = f"{silver_path}/{today_date}/"

silver_df.write.mode('overwrite').parquet(silver_path_date)