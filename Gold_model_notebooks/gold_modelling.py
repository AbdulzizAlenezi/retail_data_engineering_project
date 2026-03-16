# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run /Workspace/Users/pdmentor.help@gmail.com/retail_de_project_scd/Bronze_ingestion_notebooks/project_config

# COMMAND ----------

print(config['silver_path'])
silver_path = config['silver_path']
today_date = datetime.now().strftime("%Y-%m-%d")
silver_path_date = f"{silver_path}/{today_date}/"

# COMMAND ----------

silver_df = spark.read.parquet(f'{silver_path_date}/part-*')
silver_df.show()

# COMMAND ----------

dim_customer = silver_df.select('CustomerID','CustomerName').dropDuplicates()
dim_customer.show()

# COMMAND ----------

dim_product = silver_df.select('ProductID','ProductName').dropDuplicates()
dim_product.show()

# COMMAND ----------

dim_store = silver_df.select('StoreID','StoreLocation').dropDuplicates()
dim_store.show()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "OrderDateParsed",
    to_date(col("OrderDate"), "dd-MM-yyyy")
)

dim_date = (
    silver_df
    .select("OrderDateParsed")
    .dropDuplicates()
    .withColumn("Day", dayofmonth("OrderDateParsed"))
    .withColumn("Month", month("OrderDateParsed"))
    .withColumn("Year", year("OrderDateParsed"))
    .withColumn("MonthName", date_format("OrderDateParsed", "MMMM"))
    .withColumn("Quarter", quarter("OrderDateParsed"))
)
dim_date.show()


# COMMAND ----------

fact_sales = silver_df.withColumn("SalesID", monotonically_increasing_id()) \
    .select(
        "SalesID", "OrderID", "OrderDate", "CustomerID", "ProductID",
        "StoreID", "Quantity", "UnitPrice", "TotalPrice", "PaymentMethod"
    )
fact_sales.show()




# COMMAND ----------

