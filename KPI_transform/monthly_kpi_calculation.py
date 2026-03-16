# Databricks notebook source
from pyspark.sql import * 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

spark = SparkSession.builder.appName('kpi').getOrCreate()

# COMMAND ----------

fact_data = spark.read.table('retail_de_project.default.fact_sales')
fact_data.show()

# COMMAND ----------

fact_data = fact_data.withColumn(
    "OrderDate_parsed",
    to_date(col("OrderDate"), "dd-MM-yyyy")
)
fact_data = fact_data.withColumn(
    "yearmonth",
    date_format(col("OrderDate_parsed"), "yyyy-MM")
)
fact_data = fact_data.withColumn(
    "yearmonth",
    date_format(col("OrderDate_parsed"), "yyyy-MM")
)
fact_data = fact_data.drop("OrderDate").withColumnRenamed("OrderDate_parsed", "OrderDate")
fact_data.select("OrderDate", "yearmonth").show()

# COMMAND ----------

monthly_kpi = fact_data.groupBy("yearmonth")\
                        .agg(round(sum('TotalPrice')).alias('TotalRevenue'),
                                   countDistinct('OrderID').alias('TotalOrders'),
                                   round(sum('TotalPrice')/countDistinct('OrderID')).alias('AverageOrderValue'),
                                   countDistinct('CustomerID').alias('UniqueCustomers'))\
                        .orderBy('yearmonth')
monthly_kpi.show()

# COMMAND ----------


monthly_kpi.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("retail_de_project.default.monthly_kpi")


# COMMAND ----------

