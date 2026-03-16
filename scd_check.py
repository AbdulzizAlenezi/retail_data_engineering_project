# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

#1 ###
file_path = "/Volumes/retail_de_project/default/bronze_ingest/retail_data_500.csv"  
transaction_df = spark.read.csv(file_path, header=True, inferSchema=True)
print(f" Loaded {transaction_df.count()} transactions from: {file_path.split('/')[-1]}")
#1 ###

def apply_scd_type2(incoming_df, table_name, key_col, attribute_cols):
    
    # Check if dimension table exists - Try to access the table directly
    try:
        # Use spark.sql to properly handle table names
        existing_test = spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        table_exists = existing_test.count() >= 0
        print(f"Table {table_name} exists - will apply SCD Type 2 logic")
    except Exception as e:
        table_exists = False
        print(f"Table {table_name} does not exist - will create it")
        print(f"(Debug: {str(e)[:100]})")
    
    # ============================================================
    # FIRST DAY: Table doesn't exist → Create it
    # ============================================================
    if not table_exists:
        print(f"FIRST RUN: Creating table...")
        print(f"Table name: {table_name}")
        
        initial_df = incoming_df.withColumn("valid_from", current_date()) \
                                .withColumn("valid_to", lit(None).cast(DateType())) \
                                .withColumn("is_current", lit(True).cast(BooleanType()))
        
        # Show schema before writing
        print(f"   Schema: {initial_df.columns}")
        
        initial_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
        
        # Verify table was created
        verification = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
        print(f"Created {table_name} with {verification} records")
        print(f"All records marked as current (valid_from = today)\n")
        return
    
    # ============================================================
    # DAY 2+: Table exists → Apply SCD Type 2 logic
    # ============================================================
    print(f" DAILY RUN: Applying SCD Type 2...")
    
    # Load existing table using SQL to avoid name parsing issues
    existing_df = spark.sql(f"SELECT * FROM {table_name}")
    print(f"   Existing total records: {existing_df.count()}")
    print(f"   Existing active records: {existing_df.filter('is_current = true').count()}")
    print(f"   Existing historical records: {existing_df.filter('is_current = false').count()}")
    
    # 1. Find changed records (compare active records with incoming data)
    change_conditions = [col(f"e.{attr}") != col(f"i.{attr}") for attr in attribute_cols]
    print(f"change_conditions : {change_conditions}")
    change_filter = change_conditions[0]
    for condition in change_conditions[1:]:
        change_filter = change_filter | condition
        print(f"{change_filter} = {change_filter} | {condition}")
        print(f"change_filter : {change_filter}")

    changed = existing_df.filter("is_current = true").alias("e") \
        .join(incoming_df.alias("i"), key_col, "inner") \
        .filter(change_filter)
    
    # 2. Expire old versions (set valid_to = today, is_current = false)
    expired_cols = [col(f"e.{key_col}")] + [col(f"e.{attr}") for attr in attribute_cols] + \
                   [col("e.valid_from"), current_date().alias("valid_to"), lit(False).alias("is_current")]
    expired = changed.select(*expired_cols)
    
    # 3. Insert new versions (set valid_from = today, is_current = true)
    new_version_cols = [col(f"i.{key_col}")] + [col(f"i.{attr}") for attr in attribute_cols]
    new_versions = changed.select(*new_version_cols) \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit(None).cast(DateType())) \
        .withColumn("is_current", lit(True))
    
    # 4. Keep unchanged active records
    unchanged = existing_df.filter("is_current = true").alias("e") \
        .join(changed.select(key_col).distinct().alias("c"), key_col, "left_anti")
    
    # 5. Add new records (not in existing table)
    existing_keys = existing_df.select(key_col).distinct()
    new_records = incoming_df.join(existing_keys, key_col, "left_anti") \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit(None).cast(DateType())) \
        .withColumn("is_current", lit(True))
    
    # 6. Keep all historical records (CRITICAL: Never lose history!)
    historical = existing_df.filter("is_current = false")
    
    # 7. Combine everything
    all_cols = [key_col] + attribute_cols + ["valid_from", "valid_to", "is_current"]
    
    final_df = expired.select(all_cols) \
        .union(new_versions.select(all_cols)) \
        .union(unchanged.select(all_cols)) \
        .union(new_records.select(all_cols)) \
        .union(historical.select(all_cols))
    
    # 8. Overwrite table (includes all historical records)
    final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
    
    # Print summary
    print(f"   Changed records: {changed.count()}")
    print(f"   New records: {new_records.count()}")
    print(f"   Unchanged records: {unchanged.count()}")
    print(f"   Historical preserved: {historical.count()}")
    print(f"Updated {table_name}")
    print(f"   Total: {final_df.count()} | Active: {final_df.filter('is_current = true').count()} | Historical: {final_df.filter('is_current = false').count()}\n")


#2 ###
# Test catalog access and verify table naming
print("Current catalog:", spark.catalog.currentCatalog())
print("Current database:", spark.catalog.currentDatabase())
print("\nTesting table access...")

# Try different table name formats
test_table_name = "retail_de_project.default.dim_customer_scd2"
print(f"Will use table name: {test_table_name}")

# List existing tables
print("\nExisting tables in default schema:")
spark.sql("SHOW TABLES IN retail_de_project.default").show(truncate=False)
# 2###


#3 ###
# Extract unique customers from today's file
dim_customer = transaction_df.select('CustomerID', 'CustomerName').dropDuplicates()
print(f"Extracted {dim_customer.count()} unique customers from today's file")

# Extract unique products from today's file
dim_product = transaction_df.select('ProductID', 'ProductName', 'Category').dropDuplicates()
print(f"Extracted {dim_product.count()} unique products from today's file")

# Extract unique stores from today's file
dim_store = transaction_df.select('StoreID', 'StoreLocation').dropDuplicates()
print(f"Extracted {dim_store.count()} unique stores from today's file")
#3 ###

#4 ###
print("CUSTOMER DIMENSION")
apply_scd_type2(
    incoming_df=dim_customer,
    table_name="retail_de_project.default.dim_customer_scd2",
    key_col="CustomerID",
    attribute_cols=["CustomerName"]
)

print("PRODUCT DIMENSION")
apply_scd_type2(
    incoming_df=dim_product,
    table_name="retail_de_project.default.dim_product_scd2",
    key_col="ProductID",
    attribute_cols=["ProductName", "Category"]
)


print("STORE DIMENSION")
apply_scd_type2(
    incoming_df=dim_store,
    table_name="retail_de_project.default.dim_store_scd2",
    key_col="StoreID",
    attribute_cols=["StoreLocation"]
)



# Customer dimension - current snapshot
print("CUSTOMER DIMENSION - Current Records:")
spark.table("retail_de_project.default.dim_customer_scd2") \
    .filter("is_current = true") \
    .orderBy("CustomerID") \
    .show(10)



# Customer dimension - full history
print("CUSTOMER DIMENSION - Full History:")
spark.table("retail_de_project.default.dim_customer_scd2") \
    .orderBy("CustomerID", col("valid_from").desc()) \
    .show(20, truncate=False)



# Product dimension - current snapshot
print("PRODUCT DIMENSION - Current Records:")
spark.table("retail_de_project.default.dim_product_scd2") \
    .filter("is_current = true") \
    .orderBy("ProductID") \
    .show(10, truncate=False)



# Store dimension - current snapshot
print("STORE DIMENSION - Current Records:")
spark.table("retail_de_project.default.dim_store_scd2") \
    .filter("is_current = true") \
    .orderBy("StoreID") \
    .show(10)



def print_dimension_summary(table_name, dimension_name):
    df = spark.table(table_name)
    total = df.count()
    active = df.filter("is_current = true").count()
    historical = df.filter("is_current = false").count()
    
    print(f"{dimension_name}:")
    print(f"  Total: {total} | Active: {active} | Historical: {historical}")


print("DIMENSION TABLES SUMMARY")


print_dimension_summary("retail_de_project.default.dim_customer_scd2", "Customer")
print_dimension_summary("retail_de_project.default.dim_product_scd2", "Product")
print_dimension_summary("retail_de_project.default.dim_store_scd2", "Store")


print("SCD Type 2 processing complete!")


# COMMAND ----------

