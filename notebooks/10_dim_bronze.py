# Databricks notebook source
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze: Dimension Ingestion
# MAGIC
# MAGIC Loads raw dimension CSVs into Delta tables with explicit schemas and
# MAGIC audit columns. No cleansing — anomalies are preserved for the silver layer.
# MAGIC
# MAGIC **Inputs:** `{RAW_ROOT}/{brands,category,products,customers,date}/*.csv`
# MAGIC
# MAGIC **Outputs:** `{BRONZE}.brz_{brands,category,products,customers,calendar}`
# MAGIC
# MAGIC **Depends on:** `conf/config`, `00_catalog_setup`

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brands

# COMMAND ----------

# Define schema for the data file
brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True)
])

# COMMAND ----------

raw_data_path = f"{RAW_ROOT}/brands/*.csv"

df_brands = spark.read.option("header", "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)

# add metadata columns
df_brands = df_brands.withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

df_brands.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable(f"{BRONZE}.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

# Define schema for the data file
category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

category_path = f"{RAW_ROOT}/category/*.csv"

df_category = spark.read.option("header", "true").option("delimiter", ",").schema(category_schema).csv(category_path)

# add metadata columns
df_category = df_category.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_category)
df_category.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable(f"{BRONZE}.brz_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

# Define schema for the data file
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),    #datatype is string due to incoming data contain anomalies
    StructField("length_cm", StringType(), True),       #datatype is string due to incoming data contain anomalies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True)
])

product_path = f"{RAW_ROOT}/products/*.csv"

df_products = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(product_path)

# Add metadata columns
df_products = df_products.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_products)
df_products.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable(f"{BRONZE}.brz_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# Define schema for the data file
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

customer_path = f"{RAW_ROOT}/customers/*.csv"

df_customers = spark.read.option("header", "true").option("delimiter", ",").schema(customer_schema).csv(customer_path)

# add metadata columns
df_customers = df_customers.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_customers)
df_customers.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable(f"{BRONZE}.brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar

# COMMAND ----------

calendar_schema = StructType([
    StructField('date', StringType(), False), # Raw date in string format
    StructField('year', IntegerType(), True), # Year
    StructField('day_name', StringType(), True), # Day name
    StructField('quarter', IntegerType(), True), # Quarter
    StructField('week_of_year', IntegerType(), True) # Week of year (can be negative)
])

calendar_path = f"{RAW_ROOT}/calendar/calendar.csv"

df_calendar = spark.read.option("header", "true").option("delimiter", ",").schema(calendar_schema).csv(calendar_path)

# add metadata columns
df_calendar = df_calendar.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_date)
df_calendar.write.format("delta").mode('overwrite').option("mergeSchema", "true").saveAsTable(f"{BRONZE}.brz_calendar")

# COMMAND ----------

display(dbutils.fs.ls(f"{RAW_ROOT}/calendar"))