# Databricks notebook source
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze: Order Items Ingestion
# MAGIC
# MAGIC Loads raw order line items into a Delta table with an explicit schema and
# MAGIC audit columns. All columns land as strings — the source emits mixed
# MAGIC formats (currency symbols, spelled-out quantities, two timestamp layouts)
# MAGIC that are parsed in silver, so bronze preserves the raw values rather than
# MAGIC failing or nulling them on read.
# MAGIC
# MAGIC **Inputs:** `{RAW_ROOT}/order_items/landing/*.csv`
# MAGIC
# MAGIC **Outputs:** `{BRONZE}.brz_order_items`
# MAGIC
# MAGIC **Depends on:** `conf/config`, `00_catalog_setup`
# MAGIC
# MAGIC **Grain:** one row per `(order_id, item_seq)` — not enforced here;
# MAGIC duplicates are removed in silver.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items

# COMMAND ----------

order_items_schema = StructType([
    StructField("dt",                 StringType(), True),
    StructField("order_ts",           StringType(), True),
    StructField("customer_id",        StringType(), True),
    StructField("order_id",           StringType(), True),
    StructField("item_seq",           StringType(), True),
    StructField("product_id",         StringType(), True),
    StructField("quantity",           StringType(), True),
    StructField("unit_price_currency",StringType(), True),
    StructField("unit_price",         StringType(), True),
    StructField("discount_pct",       StringType(), True),
    StructField("tax_amount",         StringType(), True),
    StructField("channel",            StringType(), True),
    StructField("coupon_code",        StringType(), True),
])

# COMMAND ----------

# Load data using the schema defined
raw_data_path = f"{RAW_ROOT}/order_items/landing/*.csv"

df_bronze_order_items = spark.read.option("header", "true").option("delimiter", ",").schema(order_items_schema).csv(raw_data_path) \
    .withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

df_bronze_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE}.brz_order_items")