# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: Order Items Cleansing
# MAGIC
# MAGIC Parses the all-string bronze order items into typed, deduplicated silver
# MAGIC records. Cleansing only — no joins, no derived measures. Revenue
# MAGIC calculation and currency conversion happen in gold.
# MAGIC
# MAGIC **Inputs:** `{BRONZE}.brz_order_items`
# MAGIC
# MAGIC **Outputs:** `{SILVER}.slv_order_items`
# MAGIC
# MAGIC **Depends on:** `conf/config`, `11_fact_bronze`
# MAGIC
# MAGIC **Grain:** one row per `(order_id, item_seq)`, enforced here by deduplication.
# MAGIC
# MAGIC **Handled anomalies:** spelled-out quantities, currency symbols in prices,
# MAGIC percent signs in discounts, two timestamp formats, duplicate line items.

# COMMAND ----------

import pyspark.sql.functions as F

from src.transforms.order_items import (
    deduplicate_order_items,
    parse_quantity,
    clean_monetary_columns,
    standardize_categoricals,
    cast_temporal_columns,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items

# COMMAND ----------

df_bronze_order_items = spark.table(f'{BRONZE}.brz_order_items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_order_items = (
    df_bronze_order_items
    .transform(deduplicate_order_items)
    .transform(parse_quantity)
    .transform(clean_monetary_columns)
    .transform(standardize_categoricals)
    .transform(cast_temporal_columns)
    .withColumn("_processed_at", F.current_timestamp())
)

# COMMAND ----------

# Write raw data to the silver layer
df_silver_order_items.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.slv_order_items")