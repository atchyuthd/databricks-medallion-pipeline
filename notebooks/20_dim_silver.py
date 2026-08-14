# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver: Dimension Cleansing
# MAGIC
# MAGIC Cleanses bronze dimension tables into conformed, correctly typed silver
# MAGIC tables. Cleansing only — no joins and no business logic, so each silver
# MAGIC table maps one-to-one to its bronze source. Cross-entity enrichment
# MAGIC happens in gold.
# MAGIC
# MAGIC Transformations are defined as named functions and composed with
# MAGIC `DataFrame.transform()` so each step can be tested in isolation.
# MAGIC
# MAGIC **Inputs:** `{BRONZE}.brz_{brands,category,products,customers,calendar}`
# MAGIC
# MAGIC **Outputs:** `{SILVER}.slv_{brands,category,products,customers,calendar}`
# MAGIC
# MAGIC **Depends on:** `conf/config`, `10_dim_bronze`
# MAGIC
# MAGIC **Handled anomalies:** non-standard category codes, misspelled materials,
# MAGIC comma decimal separators, unit suffixes on weights, negative counts,
# MAGIC duplicate keys, missing customer identifiers, mixed-case values.

# COMMAND ----------

import pyspark.sql.functions as F

from src.transforms.dimensions import (
    clean_brand_names,
    normalize_brand_category_codes,
    normalize_category_codes,
    deduplicate_categories,
    parse_product_dimensions,
    normalize_product_codes,
    correct_material_spellings,
    clean_rating_counts,
    drop_rows_missing_customer_id,
    fill_missing_phone,
    parse_calendar_dates,
    deduplicate_calendar,
    normalize_day_names,
    add_period_labels,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brands

# COMMAND ----------

df_bronze_brands = spark.table(f"{BRONZE}.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_brands = (df_bronze_brands
                    .transform(clean_brand_names)
                    .transform(normalize_brand_category_codes)
                   )

# COMMAND ----------

df_silver_brands.write.format('delta').mode('overwrite').saveAsTable(f'{SILVER}.slv_brands')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

df_bronze_category = spark.table(f"{BRONZE}.brz_category")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_category = (
    df_bronze_category
    .transform(normalize_category_codes)
    .transform(deduplicate_categories)
)

# COMMAND ----------

# Write raw data to the silver layer
df_silver_category.write.format('delta') \
    .mode('overwrite')\
    .saveAsTable(f"{SILVER}.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

df_bronze_products = spark.read.table(f"{BRONZE}.brz_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_products = (
    df_bronze_products
    .transform(parse_product_dimensions)
    .transform(normalize_product_codes)
    .transform(correct_material_spellings)
    .transform(clean_rating_counts)
)

# COMMAND ----------

df_silver_products.write.format("delta")\
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

df_bronze_customers = spark.read.table(f"{BRONZE}.brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_customers = (
    df_bronze_customers
    .transform(drop_rows_missing_customer_id)
    .transform(fill_missing_phone, UNKNOWN_VALUE)
)

# COMMAND ----------

df_silver_customers.write.format('delta').mode('overwrite').saveAsTable(f"{SILVER}.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar

# COMMAND ----------

df_bronze_calendar = spark.read.table(f"{BRONZE}.brz_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_calendar = (
    df_bronze_calendar
    .transform(parse_calendar_dates)
    .transform(deduplicate_calendar)
    .transform(normalize_day_names)
    .transform(add_period_labels)
)

# COMMAND ----------

df_silver_calendar.write.format('delta').mode('overwrite').saveAsTable(f"{SILVER}.slv_calendar")