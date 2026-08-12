# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Shared configuration
# MAGIC `%run` this from every pipeline notebook. Defines catalog, schema,
# MAGIC and volume constants. No transformation logic here.

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce", "Target catalog")

CATALOG = dbutils.widgets.get("catalog")

BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

RAW_ROOT = f"/Volumes/{CATALOG}/source_data/raw"

# Sentinel values for unmatched lookups
UNKNOWN_VALUE = "Not Available"
UNKNOWN_REGION = "Other"
UNKNOWN_KEY = "-1"