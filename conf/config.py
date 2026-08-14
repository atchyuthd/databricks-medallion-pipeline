# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# COMMAND ----------

import sys, os

REPO_ROOT = os.path.dirname(os.getcwd())
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

print(f"Repo root: {REPO_ROOT}")
print(f"src exists: {os.path.isdir(os.path.join(REPO_ROOT, 'src'))}")