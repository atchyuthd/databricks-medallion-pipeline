# Databricks notebook source
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Setup: Catalog and Storage
# MAGIC
# MAGIC Creates the Unity Catalog objects the pipeline depends on. Idempotent —
# MAGIC safe to re-run, and required once per target catalog before any other notebook.
# MAGIC
# MAGIC **Creates**
# MAGIC - Catalog `{CATALOG}`
# MAGIC - Schemas `bronze`, `silver`, `gold`, `source_data`
# MAGIC - Volume `source_data.raw` and its per-entity folder skeleton
# MAGIC
# MAGIC **Outputs:** empty schemas and an empty raw volume
# MAGIC **Depends on:** `conf/config`
# MAGIC
# MAGIC Run `01_load_sample_data` next to populate the volume.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.source_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.source_data.raw")

# COMMAND ----------

for folder in ["brands", "category", "products", "customers", "date", "order_items/landing"]:
    dbutils.fs.mkdirs(f"{RAW_ROOT}/{folder}")