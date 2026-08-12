# Databricks notebook source
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup: Load Sample Data
# MAGIC
# MAGIC Copies the sample CSVs committed to `data/sample/` into the raw volume,
# MAGIC reproducing the source layer from the repository. Idempotent — overwrites
# MAGIC whatever is already in the volume.
# MAGIC
# MAGIC **Inputs:** `data/sample/{brands,category,products,customers,calendar,order_items/landing}/*.csv`
# MAGIC **Outputs:** files under `{RAW_ROOT}`
# MAGIC **Depends on:** `conf/config`, `00_catalog_setup`
# MAGIC
# MAGIC Requires the repo to be cloned as a Databricks Git folder so the sample
# MAGIC files are readable from the driver filesystem.

# COMMAND ----------

import os

ENTITY_FOLDERS = [
    "brands",
    "category",
    "products",
    "customers",
    "calendar",
    "order_items/landing",
]

notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
sample_root = os.path.join(repo_root, "data", "sample")

print(f"Reading sample data from: {sample_root}")

# COMMAND ----------

for folder in ENTITY_FOLDERS:
    source = f"file:{sample_root}/{folder}"
    target = f"{RAW_ROOT}/{folder}"
    dbutils.fs.cp(source, target, recurse=True)
    print(f"{folder}: {len(dbutils.fs.ls(target))} file(s) loaded")