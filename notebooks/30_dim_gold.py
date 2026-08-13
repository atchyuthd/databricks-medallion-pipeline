# Databricks notebook source
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %run ../conf/reference_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold: Dimension Publishing
# MAGIC
# MAGIC Builds BI-ready dimensions by joining and enriching silver tables.
# MAGIC
# MAGIC **Layer contract:** gold publishes business columns only. Audit columns
# MAGIC (`_source_file`, `_ingested_at`) stop at silver; lineage is recoverable by
# MAGIC joining back on the business key.
# MAGIC
# MAGIC `gld_dim_products` is deliberately denormalized — brand and category
# MAGIC attributes are folded into the product dimension rather than kept as
# MAGIC separate lookups, following Kimball star-schema practice for BI consumption.
# MAGIC
# MAGIC **Inputs:** `{SILVER}.slv_{products,brands,category,customers,calendar}`
# MAGIC
# MAGIC **Outputs:** `{GOLD}.gld_dim_{products,customers,calendar}`
# MAGIC
# MAGIC **Depends on:** `conf/config`, `conf/reference_data`, `20_dim_silver`
# MAGIC
# MAGIC **Grain:** one row per `product_id`, `customer_id`, and `date_id` respectively.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField

# COMMAND ----------

df_products = spark.read.table(f"{SILVER}.slv_products")
df_brands = spark.read.table(f"{SILVER}.slv_brands")
df_category = spark.read.table(f"{SILVER}.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.gld_dim_products AS
SELECT
    p.product_id,
    p.sku,
    p.category_code,
    COALESCE(c.category_name, '{UNKNOWN_VALUE}') AS category_name,
    p.brand_code,
    COALESCE(b.brand_name, '{UNKNOWN_VALUE}') AS brand_name,
    p.color,
    p.size,
    p.material,
    p.weight_grams,
    p.length_cm,
    p.width_cm,
    p.height_cm,
    p.rating_count
FROM {SILVER}.slv_products p
LEFT JOIN {SILVER}.slv_category c
    ON p.category_code = c.category_code
LEFT JOIN {SILVER}.slv_brands b
    ON p.brand_code = b.brand_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

df_silver_customers = spark.read.table(f"{SILVER}.slv_customers")

# COMMAND ----------

def build_region_mapping(spark, country_state_map):
    """Flatten the nested country→state→region dict into a lookup DataFrame."""
    rows = [
        Row(country=country, state=state_code, region=region)
        for country, states in country_state_map.items()
        for state_code, region in states.items()
    ]
    return spark.createDataFrame(rows)


def add_customer_region(df, mapping_df):
    """Attach region by (country, state); unmatched pairs fall back to a sentinel."""
    return df.join(mapping_df, on=["country", "state"], how="left").fillna(
        {"region": f'{UNKNOWN_REGION}'}
    )

def select_dim_customer_columns(df):
    """Fix presentation column order for the BI layer."""
    return df.select(
        "customer_id", "phone", "country_code", "country", "state", "region",
    )

def add_unknown_member(df):
    """Append the Unknown dimension member for unmatched fact rows.

    Order line items are preserved even when customer attribution is missing,
    so revenue reconciles to source and the attribution gap stays queryable.
    """
    nullable_schema = StructType([
        StructField(field.name, field.dataType, True) for field in df.schema.fields
    ])
    
    unknown = spark.createDataFrame(
    [(UNKNOWN_KEY, None, None, None, None, UNKNOWN_VALUE)],
    schema=nullable_schema,
    )
    return df.unionByName(unknown)

# COMMAND ----------

df_region_mapping = build_region_mapping(spark, COUNTRY_STATE_MAP)

df_gold_customers = (
    df_silver_customers
    .transform(add_customer_region, df_region_mapping)
    .transform(select_dim_customer_columns)
    .transform(add_unknown_member)
)

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD}.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar

# COMMAND ----------

df_silver_calendar = spark.table(f'{SILVER}.slv_calendar')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

def add_date_key(df):
    """Integer surrogate key in yyyyMMdd form."""
    return df.withColumn("date_id", F.date_format("date", "yyyyMMdd").cast("int"))


def add_month_name(df):
    return df.withColumn("month_name", F.date_format("date", "MMMM"))


def add_weekend_flag(df):
    return df.withColumn(
        "is_weekend", F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
    )


def select_dim_calendar_columns(df):
    """Fix presentation column order for the BI layer."""
    return df.select(
        "date_id", "date", "year", "month_name", "day_name",
        "is_weekend", "quarter", "quarter_label", "week_of_year", "week_label"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_gold_calendar = (df_silver_calendar
                    .transform(add_date_key)
                    .transform(add_month_name)
                    .transform(add_weekend_flag)
                    .transform(select_dim_calendar_columns)
                    )

# COMMAND ----------

# write table to gold layer
df_gold_calendar.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD}.gld_dim_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

for table, key in [
    ("gld_dim_products", "product_id"),
    ("gld_dim_customers", "customer_id"),
    ("gld_dim_calendar", "date_id"),
]:
    df = spark.table(f"{GOLD}.{table}")
    total = df.count()
    distinct = df.select(key).distinct().count()
    nulls = df.filter(F.col(key).isNull()).count()

    assert total == distinct, f"{table}: {total - distinct} duplicate {key} values"
    assert nulls == 0, f"{table}: {nulls} null {key} values"

print("Dimension grain and key checks passed.")

# COMMAND ----------

# Drop dependent foreign keys before rebuilding dimension primary keys.
# The fact table's FKs reference these PKs, so they must be released first;
# 31_fact_gold recreates them after the fact table is rebuilt.
FACT = f"{GOLD}.gld_fact_order_items"

if spark.catalog.tableExists(FACT):
    for constraint in ["fk_fact_product", "fk_fact_customer", "fk_fact_date"]:
        spark.sql(f"ALTER TABLE {FACT} DROP CONSTRAINT IF EXISTS {constraint}")

# COMMAND ----------

for table, key in [
    ("gld_dim_products", "product_id"),
    ("gld_dim_customers", "customer_id"),
    ("gld_dim_calendar", "date_id"),
]:
    spark.sql(f"ALTER TABLE {GOLD}.{table} DROP CONSTRAINT IF EXISTS pk_{table}")
    spark.sql(f"ALTER TABLE {GOLD}.{table} ALTER COLUMN {key} SET NOT NULL")
    spark.sql(f"ALTER TABLE {GOLD}.{table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({key})")

print("Dimension primary keys declared.")