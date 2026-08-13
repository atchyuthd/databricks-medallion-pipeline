# Databricks notebook source
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
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items

# COMMAND ----------

df_bronze_order_items = spark.table(f'{BRONZE}.brz_order_items')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

def deduplicate_order_items(df):
    """Keep the most recently landed row per (order_id, item_seq).

    Source may re-emit corrected line items in a later batch. Ordering by
    source filename (which encodes the batch date) makes the winner
    deterministic — _ingested_at is assigned per pipeline run, not per file,
    so it cannot distinguish rows that arrived in different batches.
    """
    window = Window.partitionBy("order_id", "item_seq").orderBy(F.col("_source_file").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

def parse_quantity(df):
    """Source data spells some quantities as words; normalize then cast."""
    return df.withColumn(
        "quantity",
        F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int"),
    )


def clean_monetary_columns(df):
    """Strip currency symbols and percent signs, then cast to numeric."""
    return (
        df.withColumn("unit_price", F.regexp_replace("unit_price", "[$]", "").cast("double"))
        .withColumn("discount_pct", F.regexp_replace("discount_pct", "%", "").cast("double"))
        .withColumn("tax_amount", F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double"))
    )


def standardize_categoricals(df):
    """Lowercase coupon codes; map channel codes to display names."""
    return df.withColumn(
        "coupon_code", F.lower(F.trim(F.col("coupon_code")))
    ).withColumn(
        "channel",
        F.when(F.col("channel") == "web", "Website")
        .when(F.col("channel") == "app", "Mobile")
        .otherwise(F.col("channel")),
    )


def cast_temporal_columns(df):
    """Source emits two timestamp formats; coalesce across both."""
    return df.withColumn(
        "dt", F.to_date("dt", "yyyy-MM-dd")
    ).withColumn(
        "order_ts",
        F.coalesce(
            F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm"),
        ),
    ).withColumn(
        "item_seq", F.col("item_seq").cast("int")
    )

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

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_order_items)
df_silver_order_items.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER}.slv_order_items")