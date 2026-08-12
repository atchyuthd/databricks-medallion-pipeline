# Databricks notebook source
# MAGIC %run ../conf/config

# COMMAND ----------

# MAGIC %run ../conf/reference_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold: Order Items Fact
# MAGIC
# MAGIC Derives revenue measures and publishes the transaction fact table.
# MAGIC
# MAGIC **Measures:** `gross_amount` (quantity × unit price), `discount_amount`,
# MAGIC `net_amount` (pre-tax), `total_amount` (tax-inclusive), and
# MAGIC `total_amount_inr` for cross-currency reporting.
# MAGIC
# MAGIC **Currency conversion** uses fixed rates from `conf/reference_data`,
# MAGIC snapshotted at `FX_RATE_AS_OF`. A production implementation would join a
# MAGIC date-effective rate table so historical orders convert at the rate in
# MAGIC effect on their transaction date.
# MAGIC
# MAGIC **Layer contract:** gold publishes business columns only — see `3_dim_gold`.
# MAGIC
# MAGIC **Inputs:** `{SILVER}.slv_order_items`
# MAGIC **Outputs:** `{GOLD}.gld_fact_order_items`
# MAGIC **Depends on:** `conf/config`, `conf/reference_data`, `21_fact_silver`
# MAGIC
# MAGIC **Grain:** one row per `(transaction_id, seq_no)`.
# MAGIC **Joins to:** `gld_dim_customers`, `gld_dim_products`, `gld_dim_calendar` (via `date_id`).

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import  IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items

# COMMAND ----------

df_silver_order_items = spark.table(f"{SILVER}.slv_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

def add_revenue_amounts(df):
    """Derive gross, discount, net (pre-tax), and total (tax-inclusive) amounts.
    discount_pct arrives as a whole number, e.g. 21 means 21%.
    """
    return (
        df
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn(
            "discount_amount",
            F.round(F.col("gross_amount") * (F.col("discount_pct") / 100.0), 2),
        )
        .withColumn(
            "net_amount",
            F.round(F.col("gross_amount") - F.col("discount_amount"), 2),
        )
        .withColumn(
            "total_amount",
            F.round(F.col("net_amount") + F.col("tax_amount"), 2),
        )
    )


def add_date_key(df):
    """Integer surrogate key joining to gld_dim_calendar."""
    return df.withColumn("date_id", F.date_format("dt", "yyyyMMdd").cast(IntegerType()))


def add_coupon_flag(df):
    """Flag rows with a usable coupon code.

    Silver trims coupon_code, so absent values may be empty strings
    rather than nulls — both count as no coupon.
    """
    return df.withColumn(
        "coupon_flag",
        F.when(
            F.col("coupon_code").isNotNull() & (F.length(F.col("coupon_code")) > 0),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )


def convert_to_reporting_currency(df, rates_df):
    """Convert total_amount to INR using fixed point-in-time rates."""
    return (
        df.join(
            rates_df,
            F.upper(F.trim(F.col("unit_price_currency"))) == rates_df.currency,
            "left",
        )
        .withColumn(
            "total_amount_inr",
            F.round(F.col("total_amount") * F.col("inr_rate"), 2),
        )
        .drop("currency")
    )


def select_fact_columns(df):
    """Project and rename to the published fact grain."""
    return df.select(
        F.col("date_id"),
        F.col("dt").alias("transaction_date"),
        F.col("order_ts").alias("transaction_ts"),
        F.col("order_id").alias("transaction_id"),
        F.col("item_seq").alias("seq_no"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("channel"),
        F.col("coupon_code"),
        F.col("coupon_flag"),
        F.col("unit_price_currency"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("gross_amount"),
        F.col("discount_pct").alias("discount_percent"),
        F.col("discount_amount"),
        F.col("net_amount"),
        F.col("tax_amount"),
        F.col("total_amount"),
        F.col("total_amount_inr"),
    )

def resolve_unknown_customers(df, dim_customers):
    """Map fact rows with no matching customer to the Unknown member.

    Order line items are preserved even when customer attribution is missing,
    so revenue reconciles to source and the attribution gap stays queryable
    rather than silently disappearing from the fact table.
    """
    valid = (
        dim_customers
        .select("customer_id")
        .withColumn("_matched", F.lit(True))
    )
    return (
        df.join(valid, "customer_id", "left")
        .withColumn(
            "customer_id",
            F.when(F.col("_matched").isNull(), F.lit(UNKNOWN_KEY))
            .otherwise(F.col("customer_id")),
        )
        .drop("_matched")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

rates_df = spark.createDataFrame(
    [(code, float(rate)) for code, rate in FX_RATES_TO_INR.items()],
    ["currency", "inr_rate"],
)

df_gold_order_items = (
    spark.table(f"{SILVER}.slv_order_items")
    .transform(add_revenue_amounts)
    .transform(add_date_key)
    .transform(add_coupon_flag)
    .transform(convert_to_reporting_currency, rates_df)
    .transform(resolve_unknown_customers, spark.table(f"{GOLD}.gld_dim_customers"))
    .transform(select_fact_columns)
)

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_fact_order_items)
df_gold_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{GOLD}.gld_fact_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

fact = spark.table(f"{GOLD}.gld_fact_order_items")

# Grain
total = fact.count()
distinct = fact.select("transaction_id", "seq_no").distinct().count()
assert total == distinct, f"fact is not unique on (transaction_id, seq_no): {total - distinct} duplicates"

# Referential integrity
for dim, key in [
    ("gld_dim_products", "product_id"),
    ("gld_dim_customers", "customer_id"),
    ("gld_dim_calendar", "date_id"),
]:
    orphans = fact.join(spark.table(f"{GOLD}.{dim}"), key, "left_anti").count()
    assert orphans == 0, f"{orphans} fact rows reference a missing {key}"

print("Fact grain and referential integrity checks passed.")

# COMMAND ----------

FACT = f"{GOLD}.gld_fact_order_items"

# Drop existing constraints so the notebook is re-runnable. Note that
# 30_dim_gold also drops these FKs before rebuilding the dimension PKs
# they reference — a PK cannot be dropped while a foreign key points at it.
for constraint in ["pk_fact_order_items", "fk_fact_product", "fk_fact_customer", "fk_fact_date"]:
    spark.sql(f"ALTER TABLE {FACT} DROP CONSTRAINT IF EXISTS {constraint}")

# Primary and foreign key columns must be non-nullable before a key is declared
for col in ["transaction_id", "seq_no", "product_id", "customer_id", "date_id"]:
    spark.sql(f"ALTER TABLE {FACT} ALTER COLUMN {col} SET NOT NULL")

spark.sql(f"""
    ALTER TABLE {FACT}
    ADD CONSTRAINT pk_fact_order_items PRIMARY KEY (transaction_id, seq_no)
""")

for name, col, dim in [
    ("fk_fact_product", "product_id", "gld_dim_products"),
    ("fk_fact_customer", "customer_id", "gld_dim_customers"),
    ("fk_fact_date", "date_id", "gld_dim_calendar"),
]:
    spark.sql(f"""
        ALTER TABLE {FACT}
        ADD CONSTRAINT {name} FOREIGN KEY ({col})
        REFERENCES {GOLD}.{dim}
    """)

print("Fact primary and foreign keys declared.")