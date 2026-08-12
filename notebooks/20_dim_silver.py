# Databricks notebook source
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

from pyspark.sql.types import IntegerType, FloatType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brands

# COMMAND ----------

df_bronze_brands = spark.table(f"{BRONZE}.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

CATEGORY_ANOMALIES = {
    "GROCERY": "GRCY",
    "TOYS": "TOY",
    "BOOKS": "BKS",
}

def clean_brand_names(df):
    """Trim whitespace from brand_name and strip non-alphanumerics from brand_code."""
    return (
        df
        .withColumn("brand_name", F.trim(F.col("brand_name")))
        .withColumn(
            "brand_code", F.regexp_replace(F.col("brand_code"), r"[^a-zA-Z0-9]", "")
        )
    )

def normalize_brand_category_codes(df):
    """Map known non-standard category_code values to their canonical codes."""
    return df.replace(CATEGORY_ANOMALIES, subset="category_code")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_brands = (df_bronze_brands
                    .transform(clean_brand_names)
                    .transform(normalize_brand_category_codes)
                   )

# COMMAND ----------

df_silver_brands.write.format('delta').mode('overwrite').option('mergeSchema', 'true').saveAsTable(f'{SILVER}.slv_brands')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

df_bronze_category = spark.table(f"{BRONZE}.brz_category")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

def deduplicate_categories(df):
    """Keep one row per category_code."""
    return df.dropDuplicates(["category_code"])


def normalize_category_codes(df):
    """Uppercase category_code."""
    return df.withColumn("category_code", F.upper(F.col("category_code")))

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

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver_category.write.format('delta') \
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
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

MATERIAL_MISSPELLINGS = {
    "Coton": "Cotton",
    "Ruber": "Rubber",
    "Alumium": "Aluminium",
}


def parse_product_dimensions(df):
    """Strip unit/format artifacts from weight_grams and length_cm, then cast to numeric."""
    return (
        df
        .withColumn(
            "weight_grams",
            F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType()),
        )
        .withColumn(
            "length_cm",
            F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType()),
        )
    )


def normalize_product_codes(df):
    """Uppercase brand_code and category_code."""
    return (
        df
        .withColumn("brand_code", F.upper(F.col("brand_code")))
        .withColumn("category_code", F.upper(F.col("category_code")))
    )


def correct_material_spellings(df):
    """Map known material misspellings to their correct values."""
    return df.replace(MATERIAL_MISSPELLINGS, subset="material")


def clean_rating_counts(df):
    """Take the absolute value of rating_count, defaulting nulls to 0."""
    return df.withColumn(
        "rating_count",
        F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
         .otherwise(F.lit(0)),
    )

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
    .option("mergeSchema", "true") \
    .saveAsTable(f"{SILVER}.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

df_bronze_customers = spark.read.table(f"{BRONZE}.brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

def drop_rows_missing_customer_id(df):
    """Drop rows with no customer_id."""
    return df.dropna(subset=["customer_id"])


def fill_missing_phone(df):
    """Replace null phone values with a placeholder."""
    return df.fillna(f"{UNKNOWN_VALUE}", subset=["phone"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline

# COMMAND ----------

df_silver_customers = (
    df_bronze_customers
    .transform(drop_rows_missing_customer_id)
    .transform(fill_missing_phone)
)

# COMMAND ----------

df_silver_customers.write.format('delta').mode('overwrite').option('mergeSchema', 'true').saveAsTable(f"{SILVER}.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar

# COMMAND ----------

df_bronze_calendar = spark.read.table(f"{BRONZE}.brz_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

def parse_calendar_dates(df):
    """Convert the date string column to a proper date type."""
    return df.withColumn("date", F.to_date(F.col("date"), "dd-MM-yyyy"))


def deduplicate_calendar(df):
    """Keep one row per date."""
    return df.dropDuplicates(["date"])


def normalize_day_names(df):
    """Capitalize the first letter of each word in day_name."""
    return df.withColumn("day_name", F.initcap(F.col("day_name")))

def add_period_labels(df):
    """Add year-scoped quarter and week labels, preserving the numeric originals.

    Numeric quarter and week_of_year are kept for sorting; the label columns
    are for display. String labels sort incorrectly (Week10 before Week9).
    """
    return (
        df
        .withColumn("week_of_year", F.abs(F.col("week_of_year")))
        .withColumn(
            "quarter_label",
            F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year")),
        )
        .withColumn(
            "week_label",
            F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year")),
        )
    )

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

df_silver_calendar.write.format('delta').mode('overwrite').option('mergeSchema', 'true').saveAsTable(f"{SILVER}.slv_calendar")