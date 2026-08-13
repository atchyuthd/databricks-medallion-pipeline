"""Dimension transformations for the silver and gold layers.

Each function takes a DataFrame and returns a DataFrame, so they compose with
``DataFrame.transform()`` and can be tested in isolation without Databricks.
Sentinel values are passed as arguments rather than read from notebook scope,
keeping ``conf/config`` the single place they are defined.
"""

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType

# Non-standard category codes observed in the brands source
CATEGORY_ANOMALIES = {
    "GROCERY": "GRCY",
    "TOYS": "TOY",
    "BOOKS": "BKS",
}

# Misspelled material values observed in the products source
MATERIAL_MISSPELLINGS = {
    "Coton": "Cotton",
    "Ruber": "Rubber",
    "Alumium": "Aluminium",
}


# --------------------------------------------------------------------------- #
# Brands
# --------------------------------------------------------------------------- #

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


# --------------------------------------------------------------------------- #
# Category
# --------------------------------------------------------------------------- #

def normalize_category_codes(df):
    """Uppercase category_code.

    Runs before deduplication so that case-variant duplicates are collapsed.
    """
    return df.withColumn("category_code", F.upper(F.col("category_code")))


def deduplicate_categories(df):
    """Keep one row per category_code."""
    return df.dropDuplicates(["category_code"])


# --------------------------------------------------------------------------- #
# Products
# --------------------------------------------------------------------------- #

def parse_product_dimensions(df):
    """Strip unit and format artifacts from physical dimensions, then cast to numeric.

    Weights arrive with a trailing unit suffix ("450g") and lengths use a comma
    decimal separator ("12,5").
    """
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


# --------------------------------------------------------------------------- #
# Customers
# --------------------------------------------------------------------------- #

def drop_rows_missing_customer_id(df):
    """Drop rows with no customer_id.

    Orders belonging to these customers are preserved in the fact table and
    attributed to the Unknown dimension member.
    """
    return df.dropna(subset=["customer_id"])


def fill_missing_phone(df, unknown_value):
    """Replace null phone values with a placeholder."""
    return df.fillna(unknown_value, subset=["phone"])


# --------------------------------------------------------------------------- #
# Calendar
# --------------------------------------------------------------------------- #

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

    Numeric quarter and week_of_year are kept for sorting; the label columns are
    for display. String labels sort incorrectly (Week10 before Week9).
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