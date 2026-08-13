"""Order item transformations for the silver and gold layers.

Each function takes a DataFrame and returns a DataFrame, so they compose with
``DataFrame.transform()`` and can be tested in isolation without Databricks.
Sentinel values are passed as arguments rather than read from notebook scope,
keeping ``conf/config`` the single place they are defined.
"""

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# Quantities the source occasionally emits as words rather than digits
SPELLED_QUANTITIES = {
    "One": "1",
    "Two": "2",
    "Three": "3",
}

# Channel codes mapped to their display names
CHANNEL_NAMES = {
    "web": "Website",
    "app": "Mobile",
}


# --------------------------------------------------------------------------- #
# Silver: cleansing
# --------------------------------------------------------------------------- #

def deduplicate_order_items(df):
    """Keep the most recently landed row per (order_id, item_seq).

    Source may re-emit corrected line items in a later batch. Ordering by
    source filename (which encodes the batch date) makes the winner
    deterministic - _ingested_at is assigned per pipeline run, not per file,
    so it cannot distinguish rows that arrived in different batches.
    """
    window = Window.partitionBy("order_id", "item_seq").orderBy(
        F.col("_source_file").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def parse_quantity(df):
    """Normalize spelled-out quantities to digits, then cast to integer.

    Words outside SPELLED_QUANTITIES cast to null rather than failing, so an
    unexpected spelling surfaces as a missing quantity rather than a wrong one.
    """
    return (
        df
        .replace(SPELLED_QUANTITIES, subset=["quantity"])
        .withColumn("quantity", F.col("quantity").cast("int"))
    )


def clean_monetary_columns(df):
    """Strip currency symbols and percent signs, then cast to numeric."""
    return (
        df
        .withColumn(
            "unit_price", F.regexp_replace("unit_price", "[$]", "").cast("double")
        )
        .withColumn(
            "discount_pct", F.regexp_replace("discount_pct", "%", "").cast("double")
        )
        .withColumn(
            "tax_amount",
            F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double"),
        )
    )


def standardize_categoricals(df):
    """Lowercase coupon codes; map channel codes to display names."""
    return (
        df
        .withColumn("coupon_code", F.lower(F.trim(F.col("coupon_code"))))
        .replace(CHANNEL_NAMES, subset=["channel"])
    )


def cast_temporal_columns(df):
    """Cast date, timestamp, and sequence columns to their proper types.

    The source emits two timestamp layouts for order_ts, so both are attempted
    and coalesced.
    """
    return (
        df
        .withColumn("dt", F.to_date("dt", "yyyy-MM-dd"))
        .withColumn(
            "order_ts",
            F.coalesce(
                F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm"),
            ),
        )
        .withColumn("item_seq", F.col("item_seq").cast("int"))
    )


# --------------------------------------------------------------------------- #
# Gold: measures and publishing
# --------------------------------------------------------------------------- #

def add_revenue_amounts(df):
    """Derive gross, discount, net (pre-tax), and total (tax-inclusive) amounts.

    discount_pct arrives as a whole number, e.g. 21 means 21%.
    """
    return (
        df
        .withColumn(
            "gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
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
    return df.withColumn(
        "date_id", F.date_format("dt", "yyyyMMdd").cast(IntegerType())
    )


def add_coupon_flag(df):
    """Flag rows with a usable coupon code.

    Silver trims coupon_code, so absent values may be empty strings rather
    than nulls - both count as no coupon.
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


def resolve_unknown_customers(df, dim_customers, unknown_key):
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
            F.when(F.col("_matched").isNull(), F.lit(unknown_key))
            .otherwise(F.col("customer_id")),
        )
        .drop("_matched")
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