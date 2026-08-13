"""Tests for order item transformations."""

from src.transforms.order_items import (
    parse_quantity, 
    clean_monetary_columns, 
    deduplicate_order_items,
    add_revenue_amounts,
    add_coupon_flag,
    resolve_unknown_customers,
)

def test_parse_quantity_converts_spelled_words(spark):
    """Quantities spelled as words are normalized to integers."""
    df = spark.createDataFrame(
        [("O1", "Two"), ("O2", "3"), ("O3", "One")],
        ["order_id", "quantity"],
    )

    result = {row.order_id: row.quantity for row in parse_quantity(df).collect()}

    assert result["O1"] == 2
    assert result["O2"] == 3
    assert result["O3"] == 1


def test_parse_quantity_returns_integer_type(spark):
    """quantity leaves the transformation as an integer, not a string."""
    df = spark.createDataFrame([("O1", "Two")], ["order_id", "quantity"])

    result = parse_quantity(df)

    assert dict(result.dtypes)["quantity"] == "int"

def test_clean_monetary_columns_strips_symbols(spark):
    """Currency symbols and percent signs are removed and values cast to double."""
    df = spark.createDataFrame(
        [("$49.99", "21%", "4.50")],
        ["unit_price", "discount_pct", "tax_amount"],
    )

    row = clean_monetary_columns(df).first()

    assert row.unit_price == 49.99
    assert row.discount_pct == 21.0
    assert row.tax_amount == 4.50


def test_deduplicate_keeps_the_later_batch(spark):
    """A corrected line item in a later file supersedes the original.

    _ingested_at cannot distinguish these rows because Spark assigns it once
    per pipeline run, so deduplication orders by source filename instead.
    """
    df = spark.createDataFrame(
        [
            ("643611", 1, 1, "landing/order_items_2025-08-01.csv"),
            ("643611", 1, 3, "landing/order_items_2025-08-02.csv"),
            ("643611", 2, 1, "landing/order_items_2025-08-01.csv"),
        ],
        ["order_id", "item_seq", "quantity", "_source_file"],
    )

    result = deduplicate_order_items(df)
    rows = {row.item_seq: row.quantity for row in result.collect()}

    assert result.count() == 2, "one row should survive per (order_id, item_seq)"
    assert rows[1] == 3, "the later batch should win"
    assert rows[2] == 1, "unduplicated rows are untouched"

def test_add_revenue_amounts_arithmetic(spark):
    """Gross, discount, net, and total are derived correctly.

    2 x 10.00 = 20.00 gross; 20% off = 4.00 discount; 16.00 net;
    plus 1.50 tax = 17.50 total.
    """
    df = spark.createDataFrame(
        [(2, 10.00, 20.0, 1.50)],
        ["quantity", "unit_price", "discount_pct", "tax_amount"],
    )

    row = add_revenue_amounts(df).first()

    assert row.gross_amount == 20.00
    assert row.discount_amount == 4.00
    assert row.net_amount == 16.00
    assert row.total_amount == 17.50


def test_net_amount_excludes_tax(spark):
    """net_amount is pre-tax; total_amount is tax-inclusive."""
    df = spark.createDataFrame(
        [(1, 100.00, 0.0, 8.25)],
        ["quantity", "unit_price", "discount_pct", "tax_amount"],
    )

    row = add_revenue_amounts(df).first()

    assert row.net_amount == 100.00, "net should not include tax"
    assert row.total_amount == 108.25, "total should include tax"


def test_coupon_flag_treats_empty_string_as_no_coupon(spark):
    """Silver trims coupon codes, so absent values may be '' rather than null."""
    df = spark.createDataFrame(
        [("O1", "save10"), ("O2", ""), ("O3", None)],
        ["order_id", "coupon_code"],
    )

    flags = {row.order_id: row.coupon_flag for row in add_coupon_flag(df).collect()}

    assert flags["O1"] == 1
    assert flags["O2"] == 0, "empty string is not a coupon"
    assert flags["O3"] == 0


def test_resolve_unknown_customers_remaps_orphans(spark):
    """Fact rows with no matching customer are remapped, not dropped."""
    fact = spark.createDataFrame(
        [("C1", 100.00), ("C_MISSING", 50.00)],
        ["customer_id", "total_amount"],
    )
    dim = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])

    result = resolve_unknown_customers(fact, dim, "-1")
    ids = sorted(row.customer_id for row in result.collect())

    assert result.count() == 2, "no rows should be dropped"
    assert ids == ["-1", "C1"]
    assert sum(row.total_amount for row in result.collect()) == 150.00