"""Tests for dimension transformations."""

from src.transforms.dimensions import (
    clean_brand_names,
    normalize_category_codes,
    deduplicate_categories,
    correct_material_spellings,
    clean_rating_counts,
    fill_missing_phone,
    add_period_labels,
    build_region_mapping,
    add_customer_region,
    add_unknown_member,
)


def test_clean_brand_names_trims_and_strips(spark):
    """Brand names lose surrounding whitespace; codes lose punctuation."""
    df = spark.createDataFrame(
        [("  Acme Corp  ", "AC-ME!")],
        ["brand_name", "brand_code"],
    )

    row = clean_brand_names(df).first()

    assert row.brand_name == "Acme Corp"
    assert row.brand_code == "ACME"


def test_category_codes_uppercase_before_dedup(spark):
    """Case-variant duplicates collapse only if normalization runs first."""
    df = spark.createDataFrame(
        [("grcy", "Grocery"), ("GRCY", "Grocery"), ("toy", "Toys")],
        ["category_code", "category_name"],
    )

    result = normalize_category_codes(df).transform(deduplicate_categories)
    codes = sorted(row.category_code for row in result.collect())

    assert codes == ["GRCY", "TOY"], "grcy and GRCY are the same category"


def test_correct_material_spellings(spark):
    """Known misspellings map to canonical values; correct ones pass through."""
    df = spark.createDataFrame(
        [("P1", "Coton"), ("P2", "Ruber"), ("P3", "Cotton")],
        ["product_id", "material"],
    )

    materials = {
        row.product_id: row.material
        for row in correct_material_spellings(df).collect()
    }

    assert materials["P1"] == "Cotton"
    assert materials["P2"] == "Rubber"
    assert materials["P3"] == "Cotton"


def test_clean_rating_counts_handles_negatives_and_nulls(spark):
    """Negative counts become positive; nulls become zero."""
    df = spark.createDataFrame(
        [("P1", -12), ("P2", None), ("P3", 5)],
        ["product_id", "rating_count"],
    )

    counts = {row.product_id: row.rating_count for row in clean_rating_counts(df).collect()}

    assert counts["P1"] == 12
    assert counts["P2"] == 0
    assert counts["P3"] == 5


def test_fill_missing_phone(spark):
    """Null phone numbers get the sentinel; present ones are untouched."""
    df = spark.createDataFrame(
        [("C1", None), ("C2", "555-0100")],
        ["customer_id", "phone"],
    )

    phones = {row.customer_id: row.phone for row in fill_missing_phone(df, "Not Available").collect()}

    assert phones["C1"] == "Not Available"
    assert phones["C2"] == "555-0100"


def test_add_period_labels_preserves_numeric_columns(spark):
    """Labels are added alongside the numerics, not in place of them.

    The numeric columns are kept because string labels sort incorrectly
    (Week10 before Week9).
    """
    df = spark.createDataFrame([(2025, 3, -14)], ["year", "quarter", "week_of_year"])

    result = add_period_labels(df)
    row = result.first()
    types = dict(result.dtypes)

    assert row.quarter_label == "Q3-2025"
    assert row.week_label == "Week14-2025", "negative weeks are absolute-valued"
    assert types["quarter"] == "bigint", "numeric quarter survives"
    assert types["week_of_year"] == "bigint", "numeric week survives"


def test_add_customer_region_falls_back_to_sentinel(spark):
    """Mapped pairs get their region; unmapped pairs get the sentinel."""
    country_state_map = {"India": {"MH": "West", "KA": "South"}}
    mapping = build_region_mapping(spark, country_state_map)

    df = spark.createDataFrame(
        [("C1", "India", "MH"), ("C2", "India", "ZZ")],
        ["customer_id", "country", "state"],
    )

    regions = {
        row.customer_id: row.region
        for row in add_customer_region(df, mapping, "Other").collect()
    }

    assert regions["C1"] == "West"
    assert regions["C2"] == "Other", "unmapped state falls back"


def test_add_unknown_member_appends_one_row(spark):
    """The Unknown member is appended without disturbing real rows."""
    df = spark.createDataFrame(
        [("C1", "555-0100", "IN", "India", "MH", "West")],
        ["customer_id", "phone", "country_code", "country", "state", "region"],
    )

    result = add_unknown_member(df, spark, "-1", "Not Available")
    ids = sorted(row.customer_id for row in result.collect())

    assert result.count() == 2
    assert ids == ["-1", "C1"]