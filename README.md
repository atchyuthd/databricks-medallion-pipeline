# E-Commerce Lakehouse — Databricks Medallion Pipeline

A production-shaped batch pipeline that ingests raw e-commerce CSVs, cleanses them
through a medallion architecture, and publishes a dimensional star schema for BI
consumption. Built on Databricks with Unity Catalog and Delta Lake.

Runs end to end on **Databricks Free Edition** with serverless compute — clone the
repo, run eight notebooks in order, and you have the full lakehouse. Sample data is
included.

---

## Architecture

![Pipeline DAG](docs/images/DAG_ecommerce_pipeline.png)

```mermaid
flowchart LR
    subgraph SRC["Source"]
        CSV["CSV files<br/>Unity Catalog Volume"]
    end

    subgraph BRZ["Bronze — raw"]
        B1["brz_brands<br/>brz_category<br/>brz_products<br/>brz_customers<br/>brz_calendar"]
        B2["brz_order_items"]
    end

    subgraph SLV["Silver — cleansed"]
        S1["slv_brands<br/>slv_category<br/>slv_products<br/>slv_customers<br/>slv_calendar"]
        S2["slv_order_items"]
    end

    subgraph GLD["Gold — star schema"]
        G1["gld_dim_products<br/>gld_dim_customers<br/>gld_dim_calendar"]
        G2["gld_fact_order_items"]
    end

    CSV --> B1
    CSV --> B2
    B1 --> S1
    B2 --> S2
    S1 --> G1
    S2 --> G2
    G1 -.-> G2
```

### Layer contracts

Each layer has a defined responsibility, and the boundaries are enforced rather
than incidental.

| Layer | Responsibility | Rules |
|---|---|---|
| **Bronze** | Ingestion | Schema-on-read with explicit `StructType` definitions. No cleansing — anomalies are preserved so the silver layer can be tested against real defects. Audit columns (`_source_file`, `_ingested_at`) added on write. |
| **Silver** | Cleansing | Type casting, deduplication, standardization. No joins and no business logic, so every silver table maps 1:1 to its bronze source. |
| **Gold** | Publishing | Joins, enrichment, derived measures. Publishes business columns only — audit columns stop at silver; lineage is recoverable by joining back on the business key. |

---

## Star schema

`gld_fact_order_items` is the transaction fact at one row per
`(transaction_id, seq_no)`, joined to three conformed dimensions.

```mermaid
erDiagram
    gld_fact_order_items }o--|| gld_dim_customers : customer_id
    gld_fact_order_items }o--|| gld_dim_products : product_id
    gld_fact_order_items }o--|| gld_dim_calendar : date_id

    gld_fact_order_items {
        int date_id FK
        string transaction_id PK
        int seq_no PK
        string customer_id FK
        string product_id FK
        string channel
        int coupon_flag
        int quantity
        double unit_price
        double gross_amount
        double discount_amount
        double net_amount
        double tax_amount
        double total_amount
        double total_amount_inr
    }

    gld_dim_customers {
        string customer_id PK
        string phone
        string country_code
        string country
        string state
        string region
    }

    gld_dim_products {
        string product_id PK
        string sku
        string category_code
        string category_name
        string brand_code
        string brand_name
        string color
        string material
        int weight_grams
        int rating_count
    }

    gld_dim_calendar {
        int date_id PK
        date date
        int year
        string month_name
        string day_name
        int is_weekend
        int quarter
        string quarter_label
        int week_of_year
        string week_label
    }
```

**Measures.** `gross_amount` (quantity × unit price), `discount_amount`,
`net_amount` (pre-tax), `total_amount` (tax-inclusive), and `total_amount_inr`
for cross-currency reporting.

---

## Source data and the defects it contains

The sample data deliberately mirrors the kind of mess real source systems emit.
The silver layer exists to handle it, so the defects are the interesting part.

| Entity | Defects handled |
|---|---|
| **order_items** | Quantities spelled as words (`"Two"`), currency symbols embedded in prices (`$49.99`), percent signs in discount rates (`21%`), two different timestamp formats in the same column, duplicate `(order_id, item_seq)` line items, non-numeric characters in tax amounts |
| **products** | Misspelled materials (`Coton`, `Ruber`, `Alumium`), unit suffixes on numeric weights (`450g`), comma decimal separators (`12,5`), inconsistent code casing, negative and null rating counts |
| **brands** | Non-standard category codes (`GROCERY` vs `GRCY`), whitespace padding, punctuation in brand codes |
| **customers** | Missing customer identifiers, null phone numbers, state codes requiring country-scoped region mapping |
| **calendar** | Negative week-of-year values, duplicate dates, inconsistent day-name casing |

`order_items` arrives as 50+ CSV files in a landing directory, simulating daily
batch drops rather than a single bulk extract.

---

## Repository layout

```
.
├── conf/
│   ├── config                  # Catalog/schema constants, widget parameterization
│   └── reference_data          # Static business lookups (regions, FX rates)
├── data/sample/                # Sample CSVs, mirrors the volume structure
│   ├── brands/
│   ├── category/
│   ├── products/
│   ├── customers/
│   ├── calendar/
│   └── order_items/landing/
└── notebooks/
    ├── 00_catalog_setup        # Unity Catalog objects and volume skeleton
    ├── 01_load_sample_data     # Copies sample CSVs into the raw volume
    ├── 10_dim_bronze           # Dimension ingestion
    ├── 11_fact_bronze          # Order items ingestion
    ├── 20_dim_silver           # Dimension cleansing
    ├── 21_fact_silver          # Order items cleansing
    ├── 30_dim_gold             # Dimension publishing
    └── 31_fact_gold            # Fact publishing
```

---

## Running it

**Prerequisites:** a Databricks workspace (Free Edition is sufficient) with Unity
Catalog enabled, and permission to create a catalog. Clone this repository as a
Databricks Git folder (**Workspace → Create → Git folder**).

### Via the Databricks Job

The repository includes a job definition (`resources/job.yml`) defining the task
DAG: bootstrap tasks run in sequence, then the dimension and fact tracks run in
parallel through bronze, silver, and gold, converging at the fact table. The
target catalog is a single job-level parameter inherited by all eight tasks.

![Successful run](docs/images/ecommerce_pipeline_run.png)

### Manually

Open `notebooks/00_catalog_setup`. A **Target catalog** widget appears at the top,
defaulting to `ecommerce`. Run notebooks `00` through `31` in numerical order,
setting the widget in each.

Every notebook reads its catalog from the same widget, so the entire pipeline can
be pointed at a different environment without editing code. This was verified by
building a complete parallel catalog from empty.

---

## Engineering notes

**Configuration is declared once.** Catalog, schema, and volume paths live in
`conf/config` and are pulled in via `%run`. The catalog name is a widget rather
than a constant, so job parameters can override it at runtime — the same mechanism
a Databricks Asset Bundle would use to deploy across environments.

**Transformations are named functions.** Every cleansing step is a small function
composed with `DataFrame.transform()` rather than a chain of anonymous
reassignments. Each step is individually testable and the pipeline cell reads as a
table of contents:

```python
df_silver_order_items = (
    df_bronze_order_items
    .transform(deduplicate_order_items)
    .transform(parse_quantity)
    .transform(clean_monetary_columns)
    .transform(standardize_categoricals)
    .transform(cast_temporal_columns)
    .withColumn("_processed_at", F.current_timestamp())
)
```

**Bronze is all-string by design** for `order_items`. The source emits mixed
formats that would null out or fail on a typed read; landing them as strings
preserves the raw values and pushes parsing into a layer where it can be handled
explicitly.

**No schema evolution flags.** Every write is a plain `mode("overwrite")`. Earlier
versions carried `mergeSchema: true` to accommodate an in-place type change in the
calendar transformation; that was resolved by emitting label columns alongside the
numeric originals instead of overwriting them. With the flags removed, an
unintended schema change fails loudly rather than being silently absorbed.

**Dimension attributes join on their own keys.** Product category and brand names
are resolved from the product's own `category_code` and `brand_code` rather than
through an intermediate brand-to-category bridge, which avoids both attribute
mismatch and row fan-out when a brand spans multiple categories.

**Column projections are allowlists.** Gold tables use explicit `select()` rather
than `drop()`, so a new upstream column never leaks into a published table.

---

## Design decisions and known limitations

**Dimensions are full-refresh, not slowly-changing.** Each run overwrites the
dimension tables, so customer attribute history is not retained. A production
implementation would use Delta `MERGE` with SCD Type 2 semantics — effective and
expiry dates plus an `is_current` flag — so a customer relocating between regions
preserves the historical attribution of their earlier orders.

**Currency conversion uses a fixed snapshot.** FX rates in `conf/reference_data`
are pinned to a single date (`FX_RATE_AS_OF`). This is adequate for a static
dataset but wrong for a growing one: historical orders should convert at the rate
in effect on their transaction date. The production version is a date-effective
rate dimension joined on transaction date rather than a dictionary lookup.

**The fact table is full-refresh.** `order_items` is read with a glob and
overwritten each run. Auto Loader with checkpointing would give incremental
ingestion, schema evolution, and idempotent reprocessing of the landing directory.

**Reference data lives in code.** Region mappings and FX rates are Python
dictionaries. This is reasonable at their current size, but data that changes on a
different cadence than the code — and that non-engineers may need to edit —
belongs in seed tables loaded at setup.

**Monetary values are stored as `double`.** `DecimalType(18, 2)` is the correct
choice for financial data and would eliminate floating-point drift in aggregates.

---

## Roadmap

- [ ] Databricks Asset Bundle defining the task DAG, replacing manual notebook runs
- [ ] Data quality assertions on grain, key nullity, and referential integrity
- [ ] Transformation functions extracted to an importable package with `pytest` coverage
- [ ] GitHub Actions CI running lint and tests on pull requests
- [ ] SCD Type 2 on the customer dimension
- [ ] Incremental fact ingestion via Auto Loader

---

## Built with

Databricks (Free Edition, serverless) · Unity Catalog · Delta Lake · PySpark · Spark SQL
