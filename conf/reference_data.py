# Databricks notebook source
# MAGIC %md
# MAGIC # Reference Data
# MAGIC
# MAGIC Static business lookups used by the gold layer. No transformation logic
# MAGIC and no widgets — see `conf/config` for environment settings.
# MAGIC
# MAGIC **Provides**
# MAGIC - `COUNTRY_STATE_MAP` — region assignment by country and subdivision code
# MAGIC - `FX_RATES_TO_INR` — fixed currency conversion rates
# MAGIC - `FX_RATE_AS_OF` — snapshot date for the rates above
# MAGIC
# MAGIC **Used by:** `3_dim_gold`, `3_fact_gold`

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}

# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
COUNTRY_STATE_MAP = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# COMMAND ----------

# Fixed FX rates to INR, as of 2025-10-15.
FX_RATE_AS_OF = "2025-10-15"

FX_RATES_TO_INR = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}