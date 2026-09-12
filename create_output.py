# Joins monthly Zillow home values with the combined income table (see
# build_income.py) into a single tidy (long) CSV, one row per county-month,
# suitable for visualization of home-value-to-income ratios over time.
#
# Zillow keys directly on 5-digit county FIPS (StateCodeFIPS + MunicipalCodeFIPS),
# matching the fips column build_income.py produces, so no manual
# name-crosswalk is needed here.
#
# Run build_income.py first to produce outputs/income_combined.csv.

import os
import re

import pandas as pd

pd.set_option("display.max_columns", None)

# UPDATE:
# Add the path to your downloaded zillow file here
# The file I select is normally ZHVI All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted ($), by County
zillow_path = "./data/zillow/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
income_path = "./outputs/income_combined.csv"
final_output_path = "./outputs/fips_zillow_income.csv"

os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# --- Zillow: wide monthly columns -> long (fips, date, zhvi) ---

zillow = pd.read_csv(
    zillow_path,
    dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str},
)
zillow = zillow.copy()
zillow["fips"] = zillow["StateCodeFIPS"].str.zfill(2) + zillow["MunicipalCodeFIPS"].str.zfill(3)

date_cols = [c for c in zillow.columns if DATE_COL_RE.match(c)]
id_cols = ["fips", "RegionName", "StateName"]

zillow_long = zillow[id_cols + date_cols].melt(
    id_vars=id_cols, var_name="date", value_name="zhvi"
)
zillow_long["date"] = pd.to_datetime(zillow_long["date"])
zillow_long["year"] = zillow_long["date"].dt.year
zillow_long["qtr"] = zillow_long["date"].dt.quarter
# Explicit year-month key (e.g. "2023-01") for consumers that key off a single
# global timestamp (e.g. a choropleth's time slider), rather than parsing "date".
zillow_long["year_month"] = zillow_long["date"].dt.strftime("%Y-%m")

# --- income: pre-combined by build_income.py (fips, year, qtr, income_est, income_source) ---

income = pd.read_csv(income_path, dtype={"fips": str})
income["fips"] = income["fips"].str.zfill(5)

# --- merge on (fips, year, qtr): each quarterly income figure maps to its 3 zillow months ---

final = pd.merge(zillow_long, income, on=["fips", "year", "qtr"], how="left")

final["price_to_income_ratio"] = final["zhvi"] / final["income_est"]

final = final[
    [
        "fips",
        "RegionName",
        "StateName",
        "year_month",
        "date",
        "year",
        "qtr",
        "zhvi",
        "income_est",
        "income_source",
        "price_to_income_ratio",
    ]
]
final = final.sort_values(["fips", "date"])
final.to_csv(final_output_path, index=False, float_format="%11.3f")
