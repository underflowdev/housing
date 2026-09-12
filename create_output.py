# Joins monthly Zillow home values with quarterly QCEW average weekly wages
# into a single tidy (long) CSV, one row per county-month, suitable for
# visualization of home-value-to-income ratios over time.
#
# Both sources key directly on 5-digit county FIPS (Zillow via
# StateCodeFIPS + MunicipalCodeFIPS, QCEW via area_fips), so no manual
# name-crosswalk is needed (unlike the old FRED-based pipeline).

import glob
import os
import re

import pandas as pd

pd.set_option("display.max_columns", None)

# UPDATE:
# Add the path to your downloaded zillow file here
# The file I select is normally ZHVI All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted ($), by County
zillow_path = "./data/zillow/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
qcew_dir = "./data/qcew"
final_output_path = "./outputs/fips_zillow_qcew.csv"

os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# --- Zillow: wide monthly columns -> long (fips, date, zhvi) ---

zillow = pd.read_csv(
    zillow_path,
    dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str},
)
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

# --- QCEW: one file per year/quarter, already long (area_fips, year, qtr, avg_wkly_wage) ---

qcew_files = glob.glob(os.path.join(qcew_dir, "qcew_*.csv"))
qcew = pd.concat(
    (pd.read_csv(f, dtype={"area_fips": str}) for f in qcew_files),
    ignore_index=True,
)
qcew["fips"] = qcew["area_fips"].str.zfill(5)
qcew["year"] = qcew["year"].astype(int)
qcew["qtr"] = qcew["qtr"].astype(int)
qcew["avg_wkly_wage"] = pd.to_numeric(qcew["avg_wkly_wage"], errors="coerce")

# Annualized estimate of income from QCEW's average weekly wage per job.
# Note: this is wages per covered job, not per-capita personal income like
# the old FRED series (no transfer income, dividends, etc. included), and
# each quarter's wage is applied to all 3 months in that quarter since QCEW
# doesn't publish at monthly granularity.
qcew["annual_wage_est"] = qcew["avg_wkly_wage"] * 52

# --- merge on (fips, year, qtr): each QCEW quarterly wage maps to its 3 zillow months ---

final = pd.merge(
    zillow_long,
    qcew[["fips", "year", "qtr", "avg_wkly_wage", "annual_wage_est"]],
    on=["fips", "year", "qtr"],
    how="left",
)

final["price_to_income_ratio"] = final["zhvi"] / final["annual_wage_est"]

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
        "avg_wkly_wage",
        "annual_wage_est",
        "price_to_income_ratio",
    ]
]
final = final.sort_values(["fips", "date"])
final.to_csv(final_output_path, index=False, float_format="%11.3f")
