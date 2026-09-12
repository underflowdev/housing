# Builds a compact dataset for the static web visualization out of the
# existing pipeline outputs (Zillow + income_combined.csv). Ships one JSON
# file the client loads once:
#   - months: ordered ["YYYY-MM", ...] covering Zillow's full range
#   - counties: [{fips, name, state}, ...]
#   - zhvi: counties x months matrix of home values (null where missing)
#   - income: counties x months matrix of income (same shape as zhvi) -
#     already interpolated month-by-month in build_income.py, not a flat
#     per-year value, so the client doesn't need any year-level logic
#   - incomeEstimatedYears: {fips: [year, ...]} - years whose income_est is
#     extrapolated from a QCEW growth rate rather than a real FRED figure
#     (income_source starts with "fred_estimated_"), so the client can flag
#     that portion of the income line rather than presenting it as identical
#     to a real published figure
#
# Run after build_income.py / create_output.py (or standalone - it only
# needs the Zillow CSV and outputs/income_combined.csv).

import json
import os
import re
import shutil

import pandas as pd

zillow_path = "./data/zillow/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
income_path = "./outputs/income_combined.csv"
geo_path = "./data/geo/counties-10m.json"
output_path = "./web/data/dataset.json"
geo_output_path = "./web/data/counties-10m.json"

os.makedirs(os.path.dirname(output_path), exist_ok=True)
shutil.copyfile(geo_path, geo_output_path)

DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

zillow = pd.read_csv(zillow_path, dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str})
zillow = zillow.copy()
zillow["fips"] = zillow["StateCodeFIPS"].str.zfill(2) + zillow["MunicipalCodeFIPS"].str.zfill(3)

date_cols = sorted([c for c in zillow.columns if DATE_COL_RE.match(c)])
months = [c[:7] for c in date_cols]  # "YYYY-MM-DD" -> "YYYY-MM"

zillow = zillow.sort_values("fips").reset_index(drop=True)
counties = [
    {"fips": row.fips, "name": row.RegionName, "state": row.StateName}
    for row in zillow.itertuples()
]

def round_or_none(v):
    return None if pd.isna(v) else round(float(v))

zhvi_matrix = [
    [round_or_none(v) for v in row]
    for row in zillow[date_cols].itertuples(index=False)
]

income = pd.read_csv(income_path, dtype={"fips": str})
income["fips"] = income["fips"].str.zfill(5)
income["year_month"] = income["year"].astype(str) + "-" + income["month"].astype(str).str.zfill(2)

# One row per (fips, year_month) already (see build_income.py), so this
# pivot is exact - no aggregation needed, just reshaping.
income_pivot = income.pivot(index="fips", columns="year_month", values="income_est")
income_pivot = income_pivot.reindex(index=[c["fips"] for c in counties], columns=months)

income_matrix = [
    [round_or_none(v) for v in row] for row in income_pivot.itertuples(index=False)
]

estimated = income[income["income_source"].str.startswith("fred_estimated_")]
estimated_years_by_fips = (
    estimated.groupby("fips")["year"]
    .apply(lambda years: sorted(int(y) for y in years.unique()))
    .to_dict()
)

dataset = {
    "months": months,
    "counties": counties,
    "zhvi": zhvi_matrix,
    "income": income_matrix,
    "incomeEstimatedYears": estimated_years_by_fips,
}

with open(output_path, "w") as f:
    json.dump(dataset, f, separators=(",", ":"))

print(f"Wrote {output_path}")
print(f"  {len(counties)} counties, {len(months)} months")
print(f"  size: {os.path.getsize(output_path) / 1_000_000:.2f} MB")
