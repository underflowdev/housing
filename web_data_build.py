# Builds a compact dataset for the static web visualization out of the
# existing pipeline outputs (Zillow + income_combined.csv). Ships one JSON
# file the client loads once:
#   - months: ordered ["YYYY-MM", ...] covering Zillow's full range
#   - counties: [{fips, name, state}, ...]
#   - zhvi: {home_type: counties x months matrix of home values (null where
#     missing)} - one matrix per Zillow home-value series (see zillow_paths
#     below), so the client can switch between them
#   - income: counties x months matrix of income (same shape as one zhvi
#     matrix) - already interpolated month-by-month in build_income.py, not a
#     flat per-year value, so the client doesn't need any year-level logic.
#     Not home_type-specific - income is the same series regardless of which
#     Zillow home-value series it's being compared against.
#   - incomeEstimatedYears: {fips: [year, ...]} - years whose income_est is
#     extrapolated from a QCEW growth rate rather than a real FRED figure
#     (income_source starts with "fred_estimated_"), so the client can flag
#     that portion of the income line rather than presenting it as identical
#     to a real published figure
#
# Run after build_income.py / create_output.py (or standalone - it only
# needs the Zillow CSVs and outputs/income_combined.csv).

import json
import os
import re
import shutil

import pandas as pd

# UPDATE:
# Zillow files to include, keyed by the home_type tag used in zhvi below.
zillow_paths = {
    "all_homes": "./data/zillow/all-homes/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
    "single_family": "./data/zillow/single-family-homes/County_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv",
}
income_path = "./outputs/income_combined.csv"
geo_path = "./data/geo/counties-10m.json"
output_path = "./web/data/dataset.json"
geo_output_path = "./web/data/counties-10m.json"

os.makedirs(os.path.dirname(output_path), exist_ok=True)
shutil.copyfile(geo_path, geo_output_path)

DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def round_or_none(v):
    return None if pd.isna(v) else round(float(v))

# Load the first home_type to establish the county list/order and the set of
# months - all home_type files are assumed to cover the same counties/months
# (both are county-level ZHVI series from the same Zillow release cadence).
first_home_type = next(iter(zillow_paths))
zillow_frames = {}
for home_type, zillow_path in zillow_paths.items():
    zillow = pd.read_csv(zillow_path, dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str})
    zillow = zillow.copy()
    zillow["fips"] = zillow["StateCodeFIPS"].str.zfill(2) + zillow["MunicipalCodeFIPS"].str.zfill(3)
    zillow = zillow.sort_values("fips").reset_index(drop=True)
    zillow_frames[home_type] = zillow

date_cols = sorted([c for c in zillow_frames[first_home_type].columns if DATE_COL_RE.match(c)])
months = [c[:7] for c in date_cols]  # "YYYY-MM-DD" -> "YYYY-MM"

counties = [
    {"fips": row.fips, "name": row.RegionName, "state": row.StateName}
    for row in zillow_frames[first_home_type].itertuples()
]

zhvi_matrices = {}
for home_type, zillow in zillow_frames.items():
    # Reindex to the shared county order in case a home_type's file is
    # missing/orders counties differently, so matrices stay aligned.
    zillow = zillow.set_index("fips").reindex([c["fips"] for c in counties])
    zhvi_matrices[home_type] = [
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
    "zhvi": zhvi_matrices,
    "income": income_matrix,
    "incomeEstimatedYears": estimated_years_by_fips,
}

with open(output_path, "w") as f:
    json.dump(dataset, f, separators=(",", ":"))

print(f"Wrote {output_path}")
print(f"  {len(counties)} counties, {len(months)} months")
print(f"  size: {os.path.getsize(output_path) / 1_000_000:.2f} MB")
