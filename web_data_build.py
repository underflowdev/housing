# Builds a compact dataset for the static web visualization out of the
# existing pipeline outputs (Zillow + income_combined.csv). Ships one JSON
# file the client loads once:
#   - months: ordered ["YYYY-MM", ...] covering Zillow's full range
#   - counties: [{fips, name, state}, ...]
#   - zhvi: {home_type: counties x months matrix of home values (null where
#     missing)} - one matrix per Zillow home-value series (see zillow_paths
#     below), so the client can switch between them
#   - income: counties x months matrix of per-capita income (same shape as
#     one zhvi matrix) - already interpolated month-by-month in
#     build_income.py, not a flat per-year value, so the client doesn't need
#     any year-level logic. Not home_type-specific - income is the same
#     series regardless of which Zillow home-value series it's being
#     compared against.
#   - incomeEstimatedYears: {fips: [year, ...]} - years whose income_est is
#     extrapolated from a QCEW growth rate rather than a real FRED figure
#     (income_source starts with "fred_estimated_"), so the client can flag
#     that portion of the income line rather than presenting it as identical
#     to a real published figure
#   - householdIncome / householdIncomeEstimatedYears: same shape/purpose as
#     income / incomeEstimatedYears, but for median HOUSEHOLD income (Census
#     SAIPE, see build_income.py) rather than per-capita - a second,
#     independent metric/ratio basis in the client, not a replacement for
#     the per-capita figures above.
#   - rent: counties x months matrix of Zillow Observed Rent Index (typical
#     monthly asking rent, $/mo; null where missing - most counties/months,
#     since ZORI covers far fewer counties than ZHVI and only from 2015).
#     Not home_type-specific. Rent burden (rent * 12 / householdIncome) is
#     computed client-side rather than shipped as its own matrix.
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
# UPDATE:
# Zillow Observed Rent Index (ZORI) county file - set to None to ship no rent data.
zillow_rent_path = "./data/zillow/rentals-all-homes/County_zori_uc_sfrcondomfr_sm_month.csv"
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

rent_matrix = None
if zillow_rent_path:
    rent = pd.read_csv(zillow_rent_path, dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str}).copy()
    rent["fips"] = rent["StateCodeFIPS"].str.zfill(2) + rent["MunicipalCodeFIPS"].str.zfill(3)
    # Rename ZORI's "YYYY-MM-DD" columns to "YYYY-MM" and reindex onto the
    # ZHVI-derived county/month axes - counties ZORI doesn't cover, and
    # months outside ZHVI's range (e.g. a ZORI release a month ahead of the
    # ZHVI one), drop out / become null.
    rent_date_cols = [c for c in rent.columns if DATE_COL_RE.match(c)]
    rent = rent.set_index("fips")[rent_date_cols].rename(columns=lambda c: c[:7])
    rent = rent.reindex(index=[c["fips"] for c in counties], columns=months)
    rent_matrix = [[round_or_none(v) for v in row] for row in rent.itertuples(index=False)]

income = pd.read_csv(
    income_path,
    dtype={"fips": str, "income_source": str, "household_income_source": str},
)
income["fips"] = income["fips"].str.zfill(5)
income["year_month"] = income["year"].astype(str) + "-" + income["month"].astype(str).str.zfill(2)

# One row per (fips, year_month) already (see build_income.py), so this
# pivot is exact - no aggregation needed, just reshaping.
income_pivot = income.pivot(index="fips", columns="year_month", values="income_est")
income_pivot = income_pivot.reindex(index=[c["fips"] for c in counties], columns=months)

income_matrix = [
    [round_or_none(v) for v in row] for row in income_pivot.itertuples(index=False)
]

def estimated_years_by_fips(frame, source_col, prefix):
    estimated = frame[frame[source_col].str.startswith(prefix, na=False)]
    return (
        estimated.groupby("fips")["year"]
        .apply(lambda years: sorted(int(y) for y in years.unique()))
        .to_dict()
    )

household_pivot = income.pivot(index="fips", columns="year_month", values="household_income_est")
household_pivot = household_pivot.reindex(index=[c["fips"] for c in counties], columns=months)
household_income_matrix = [
    [round_or_none(v) for v in row] for row in household_pivot.itertuples(index=False)
]

dataset = {
    "months": months,
    "counties": counties,
    "zhvi": zhvi_matrices,
    "income": income_matrix,
    "incomeEstimatedYears": estimated_years_by_fips(income, "income_source", "fred_estimated_"),
    "householdIncome": household_income_matrix,
    "householdIncomeEstimatedYears": estimated_years_by_fips(
        income, "household_income_source", "saipe_estimated_"
    ),
}
if rent_matrix is not None:
    dataset["rent"] = rent_matrix

with open(output_path, "w") as f:
    json.dump(dataset, f, separators=(",", ":"))

print(f"Wrote {output_path}")
print(f"  {len(counties)} counties, {len(months)} months")
print(f"  size: {os.path.getsize(output_path) / 1_000_000:.2f} MB")
