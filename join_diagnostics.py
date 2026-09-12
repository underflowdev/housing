# One-off report on how much data is lost on each side of the joins in
# build_income.py / create_output.py: FRED county names that never matched
# a FIPS at all, and counties present in one of {Zillow, FRED-derived income}
# but missing from the other. Not part of the regular pipeline - run manually:
#   python join_diagnostics.py

import json
import os
import re

import pandas as pd

fred_data_dir = "./data/fred"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
zillow_path = "./data/zillow/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
income_path = "./outputs/income_combined.csv"

# fmt: off
state_map = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}
# fmt: on

# --- 1. FRED county names that never matched a FIPS via the NRCS crosswalk ---

fred_names = set()  # (state_abbr, county_name) seen anywhere in the raw FRED JSON
for fred_file in os.listdir(fred_data_dir):
    fred_state = fred_file.split("-")[0]
    fred_abbr = state_map.get(fred_state)
    if fred_abbr is None:
        continue
    with open(os.path.join(fred_data_dir, fred_file)) as f:
        elements = json.load(f).get("elements", {})
    for element in elements.values():
        fred_names.add((fred_abbr, element.get("name")))

fips = pd.read_csv(fips_path, dtype=str)
nrcs_names = set(zip(fips["nrcs_abbr"], fips["nrcs_county"]))

fred_unmatched = fred_names - nrcs_names

print("=== FRED name -> FIPS crosswalk (data/nrcs/nrcs_fips_codes.csv) ===")
print(f"Distinct (state, county) names seen across all FRED files: {len(fred_names)}")
print(f"Of those, never matched to a FIPS code:                   {len(fred_unmatched)}")
print("Sample unmatched names:")
for name in sorted(fred_unmatched)[:15]:
    print("  ", name)
print()

# --- 2. Zillow counties vs FRED-derived income counties (fips-level) ---

zillow = pd.read_csv(zillow_path, dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str})
zillow = zillow.copy()
zillow["fips"] = zillow["StateCodeFIPS"].str.zfill(2) + zillow["MunicipalCodeFIPS"].str.zfill(3)
zillow_fips = set(zillow["fips"])

income = pd.read_csv(income_path, dtype={"fips": str})
income["fips"] = income["fips"].str.zfill(5)
income_fips = set(income["fips"])

zillow_only = zillow_fips - income_fips
income_only = income_fips - zillow_fips
both = zillow_fips & income_fips

print("=== County (FIPS) coverage: Zillow vs FRED-derived income ===")
print(f"Zillow counties:                     {len(zillow_fips)}")
print(f"Income (FRED, FIPS-matched) counties: {len(income_fips)}")
print(f"In both:                              {len(both)}")
print(f"In Zillow but no income data at all:  {len(zillow_only)}  (no ratio possible for these, any year)")
print(f"In income but no Zillow data at all:  {len(income_only)}  (no ratio possible for these, any year)")
print()
print("Sample Zillow-only fips (with name):")
sample = zillow[zillow["fips"].isin(sorted(zillow_only)[:10])][["fips", "RegionName", "StateName"]]
print(sample.to_string(index=False))
print()

# --- 3. Row/tuple-level gaps within the overlapping year range ---
# (restricting to years where FRED has *some* data anywhere, so we're not
# just counting "Zillow has 2025 data and FRED hasn't published 2025 yet" -
# a coverage-lag issue, not a join-matching issue.)

min_income_year = int(income["year"].min())
max_income_year = int(income["year"].max())

DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
date_cols = [c for c in zillow.columns if DATE_COL_RE.match(c)]
zlong = zillow[["fips"] + date_cols].melt(id_vars="fips", var_name="date", value_name="zhvi")
zlong["date"] = pd.to_datetime(zlong["date"])
zlong = zlong[(zlong["date"].dt.year >= min_income_year) & (zlong["date"].dt.year <= max_income_year)]
zlong["year"] = zlong["date"].dt.year
zlong["month"] = zlong["date"].dt.month
z_tuples = zlong[["fips", "year", "month"]].drop_duplicates()

i_tuples = income[["fips", "year", "month"]].drop_duplicates()

merged = pd.merge(z_tuples, i_tuples, on=["fips", "year", "month"], how="outer", indicator=True)
counts = merged["_merge"].value_counts()

print(f"=== (fips, year, month) tuple coverage, restricted to {min_income_year}-{max_income_year} (FRED's full data range) ===")
print(f"Matched (both):                         {counts.get('both', 0)}")
print(f"Zillow has a tuple, income is missing:   {counts.get('left_only', 0)}")
print(f"Income has a tuple, Zillow is missing:   {counts.get('right_only', 0)}")
print("(Most 'income missing Zillow' here is structural: FRED goes back to 1969,")
print(" decades before Zillow's data starts in 2000 - not a join defect.)")
print()

# Same comparison, but restricted to years where *both* sources could plausibly
# have data (Zillow's own start year through FRED's latest year) - this is the
# more meaningful "did the join actually miss something" figure.
zillow_min_year = int(zlong["year"].min()) if len(date_cols) else min_income_year
overlap_start = max(min_income_year, 2000)  # Zillow's file starts in 2000
overlap_end = max_income_year

o_i_tuples = i_tuples[(i_tuples["year"] >= overlap_start) & (i_tuples["year"] <= overlap_end)]
o_z_tuples = z_tuples[(z_tuples["year"] >= overlap_start) & (z_tuples["year"] <= overlap_end)]
overlap_merged = pd.merge(o_z_tuples, o_i_tuples, on=["fips", "year", "month"], how="outer", indicator=True)
overlap_counts = overlap_merged["_merge"].value_counts()

print(f"=== Same, restricted to the actual overlap range {overlap_start}-{overlap_end} ===")
print(f"Matched (both):                         {overlap_counts.get('both', 0)}")
print(f"Zillow has a tuple, income is missing:   {overlap_counts.get('left_only', 0)}")
print(f"Income has a tuple, Zillow is missing:   {overlap_counts.get('right_only', 0)}")
