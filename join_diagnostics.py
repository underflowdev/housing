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
# Match rate is driven by county naming/FIPS coverage, not by which Zillow
# home-value series is used, so any one home_type file works here.
zillow_path = "./data/zillow/all-homes/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
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

# --- 2/3. Zillow vs each income measure, fips-level and (fips, year, month)
# --- tuple-level. income_combined.csv is an OUTER join of two measures with
# --- very different coverage (per-capita back to FRED's earliest year,
# --- household only from SAIPE's start year - see build_income.py), so
# --- checking them separately is the only way these numbers mean anything;
# --- blending them (e.g. "does this fips have EITHER measure") would credit
# --- a fips with per-capita coverage just because it happens to have
# --- household data, or vice versa.

zillow = pd.read_csv(zillow_path, dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str})
zillow = zillow.copy()
zillow["fips"] = zillow["StateCodeFIPS"].str.zfill(2) + zillow["MunicipalCodeFIPS"].str.zfill(3)
zillow_fips = set(zillow["fips"])

income = pd.read_csv(
    income_path,
    dtype={"fips": str, "income_source": str, "household_income_source": str},
)
income["fips"] = income["fips"].str.zfill(5)

DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
date_cols = [c for c in zillow.columns if DATE_COL_RE.match(c)]


def report_measure(label, value_col):
    measure = income[income[value_col].notna()]
    measure_fips = set(measure["fips"])

    zillow_only = zillow_fips - measure_fips
    measure_only = measure_fips - zillow_fips
    both = zillow_fips & measure_fips

    print(f"--- {label}: county (FIPS) coverage vs Zillow ---")
    print(f"Zillow counties:                        {len(zillow_fips)}")
    print(f"{label} counties:                        {len(measure_fips)}")
    print(f"In both:                                 {len(both)}")
    print(f"In Zillow but no {label} data at all:    {len(zillow_only)}  (no ratio possible for these, any year)")
    print(f"In {label} but no Zillow data at all:    {len(measure_only)}  (no ratio possible for these, any year)")
    if zillow_only:
        print(f"Sample Zillow-only fips (with name), missing from {label}:")
        sample = zillow[zillow["fips"].isin(sorted(zillow_only)[:10])][["fips", "RegionName", "StateName"]]
        print(sample.to_string(index=False))
    print()

    if measure.empty:
        print(f"({label} has no data at all yet - skipping tuple-level check)")
        print()
        return

    min_year = int(measure["year"].min())
    max_year = int(measure["year"].max())

    zlong = zillow[["fips"] + date_cols].melt(id_vars="fips", var_name="date", value_name="zhvi")
    zlong["date"] = pd.to_datetime(zlong["date"])
    zlong = zlong[(zlong["date"].dt.year >= min_year) & (zlong["date"].dt.year <= max_year)]
    zlong["year"] = zlong["date"].dt.year
    zlong["month"] = zlong["date"].dt.month
    z_tuples = zlong[["fips", "year", "month"]].drop_duplicates()

    i_tuples = measure[["fips", "year", "month"]].drop_duplicates()

    merged = pd.merge(z_tuples, i_tuples, on=["fips", "year", "month"], how="outer", indicator=True)
    counts = merged["_merge"].value_counts()

    print(f"--- {label}: (fips, year, month) tuple coverage, restricted to {min_year}-{max_year} ({label}'s own data range) ---")
    print(f"Matched (both):                          {counts.get('both', 0)}")
    print(f"Zillow has a tuple, {label} is missing:  {counts.get('left_only', 0)}")
    print(f"{label} has a tuple, Zillow is missing:  {counts.get('right_only', 0)}")
    print(f"(Zillow starts in 2000; {label} starts in {min_year} - a 'missing Zillow' tuple")
    print(" before 2000 is structural lag, not a join defect. A large 'missing")
    print(f" {label}' count within {label}'s own {min_year}-{max_year} range, though, points at a real gap.)")
    print()


report_measure("per-capita income", "income_est")
report_measure("household income", "household_income_est")
