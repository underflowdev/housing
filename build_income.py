# Builds a fips-keyed, quarterly-granularity income table from FRED
# per-capita personal income (annual data, broadcast across all 4 quarters
# of each year so it merges the same way a quarterly source would).
#
# QCEW is not currently wired in here - see CLAUDE.md ("QCEW: parked for
# later") for the county-level BLS wage data path (qcew_fetch.py,
# data/qcew/) and how it would plug back into this file if a sub-annual
# income source is needed again.
#
# Output: outputs/income_combined.csv (fips, year, qtr, income_est, income_source)

import json
import os

import pandas as pd

fred_data_dir = "./data/fred"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
output_path = "./outputs/income_combined.csv"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

# fmt: off
state_map = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
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

# --- FRED: per-capita personal income, annual ---

fred_rows = []
for fred_file in os.listdir(fred_data_dir):
    fred_state, fred_year = fred_file.split("-")[0], fred_file.split("-")[1]
    fred_year = int(fred_year)

    fred_abbr = state_map.get(fred_state)
    if fred_abbr is None:
        continue

    with open(os.path.join(fred_data_dir, fred_file)) as f:
        elements = json.load(f).get("elements", {})

    for element in elements.values():
        value = element.get("observation_value")
        if value in (None, "."):
            continue
        fred_rows.append(
            {
                "fred_state_abbr": fred_abbr,
                "fred_county": element.get("name"),
                "year": fred_year,
                "income_est": float(value.replace(",", "")),
            }
        )

fred = pd.DataFrame(fred_rows)

# Attach FIPS by matching state abbr + county name against the NRCS crosswalk.
# Known imperfect: e.g. Alaska boroughs are named differently in FRED
# ("... Borough") vs this crosswalk ("... County"), so a handful of counties
# won't match here - a pre-existing gap in this crosswalk, not new.
fips = pd.read_csv(fips_path, dtype=str)
fred = pd.merge(
    fips,
    fred,
    left_on=["nrcs_abbr", "nrcs_county"],
    right_on=["fred_state_abbr", "fred_county"],
)
fred = fred.rename(columns={"nrcs_fips": "fips"})[["fips", "year", "income_est"]]
fred["income_source"] = "fred_pcpi_annual"

# One row per fips/year -> expand to all 4 quarters so downstream code can
# merge on (fips, year, qtr) the same way it would against a quarterly source.
income = pd.concat([fred.assign(qtr=q) for q in (1, 2, 3, 4)], ignore_index=True)
income = income.sort_values(["fips", "year", "qtr"])
income.to_csv(output_path, index=False, float_format="%11.3f")
