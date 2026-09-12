# Builds a fips-keyed, quarterly-granularity income table from FRED
# per-capita personal income (annual data, broadcast across all 4 quarters
# of each year so it merges the same way a quarterly source would).
#
# FRED's own income data is not currently published for years beyond
# FRED_MAX_YEAR below (BEA's county personal income release lags roughly a
# year). For those later years, this extrapolates forward from each
# county's last real FRED figure using a growth RATE (not a level) taken
# from QCEW's real average-weekly-wage data, which is available more
# recently: county rate if QCEW has that county both years, else state
# rate, else national rate. This is a rate transfer, not splicing QCEW
# levels into FRED's series (see the QCEW section in CLAUDE.md for why
# mixing levels directly would be a methodology break).
#
# Output: outputs/income_combined.csv (fips, year, qtr, income_est, income_source)

import glob
import json
import os

import pandas as pd

fred_data_dir = "./data/fred"
qcew_dir = "./data/qcew"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
output_path = "./outputs/income_combined.csv"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

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

# --- extrapolate years beyond FRED's coverage using QCEW growth rates ---

qcew_files = glob.glob(os.path.join(qcew_dir, "qcew_*.csv"))
fred_max_year = int(fred["year"].max())

if qcew_files:
    qcew = pd.concat(
        (pd.read_csv(f, dtype={"area_fips": str}) for f in qcew_files),
        ignore_index=True,
    )
    qcew["fips"] = qcew["area_fips"].str.zfill(5)
    qcew["year"] = qcew["year"].astype(int)
    qcew["qtr"] = qcew["qtr"].astype(int)
    qcew["avg_wkly_wage"] = pd.to_numeric(qcew["avg_wkly_wage"], errors="coerce")

    fips_to_abbr = dict(zip(fips["nrcs_fips"].str[:2], fips["nrcs_abbr"]))
    qcew["state_abbr"] = qcew["fips"].map(lambda f: fips_to_abbr.get(f[:2]))

    county_wage = qcew.set_index(["fips", "year", "qtr"])["avg_wkly_wage"].to_dict()
    state_wage = (
        qcew.groupby(["state_abbr", "year", "qtr"])["avg_wkly_wage"].mean().to_dict()
    )
    national_wage = qcew.groupby(["year", "qtr"])["avg_wkly_wage"].mean().to_dict()

    def annual_rate(wage_lookup, key, year_from, year_to):
        # Compare only quarters present in year_to (year_to may be a
        # partial year, e.g. 2026 with only Q1 published so far) against
        # the same quarters in year_from, so a partial year isn't biased
        # by seasonal effects.
        quarters_to = [q for q in (1, 2, 3, 4) if (key + (year_to, q)) in wage_lookup]
        if not quarters_to:
            return None
        vals_from = [wage_lookup.get(key + (year_from, q)) for q in quarters_to]
        if any(v is None for v in vals_from):
            return None
        vals_to = [wage_lookup[key + (year_to, q)] for q in quarters_to]
        return (sum(vals_to) / len(vals_to)) / (sum(vals_from) / len(vals_from))

    qcew_max_year = int(qcew["year"].max())
    fred_income_by_fips = dict(
        zip(zip(fred["fips"], fred["year"]), fred["income_est"])
    )
    fips_to_state = dict(zip(fips["nrcs_fips"], fips["nrcs_abbr"]))

    extrapolated_rows = []
    for target_year in range(fred_max_year + 1, qcew_max_year + 1):
        base_year = target_year - 1
        for county_fips in fred[fred["year"] == fred_max_year]["fips"].unique():
            base_income = fred_income_by_fips.get((county_fips, base_year))
            if base_income is None:
                continue  # nothing to extrapolate from for this county/year

            state_abbr = fips_to_state.get(county_fips)
            rate, source = None, None
            r = annual_rate(county_wage, (county_fips,), base_year, target_year)
            if r is not None:
                rate, source = r, "fred_estimated_county_qcew_rate"
            elif state_abbr is not None:
                r = annual_rate(state_wage, (state_abbr,), base_year, target_year)
                if r is not None:
                    rate, source = r, "fred_estimated_state_qcew_rate"
            if rate is None:
                r = annual_rate(national_wage, (), base_year, target_year)
                if r is not None:
                    rate, source = r, "fred_estimated_national_qcew_rate"

            if rate is None:
                continue  # even national QCEW has no comparable data - leave blank

            estimated_income = base_income * rate
            fred_income_by_fips[(county_fips, target_year)] = estimated_income
            extrapolated_rows.append(
                {
                    "fips": county_fips,
                    "year": target_year,
                    "income_est": estimated_income,
                    "income_source": source,
                }
            )

    if extrapolated_rows:
        fred = pd.concat([fred, pd.DataFrame(extrapolated_rows)], ignore_index=True)

# One row per fips/year -> expand to all 4 quarters so downstream code can
# merge on (fips, year, qtr) the same way it would against a quarterly source.
income = pd.concat([fred.assign(qtr=q) for q in (1, 2, 3, 4)], ignore_index=True)
income = income.sort_values(["fips", "year", "qtr"])
income.to_csv(output_path, index=False, float_format="%11.3f")
