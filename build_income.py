# Builds a fips-keyed, MONTHLY income table combining two FRED-sourced
# measures: per-capita personal income (BEA, release 175) and median
# household income (Census SAIPE, release 346, fetched by
# fred_household_fetch.py) - both published annually. Rather than
# broadcasting each year's figure flat across its 12 months (a step function
# - visibly "jumps" at every year boundary, both in a per-county chart and
# at the choropleth's yearly color transitions), each annual figure is
# anchored at the middle of its year (July 1) and linearly interpolated
# between consecutive years' anchors to produce a smooth monthly series.
# Months before the first anchor or after the last hold flat at the nearest
# anchor's value (no invented trend beyond the known data range). Both
# measures go through the same interpolate_monthly() helper.
#
# Neither source is published for the most recent year(s) at fetch time
# (BEA/SAIPE both lag roughly a year). For those later years, each measure
# is extrapolated forward from each county's last real figure using a growth
# RATE (not a level) from QCEW's quarterly average weekly wage (state, else
# national) - a different measure (per-job wages, not per-resident or
# per-household income), so this is a rate transfer only, not a level
# splice (see the QCEW section in CLAUDE.md for why mixing levels directly
# would be a methodology break). Both QCEW and the FRED/SAIPE levels are
# nominal. Backtested against 2016-2024 real figures, this beat:
#   - the county's own QCEW rate: single-county wage growth barely
#     correlates with county income growth (r ~0.0-0.2), and its noise made
#     estimates worse than the state rate every year.
#   - (household) Census's state/national median household income growth
#     (CPS ASEC) - small state samples made it the noisiest option.
# The interpolation happens after extrapolation, so estimated years become
# anchors too.
#
# Output: outputs/income_combined.csv
#   (fips, year, month,
#    income_est, income_source,
#    household_income_est, household_income_source)

import glob
import json
import os

import numpy as np
import pandas as pd

fred_data_dir = "./data/fred"
fred_household_dir = "./data/fred_household"
qcew_dir = "./data/qcew"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
output_path = "./outputs/income_combined.csv"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

fips = pd.read_csv(fips_path, dtype=str)

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


def interpolate_monthly(annual, value_col, source_col):
    """annual: DataFrame with fips, year, <value_col>, <source_col> - one row
    per fips/year (the "anchor" for that year). Returns a monthly frame
    (fips, year, month, <value_col>, <source_col>), linearly interpolated
    between each county's year-anchors (each placed at that year's midpoint,
    July 1), holding flat before the first anchor / after the last."""
    min_year = int(annual["year"].min())
    max_year = int(annual["year"].max())
    target_years = np.repeat(np.arange(min_year, max_year + 1), 12)
    target_months = np.tile(np.arange(1, 13), max_year - min_year + 1)
    target_x = target_years + (target_months - 0.5) / 12

    monthly_rows = []
    for county_fips, grp in annual.groupby("fips"):
        grp = grp.sort_values("year")
        anchor_x = grp["year"].to_numpy(dtype=float) + 0.5
        anchor_y = grp[value_col].to_numpy()
        anchor_source = grp[source_col].to_numpy()

        interp_y = np.interp(target_x, anchor_x, anchor_y)
        # Tag each interpolated month with its nearest anchor's source, so an
        # interpolated month between a real year and an estimated year still
        # reads as (mostly) one or the other rather than losing that
        # distinction entirely.
        nearest_idx = np.abs(anchor_x[:, None] - target_x[None, :]).argmin(axis=0)

        monthly_rows.append(
            pd.DataFrame(
                {
                    "fips": county_fips,
                    "year": target_years,
                    "month": target_months,
                    value_col: interp_y,
                    source_col: anchor_source[nearest_idx],
                }
            )
        )

    return pd.concat(monthly_rows, ignore_index=True)


# ============================================================
# --- per-capita personal income (FRED/BEA release 175) ---
# ============================================================

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

pcpi = pd.DataFrame(fred_rows)

# Attach FIPS by matching state abbr + county name against the NRCS crosswalk.
# Known imperfect: e.g. Alaska boroughs are named differently in FRED
# ("... Borough") vs this crosswalk ("... County"), so a handful of counties
# won't match here - a pre-existing gap in this crosswalk, not new.
pcpi = pd.merge(
    fips,
    pcpi,
    left_on=["nrcs_abbr", "nrcs_county"],
    right_on=["fred_state_abbr", "fred_county"],
)
pcpi = pcpi.rename(columns={"nrcs_fips": "fips"})[["fips", "year", "income_est"]]
pcpi["income_source"] = "fred_pcpi_annual"

# --- extrapolate years beyond FRED's coverage using QCEW growth rates ---

qcew_files = glob.glob(os.path.join(qcew_dir, "qcew_*.csv"))

if qcew_files:
    qcew = pd.concat(
        (pd.read_csv(f, dtype={"area_fips": str}) for f in qcew_files),
        ignore_index=True,
    )
    qcew["fips"] = qcew["area_fips"].str.zfill(5)
    # qcew_fetch.py's saved files also carry metro areas ("C1010", ...) and
    # each state's "statewide, unknown" xx999 row - keep real counties only,
    # or they leak into the state/national rates below.
    qcew = qcew[qcew["fips"].str.isdigit() & ~qcew["fips"].str.endswith("999")].copy()
    qcew["year"] = qcew["year"].astype(int)
    qcew["qtr"] = qcew["qtr"].astype(int)
    qcew["avg_wkly_wage"] = pd.to_numeric(qcew["avg_wkly_wage"], errors="coerce")

    fips_to_abbr = dict(zip(fips["nrcs_fips"].str[:2], fips["nrcs_abbr"]))
    qcew["state_abbr"] = qcew["fips"].map(lambda f: fips_to_abbr.get(f[:2]))

    county_wage = (
        qcew.dropna(subset=["avg_wkly_wage"])
        .set_index(["fips", "year", "qtr"])["avg_wkly_wage"]
        .to_dict()
    )
    state_counties = qcew.groupby("state_abbr")["fips"].unique().to_dict()

    def qcew_annual_rate(wage_lookup, key, year_from, year_to):
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

    state_counties["US"] = qcew["fips"].unique()
    state_rate_cache = {}

    def qcew_state_rate(state_abbr, year_from, year_to):
        # Median of the state's per-county rates, over counties present in
        # both years - not a ratio of state-average wage levels, which breaks
        # when a state's county set changes (Connecticut's QCEW switched from
        # 8 counties to 9 planning regions in 2024, so 2023 vs 2024 averages
        # compare different areas). No county in both years -> None, and the
        # caller falls back to national ("US": every county, same method).
        key = (state_abbr, year_from, year_to)
        if key not in state_rate_cache:
            rates = [
                qcew_annual_rate(county_wage, (f,), year_from, year_to)
                for f in state_counties.get(state_abbr, [])
            ]
            rates = [r for r in rates if r is not None]
            state_rate_cache[key] = float(np.median(rates)) if rates else None
        return state_rate_cache[key]

    qcew_max_year = int(qcew["year"].max())
    pcpi_by_fips = dict(zip(zip(pcpi["fips"], pcpi["year"]), pcpi["income_est"]))
    fips_to_state = dict(zip(fips["nrcs_fips"], fips["nrcs_abbr"]))

    # Each county is extended from its OWN last real year, not the global
    # latest one - a county whose data stops early (Connecticut's switch to
    # planning regions, a BEA-suppressed year) would otherwise be held flat
    # at a years-old value and still read as real data.
    pcpi_last_year = pcpi.groupby("fips")["year"].max().to_dict()

    extrapolated_rows = []
    for target_year in range(min(pcpi_last_year.values()) + 1, qcew_max_year + 1):
        base_year = target_year - 1
        for county_fips, last_year in pcpi_last_year.items():
            if target_year <= last_year:
                continue
            base_income = pcpi_by_fips.get((county_fips, base_year))
            if base_income is None:
                continue  # nothing to extrapolate from for this county/year

            state_abbr = fips_to_state.get(county_fips)
            rate, source = None, None
            if state_abbr is not None:
                r = qcew_state_rate(state_abbr, base_year, target_year)
                if r is not None:
                    rate, source = r, "fred_estimated_state_qcew_rate"
            if rate is None:
                r = qcew_state_rate("US", base_year, target_year)
                if r is not None:
                    rate, source = r, "fred_estimated_national_qcew_rate"

            if rate is None:
                continue  # even national QCEW has no comparable data - leave blank

            estimated_income = base_income * rate
            pcpi_by_fips[(county_fips, target_year)] = estimated_income
            extrapolated_rows.append(
                {
                    "fips": county_fips,
                    "year": target_year,
                    "income_est": estimated_income,
                    "income_source": source,
                }
            )

    if extrapolated_rows:
        pcpi = pd.concat([pcpi, pd.DataFrame(extrapolated_rows)], ignore_index=True)

pcpi_monthly = interpolate_monthly(pcpi, "income_est", "income_source")

# ============================================================
# --- median household income (FRED/Census SAIPE release 346) ---
# ============================================================

household_rows = []
for saipe_file in glob.glob(os.path.join(fred_household_dir, "*.json")):
    county_fips = os.path.splitext(os.path.basename(saipe_file))[0]
    with open(saipe_file) as f:
        observations = json.load(f).get("observations", [])
    for obs in observations:
        value = obs.get("value")
        if value in (None, "."):
            continue
        household_rows.append(
            {
                "fips": county_fips,
                "year": int(obs["date"][:4]),
                "household_income_est": float(value),
                "household_income_source": "saipe_household_annual",
            }
        )

household = pd.DataFrame(household_rows)

# --- extrapolate years beyond SAIPE's coverage, using the same QCEW wage
# --- rate as per-capita income above (state, else national) - a rate
# --- transfer, not a level splice, but per-job wages rather than
# --- per-household income, so tagged distinctly (household_income_source
# --- ending "_qcew_wage_rate"). Census's own state median household income
# --- series (CPS ASEC, FRED release 249) was tried as the rate source
# --- first (since removed), but
# --- backtested against 2016-2024 SAIPE it was the least accurate option:
# --- mean abs error 6.1% at state level (small CPS state samples) and 4.9%
# --- national, vs ~4.6% for the QCEW rate.

if qcew_files:
    household_by_fips = dict(
        zip(zip(household["fips"], household["year"]), household["household_income_est"])
    )

    # Per county from its own last real year - see the per-capita loop above.
    household_last_year = household.groupby("fips")["year"].max().to_dict()

    extrapolated_rows = []
    for target_year in range(min(household_last_year.values()) + 1, qcew_max_year + 1):
        base_year = target_year - 1
        for county_fips, last_year in household_last_year.items():
            if target_year <= last_year:
                continue
            base_income = household_by_fips.get((county_fips, base_year))
            if base_income is None:
                continue

            state_abbr = fips_to_state.get(county_fips)
            rate, source = None, None
            if state_abbr is not None:
                r = qcew_state_rate(state_abbr, base_year, target_year)
                if r is not None:
                    rate, source = r, "saipe_estimated_state_qcew_wage_rate"
            if rate is None:
                r = qcew_state_rate("US", base_year, target_year)
                if r is not None:
                    rate, source = r, "saipe_estimated_national_qcew_wage_rate"

            if rate is None:
                continue

            estimated_income = base_income * rate
            household_by_fips[(county_fips, target_year)] = estimated_income
            extrapolated_rows.append(
                {
                    "fips": county_fips,
                    "year": target_year,
                    "household_income_est": estimated_income,
                    "household_income_source": source,
                }
            )

    if extrapolated_rows:
        household = pd.concat([household, pd.DataFrame(extrapolated_rows)], ignore_index=True)

household_monthly = interpolate_monthly(
    household, "household_income_est", "household_income_source"
)

# ============================================================
# --- combine and write ---
# ============================================================

income = pd.merge(pcpi_monthly, household_monthly, on=["fips", "year", "month"], how="outer")
income = income.sort_values(["fips", "year", "month"])
income.to_csv(output_path, index=False, float_format="%11.3f")
