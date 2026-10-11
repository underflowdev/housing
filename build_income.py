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
# RATE (not a level) taken from a faster-published source of the SAME
# measure where possible:
#   - per-capita income: QCEW average-weekly-wage rate (state, else
#     national) - a different measure (per-job wages, not per-resident
#     income), so this is a rate transfer only, not a level splice (see the
#     QCEW section in CLAUDE.md for why mixing levels directly would be a
#     methodology break). Deliberately NOT the county's own QCEW rate:
#     backtested against 2016-2024 real figures, single-county wage growth
#     barely correlates with county income growth (r ~0.0-0.2) and its noise
#     made estimates worse than a state rate every year.
#   - household income: nominal (current-dollar) median household income
#     rate from FRED release 249 (state, else national - no faster
#     county-level source exists for this measure) - the same concept as
#     SAIPE, just less granular for the most recent year, so no
#     per-job-vs-per-household mismatch to flag. Must be the nominal series
#     (A646N), not the real one (A672N): SAIPE levels are nominal, so a real
#     rate would drop inflation every estimated year.
# All of these are nominal, matching the nominal FRED/SAIPE levels.
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
fred_household_growth_dir = "./data/fred_household_growth"
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
pcpi_max_year = int(pcpi["year"].max())

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

    state_wage = (
        qcew.groupby(["state_abbr", "year", "qtr"])["avg_wkly_wage"].mean().to_dict()
    )
    national_wage = qcew.groupby(["year", "qtr"])["avg_wkly_wage"].mean().to_dict()

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

    qcew_max_year = int(qcew["year"].max())
    pcpi_by_fips = dict(zip(zip(pcpi["fips"], pcpi["year"]), pcpi["income_est"]))
    fips_to_state = dict(zip(fips["nrcs_fips"], fips["nrcs_abbr"]))

    extrapolated_rows = []
    for target_year in range(pcpi_max_year + 1, qcew_max_year + 1):
        base_year = target_year - 1
        for county_fips in pcpi[pcpi["year"] == pcpi_max_year]["fips"].unique():
            base_income = pcpi_by_fips.get((county_fips, base_year))
            if base_income is None:
                continue  # nothing to extrapolate from for this county/year

            state_abbr = fips_to_state.get(county_fips)
            rate, source = None, None
            if state_abbr is not None:
                r = qcew_annual_rate(state_wage, (state_abbr,), base_year, target_year)
                if r is not None:
                    rate, source = r, "fred_estimated_state_qcew_rate"
            if rate is None:
                r = qcew_annual_rate(national_wage, (), base_year, target_year)
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

# --- extrapolate years beyond SAIPE's coverage. Primary source is the
# --- state/national nominal median-household-income growth rate (release
# --- 249, Census CPS ASEC) - the same concept SAIPE itself estimates, just
# --- less granular, so (unlike the QCEW fallback below) there's no
# --- per-job-vs-per-household mismatch to flag. It's published annually
# --- (each September) only a few months ahead of SAIPE (each December), so
# --- it often has no year SAIPE lacks. For years beyond release 249's own
# --- coverage too, fall back to QCEW's nominal
# --- average-weekly-wage rate (state, else national) - the
# --- SAME proxy already used for per-capita income above, and it carries
# --- the SAME caveat (per-job wages, not per-household income - a rate
# --- transfer, not a level splice), tagged distinctly
# --- (household_income_source ending "_qcew_wage_rate") so it's never
# --- mistaken for the same-measure CPS-rate estimate above it in the
# --- fallback order.

growth_files = glob.glob(os.path.join(fred_household_growth_dir, "*.json"))
household_max_year = int(household["year"].max())

if growth_files or qcew_files:
    growth_series = {}
    for growth_file in growth_files:
        abbr = os.path.splitext(os.path.basename(growth_file))[0]
        with open(growth_file) as f:
            observations = json.load(f).get("observations", [])
        by_year = {}
        for obs in observations:
            value = obs.get("value")
            if value in (None, "."):
                continue
            by_year[int(obs["date"][:4])] = float(value)
        growth_series[abbr] = by_year

    def growth_rate(abbr, year_from, year_to):
        by_year = growth_series.get(abbr)
        if not by_year or year_from not in by_year or year_to not in by_year:
            return None
        return by_year[year_to] / by_year[year_from]

    growth_max_year = max(
        (max(by_year) for by_year in growth_series.values() if by_year), default=household_max_year
    )
    # QCEW is quarterly, so it's typically more current than either annual,
    # CPS-ASEC-derived source above - this is what lets extrapolation reach
    # into years neither SAIPE nor release 249 has published yet.
    extrapolation_max_year = max(growth_max_year, qcew_max_year) if qcew_files else growth_max_year

    household_by_fips = dict(
        zip(zip(household["fips"], household["year"]), household["household_income_est"])
    )
    fips_to_state = dict(zip(fips["nrcs_fips"], fips["nrcs_abbr"]))

    extrapolated_rows = []
    for target_year in range(household_max_year + 1, extrapolation_max_year + 1):
        base_year = target_year - 1
        for county_fips in household[household["year"] == household_max_year]["fips"].unique():
            base_income = household_by_fips.get((county_fips, base_year))
            if base_income is None:
                continue

            state_abbr = fips_to_state.get(county_fips)
            rate, source = None, None
            if state_abbr is not None:
                r = growth_rate(state_abbr, base_year, target_year)
                if r is not None:
                    rate, source = r, "saipe_estimated_state_cps_rate"
            if rate is None:
                r = growth_rate("US", base_year, target_year)
                if r is not None:
                    rate, source = r, "saipe_estimated_national_cps_rate"

            # Neither CPS-rate tier has this year yet (release 249 is
            # annual and often no further ahead than SAIPE) - fall back to
            # QCEW's wage rate, same proxy
            # and same rate-transfer-not-level-splice caveat as per-capita's
            # QCEW fallback above, tagged distinctly so it reads as a
            # rougher, cross-measure estimate rather than a CPS rate.
            if rate is None and qcew_files:
                if state_abbr is not None:
                    r = qcew_annual_rate(state_wage, (state_abbr,), base_year, target_year)
                    if r is not None:
                        rate, source = r, "saipe_estimated_state_qcew_wage_rate"
                if rate is None:
                    r = qcew_annual_rate(national_wage, (), base_year, target_year)
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
