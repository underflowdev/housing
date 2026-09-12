# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small Python data-munging pipeline that joins Zillow home-value data with BLS QCEW wage data at the US county level, producing a single tidy CSV (one row per county-month) suitable for downstream visualization (e.g. price-to-income ratios over time). The code is intentionally simplistic ("definitely contains inaccuracies" per the README) — favor small, direct fixes over refactoring.

## Setup

```
pip install -r requirements.txt
```

Dependencies: `requests`, `pandas`.

## Pipeline / commands

There is no build/lint/test tooling — this is a two-script data pipeline run manually, in order:

1. **`qcew_fetch.py`** — fetches county-level average weekly wage data from the BLS QCEW Open Data API (`https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/10.csv`, industry code 10 = total all industries), one CSV per year/quarter into `data/qcew/`. Each request returns *every* county nationwide in one file (no per-state looping needed), filtered down to `own_code == 0` (total covered, all ownerships) and `agglvl_code == 70` (county-level total). Uses `time.sleep(1)` between requests to be polite to the API; skips year/quarter files that already exist, so it's safe to re-run to extend the range.
2. Manually download county-level Zillow data (e.g. ZHVI All Homes, Smoothed/Seasonally Adjusted) from https://www.zillow.com/research/data/ and place it at `data/zillow/<filename>`, then update the `zillow_path` variable in `create_output.py` to match.
3. **`create_output.py`** — reads the Zillow CSV and the QCEW CSVs and merges them directly on 5-digit county FIPS into `outputs/fips_zillow_qcew.csv`. Run with:
   ```
   python create_output.py
   ```

Search the codebase for `# UPDATE:` comments — these mark the specific values/paths a user needs to edit before running (Zillow file path).

## Fetch policy

Any code that fetches from an external API in this repo (FRED, BLS QCEW, Zillow, or any future source) must:
- Rate-limit to **no more than 1 request per second**, and never issue requests in parallel/concurrently — always a serial loop with a delay between requests.
- Check for and rigorously respect that source's `robots.txt` and any documented rate-limit/usage policy or terms of service before adding or changing a fetch loop.
- Skip re-fetching data already saved to disk (as `qcew_fetch.py` and `fred_fetch.py` already do) rather than re-requesting it, to keep total request volume to a minimum.

This applies to scripts you write as well as one-off fetches you run yourself (e.g. via `curl`/`WebFetch`) while exploring an API's format.

## Architecture notes

- **FIPS is the join key, no name-crosswalk needed.** Zillow's county files carry `StateCodeFIPS`/`MunicipalCodeFIPS`, and QCEW carries `area_fips` — both resolve to the same 5-digit county FIPS, so `create_output.py` merges the two sources directly rather than going through a hand-built name map.
- **Quarterly → monthly broadcast:** QCEW only publishes at quarterly granularity. `create_output.py` melts the Zillow wide monthly columns into long form (`fips, date, zhvi`), derives `year`/`qtr` from each month's date, and merges against QCEW's `(fips, year, qtr)` — so each quarter's wage figure is applied to all 3 months in that quarter.
- **Income proxy caveat:** QCEW's `avg_wkly_wage` is wages per *covered job*, annualized as `avg_wkly_wage * 52` (`create_output.py`). This is not the same measure as the old FRED per-capita personal income (no transfer income/dividends/etc., and it's per-job rather than per-resident) — treat `price_to_income_ratio` as a proxy, not a precise recomputation of the old ratio.
- **Output shape changed:** the output is long/tidy (one row per county-month with a `price_to_income_ratio` column), not the old wide format (one row per county with `ratio-<month>` columns per date).
- **Legacy FRED pipeline still in the repo** (`fred_fetch.py`, `data/fred/`, `data/zfmap/`, `data/nrcs/`) — superseded by the QCEW approach above, kept for reference. It required a hand-built `data/zfmap/full_zf_map.csv` crosswalk (FRED county names ↔ Zillow county names) since FRED had no FIPS/consistent naming to join on directly, and its per-capita income data had no 2024/2025 observations (worked around with a +2%/year estimate) — QCEW's quarterly cadence and native FIPS keys resolve both problems.
- `data/zillow/*` and `outputs/*` are gitignored — the Zillow source file and generated outputs are not checked in and must be produced locally.
