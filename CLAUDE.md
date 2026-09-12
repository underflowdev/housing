# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small Python data-munging pipeline that joins Zillow home-value data with FRED per-capita income data at the US county level, producing a single tidy CSV (one row per county-month) suitable for downstream visualization (e.g. price-to-income ratios over time). The code is intentionally simplistic ("definitely contains inaccuracies" per the README) — favor small, direct fixes over refactoring.

## Setup

```
pip install -r requirements.txt
```

Dependencies: `requests`, `pandas`.

## Pipeline / commands

There is no build/lint/test tooling — this is a three-script data pipeline run manually, in order:

1. **`fred_fetch.py`** — fetches per-capita personal income data from FRED (release 175, one element per state) into `data/fred/<State>-<year>-01-01.json`. Reads the API key from `fred_key.txt` (gitignored, not committed — see `# UPDATE:` comment). Flags: `--start-year`/`--end-year` to bound the range, `--force` to re-fetch and overwrite years that already have a file (useful for checking whether a previously-blank/unpublished year has since been released — BEA's county personal income release lags roughly a year, so a given year is often blank on first fetch and populated later), `--limit` to cap the number of requests for a quick test. Sleeps 2s between requests, matching `fred.stlouisfed.org`'s declared `Crawl-delay: 2` (stricter than this repo's 1 req/sec policy floor, so the stricter one wins).
2. Manually download county-level Zillow data (e.g. ZHVI All Homes, Smoothed/Seasonally Adjusted) from https://www.zillow.com/research/data/ and place it at `data/zillow/<filename>`, then update the `zillow_path` variable in `create_output.py` to match.
3. **`build_income.py`** — flattens the FRED JSON files into a fips-keyed income table (`fips, year, qtr, income_est, income_source`), attaching FIPS via `data/nrcs/nrcs_fips_codes.csv` (matched on state abbr + county name, not a hand-built crosswalk — see Architecture notes). FRED's annual figure is broadcast across all 4 quarters of that year. Writes `outputs/income_combined.csv`.
4. **`create_output.py`** — reads the Zillow CSV and `outputs/income_combined.csv`, melts Zillow's wide monthly columns to long form, and merges on `(fips, year, qtr)` so each year's income figure applies to all 12 of that year's Zillow months. Writes `outputs/fips_zillow_income.csv`. Run with:
   ```
   python build_income.py && python create_output.py
   ```

Search the codebase for `# UPDATE:` comments — these mark the specific values/paths a user needs to edit before running (FRED key file, Zillow file path).

## QCEW: parked for later

BLS's Quarterly Census of Employment and Wages was tried as a *quarterly* (sub-annual) income source and works end-to-end, but is currently unused — the active pipeline is FRED-only per above. If sub-annual income granularity is needed again:

- **`qcew_fetch.py`** already fetches it: `https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/10.csv` (industry code 10 = total all industries), one CSV per year/quarter covering every county nationwide in one request, filtered to `own_code == 0` (total covered) and `agglvl_code == 70` (county-level total). Coverage is **2014 Q1 onward only** — confirmed directly against the API; earlier years 404 (BLS's *bulk downloadable* QCEW files go back further, but that's a different, unparsed format). `--start-year`/`--end-year`/`--limit` flags exist for bounded test runs. Output already sits in `data/qcew/` (gitignored — see Fetch policy note below on why data.bls.gov was used despite its robots.txt).
- **`data/qcew/LICENSE.txt`** has BLS's public-domain/attribution policy, saved for citing alongside a map/chart.
- To wire it back in: `build_income.py` would need a QCEW branch (annualize `avg_wkly_wage * 52`, tag `income_source = "qcew_wage_annualized"`) merged alongside the FRED branch, splitting on year (FRED for pre-2014, QCEW for 2014+). This was implemented once already — check git history around the QCEW integration commits for the working version to resurrect rather than rewriting from scratch.
- **Caveat if resurrected:** QCEW's average weekly wage (per covered job) and FRED's per-capita personal income (per resident, all income types) are not the same measure — splicing them creates a real methodology break at the 2014 boundary, not just a data-source swap. Keep an `income_source` column in the output so this is visible/filterable rather than papered over.
- **robots.txt note:** `data.bls.gov` (all paths) and `api.bls.gov` both declare a blanket `Disallow: /`, and `www.bls.gov` actively blocks plain automated requests. FRED (`api.stlouisfed.org`, robots-compliant) mirrors some BLS QCEW series but only at MSA/metro granularity, not per-county — so there's no fully robots.txt-compliant path to county-level QCEW data. Using `data.bls.gov`'s documented Open Data API anyway (treating the disallow as aimed at crawlers, not this documented third-party API) was a deliberate, explicit call — re-confirm that judgment call still holds before reviving this path.

## Fetch policy

Any code that fetches from an external API in this repo (FRED, BLS QCEW, Zillow, or any future source) must:
- Rate-limit to **no more than 1 request per second** as a ceiling — go slower still (e.g. FRED's 2s) if the source's own robots.txt/crawl-delay asks for it — and never issue requests in parallel/concurrently, always a serial loop with a delay between requests.
- Check for and rigorously respect that source's `robots.txt` and any documented rate-limit/usage policy or terms of service before adding or changing a fetch loop.
- Skip re-fetching data already saved to disk (as `qcew_fetch.py` and `fred_fetch.py` already do) rather than re-requesting it, to keep total request volume to a minimum — except when deliberately re-checking a previously-blank/unpublished period (`--force`).

This applies to scripts you write as well as one-off fetches you run yourself (e.g. via `curl`/`WebFetch`) while exploring an API's format.

## Architecture notes

- **FIPS is the join key on the Zillow side, no hand-built crosswalk needed there.** Zillow's county files carry `StateCodeFIPS`/`MunicipalCodeFIPS` directly. FRED has no FIPS of its own, so `build_income.py` attaches it via `data/nrcs/nrcs_fips_codes.csv`, matched on state abbreviation + county name (not the `data/zfmap/` crosswalk the legacy single-script pipeline used — that crosswalk bridged FRED names directly to Zillow names; going through FIPS instead is more direct and doesn't require Zillow-specific name matching). This name-match is imperfect for a handful of counties (e.g. FRED's "... Borough" vs. this crosswalk's "... County" for some Alaska boroughs) — a pre-existing gap, not new.
- **Annual → monthly broadcast:** FRED publishes once a year. `build_income.py` expands each `(fips, year)` income figure to all 4 quarters; `create_output.py` melts Zillow's wide monthly columns to long form, derives `year`/`qtr` from each month's date, and merges on `(fips, year, qtr)` — so one year's income figure ends up applied to all 12 of that year's Zillow months.
- **Recency gap:** BEA's county personal income release lags roughly a year, so the most recent 1-2 years are often blank in a freshly-fetched FRED file until BEA publishes them — re-run `fred_fetch.py --force` on just the recent years periodically to pick up newly-published data (see Pipeline step 1).
- **Output shape:** the output is long/tidy (one row per county-month with a `price_to_income_ratio` column and an `income_source` column), not the old wide format (one row per county with `ratio-<month>` columns per date).
- `data/zillow/*`, `outputs/*`, `data/fred/*.json`, `data/qcew/qcew_*.csv`, and `fred_key.txt` are gitignored — none of these are checked in; they're either regenerated by the fetch/build scripts or (Zillow) must be downloaded manually per Pipeline step 2. `data/qcew/LICENSE.txt` is the one tracked exception in `data/qcew/`.
