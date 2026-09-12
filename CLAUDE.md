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
3. **`build_income.py`** — flattens the FRED JSON files into a fips-keyed income table (`fips, year, qtr, income_est, income_source`), attaching FIPS via `data/nrcs/nrcs_fips_codes.csv` (matched on state abbr + county name, not a hand-built crosswalk — see Architecture notes). FRED's annual figure is broadcast across all 4 quarters of that year. For years beyond FRED's own coverage (currently 2025+), it extrapolates each county's last real FRED figure forward using a growth *rate* taken from QCEW's real average-weekly-wage data (`data/qcew/`) — county-level rate if QCEW has that county both years, else state-level, else national — tagged `income_source = fred_estimated_{county,state,national}_qcew_rate`. This is a rate transfer, not splicing QCEW income levels into FRED's series (see QCEW section below for why mixing levels would be a methodology break; a rate multiplier avoids that). Writes `outputs/income_combined.csv`.
4. **`create_output.py`** — reads the Zillow CSV and `outputs/income_combined.csv`, melts Zillow's wide monthly columns to long form, and merges on `(fips, year, qtr)` so each year's income figure applies to all 12 of that year's Zillow months. Writes `outputs/fips_zillow_income.csv`. Run with:
   ```
   python build_income.py && python create_output.py
   ```

Search the codebase for `# UPDATE:` comments — these mark the specific values/paths a user needs to edit before running (FRED key file, Zillow file path).

5. **`join_diagnostics.py`** — one-off report (not part of the regular pipeline, run manually) on how much data is lost joining Zillow ↔ FRED-derived income: names FRED reports that never matched a FIPS via the NRCS crosswalk, counties present in one source but not the other, and (fips, year, qtr) tuple-level coverage. Run it after changing `data/nrcs/nrcs_fips_codes.csv` or re-fetching FRED to check whether match rate improved. As of the last pass: 3,056 of 3,071 Zillow counties matched (99.5%) — remaining gaps are ~15 Virginia jurisdictions (Fairfax County among them) that have no FRED per-capita income entry under any name, a genuine gap in FRED's own release-175 table, not a crosswalk problem.

## Web visualization (`web/`)

A single-page vanilla JS + D3 app (no build step, no framework) — national choropleth with a bottom timeline slider, click a county to drill into its state (county list on the left), click a county there to open a detail inset with dual-line home-value/income charts. Hash-based routing (`#/state/<fips>/<monthIndex>/county/<fips>`) rather than History API pushState, specifically because **this is meant to be deployed as a static site to S3 behind CloudFront** — hash routing needs no server-side/CloudFront rewrite rules for deep links to work, unlike pushState routing which would need a custom-error-response-to-index.html rule.

- **`web_data_build.py`** — reads the Zillow CSV and `outputs/income_combined.csv` and writes `web/data/dataset.json` (counties, months, a counties×months ZHVI matrix, and per-fips-per-year income — income isn't duplicated 12x per year in the payload, the client applies each year's figure to all 12 months) plus copies `data/geo/counties-10m.json` into `web/data/`. Run it after `build_income.py`/`create_output.py`. `web/data/*.json` is gitignored (regenerated, like `outputs/*`) — run this script before serving `web/` locally or deploying it.
- **`web/vendor/`** — D3 v7 and topojson-client, vendored (downloaded, not loaded from a CDN at runtime) so the deployed static site has no external runtime dependency.
- **`web/app.js`** is a single file; state lives in one `state` object (`view`, `stateFips`, `countyFips`, `monthIndex`), hash ↔ state is synced both ways (`applyHash`/`pushHash`), and there's one `render()` entry point that redraws whatever's visible. Kept intentionally simple/flat rather than componentized, matching this repo's existing "don't over-engineer" style.
- **Color scale** is fixed (not month-relative) — computed once from a 2nd/98th-percentile sample across all counties/months, so a color means the same ratio at any point on the timeline, not a re-normalized "this month's relative spread."
- **Default month on load** is the latest month with *any* income coverage, not Zillow's own latest month — FRED's annual income lags Zillow's monthly data by a year or more, so defaulting to Zillow's latest month would show an all-gray "no data" map on first load. See `latestMonthWithIncomeData()`.
- Counties/list entries with no income data for the selected month render gray / grayed-out text (per-month, not just per-county — a county can have data for some months and not others depending on FRED's coverage).
- Deploying: `web/` is the entire deployable unit — sync its contents (after running `web_data_build.py`) to the S3 bucket root.
- **Attribution:** the map has a "Data sources" box (bottom-right of `#map-panel`) linking to Zillow, FRED/BEA, BLS QCEW, and us-atlas/Census TIGER — required/expected attribution for each (Zillow's terms ask for "Data Provided by Zillow Group" cited on every page displaying their data; FRED asks both the source agency and FRED be cited). Update this if a data source changes.

## QCEW: growth-rate source now, parked as a level/income source

BLS's Quarterly Census of Employment and Wages was originally tried as a *quarterly, sub-annual* replacement income source (splicing QCEW wage levels directly in alongside FRED's) — that's parked (see below), the active pipeline is FRED-only for actual income *levels*. It's now used for a narrower purpose: `build_income.py`'s extrapolation step uses QCEW's real average-weekly-wage year-over-year change as a growth *rate* to project FRED's last known figure forward into years FRED hasn't published yet. That distinction matters: a rate transfer doesn't inherit the units mismatch that splicing levels would (see Caveat below).

If sub-annual *income-level* granularity is needed again (the original, larger idea):

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
- **Recency gap:** BEA's county personal income release lags roughly a year, so the most recent 1-2 years are often blank in a freshly-fetched FRED file until BEA publishes them — re-run `fred_fetch.py --force` on just the recent years periodically to pick up newly-published data (see Pipeline step 1). Years past FRED's coverage are extrapolated from QCEW growth rates (see step 3) rather than left blank; if BEA later publishes a real figure for one of those years, re-running `fred_fetch.py --force` for it and rebuilding will replace the estimate with the real value (real FRED rows always take priority — the extrapolation loop only fills years with no real FRED entry for any county).
- **Output shape:** the output is long/tidy (one row per county-month with a `price_to_income_ratio` column and an `income_source` column), not the old wide format (one row per county with `ratio-<month>` columns per date).
- `data/zillow/*`, `outputs/*`, `data/fred/*.json`, `data/qcew/qcew_*.csv`, and `fred_key.txt` are gitignored — none of these are checked in; they're either regenerated by the fetch/build scripts or (Zillow) must be downloaded manually per Pipeline step 2. `data/qcew/LICENSE.txt` is the one tracked exception in `data/qcew/`.
