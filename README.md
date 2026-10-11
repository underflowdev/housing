# housing vs income

A small Python pipeline that joins Zillow home-value and rent data with FRED income
data — per-capita personal income, and median household income via Census
SAIPE — at the US county level, producing a single tidy CSV (one row per
county-month) suitable for visualization of housing-price-to-income ratios over
time, on either income basis. There's also a static web app (`web/`) that
renders the result as an interactive choropleth.

Definitely contains inaccuracies.

## Setup

```
pip install -r requirements.txt
```

Requires `requests` and `pandas`.

## 1. Get a FRED API key

The income data comes from FRED (Federal Reserve Economic Data), which requires
a free API key:

1. Request one at https://fred.stlouisfed.org/docs/api/api_key.html
2. Save it as a plain-text file named `fred_key.txt` in the repo root (just the
   key, nothing else). This file is gitignored and never committed.

## 2. Fetch the per-capita income data

```
python fred_fetch.py
```

This pulls per-capita personal income (FRED release 175) for every state into
`data/fred/<State>-<year>-01-01.json`, one file per state per year. It sleeps
2 seconds between requests to stay under FRED's crawl-delay policy, so a full
fetch (all states, ~25 years) takes a while — use `--limit N` to do a quick
test run first, and `--start-year`/`--end-year` to narrow the range.

BEA's county income data lags real time by roughly a year, so the most recent
year or two will often come back blank on first fetch. Re-run with `--force`
on just those years later to check whether they've since been published:

```
python fred_fetch.py --start-year 2024 --end-year 2025 --force
```

## 3. Fetch the household income data

```
python fred_household_fetch.py
python fred_household_growth_fetch.py
```

`fred_household_fetch.py` pulls median household income (Census SAIPE, mirrored
by FRED as release 346) into `data/fred_household/<fips>.json` — one request
per county (its whole history in one call, back to `--start-year`, default
2000), since SAIPE's FRED series id already embeds the FIPS code, unlike the
per-capita fetch above. A full run is ~3,150 counties at the same 2s delay, so
also budget a while for this one (~1.5-2 hours from empty); it skips counties
it's already fetched, so it's safe to re-run after an interruption.
`fred_household_growth_fetch.py` pulls a much smaller companion dataset — real
median household income by state and nationally (FRED release 249, ~51
series) — that `build_income.py` uses to extrapolate SAIPE into years it
hasn't published yet, the same role QCEW plays for per-capita income below.

## 4. Fetch QCEW wage data (optional, but recommended)

```
python qcew_fetch.py
```

FRED's own income data typically lags 1-2 years behind the present. This
pulls BLS QCEW's quarterly average-weekly-wage data (2014 Q1 onward — earlier
quarters aren't available via this API) into `data/qcew/qcew_<year>_q<qtr>.csv`,
one request per year/quarter covering every county nationwide. `build_income.py`
uses the year-over-year change in this data as a growth rate to extrapolate
each county's last known FRED figure into the years FRED hasn't published yet.
Use `--limit N` for a quick test, `--start-year`/`--end-year` to narrow the
range. If you skip this step, `build_income.py` still runs fine — those recent
years are simply left blank instead of estimated.

## 5. Download the Zillow data (manual step)

Zillow's data isn't available through an API, so this step is manual:

1. Go to https://www.zillow.com/research/data/
2. Download one or more **county-level** datasets — this pipeline was built
   against "ZHVI All Homes (SFR, Condo/Co-op) Time Series, Smoothed,
   Seasonally Adjusted ($), by County", but any similarly-shaped county-level
   Zillow CSV should work (e.g. the single-family-only variant).
3. Save each file under its own subfolder of `data/zillow/`, named for the
   home-value series it holds, e.g. `data/zillow/all-homes/` and
   `data/zillow/single-family-homes/`.
4. Open `create_output.py` and `web_data_build.py` and update the
   `zillow_paths` dict in each (near the top, marked `# UPDATE:`) so each
   entry's key is a short `home_type` tag (e.g. `all_homes`,
   `single_family`) and its value is the path to the matching file.

## 6. Build the output

```
python build_income.py && python create_output.py
```

- `build_income.py` flattens both FRED-sourced income measures into a
  fips-keyed monthly table: per-capita income (interpolating FRED's annual
  figures into a smooth monthly series, extrapolating years FRED hasn't
  published yet using BLS QCEW wage growth rates) and median household
  income (same interpolation, extrapolated using the release-249
  growth rate fetched in step 3). Writes `outputs/income_combined.csv`.
- `create_output.py` merges that with each configured Zillow file on
  `(fips, year, month)`. Writes `outputs/fips_zillow_income.csv` — the final
  tidy output, one row per county-month-home_type (see `home_type` column)
  with `price_to_income_ratio` (per-capita) and `price_to_household_income_ratio`
  columns.

Search the codebase for `# UPDATE:` comments to find every value/path you may
need to edit before running.

## Optional: check join coverage

```
python join_diagnostics.py
```

One-off report on how well Zillow and FRED-derived income actually match up
(by FIPS) — useful after touching `data/nrcs/nrcs_fips_codes.csv` or
re-fetching FRED data.

## Web visualization

`web/` is a static, no-build-step JS+D3 app that renders the pipeline's output
as an interactive map — a Ratio/Price/Income/Household Income metric toggle,
plus a per-capita/household basis toggle for the Ratio metric. After running
the pipeline above:

```
python web_data_build.py
```

This writes `web/data/dataset.json`. Then serve `web/` with any static file
server, e.g.:

```
cd web && python3 -m http.server
```

and open it in a browser.

## More detail

See `CLAUDE.md` for a full architectural breakdown of each script, the FRED/
QCEW extrapolation logic, and the web app's internals.
