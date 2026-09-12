# housing vs income

A very simplistic bit of Python to munge data from Zillow (https://www.zillow.com/research/data/) and BLS QCEW (https://www.bls.gov/cew/additional-resources/open-data/home.htm) and map them into a single output file, suitable for visualization.

Definitely contains inaccuracies.

Both sources key directly on 5-digit county FIPS codes (Zillow via its `StateCodeFIPS`/`MunicipalCodeFIPS` columns, QCEW via `area_fips`), so no manual name-crosswalk is needed. QCEW publishes average weekly wage per covered job quarterly rather than annually, so each quarter's wage is applied to all 3 months in that quarter to keep the output at Zillow's monthly granularity. The output is long/tidy (one row per county-month), not one row per county with wide date columns.

See comments of # UPDATE: for the items that you'll need to update.

See qcew_fetch.py if you want to re-fetch the wage data.

You'll need to download a fresh Zillow data file from https://www.zillow.com/research/data/ and put it at /data/zillow/<filename> and update paths in create_output.py, the zillow_path variable.  Remember, get the COUNTY level data.

## Legacy FRED-based pipeline

`fred_fetch.py`, `data/fred/`, `data/zfmap/`, and `data/nrcs/` are the previous approach: per-capita income from FRED, hand-matched to Zillow county names via `data/zfmap/full_zf_map.csv`, annual only, and blank for 2024/2025 (worked around with a 2%/year estimate). Kept for reference but superseded by the QCEW pipeline above.
