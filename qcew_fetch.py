import argparse
import os
import time

import pandas as pd
import requests

# QCEW Open Data API: one CSV per year+quarter covers every county in the US
# (no per-state looping needed, unlike fred_fetch.py).
# https://www.bls.gov/cew/additional-resources/open-data/home.htm
#
# URL shape: https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/{industry_code}.csv
#   qtr: "1", "2", "3", "4" (quarterly) or "a" (annual average)
#   industry_code "10" = Total, all industries
qcew_base = "https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/10.csv"

# own_code 0 = Total covered (private + government combined)
# industry_code "10" = Total, all industries
# area_fips: 5-digit county FIPS for counties (2-digit + "000" for states, "US000" for national)
OWN_CODE_TOTAL = 0
INDUSTRY_CODE_TOTAL = "10"

out_dir = "./data/qcew"

parser = argparse.ArgumentParser(description="Fetch county-level QCEW average weekly wage data.")
# The QCEW Open Data API (as opposed to BLS's bulk downloadable files) only serves
# 2014 Q1 onward; earlier years 404. Confirmed directly against the API.
parser.add_argument("--start-year", type=int, default=2014)
parser.add_argument("--end-year", type=int, default=2025)
parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Max number of year/quarter files to fetch this run (for a quick test), e.g. --limit 2",
)
args = parser.parse_args()

os.makedirs(out_dir, exist_ok=True)

quarters = ["1", "2", "3", "4"]

fetch_count = 0
for year in range(args.start_year, args.end_year + 1):
    for qtr in quarters:
        if args.limit is not None and fetch_count >= args.limit:
            break

        out_path = f"{out_dir}/qcew_{year}_q{qtr}.csv"
        if os.path.exists(out_path):
            continue

        url = qcew_base.format(year=year, qtr=qtr)
        r = requests.get(url)
        if r.status_code != 200:
            print(f"skip {year} q{qtr}: HTTP {r.status_code}")
            fetch_count += 1
            time.sleep(1)
            continue

        raw_path = f"{out_dir}/_raw_{year}_q{qtr}.csv"
        with open(raw_path, "w") as f:
            f.write(r.text)

        df = pd.read_csv(raw_path, dtype=str)
        df = df[
            (df["own_code"].astype(int) == OWN_CODE_TOTAL)
            & (df["industry_code"] == INDUSTRY_CODE_TOTAL)
            & (df["area_fips"].str.len() == 5)
            & (~df["area_fips"].str.endswith("000"))
        ]
        df = df[["area_fips", "year", "qtr", "avg_wkly_wage"]]
        df.to_csv(out_path, index=False)
        os.remove(raw_path)

        print(out_path)
        fetch_count += 1
        time.sleep(1)
    else:
        continue
    break
