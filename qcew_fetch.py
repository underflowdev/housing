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
os.makedirs(out_dir, exist_ok=True)

start_year = 2001
end_year = 2025
quarters = ["1", "2", "3", "4"]

for year in range(start_year, end_year + 1):
    for qtr in quarters:
        out_path = f"{out_dir}/qcew_{year}_q{qtr}.csv"
        if os.path.exists(out_path):
            continue

        url = qcew_base.format(year=year, qtr=qtr)
        r = requests.get(url)
        if r.status_code != 200:
            print(f"skip {year} q{qtr}: HTTP {r.status_code}")
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
        time.sleep(1)
