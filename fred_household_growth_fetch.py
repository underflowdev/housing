import argparse
import json
import os
import time

import requests

# Fetches state- and national-level REAL median household income
# (Census/CPS ASEC, mirrored by FRED as release 249: "Real Median Household
# Income by State, Annual") - not for output directly, but as the growth
# RATE build_income.py's household-income branch uses to extrapolate each
# county's last known SAIPE figure into years SAIPE hasn't published yet
# (SAIPE lags similarly to BEA's per-capita release - see CLAUDE.md). This
# is the household-income equivalent of the QCEW-rate extrapolation already
# used for per-capita income, but using an actual household-income measure
# as the rate source instead of QCEW's per-job average weekly wage, so it
# doesn't carry the same per-job-vs-per-household methodology-break caveat.
#
# Only 51 series total (50 states + DC) plus 1 national - small enough to
# always fetch in full, no --states/--limit narrowing needed.

fred_base = "https://api.stlouisfed.org/fred/series/observations"
out_dir = "./data/fred_household_growth"

with open("fred_key.txt") as f:
    api_key = f.read().strip()

# fmt: off
state_abbrs = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
]
# fmt: on

parser = argparse.ArgumentParser(
    description="Fetch state/national real median household income growth-rate series from FRED."
)
parser.add_argument("--start-year", type=int, default=2000)
parser.add_argument("--force", action="store_true")
args = parser.parse_args()

os.makedirs(out_dir, exist_ok=True)

payload = {
    "api_key": api_key,
    "file_type": "json",
    "observation_start": f"{args.start_year}-01-01",
}

REQUEST_DELAY_SECONDS = 2  # same host as fred_fetch.py/fred_household_fetch.py

# "US" (national) plus every state abbr; series id is
# MEHOINUS<abbr>A672N, national is just MEHOINUSA672N (no abbr segment).
targets = [("US", "MEHOINUSA672N")] + [
    (abbr, f"MEHOINUS{abbr}A672N") for abbr in state_abbrs
]

for abbr, series_id in targets:
    path = os.path.join(out_dir, f"{abbr}.json")
    if os.path.exists(path) and not args.force:
        continue

    payload["series_id"] = series_id
    try:
        r = requests.get(fred_base, params=payload, timeout=30)
        body = r.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"{abbr}: request failed - {e}")
        time.sleep(REQUEST_DELAY_SECONDS)
        continue

    if "error_code" in body:
        print(f"{abbr}: {body.get('error_message')}")
    else:
        print(path)
        # Atomic write - see fred_household_fetch.py for why (a half-written
        # file would otherwise look "already fetched" forever).
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as outfile:
            json.dump(body, outfile)
        os.replace(tmp_path, path)

    time.sleep(REQUEST_DELAY_SECONDS)
