import argparse
import json
import os
import time

import requests

# Fetches county-level median household income (Census SAIPE, mirrored by
# FRED as release 346) - a different measure from fred_fetch.py's per-capita
# personal income (BEA, release 175), fetched a different way too: SAIPE has
# one series PER COUNTY (its whole 1989-present history in one request),
# rather than release 175's one-table-per-state-per-year. FIPS is embedded
# directly in the series id (MHI<state_abbr><fips5>A052NCEN), so this needs
# no name-matching crosswalk the way build_income.py's per-capita branch
# does against data/nrcs/nrcs_fips_codes.csv - it only uses that file here
# as a (fips, state_abbr) list to build series ids from.

fred_base = "https://api.stlouisfed.org/fred/series/observations"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
out_dir = "./data/fred_household"

# UPDATE: put your FRED API key in fred_key.txt (gitignored) - same key
# fred_fetch.py uses. Request one from https://fred.stlouisfed.org/docs/api/api_key.html
with open("fred_key.txt") as f:
    api_key = f.read().strip()

parser = argparse.ArgumentParser(
    description="Fetch county-level median household income (SAIPE) from FRED."
)
parser.add_argument("--start-year", type=int, default=2000)
parser.add_argument(
    "--force",
    action="store_true",
    help="Re-fetch and overwrite counties that already have a file.",
)
parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Max number of requests to make this run (for a quick test), e.g. --limit 5",
)
args = parser.parse_args()

os.makedirs(out_dir, exist_ok=True)

# Census SAIPE only covers the 50 states + DC, not territories - but the
# NRCS crosswalk (built for the per-capita side, where FRED release 175 does
# cover some of these) includes American Samoa (60), Guam (66), Northern
# Mariana Islands (69), Puerto Rico (72), and the US Virgin Islands (78).
# Requesting a SAIPE series for any of these always 400s ("The series does
# not exist") - a real, permanent gap in SAIPE's own coverage, not a fetch
# bug - so skip them upfront rather than burning a request (and a 2s sleep)
# on each one every run.
NON_SAIPE_FIPS_PREFIXES = ("60", "66", "69", "72", "78")

with open(fips_path) as f:
    import csv

    # dict.fromkeys dedupes by fips (keeping the first abbr seen) - the
    # crosswalk has ~29 fips appearing twice under different name spellings
    # (e.g. Virginia independent cities - see CLAUDE.md's Architecture
    # notes), which would otherwise inflate the county count and, under
    # --force, re-fetch the same county twice for no reason.
    counties = list(
        {
            row["nrcs_fips"]: row["nrcs_abbr"]
            for row in csv.DictReader(f)
            if row["nrcs_fips"][:2] not in NON_SAIPE_FIPS_PREFIXES
        }.items()
    )

payload = {
    "api_key": api_key,
    "file_type": "json",
    "observation_start": f"{args.start_year}-01-01",
}

# Same host as fred_fetch.py (api.stlouisfed.org), so the same crawl-delay
# applies - see CLAUDE.md's Fetch policy.
REQUEST_DELAY_SECONDS = 2

fetch_count = 0
for fips, abbr in counties:
    if args.limit is not None and fetch_count >= args.limit:
        break

    path = os.path.join(out_dir, f"{fips}.json")
    if os.path.exists(path) and not args.force:
        continue

    payload["series_id"] = f"MHI{abbr}{fips}A052NCEN"
    try:
        r = requests.get(fred_base, params=payload, timeout=30)
        body = r.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        # A network blip or a non-JSON response (e.g. a 502 HTML page) -
        # log and move on rather than letting one flaky request kill an
        # ~1.8-hour unattended run. This county has no file yet, so a
        # re-run will retry it same as any other not-yet-fetched county.
        print(f"{fips} ({abbr}): request failed - {e}")
        fetch_count += 1
        time.sleep(REQUEST_DELAY_SECONDS)
        continue

    if "error_code" in body:
        # Most commonly: this county has no SAIPE series under this id (a
        # naming mismatch, or a jurisdiction SAIPE doesn't cover) - log and
        # move on rather than writing an error blob that build_income.py
        # would need to special-case.
        print(f"{fips} ({abbr}): {body.get('error_message')}")
    else:
        print(path)
        # Write to a temp file and rename into place (atomic on the same
        # filesystem) rather than writing `path` directly - if the process
        # is killed mid-write, a half-written file at `path` would look
        # "already fetched" to the os.path.exists() check above forever,
        # silently and permanently skipping this county on every future run.
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as outfile:
            json.dump(body, outfile)
        os.replace(tmp_path, path)

    fetch_count += 1
    time.sleep(REQUEST_DELAY_SECONDS)
