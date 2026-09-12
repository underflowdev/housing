import argparse
import os
import time

import requests

fred_base = "https://api.stlouisfed.org/fred/release/tables"

# UPDATE: put your FRED API key in fred_key.txt (gitignored).
# Request one from https://fred.stlouisfed.org/docs/api/api_key.html
with open("fred_key.txt") as f:
    api_key = f.read().strip()

file_type = "json"
include_observation_values = "true"

release_id = "175"

payload = {
    "api_key": api_key,
    "file_type": file_type,
    "include_observation_values": include_observation_values,
    "release_id": release_id,
}

# fmt: off
states = [
    {"key": "266091", "name": "Alabama"},
    {"key": "266159", "name": "Alaska"},
    {"key": "266213", "name": "Arizona"},
    {"key": "266229", "name": "Arkansas"},
    {"key": "266305", "name": "California"},
    {"key": "266364", "name": "Colorado"},
    {"key": "266429", "name": "Connecticut"},
    {"key": "266438", "name": "Delaware"},
    # 266442 is also labeled "Delaware" by FRED's own table name, but its
    # element is actually the District of Columbia - a bug in the original
    # hand-built list here clobbered real Delaware data with this every
    # fetch (both wrote to "Delaware-<date>.json"). Kept separate now.
    {"key": "266442", "name": "District of Columbia"},
    {"key": "266444", "name": "Florida"},
    {"key": "266512", "name": "Georgia"},
    {"key": "266672", "name": "Hawaii"},
    {"key": "266677", "name": "Idaho"},
    {"key": "266722", "name": "Illinois"},
    {"key": "266825", "name": "Indiana"},
    {"key": "266918", "name": "Iowa"},
    {"key": "267018", "name": "Kansas"},
    {"key": "267124", "name": "Kentucky"},
    {"key": "267245", "name": "Louisiana"},
    {"key": "267310", "name": "Maine"},
    {"key": "267327", "name": "Maryland"},
    {"key": "267352", "name": "Massachusetts"},
    {"key": "267367", "name": "Michigan"},
    {"key": "267451", "name": "Minnesota"},
    {"key": "267539", "name": "Mississippi"},
    {"key": "267622", "name": "Missouri"},
    {"key": "267738", "name": "Montana"},
    {"key": "267795", "name": "Nebraska"},
    {"key": "267889", "name": "Nevada"},
    {"key": "267907", "name": "New Hampshire"},
    {"key": "267918", "name": "New Jersey"},
    {"key": "267940", "name": "New Mexico"},
    {"key": "267974", "name": "New York"},
    {"key": "268037", "name": "North Carolina"},
    {"key": "268138", "name": "North Dakota"},
    {"key": "268192", "name": "Ohio"},
    {"key": "268281", "name": "Oklahoma"},
    {"key": "268359", "name": "Oregon"},
    {"key": "268396", "name": "Pennsylvania"},
    {"key": "268464", "name": "Rhode Island"},
    {"key": "268470", "name": "South Carolina"},
    {"key": "268517", "name": "South Dakota"},
    {"key": "268584", "name": "Tennessee"},
    {"key": "268680", "name": "Texas"},
    {"key": "268935", "name": "Utah"},
    {"key": "268965", "name": "Vermont"},
    {"key": "268980", "name": "Virginia"},
    {"key": "269081", "name": "Washington"},
    {"key": "269121", "name": "West Virginia"},
    {"key": "269177", "name": "Wisconsin"},
    {"key": "269251", "name": "Wyoming"},
]
# fmt: on

parser = argparse.ArgumentParser(description="Fetch county-level per-capita income data from FRED.")
parser.add_argument("--start-year", type=int, default=2001)
parser.add_argument("--end-year", type=int, default=2025)
parser.add_argument(
    "--force",
    action="store_true",
    help="Re-fetch and overwrite years that already have a file (e.g. to check if a "
    "previously-blank year has since been published).",
)
parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Max number of requests to make this run (for a quick test), e.g. --limit 2",
)
parser.add_argument(
    "--states",
    type=str,
    default=None,
    help="Comma-separated state names to fetch (default: all). E.g. --states Delaware "
    "to re-fetch just one state's years with --force.",
)
args = parser.parse_args()

if args.states:
    wanted = {s.strip() for s in args.states.split(",")}
    states = [s for s in states if s["name"] in wanted]

os.makedirs("./data/fred", exist_ok=True)

dates = [f"{year}-01-01" for year in range(args.start_year, args.end_year + 1)]

# fred.stlouisfed.org's robots.txt declares Crawl-delay: 2 for api.stlouisfed.org -
# stricter than this project's 1 req/sec policy floor, so we honor the stricter one.
REQUEST_DELAY_SECONDS = 2

fetch_count = 0
for date in dates:
    if args.limit is not None and fetch_count >= args.limit:
        break
    for state in states:
        if args.limit is not None and fetch_count >= args.limit:
            break

        path = f'./data/fred/{state.get("name")}-{date}.json'
        if os.path.exists(path) and not args.force:
            continue

        payload["observation_date"] = date
        payload["element_id"] = state.get("key")
        r = requests.get(fred_base, params=payload)
        print(path)
        with open(path, "w") as outfile:
            outfile.write(r.text)

        fetch_count += 1
        time.sleep(REQUEST_DELAY_SECONDS)
