Download county-level files from https://www.zillow.com/research/data/ and
place each under its own home-type subfolder, e.g.:

data/zillow/all-homes/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon_210526.csv
data/zillow/single-family-homes/County_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_mon_210526.csv

`create_output.py` and `web_data_build.py` both read from a `zillow_paths`
dict keyed by home_type (`all_homes`, `single_family`, ...) - update those
dicts (marked `# UPDATE:`) to match the files/folders you've downloaded.
