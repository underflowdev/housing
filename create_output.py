# Flattens the list of FRED data into a single CSV

import json
import os
import pandas as pd
import numpy as np

# pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", None)

fred_data_dir = "./data/fred"
fred_output_path = "./outputs/fred_output.csv"
# Add the path to your downloaded zillow file here
# The file I select is normally ZHVI All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted ($), by County
zillow_path = "./data/zillow/county zillow filename"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
final_output_path = "./outputs/fips_fred_zillow.csv"
zfmap_path = "./data/zfmap/zillow_fred_map.csv"


# map the state names to the state abbreviations
state_map = {
    "Alabama":	        "AL",
    "Alaska":	        "AK",
    "Arizona":  	    "AZ",
    "Arkansas":	        "AR",
    "California":	    "CA",
    "Colorado":	        "CO",
    "Connecticut":	    "CT",
    "Delaware":	        "DE",
    "Florida":	        "FL",
    "Georgia":	        "GA",
    "Hawaii":	        "HI",
    "Idaho":	        "ID",
    "Illinois":	        "IL",
    "Indiana":	        "IN",
    "Iowa":	            "IA",
    "Kansas":	        "KS",
    "Kentucky":	        "KY",
    "Louisiana":	    "LA",
    "Maine":	        "ME",
    "Maryland":	        "MD",
    "Massachusetts":	"MA",
    "Michigan":     	"MI",
    "Minnesota":	    "MN",
    "Mississippi":	    "MS",
    "Missouri":	        "MO",
    "Montana":	        "MT",
    "Nebraska":	        "NE",
    "Nevada":	        "NV",
    "New Hampshire":	"NH",
    "New Jersey":	    "NJ",
    "New Mexico":	    "NM",
    "New York":	        "NY",
    "North Carolina":	"NC",
    "North Dakota":	    "ND",
    "Ohio":	            "OH",
    "Oklahoma":	        "OK",
    "Oregon":	        "OR",
    "Pennsylvania":	    "PA",
    "Rhode Island":	    "RI",
    "South Carolina":	"SC",
    "South Dakota":	    "SD",
    "Tennessee":	    "TN",
    "Texas":	        "TX",
    "Utah":	            "UT",
    "Vermont":	        "VT",
    "Virginia":	        "VA",
    "Washington":	    "WA",
    "West Virginia":	"WV",
    "Wisconsin":	    "WI",
    "Wyoming":	        "WY"
}

# map the years for the output
year_map = {
    # "fred_1996": ["1996-01-31", "1996-02-29", "1996-03-31", "1996-04-30", "1996-05-31", "1996-06-30", "1996-07-31", "1996-08-31", "1996-09-30", "1996-10-31", "1996-11-30", "1996-12-31"],
    # "fred_1997": ["1997-01-31", "1997-02-28", "1997-03-31", "1997-04-30", "1997-05-31", "1997-06-30", "1997-07-31", "1997-08-31", "1997-09-30", "1997-10-31", "1997-11-30", "1997-12-31"],
    # "fred_1998": ["1998-01-31", "1998-02-28", "1998-03-31", "1998-04-30", "1998-05-31", "1998-06-30", "1998-07-31", "1998-08-31", "1998-09-30", "1998-10-31", "1998-11-30", "1998-12-31"],
    # "fred_1999": ["1999-01-31", "1999-02-28", "1999-03-31", "1999-04-30", "1999-05-31", "1999-06-30", "1999-07-31", "1999-08-31", "1999-09-30", "1999-10-31", "1999-11-30", "1999-12-31"],
    "fred_2000": ["2000-01-31", "2000-02-29", "2000-03-31", "2000-04-30", "2000-05-31", "2000-06-30", "2000-07-31", "2000-08-31", "2000-09-30", "2000-10-31", "2000-11-30", "2000-12-31"],
    "fred_2001": ["2001-01-31", "2001-02-28", "2001-03-31", "2001-04-30", "2001-05-31", "2001-06-30", "2001-07-31", "2001-08-31", "2001-09-30", "2001-10-31", "2001-11-30", "2001-12-31"],
    "fred_2002": ["2002-01-31", "2002-02-28", "2002-03-31", "2002-04-30", "2002-05-31", "2002-06-30", "2002-07-31", "2002-08-31", "2002-09-30", "2002-10-31", "2002-11-30", "2002-12-31"],
    "fred_2003": ["2003-01-31", "2003-02-28", "2003-03-31", "2003-04-30", "2003-05-31", "2003-06-30", "2003-07-31", "2003-08-31", "2003-09-30", "2003-10-31", "2003-11-30", "2003-12-31"],
    "fred_2004": ["2004-01-31", "2004-02-29", "2004-03-31", "2004-04-30", "2004-05-31", "2004-06-30", "2004-07-31", "2004-08-31", "2004-09-30", "2004-10-31", "2004-11-30", "2004-12-31"],
    "fred_2005": ["2005-01-31", "2005-02-28", "2005-03-31", "2005-04-30", "2005-05-31", "2005-06-30", "2005-07-31", "2005-08-31", "2005-09-30", "2005-10-31", "2005-11-30", "2005-12-31"],
    "fred_2006": ["2006-01-31", "2006-02-28", "2006-03-31", "2006-04-30", "2006-05-31", "2006-06-30", "2006-07-31", "2006-08-31", "2006-09-30", "2006-10-31", "2006-11-30", "2006-12-31"],
    "fred_2007": ["2007-01-31", "2007-02-28", "2007-03-31", "2007-04-30", "2007-05-31", "2007-06-30", "2007-07-31", "2007-08-31", "2007-09-30", "2007-10-31", "2007-11-30", "2007-12-31"],
    "fred_2008": ["2008-01-31", "2008-02-29", "2008-03-31", "2008-04-30", "2008-05-31", "2008-06-30", "2008-07-31", "2008-08-31", "2008-09-30", "2008-10-31", "2008-11-30", "2008-12-31"],
    "fred_2009": ["2009-01-31", "2009-02-28", "2009-03-31", "2009-04-30", "2009-05-31", "2009-06-30", "2009-07-31", "2009-08-31", "2009-09-30", "2009-10-31", "2009-11-30", "2009-12-31"],
    "fred_2010": ["2010-01-31", "2010-02-28", "2010-03-31", "2010-04-30", "2010-05-31", "2010-06-30", "2010-07-31", "2010-08-31", "2010-09-30", "2010-10-31", "2010-11-30", "2010-12-31"],
    "fred_2011": ["2011-01-31", "2011-02-28", "2011-03-31", "2011-04-30", "2011-05-31", "2011-06-30", "2011-07-31", "2011-08-31", "2011-09-30", "2011-10-31", "2011-11-30", "2011-12-31"],
    "fred_2012": ["2012-01-31", "2012-02-29", "2012-03-31", "2012-04-30", "2012-05-31", "2012-06-30", "2012-07-31", "2012-08-31", "2012-09-30", "2012-10-31", "2012-11-30", "2012-12-31"],
    "fred_2013": ["2013-01-31", "2013-02-28", "2013-03-31", "2013-04-30", "2013-05-31", "2013-06-30", "2013-07-31", "2013-08-31", "2013-09-30", "2013-10-31", "2013-11-30", "2013-12-31"],
    "fred_2014": ["2014-01-31", "2014-02-28", "2014-03-31", "2014-04-30", "2014-05-31", "2014-06-30", "2014-07-31", "2014-08-31", "2014-09-30", "2014-10-31", "2014-11-30", "2014-12-31"],
    "fred_2015": ["2015-01-31", "2015-02-28", "2015-03-31", "2015-04-30", "2015-05-31", "2015-06-30", "2015-07-31", "2015-08-31", "2015-09-30", "2015-10-31", "2015-11-30", "2015-12-31"],
    "fred_2016": ["2016-01-31", "2016-02-29", "2016-03-31", "2016-04-30", "2016-05-31", "2016-06-30", "2016-07-31", "2016-08-31", "2016-09-30", "2016-10-31", "2016-11-30", "2016-12-31"],
    "fred_2017": ["2017-01-31", "2017-02-28", "2017-03-31", "2017-04-30", "2017-05-31", "2017-06-30", "2017-07-31", "2017-08-31", "2017-09-30", "2017-10-31", "2017-11-30", "2017-12-31"],
    "fred_2018": ["2018-01-31", "2018-02-28", "2018-03-31", "2018-04-30", "2018-05-31", "2018-06-30", "2018-07-31", "2018-08-31", "2018-09-30", "2018-10-31", "2018-11-30", "2018-12-31"],
    "fred_2019": ["2019-01-31", "2019-02-28", "2019-03-31", "2019-04-30", "2019-05-31", "2019-06-30", "2019-07-31", "2019-08-31", "2019-09-30", "2019-10-31", "2019-11-30", "2019-12-31"],
    "fred_2020": ["2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30", "2020-05-31", "2020-06-30", "2020-07-31", "2020-08-31", "2020-09-30", "2020-10-31", "2020-11-30", "2020-12-31"],
    "fred_2021": ["2021-01-31", "2021-02-28", "2021-03-31", "2021-04-30", "2021-05-31", "2021-06-30", "2021-07-31", "2021-08-31", "2021-09-30", "2021-10-31", "2021-11-30", "2021-12-31"],
    "fred_2022": ["2022-01-31", "2022-02-28", "2022-03-31", "2022-04-30", "2022-05-31", "2022-06-30", "2022-07-31", "2022-08-31", "2022-09-30", "2022-10-31", "2022-11-30", "2022-12-31"],
    "fred_2023": ["2023-01-31", "2023-02-28", "2023-03-31", "2023-04-30", "2023-05-31", "2023-06-30", "2023-07-31", "2023-08-31", "2023-09-30", "2023-10-31", "2023-11-30", "2023-12-31"],
    "fred_2024": ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31", "2024-06-30", "2024-07-31", "2024-08-31", "2024-09-30", "2024-10-31", "2024-11-30", "2024-12-31"],
    "fred_2025": ["2025-01-31"],
    # "fred_2025": ["2025-01-31", "2025-02-29", "2025-03-31", "2025-04-30", "2025-05-31", "2025-06-30", "2025-07-31", "2025-08-31", "2025-09-30", "2025-10-31", "2025-11-30", "2025-12-31"],
}

header_written = False

# read in the zillow values
ZILLOW_COUNTY = "RegionName"
ZILLOW_STATE_ABBR = "StateName"
zillow = pd.read_csv(zillow_path)


# https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697,
# Add " County" or " Borough" or "Parish" or remove spaces... to each name for a merge
fips = pd.read_csv(fips_path, dtype=str)

# map the zillow county names to the fred county names
# manual.  Ugh.
zfmap = pd.read_csv(zfmap_path, dtype=str)

# https://fred.stlouisfed.org/release/tables?rid=175&eid=266090
fred_files = os.listdir(fred_data_dir)
result = {}
for fred_file in fred_files:
    file_split = fred_file.split("-")
    fred_state = file_split[0]
    fred_year = file_split[1]

    with open(os.path.join(fred_data_dir, fred_file)) as fred_json:
        fred_data = json.load(fred_json)
        fred_abbr = state_map.get(fred_state)
        elements = fred_data.get("elements", {})
        for element in elements.values():
            fred_county = element.get("name")
            fred_value = element.get("observation_value")
            if (fred_value == "."):
                fred_value = None
            else:
                fred_value = fred_value.replace(",", "")
                fred_value = float(fred_value)
            fred_key = fred_abbr + "-" + fred_county
            if not fred_key in result:
                result[fred_key] = {}

            result[fred_key]["fred_state"] = fred_state
            result[fred_key]["fred_state_abbr"] = fred_abbr
            result[fred_key]["fred_county"] = fred_county
            result[fred_key]["fred_" + fred_year] = fred_value


fred = pd.DataFrame(result.values())
# kick fred data out to a csv for inspection
fred.to_csv(fred_output_path, index=False)

# The fred map bridges missing and misnamed values between fred and zillow.
fred_map = pd.merge(zfmap, fred, left_on=[
    "map_fred_state_abbr", "map_fred_county"], right_on=["fred_state_abbr", "fred_county"])


# 40 values in zillow but not fred
# 304 values in fred but not zillow
# Find missing values by doing how="left" or how="right" and followup merge "left_only" or "right_only"
# https://stackoverflow.com/questions/50543326/how-to-do-left-outer-join-exclusion-in-pandas
# zillow_fred = pd.merge(fred_map, zillow, left_on=[
#                        "map_zillow_state_abbr", "map_zillow_county"], right_on=[ZILLOW_STATE_ABBR, ZILLOW_COUNTY], how="left", indicator=True)
# zillow_fred = zillow_fred[zillow_fred['_merge'] == 'left_only']

fred_zillow = pd.merge(fred_map, zillow, left_on=[
    "map_zillow_state_abbr", "map_zillow_county"], right_on=[ZILLOW_STATE_ABBR, ZILLOW_COUNTY])

# add fips codes
final = pd.merge(fips, fred_zillow, left_on=[
    "nrcs_abbr", "nrcs_county"], right_on=["fred_state_abbr", "fred_county"])

# losing some on the fips merge, not sure why
zillow_path = "./data/zillow/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
fips_path = "./data/nrcs/nrcs_fips_codes.csv"
final_output_path = "./outputs/fips_fred_zillow.csv"
print(final)

# guessing on the inflation, since the FRED data doesn't have income past 2023
final["fred_2024"] = final["fred_2023"] * 1.02
final["fred_2025"] = final["fred_2024"] * 1.02

# Create a list to store the new ratio columns
new_columns = []

for year, months in year_map.items():
    for month in months:
        ratio_column = final[month] / final[year]
        new_columns.append(ratio_column.rename(f"ratio-{month}"))

# concat in all new columns in a single operation
final = pd.concat([final] + new_columns, axis=1)

final.to_csv(final_output_path, index=False, float_format="%11.3f")
