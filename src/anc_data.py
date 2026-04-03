import os
import pandas as pd
import numpy as np

############################################################################################################
#### Extracting anc attendance records from Big Table file
############################################################################################################
# anc columns
anc_cols = [
    "f2a_participant_id",
    "Country",
    "exposure_day",
    "village_code",
    "facility_code",
]
# read anc data
anc = pd.read_csv(r"../data/external/Participant_Exposures.csv").rename(
    columns={"Village code": "village_code"}
)
# handle data types
anc['exposure_day'] = pd.to_datetime(anc['exposure_day'], format="%Y-%m-%d")
anc[["village_code", "facility_code"]] = anc[
    ["village_code", "facility_code"]].apply(pd.to_numeric, errors='coerce')
############################################################################################################
#### Checking anc data by country
############################################################################################################
# total women by country
for country in anc["Country"].unique():
    total = anc[anc["Country"] == country].groupby("f2a_participant_id").size()
    print(f"{country}: {len(total)} women")

anc[anc_cols].to_csv(r"../data/interim/anc_daily_attendance.csv", index=False)

# anc[anc["Country"] == "Kenya"][anc_cols]
