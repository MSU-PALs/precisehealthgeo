################################################################################
## Analysis of daily heat exposure during pregnancy
################################################################################
import os
import pandas as pd
import numpy as np

data_dir = r"../data/interim/"

temperature_df = pd.read_csv(
    os.path.join(data_dir, "Daily_Big_Table_extreme_hot_days_heatwaves29032026.csv")
)
# demography columns
demography_cols = [
    "f2a_participant_id",
    "Country",
    "exposure_day",
    "conception_date",
    "Visit 1",
    "Visit 2",
    "delivery_date",
]
################################################################################
## Ambient temperature - village level analysis
################################################################################
ambient_cols = [
    "T2M_mean",
    "T2MWET_mean",
    "T2M_mean_extreme_hot_day",
    "T2MWET_mean_extreme_hot_day",
    "T2M_mean_heatwave_day",
    "T2M_mean_heatwave_length",
    "T2MWET_mean_heatwave_day",
    "T2MWET_mean_heatwave_length",
]
