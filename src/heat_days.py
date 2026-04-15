import os
import pandas as pd
import numpy as np
from DataAnalyst_v4.heat import Heat

data_dir = r"../data/interim/"

# get anc attendance records
anc_data = pd.read_csv(os.path.join(data_dir, r"anc_daily_attendance.csv"))

anc_data["exposure_day"] = pd.to_datetime(
    anc_data["exposure_day"], format="%Y-%m-%d", errors="coerce"
)


############################################################################################################
## Heat wave analysis at village level
############################################################################################################
def extreme_heat_days(data_source: str, spatial_level: str) -> pd.DataFrame:
    """
    Calculate the proportion of extreme heat days for each participant.

    Parameters:
    data_source (str): Source of exposure data. Options: "ERA-5", "MERRA-2".
    spatial_level (str): Spatial level of analysis. Options: "village", "centroid", "facility".

    Returns:
    pd.DataFrame: DataFrame with extreme heat day proportions for three extreme heat definitions.
    """

    out_dir = f"../data/processed/heat_days/{spatial_level}-level"

    os.makedirs(out_dir, exist_ok=True)

    final_df = pd.DataFrame()

    # extreme heat definitions based on exposure percentiles
    percentiles = {
        "hd_p75": 0.75,
        "hd_p90": 0.90,
        "hd_p95": 0.95,
    }  # moderate, high and extreme heat days

    for country in [
        "Gambia",
        "Kenya",
        "Mozambique",
    ]:
        # filter anc data for country
        women = anc_data[anc_data["Country"] == country]

        # preparing heat wave data for country
        exposure_df = Heat(
            os.path.join(
                data_dir, f"{country}", f"{data_source}_daily_{spatial_level}_mean.pkl"
            ),
            temperature_col=f"{data_source}_mean",
        )
        # get location column name based on spatial level
        location_col = exposure_df.getLocation()

        proportion_hot_days = pd.DataFrame()

        for label, pct in percentiles.items():
            # identify extreme heat days
            df = exposure_df.heatwave(percentile=pct)

            # merge anc data with heat data
            analysis_df = (
                pd.merge(
                    women,
                    df,
                    on=["Country", location_col, "exposure_day"],
                    how="left",
                    validate="m:1",
                ).sort_values(by=["f2a_participant_id", "exposure_day"])
            )[["f2a_participant_id", "exposure_day", "is_hot_day"]]

            # proportion of hot days by participant
            stats = (
                analysis_df.groupby("f2a_participant_id", as_index=False)["is_hot_day"]
                .mean()
                .rename(columns={"is_hot_day": label})
            )
            # merge with previous stats
            proportion_hot_days = (
                stats
                if proportion_hot_days.empty
                else pd.merge(
                    proportion_hot_days,
                    stats,
                    on="f2a_participant_id",
                    how="inner",
                    validate="1:1",
                )
            )
        # add country column and merge with final df
        proportion_hot_days["Country"] = country
        final_df = pd.concat([final_df, proportion_hot_days])

    # export final df
    final_df.to_csv(os.path.join(out_dir, f"{data_source}_heat_days.csv"), index=False)
    return final_df


if __name__ == "__main__":
    for data_source in ["ERA-5", "MERRA-2"]:
        for spatial_level in ["village", "centroid", "facility"]:
            extreme_heat_days(data_source, spatial_level)
