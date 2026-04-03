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
###############################################################################################################
def heat_wave_exposure(data_source: str, spatial_level: str) -> pd.DataFrame:
    """
    Calculate heat wave exposure for each participant.

    Parameters:
    data_source (str): Source of exposure data. Options: "ERA-5", "MERRA-2".
    spatial_level (str): Spatial level of analysis. Options: "village", "centroid", "facility".

    Returns:
    pd.DataFrame: DataFrame with heat wave exposure for each participant.
    """

    out_dir = f"../data/processed/heat_waves/{spatial_level}-level"

    os.makedirs(out_dir, exist_ok=True)

    final_df = pd.DataFrame()

    # extreme heat definitions based on exposure percentiles
    percentiles = {
        "hd1": 0.75,
        "hd2": 0.90,
        "hd3": 0.95,
    }  # moderate, high, extreme heat days

    # heatwave duration threshold (number of consecutive hot days)
    duration = dict(zip(["a", "b", "c"], range(2, 5)))  # 2 - 4 days

    for country in ["Gambia"]:
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

        heat_waves = pd.DataFrame()

        for hd, pct in percentiles.items():
            for dur, days in duration.items():
                # heat wave definition
                hd_label = hd + dur

                # identify extreme heat days and heat waves
                df = exposure_df.heatwave(percentile=pct, threshold=days)

                # merge anc data with heat wave data
                analysis_df = pd.merge(
                    women,
                    df,
                    on=["Country", location_col, "exposure_day"],
                    how="left",
                    validate="m:1",
                ).sort_values(by=["f2a_participant_id", "exposure_day"])[
                    [
                        "f2a_participant_id",
                        "exposure_day",
                        "is_heatwave_day",
                        "block",
                        "consecutive_hot_days",
                    ]
                ]

                # count how many heatwaves each participant has experienced
                num_heatwaves = (
                    analysis_df[analysis_df["is_heatwave_day"]]
                    .groupby(["f2a_participant_id", "block"])
                    .size()
                    .groupby("f2a_participant_id")
                    .size()
                )

                # calculate average number of days per heatwave
                avg_duration = (
                    analysis_df[analysis_df["is_heatwave_day"]]
                    .groupby(["f2a_participant_id", "block"])["consecutive_hot_days"]
                    .first()
                    .groupby("f2a_participant_id")
                    .mean()
                )

                # combining results
                stats = pd.DataFrame(
                    {f"{hd_label}_num": num_heatwaves, f"{hd_label}_avg": avg_duration}
                ).reset_index()

                # merge with previous stats
                heat_waves = (
                    stats
                    if heat_waves.empty
                    else pd.merge(
                        heat_waves,
                        stats,
                        on="f2a_participant_id",
                        how="inner",
                        validate="1:1",
                    )
                )
        # add country column and merge with final df
        heat_waves["Country"] = country
        final_df = pd.concat([final_df, heat_waves], ignore_index=True)

    # export final df
    final_df.to_csv(os.path.join(out_dir, f"{data_source}_heat_waves.csv"), index=False)
    return final_df


if __name__ == "__main__":
    for data_source in [
        "ERA-5",
    ]:  # "MERRA-2"]:
        for spatial_level in ["village", "centroid", "facility"]:
            heat_wave_exposure(data_source, spatial_level)
