# -*- coding: utf-8 -*-
"""
Created on Fri May 07 13:27:30 2025

@author: reason
"""

import pandas as pd
import numpy as np


class Heat:
    def __init__(self, filepath: str, temperature_col: str):
        """
        Args:
            filepath (str): Pickle file with temperature data.
                Expects file to have columns ['exposure_day', <location_code>, '<temperature_col>'].
                    <location_code> is the column name of the location unique identifier and
                    should end with the word 'code', e.g., "neighborhood_code".
        """
        df = pd.read_pickle(filepath)
        # format dates
        df["exposure_day"] = pd.to_datetime(df["exposure_day"])
        # get location column
        self.__location = df.columns[df.columns.str.contains("code")].values[0]
        # sort by exposure date and location
        df = df.sort_values(by=[self.__location, "exposure_day"]).reset_index(drop=True)
        self.__data = df.copy()
        self.__temperature_col = temperature_col

    def getData(self):
        return self.__data

    def getLocation(self):
        return self.__location

    def heatwave(self, percentile: float = 0.95, threshold: int = 2) -> pd.DataFrame:
        """
        Function for identifying extreme heat days and heat waves.
        Extreme heat is defined as temperature exceeding the nth
        percentile, and a heatwave is defined as N consecutive days
        of extreme temperature.

        Args:
            percentile (float, optional): Percentile value defining
                extreme temperature. Defaults to 0.95
            threshold (int, optional): Minimum consecutive days of extreme temperature
                defining a heatwave. Defaults to 2

        Returns:
            pd.DataFrame: Temperature data with columns identifying extreme
                heat days, heatwave days, heatwave labels ('block') and number
                of heat wave days ('consecutive_hot_days')
        """
        df = self.getData()
        # Step 1: Calculate the nth percentile of temperature for each location
        df["extreme_heat"] = df.groupby(f"{self.__location}")[
            self.__temperature_col
        ].transform(lambda group: group.quantile(percentile))
        # Step 2: Identify days that exceed the nth percentile (i.e., 'hot' days)
        df["is_hot_day"] = df[self.__temperature_col] > df["extreme_heat"]
        # Step 3: Identify Heat Waves - at least 2 consecutive days above the nth percentile
        # label consecutive hot days
        df["block"] = df.groupby(f"{self.__location}")["is_hot_day"].transform(
            lambda group: (group.shift(1) != group).cumsum()
        )
        # count consecutive hot days
        df["consecutive_hot_days"] = df.groupby([f"{self.__location}", "block"])[
            "is_hot_day"
        ].transform("sum")
        # label the heatwaves (i.e., n or more consecutive hot days)
        df["is_heatwave_day"] = df["consecutive_hot_days"] >= threshold
        return df
