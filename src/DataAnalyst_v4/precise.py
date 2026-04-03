# -*- coding: utf-8 -*-
"""
Created on Fri May 02 14:04:38 2025

@author: reason
"""

import pandas as pd
import numpy as np


class Precise:
    def __init__(self, country, filepath):
        self.__data = self.__readData(country, filepath)
        (self.__structureData().__formatDates())
        #  .__computeGestation())
        #  .__village_location()
        #  .__facility_location()
        #  .__export_bigTable())

    def __readData(self, country: str, filepath: str) -> pd.DataFrame:
        """
        Read ANC data for specified country.

        Reads data from csv and filters relevant columns for specified country.

        Parameters
        ----------
        country : str
            One of ['Mozambique', 'Kenya', 'The Gambia'].
        filepath : str
            Path to ANC csv file.

        Returns
        -------
        Dataframe
            ANC dataframe in the self.data attribute.

        """
        columns = [
            "f2a_participant_id",
            "visitevent",
            "visit_date",
            "country",
            "health_facility",
            "village",
            "conception_date",
            "delivery_date",
        ]
        # read data file
        df = pd.read_csv(filepath, usecols=columns)
        # filter country
        if country.lower().endswith("gambia"):
            country = "The Gambia"
        return df[df["country"] == country.title()]

    def __structureData(self):
        """
        Structure data for analysis.

        Takes a dataframe and structures it for analysis by filtering relevant columns.

        Returns
        -------
        pd.DataFrame
            Structured dataframe.
        """
        # filter relevant columns
        df = self.getData()
        columns = df.columns.tolist()
        visit_cols = [
            col
            for col in columns
            if col not in ["country", "visitevent", "conception_date", "delivery_date"]
        ]
        # filter records by visit date
        visit1 = df[df["visitevent"] == "Visit 1"][visit_cols]
        visit2 = df[df["visitevent"] == "Visit 2"][visit_cols]
        delivery = (
            df[
                (df["visitevent"] == "Birth visit - after birth")
                & (df["conception_date"].notna())
            ]
            .drop_duplicates(subset="f2a_participant_id")
            .drop(columns=["visitevent", "visit_date", "country"])
        )
        # merge records
        self.__data = (
            visit1.merge(
                visit2,
                on="f2a_participant_id",
                how="outer",
                validate="1:1",
                suffixes=("_visit1", "_visit2"),
            )
            .merge(delivery, on="f2a_participant_id", validate="1:1")
            .rename(
                columns={
                    "delivery_date": "visit_date_delivery",
                    "health_facility": "health_facility_visit3",
                    "village": "village_visit3",
                }
            )
        )
        return self

    def __formatDates(self):
        """
        Format datetime-like types to appropriate formats.

        Converts all `date` fields to datetime format.

        Returns
        -------
        Precise object
            self.data attribute updated with formatted datetime fields.

        """
        df = self.getData()
        # filter date columns
        columns = df.columns.tolist()
        date_cols = [col for col in columns if "date" in col]
        # format date columns
        self.__data[date_cols] = df[date_cols].apply(
            lambda x: pd.to_datetime(x, format="%d-%b-%y")
            if pd.notna(x).any()
            else np.nat,
            axis=1,
        )
        return self

    def getData(self):
        return self.__data

    def assign_village(self):
        """
        Assign village location during gestation period.

        Extracts village location for each visit and assigns the location to each day
        of the gestation period. This will affect only records with a valid gestation period.

        Returns
        -------
        Precise object
            gestation months attributes updated with locations.

        """

        def extract_location(row, current_day):
            # compute gestation period
            # gestation_dats=list(pd.date_range(start=row['conception_date'], end=row['visit_date_delivery'], freq='D'))
            last_trimester_start = row["conception_date"] + pd.DateOffset(weeks=27)
            # filter village columns
            village_cols = [col for col in row.index if "village" in col]
            # case 1: where two locations are given
            if row[village_cols].notnull().sum() == 2:
                # assign visit_1_location to first 2 trimesters and visit_3_location to third trimester
                if current_day < last_trimester_start:
                    return row["village_visit1"]
                else:
                    return row["village_visit3"]
            # case 2: where all 3 locations are given
            if row[village_cols].all():
                if current_day < row["visit_date_visit2"]:
                    # assign visit_1_location
                    return row["village_visit1"]
                elif (
                    current_day >= row["visit_date_visit2"]
                    and current_day < last_trimester_start
                ):
                    # assign visit_2_location up to before third trimester
                    return row["village_visit2"]
                else:
                    # assign visit_3_location to third trimester
                    return row["village_visit3"]
            return row

        village_rows = []
        df = self.getData()
        for _, row in df.iterrows():
            current_day = row["conception_date"]
            while current_day <= row["visit_date_delivery"]:
                village_rows.append(
                    {
                        "f2a_participant_id": row["f2a_participant_id"],
                        "date": current_day,
                        "village": extract_location(row, current_day),
                    }
                )
                current_day += pd.DateOffset(days=1)
        # self.__villages=pd.DataFrame(village_df_rows)
        # return self
        return pd.DataFrame(village_rows)
