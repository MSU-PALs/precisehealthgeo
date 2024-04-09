# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 16:59:38 2024

@author: s1465450
"""

import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd
from datetime import timedelta as td

class Precise():
    
    def __init__(self, country, filepath):
        
        self.__data=self.__readData(country, filepath)
        (self.__dropDuplicates()
         .__formatDates()
         .__calculate_gestation()
         .__village_location()
         .__facility_location()
         .__export_bigTable())
    
    def __readData(self, country: str, filepath: str): 
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
        cols_gen=['f2a_participant_id', 'cohort', 'country', 'health_facility_V1', 'health_facility_V2', 'health_facility_delivery',\
                  'visit1_date', 'GA_PRECISE_V1', 'visit2_date', 'GA_PRECISE_V2', 'delivery_date', 'GA_PRECISE_delivery']
        country_cols=dict(kenya=['KE_Village_V1', 'KE_Village_V2', 'KE_Village_delivery'],\
                         gambia=['GM_village_V1', 'GM_village_V2', 'GM_village_delivery'],\
                         mozambique=['f2_mz_nbr_V1', 'f2_mz_nbr_V2', 'f2_mz_nbr_delivery'])
        # read data file
        cols_gen.extend(country_cols[country.lower()])
        df=pd.read_csv(filepath)[cols_gen]
        # rename cols
        if country.lower()=='gambia':
            df=df.rename(columns={'GM_village_V1':'village_V1', 'GM_village_V2':'village_V2', 'GM_village_delivery': 'village_V3'})
        elif country.lower()=='kenya':
            df=df.rename(columns={'KE_Village_V1':'village_V1', 'KE_Village_V2':'village_V2', 'KE_Village_delivery':'village_V3'})
        elif country.lower()=='mozambique':
            df=df.rename(columns={'f2_mz_nbr_V1':'village_V1', 'f2_mz_nbr_V2':'village_V2', 'f2_mz_nbr_delivery':'village_V3'})
        df=(df.rename(columns={'health_facility_delivery':'health_facility_V3', 'delivery_date':'visit3_date', 'GA_PRECISE_delivery':'GA_PRECISE_V3'})
            .set_index('f2a_participant_id'))
        return df[(df.country.str.endswith(country.capitalize()))&(df.cohort=='UNS')]
    
    def getData(self):       
        return self.__data
    
    def __dropDuplicates(self):
        """
        Drop duplicate records from anc surveillance data.
        
        Checks for duplicated participant IDs and drops all duplicates.

        Returns
        -------
        Precise object
            self.data attribute updated for duplicates.

        """
        data=self.getData()
        self.__data=data[~data.index.duplicated(keep=False)]
        return self
    
    def __formatDates(self):
        """
        Format datetime-like types to appropriate formats.
        
        Converts all `visit_date` fields to datetime and `ga_at_visit` fields to timedelta formats.

        Returns
        -------
        Precise object
            self.data attribute updated with formatted datetime fields.

        """
        data=self.getData()
        self.__data[['visit1_date', 'visit2_date', 'visit3_date']]=data[['visit1_date', 'visit2_date', 'visit3_date']].apply(lambda x: pd.to_datetime(x, format='%d%b%Y'))
        self.__data[['GA_PRECISE_V1', 'GA_PRECISE_V2', 'GA_PRECISE_V3']]=data[['GA_PRECISE_V1', 'GA_PRECISE_V2', 'GA_PRECISE_V3']].apply(lambda x: pd.to_timedelta(x, unit='W'))
        return self
    
    def __calculate_gestation(self):
        """
        Compute gestation period.
        
        Uses visit dates and ga_at_visit to determine conception date then delivery date to determine gestation period.

        Returns
        -------
        Precise object
            self.data attribute updated with GA field.

        """
        data=(self.__data.apply(conception, axis=1)
              .apply(gestation, axis=1))        
        self.__data=data          
        return self
    
    def __village_location(self):
        """
        Assign village location during gestation period.
        
        Extracts village location for each visit and assigns the location to each month
        of the gestation period. This will affect only records with a valid gestation period.

        Returns
        -------
        Precise object
            gestation months attributes updated with locations.

        """       
        def assign_loc(row):
            
            #case 1: where only a single location has been given
            if pd.notna(row['gestation']) and row[['village_V1', 'village_V2', 'village_V3']].isnull().sum()==2:
                for month in row['months']:
                    #assign the given location
                    if pd.notna(row['village_V1']):
                        row[month.strftime("%Y_%m")]=row['village_V1'] 
                    elif pd.notna(row['village_V2']):
                        row[month.strftime("%Y_%m")]=row['village_V2'] 
                    elif pd.notna(row['village_V3']):
                        row[month.strftime("%Y_%m")]=row['village_V3']
            #case 2: where two locations are given
            if pd.notna(row['gestation']) and row[['village_V1', 'village_V2', 'village_V3']].notnull().sum()==2:                
                for month in row['months']:
                    if pd.notna(row['village_V2']):
                        #assign visit_1_location to months prior to visit 2 and visit_2_location to remaining months
                        row[month.strftime("%Y_%m")]=row['village_V1'] if month<row['visit2_date']+MonthEnd(1) else row['village_V2']
                    elif pd.notna(row['village_V3']):
                        #assign visit_1_location to first 2 trimesters and visit_3_location to third trimester
                        row[month.strftime("%Y_%m")]=row['village_V1'] if list(row['months']).index(month)<len(row['months'])-3 else row['village_V3']                
            #case 3: where all 3 locations are given
            if pd.notna(row['gestation']) and row[['village_V1', 'village_V2', 'village_V3']].all():
                for month in row['months']:
                    if month<row['visit2_date']+MonthEnd(1):
                        #assign visit_1_location
                        row[month.strftime("%Y_%m")]=row['village_V1']            
                    elif month>=row['visit2_date']+MonthEnd(1) and list(row['months']).index(month)<len(row['months'])-3:
                        #assign visit_2_location to end of second trimester
                        row[month.strftime("%Y_%m")]=row['village_V2']  
                    elif list(row['months']).index(month)>=len(row['months'])-3:
                        #assign visit_3_location to third trimester
                        row[month.strftime("%Y_%m")]=row['village_V3'] 
            return row
        
        data=self.getData()
        self.__villages=(data.apply(lambda x: gestation(x, show_range=True), axis=1)
                   .apply(assign_loc, axis=1))                
        return self    
    
    def __facility_location(self):
        """
        Assign facility location during gestation period.
        
        Extracts health facility location for each visit and assigns the location to each month
        of the gestation period. This will affect only records with a valid gestation period.

        Returns
        -------
        Precise object
            gestation months attributes updated with health facility locations.

        """       
        def assign_loc(row):
            
            #case 1: where only a single location has been given
            if pd.notna(row['gestation']) and row[['health_facility_V1', 'health_facility_V2', 'health_facility_V3']].isnull().sum()==2:
                for month in row['months']:
                    #assign the given location
                    if pd.notna(row['health_facility_V1']):
                        row[month.strftime("%Y_%m")]=row['health_facility_V1'] 
                    elif pd.notna(row['health_facility_V2']):
                        row[month.strftime("%Y_%m")]=row['health_facility_V2'] 
                    elif pd.notna(row['health_facility_V3']):
                        row[month.strftime("%Y_%m")]=row['health_facility_V3']
            #case 2: where two locations are given
            if pd.notna(row['gestation']) and row[['health_facility_V1', 'health_facility_V2', 'health_facility_V3']].notnull().sum()==2:                
                for month in row['months']:
                    if pd.notna(row['health_facility_V2']):
                        #assign visit_1_location to months prior to visit 2 and visit_2_location to remaining months
                        row[month.strftime("%Y_%m")]=row['health_facility_V1'] if month<row['visit2_date']+MonthEnd(1) else row['health_facility_V2']
                    elif pd.notna(row['health_facility_V3']):
                        #assign visit_1_location to first 2 trimesters and visit_3_location to third trimester
                        row[month.strftime("%Y_%m")]=row['health_facility_V1'] if list(row['months']).index(month)<len(row['months'])-3 else row['health_facility_V3']                
            #case 3: where all 3 locations are given
            if pd.notna(row['gestation']) and row[['health_facility_V1', 'health_facility_V2', 'health_facility_V3']].all():
                for month in row['months']:
                    if month<row['visit2_date']+MonthEnd(1):
                        #assign visit_1_location
                        row[month.strftime("%Y_%m")]=row['health_facility_V1']            
                    elif month>=row['visit2_date']+MonthEnd(1) and list(row['months']).index(month)<len(row['months'])-3:
                        #assign visit_2_location to end of second trimester
                        row[month.strftime("%Y_%m")]=row['health_facility_V2']  
                    elif list(row['months']).index(month)>=len(row['months'])-3:
                        #assign visit_3_location to third trimester
                        row[month.strftime("%Y_%m")]=row['health_facility_V3'] 
            return row
        
        data=self.getData()
        self.__facilities=(data.apply(lambda x: gestation(x, show_range=True), axis=1)
                   .apply(assign_loc, axis=1))               
        return self
    
    def __export_bigTable(self):
        """
        Export ANC dataframe in analysis ready structure.
        
        Restructures the anc data into the long format, where each row is a data point showing the participant
        conception date and neighborhood location for each exposure month of their gestation period. 

        Returns
        -------
        Dataframe
            Anc table with all participants and their localities for all exposure months.

        """
        # retrieve df cols
        cols=self.getData().columns
        # restructure village locations
        df1=self.__villages.drop(columns=[col for col in cols if col not in ['conception_date', 'visit3_date']])
        df1=(df1.rename(columns={'visit3_date': 'delivery_date'})
                .reset_index()
                .dropna(subset=['conception_date', 'delivery_date'])
                .melt(id_vars=['f2a_participant_id', 'conception_date', 'delivery_date'], var_name='exposure_month', value_name='village_name', ignore_index=True)
                .sort_values(by=['f2a_participant_id', 'exposure_month'])
                .dropna()
                .set_index(['f2a_participant_id', 'exposure_month']))
        
        # restructure facility locations
        df2=self.__facilities.drop(columns=[col for col in cols if col not in ['conception_date', 'visit3_date']])
        df2=(df2.rename(columns={'visit3_date': 'delivery_date'})
                .reset_index()
                .dropna(subset=['conception_date', 'delivery_date'])
                .melt(id_vars=['f2a_participant_id', 'conception_date', 'delivery_date'], var_name='exposure_month', value_name='facility_name', ignore_index=True)
                .sort_values(by=['f2a_participant_id', 'exposure_month'])
                .dropna()
                .set_index(['f2a_participant_id', 'exposure_month']))
        self.__data=pd.merge(df1, df2.drop(columns=['conception_date', 'delivery_date']), left_index=True, right_index=True, how='outer', validate='1:1')
        
        return self
    
def conception(df):
    """
    Function to calculate conception date.
    
    Uses `visit_date` and `ga_at_visit` to determine start date of pregnancy. The final conception date
    is taken as the minimum from the three sets of dates.

    Parameters
    ----------
    df : Dataframe
        DESCRIPTION.

    Returns
    -------
    df : Dataframe
        DESCRIPTION.

    """
    df['conception_date']=min([pd.to_datetime(df['visit{}_date'.format(i)]-df['GA_PRECISE_V{}'.format(i)], unit='D') for i in range(1, 4)], default=np.nan)
    return df

def gestation(df, show_range=False):
    """
    Function to calculate gestation period.

    Parameters
    ----------
    df : Dataframe
        ANC surveillance data.
    show_range : bool, optional
        Return full gestation range or days count. The default is False.

    Returns
    -------
    Dataframe
        ANC surveillance data with `gestation` field.

    """
    #use conception date and delivery date to calculate gestation period
    if pd.notna(df['conception_date']) and pd.notna(df['visit3_date']):
        df['gestation']=df['visit3_date']-df['conception_date']
        #restrict gestation period to not more than 9 months but keep track of actual gestation period
        df['months']=pd.date_range(start=df['conception_date']-td(days=30), end=df['visit3_date'], freq='MS', normalize=True) if df['gestation']<=td(days=294) else pd.date_range(end=df['visit3_date'], periods=9, freq='MS', normalize=True)
    return df if show_range else df.drop(columns='months')