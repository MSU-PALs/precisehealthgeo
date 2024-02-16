# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 12:37:02 2023

@author: mlamb
"""
import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd
from datetime import timedelta as td
import os

class Precise():
    
    def __init__(self, country, filepath):
        
        self.__country=None
        self.__sections=['T02_general_info', 'T03_baseline_maternal', 'T04_environment']
        self.filepath=os.path.join(filepath, r'{}.dta')
        self.__columns={
            'general_info': [
                #profile info
                'f2_visit_date', 'f2_ga_at_visit', 'redcap_event_name',
                #spatial access indicators
                'f2_location_from', 'f2_location_from_name', 
                'f2_location_from_other', 'f2_mode_of_transport_1_4', 'f2_mode_of_transport_other_1_4', 
                'f2_travel_duration', 'f2_woman_addr'
            ],
            'baseline_maternal': [
                #sociogeo indicators
                'f3_highest_school_level', 'f3_religion', 'f3_marital_status', 
                'f3_live_with_partner', 'f3_occupation', 'f3_duration_of_living_together', 'f3_year_of_birth'
            ],
            'environment': [
                #sociogeo indicators
                #3 variable names have been truncated during conversion from ODK to stata
                'f3_neighbor_help_pregnancy_probl', 'f3_form_of_help_received', 'f3_community_help_pregnancy_prob',
                'f3_participation_in_community_gr', 'f3_decision_maker_money', 'f3_decision_maker_pregnancy', 
                'f3_woman_has_money_for_transport', 'f3_toilet_facility'
            ]
        }
        self.__data=dict()
        
        if country.lower().endswith('gambia'):
            self.__country='gm'
        elif country.lower().endswith('kenya'):
            self.__country='ke'
        elif country.lower().endswith('mozambique'):
            self.__country='mz'
        self.__columns['general_info'].append('f2_{}_health_facility'.format(self.__country))
            
        for i in self.__sections:
            df=self.__readData(self.filepath.format(i), cols=i[4:])
            self.__data[i[4:]]=df[df['country']==country.title()]
        
        self.__formatDates()
        
    
    def __readData(self, filepath, cols):
        
        columns=self.__columns[cols].copy()
        columns.extend(['f2a_participant_id', 'country'])
        if cols=='general_info':
            df=pd.read_stata(filepath, index_col='f2a_participant_id', convert_categoricals=False)
            columns.extend(df.filter(like='f2_{}_v'.format(self.__country)).columns.to_list())
        return pd.read_stata(filepath, index_col='f2a_participant_id', columns=columns)
    
    def getData(self, *args):
        
        visit={
            1:'precise_visit_1_arm_1',
            2:'precise_visit_2_arm_1',
            3:'birth_mother_arm_1',
            4:'postpartum_mother_arm_1'
            }
        df=self.__data['general_info']
        
        if args:
            for v in args:
                if args.index(v)==0:
                    df=df[df['redcap_event_name']==visit[v]] 
        else:
            df=df[df['redcap_event_name']!=visit[4]]               
        return df 
    
    
#     def getCohort(self, abnormal_only: bool=False, drop_abnormal: bool=False, **kwargs):
#         """
#         Retrieve anc surveillance data by filter.
        
#         Filters the merged anc dataframe by either `visit` or `cohort` and additionally by
#         whether gestation age is normal (i.e., >= 294 days or Null) or abnormal.

#         Parameters
#         ----------
#         abnormal_only : bool, optional
#             Return only records with abnormal gestation period. The default is False.
#         drop_abnormal : bool, optional
#             Drop records with abnormal gestation period from the result. The default is False.
#         **kwargs : optional
#             visit: int(1, 2, 3), optional
#                 Filter by visit number.
#             cohort: int(1, 2, 3), optional
#                 Filter by cohort number.

#         Returns
#         -------
#         Dataframe
#             ANC surveillance records specified by the filters.

#         """
#         for k, v in kwargs.items():
#             k='f2_visit_date_visit_{}'.format(v) if k=='visit' else 'f2a_cohort'
#             data=self.mergedData[self.mergedData[k]==v] if k=='f2a_cohort' else self.mergedData[self.mergedData[k].notnull()]      
#         data=data if kwargs else self.mergedData
#         if abnormal_only:
#             return data[(data['gestation']>td(days=294))|(data['gestation'].isnull())]  
#         elif drop_abnormal:
#             return data[data['gestation']<=td(days=294)]
#         else:
#             return data
    
    def __formatDates(self):
        """
        Format datetime-like types to appropriate formats.
        
        Converts fields like `f2_visit_date` datetime and `f2_ga_at_visit` to timedelta formats.

        Returns
        -------
        Precise object
            self.mergedData attribute updated with formatted datetime fields.

        """
        
        self.__data['general_info'][['f2_visit_date']]=self.__data['general_info'][['f2_visit_date']].apply(lambda x: pd.to_datetime(x, infer_datetime_format=True, errors='ignore'), axis=1)
        self.__data['general_info'][['f2_ga_at_visit']]=self.__data['general_info'][['f2_ga_at_visit']].apply(lambda x: pd.to_timedelta(x.astype(float, errors='ignore'), unit='W', errors='coerce'), axis=1)
        return self
    
#     def addTracer(self, tracer):
#         """
#         Add gestation age data from the Tracer app.
        
#         Replaces GA calculated from LMP with GA calculated from Tracer. This will affect only
#         those participants whose GA was measured with Tracer.

#         Parameters
#         ----------
#         tracer : Tracer object
#             The tracer data to use.

#         Returns
#         -------
#         Precise object
#             self.mergedData attribute updated with Tracer GAs.

#         """
#         if isinstance(tracer, Tracer):
#             #perform a left-outer join of 1:1 mapping between anc surveillance records and tracer GA estimate data
#             self.mergedData=self.mergedData.merge(right=tracer.getData(), how='left', right_index=True, left_index=True, validate='1:1')
#         return self
    
#     def getSocialIndicators(self):
#         """
#         Retrieve data for sociogeographical indicators (found in visit 1 data).
        
#         Filters records by sociogeo fields and recodes values as specified in the statistical
#         analysis plan. See Shirley's recoding table for reference.

#         Returns
#         -------
#         sociogeo : Dataframe
#             ANC surveillance data filtered by sociogeo fields.

#         """
#         #retrieve sociogeo fields only
#         sociogeo=(self.getCohort(visit=1)
#                   .filter(like='f3'))
#         #recode fields
#         # sociogeo[['f3_decision_maker_money', 'f3_decision_maker_pregnancy']]=(sociogeo[['f3_decision_maker_money', 'f3_decision_maker_pregnancy']].apply(lambda x: pd.to_numeric(x, errors='coerce'), axis=1)
#         #                                                                       .replace([list(range(1, 11)), 11], [0, 1]))
#         #recode all missing fields to 99 and rename columns
#         sociogeo=(sociogeo.fillna(99)
#                   .rename(columns={'f3_neighbor_help_pregnancy_problem': 'comm_hlp', 'f3_form_of_help_received': 'form_hlp', 'f3_community_help_pregnancy_problem': 'str_aware', 'f3_participation_in_community_group': 'mat_soc_incl',
#                                    'f3_decision_maker_money': 'fin_aut_gen', 'f3_decision_maker_pregnancy': 'fin_aut_preg', 'f3_woman_has_money_for_transport': 'trans_prep', 'f3_highest_school_level': 'lev_edu', 'f3_religion': 'relgn',
#                                    'f3_marital_status': 'mar_sts', 'f3_live_with_partner': 'part_avail', 'f3_occupation': 'occup', 'f3_duration_of_living_together': 'len_mar', 'f3_year_of_birth': 'mat_birthyear', 'f3_toilet_facility': 'toilet'}))
#         return sociogeo
    
    def calculate_gestation(self):
        """
        Compute gestation period.
        
        Uses visit_date and ga_at_visit to determine conception date then delivery date to determine gestation period.

        Returns
        -------
        Precise object
            self.mergedData attribute updated with GA field.

        """
        data=(self.getData()
              .apply(conception, axis=1)
              .groupby(by='f2a_participant_id')
              .agg({'f2_visit_date': 'max', 'redcap_event_name': 'min', 'conception_date': 'max'})
              .apply(gestation, axis=1))        
        self.__data['general_info']=data#[data['gestation']<=td(days=294)] #deal with this later           
        return self
    
#     def pregnancy_location(self):
#         """
#         Assign pregnancy location during gestation period.
        
#         Extracts participant location for each visit and assigns the location to each month
#         of the gestation period. This will affect only records with a valid gestation period.

#         Returns
#         -------
#         Precise object
#             gestation months attributes updated with locations.

#         """       
#         def assign_loc(row):
            
#             #case 1: where only a single location has been given
#             if pd.notna(row['gestation']) and row[['f2_village_visit_1', 'f2_village_visit_2', 'f2_village_visit_3']].isnull().sum()==2:
#                 for month in row['months']:
#                     #assign the given location
#                     if ~pd.notna(row['f2_village_visit_1']):
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_1'] 
#                     elif ~pd.notna(row['f2_village_visit_2']):
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_2'] 
#                     elif ~pd.notna(row['f2_village_visit3']):
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_3']
#             #case 2: where two locations are given
#             if pd.notna(row['gestation']) and row[['f2_village_visit_1', 'f2_village_visit_2', 'f2_village_visit_3']].notnull().sum()==2:                
#                 for month in row['months']:
#                     if ~pd.notna(row['f2_village_visit_2']):
#                         #assign visit_1_location to months prior to visit 2 and visit_2_location to remaining months
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_1'] if month<row['f2_visit_date_visit_2']+MonthEnd(1) else row['f2_village_visit_2']
#                     elif ~pd.notna(row['f2_village_visit_3']):
#                         #assign visit_1_location to first 2 trimesters and visit_3_location to third trimester
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_1'] if list(row['months']).index(month)<len(row['months'])-3 else row['f2_village_visit_3']                
#             #case 3: where all 3 locations are given
#             if pd.notna(row['gestation']) and row[['f2_village_visit_1', 'f2_village_visit_2', 'f2_village_visit_3']].all():
#                 for month in row['months']:
#                     if month<row['f2_visit_date_visit_2']+MonthEnd(1):
#                         #assign visit_1_location
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_1']            
#                     elif month>=row['f2_visit_date_visit_2']+MonthEnd(1) and list(row['months']).index(month)<len(row['months'])-3:
#                         #assign visit_2_location to end of second trimester
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_2']  
#                     elif list(row['months']).index(month)>=len(row['months'])-3:
#                         #assign visit_3_location to third trimester
#                         row[month.strftime("%Y_%m")]=row['f2_village_visit_3'] 
#             return row
            
#         self.mergedData=(self.mergedData.apply(lambda x: gestation(x, show_range=True), axis=1)
#                          .apply(assign_loc, axis=1))                
#         return self
    
#     def export_bigTable(self):
#         """
#         Export ANC dataframe in analysis ready structure.
        
#         Performs a left-outer SQL join between anc surveillance table and physical geo exposures
#         table. The join is based on village-id and exposure month corresponding to gestation period. 

#         Parameters
#         ----------
#         exposure_df : Dataframe
#             Dataframe containing the exposure values.

#         Returns
#         -------
#         Dataframe
#             Combined big table with all participants and their exposure values for physical indicators.

#         """
#         big_df=self.getCohort(drop_abnormal=False).drop(columns=['f3_neighbor_help_pregnancy_problem', 'f3_form_of_help_received', 'f3_community_help_pregnancy_problem', 
#                                                       'f3_participation_in_community_group', 'f3_decision_maker_money', 'f3_decision_maker_pregnancy', 'f3_woman_has_money_for_transport', 
#                                                       'gestation', 'months', 'tracer_ga', 'tracer_visit_date', 'f2_visit_date_visit_1', 'f2_visit_date_visit_2', 'f2_visit_date_visit_3', 
#                                                       'f2_ga_at_visit_visit_1',	'f2_ga_at_visit_visit_2', 'f2_ga_at_visit_visit_3', 'f2_health_facility_visit_1', 'f2_health_facility_visit_2', 
#                                                       'f2_health_facility_visit_3', 'f2_village_visit_1', 'f2_village_visit_2', 'f2_village_visit_3', 'f2a_cohort', 'f3_highest_school_level', 
#                                                       'f3_religion', 'f3_marital_status', 'f3_live_with_partner', 'f3_occupation', 'f3_duration_of_living_together', 'f3_year_of_birth', 'f3_toilet_facility'])
#         big_df=(big_df.rename(columns={'f2_visit_date_visit_3': 'delivery_date'})
#                 .reset_index()
#                 .dropna(subset=['conception_date', 'delivery_date'])
#                 .melt(id_vars=['f2a_participant_id', 'conception_date', 'delivery_date'], var_name='exposure_month', value_name='village_code', ignore_index=True)
#                 .sort_values(by=['f2a_participant_id', 'exposure_month'])
#                 .dropna()
#                 .set_index(['f2a_participant_id', 'exposure_month']))
#         return big_df
        
    
#     # def assign_exposures(self, exposure_df):
#     #     """
#     #     Assign exposure values for physical geo indicators to participants.
        
#     #     Performs a left-outer SQL join between anc surveillance table and physical geo exposures
#     #     table. The join is based on village-id and exposure month corresponding to gestation period. 

#     #     Parameters
#     #     ----------
#     #     exposure_df : Dataframe
#     #         Dataframe containing the exposure values.

#     #     Returns
#     #     -------
#     #     Dataframe
#     #         Combined big table with all participants and their exposure values for physical indicators.

#     #     """
#     #     big_df=self.getCohort(drop_abnormal=True).drop(columns=['f3_neighbor_help_pregnancy_problem', 'f3_form_of_help_received', 'f3_community_help_pregnancy_problem', 
#     #                                                   'f3_participation_in_community_group', 'f3_decision_maker_money', 'f3_decision_maker_pregnancy', 'f3_woman_has_money_for_transport', 
#     #                                                   'gestation', 'months', 'tracer_ga', 'tracer_visit_date', 'f2_visit_date_visit_1', 'f2_visit_date_visit_2', 'f2_visit_date_visit_3', 
#     #                                                   'f2_ga_at_visit_visit_1',	'f2_ga_at_visit_visit_2', 'f2_ga_at_visit_visit_3', 'f2_health_facility_visit_1', 'f2_health_facility_visit_2', 
#     #                                                   'f2_health_facility_visit_3', 'f2_village_visit_1', 'f2_village_visit_2', 'f2_village_visit_3', 'f2a_cohort'])
#     #     big_df=(big_df.reset_index()
#     #             .melt(id_vars=['f2a_participant_id', 'conception_date'], var_name='exposure_month', value_name='village', ignore_index=True)
#     #             .sort_values(by=['f2a_participant_id', 'exposure_month'])
#     #             .dropna()
#     #             .merge(right=exposure_df, on=['exposure_month', 'village'], how='left'))
#     #     return big_df[['f2a_participant_id', 'conception_date', 'exposure_month', 'village', 'village_name', 'lst', 'ndvi', 'precip']]
    
# class Tracer():
    
#     def __init__(self, country: str, filepath: str):
        
#         self.filepath=filepath
#         self.country="The Gambia" if country.lower()=="gambia" else country.capitalize()     
        
#         self.readData(self.country)
        
#     def readData(self, country):
        
#         data=pd.read_csv(self.filepath)
#         data=data[data['Country']==country]
#         data=(data[['f2a_participant_id', 'Date_of_scan', 'TraCer_GA']].rename(columns={'Date_of_scan': 'tracer_visit_date', 'TraCer_GA': 'tracer_ga'}).
#               set_index('f2a_participant_id'))
#         self.data=data
#         return self
    
#     def getData(self):
        
#         return self.data
    
#     def formatDates(self):
        
#         self.data['tracer_visit_date']=self.data['tracer_visit_date'].apply(lambda x: pd.to_datetime(x, infer_datetime_format=True))
#         self.data['tracer_ga']=self.data['tracer_ga'].apply(lambda x: pd.to_timedelta(x, unit='W', errors='coerce'))
#         return self
    
def conception(df):
    #subtract ga_at_visit from visit_date to calculate start of pregnancy(conception date)
    df['conception_date']=pd.to_datetime(df['f2_visit_date']-df['f2_ga_at_visit'], unit='D')
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
    if pd.notna(df['conception_date']) and df['redcap_event_name']=='birth_mother_arm_1':
        df['gestation']=df['f2_visit_date']-df['conception_date']
        #restrict gestation period to not more than 9 months but keep track of actual gestation period
        df['months']=pd.date_range(start=df['conception_date']-td(days=30), end=df['f2_visit_date'], freq='MS', normalize=True) #if df['gestation']<=td(days=294) else pd.date_range(end=df['f2_visit_date_visit_3'], periods=9, freq='MS', normalize=True)
    return df #if show_range else df.drop(columns='months')

# def remap(row):
#     if isinstance(row['select_choices_or_calculations'], str):
#         localities=dict()
#         #todo: deal with villages in 'other' and 'dont know' columns
#         for locality in row['select_choices_or_calculations'].split('|'):
#             k, v=locality.split(',')
#             localities[int(k)]=v.strip()
#         row['localities']=localities
#     return row
# creating new columns in the dataset mapping data values for each locality using the respective dictionary.
# def set_locality(df, **data_dict):
#     for key, value in data_dict.items():
#         for k, v in value.items():
#             df[k]=df[k].map(v)
#         df['village']=(df.filter([k for k in data_dict['village_dic']], axis=1)
#                                    .apply(lambda x: max(x.dropna()) if x.dropna().any() else np.nan, axis=1))
#         df['chu']=(df.filter([k for k in data_dict['chu_dic']], axis=1)
#                                .apply(lambda x: max(x.dropna()) if x.dropna().any() else np.nan, axis=1))
#         df['linkfacility']=(df.filter([k for k in data_dict['linkfacility_dic']], axis=1)
#                                    .apply(lambda x: max(x.dropna()) if x.dropna().any() else np.nan, axis=1))
#         df['county']=(df.filter([k for k in data_dict['county_dic']], axis=1)
#                                .apply(lambda x: max(x.dropna()) if x.dropna().any() else np.nan, axis=1))
#         df['subcounty']=(df.filter([k for k in data_dict['subcounty_dic']], axis=1)
#                                    .apply(lambda x: max(x.dropna()) if x.dropna().any() else np.nan, axis=1))
#         df['healthfacility']=(df.filter([k for k in data_dict['healthfacility_dic']], axis=1)
#                                .apply(lambda x: max(x.dropna()) if x.dropna().any() else np.nan, axis=1))

# def extract_villages(dict_path, anc_path, country, out_path):
    
#     data_dict=pd.read_csv(dict_path, index_col=0)
    
#     # creating a nested dictionary containing dictionaries of the localities        
#     prefixes= ['f2_ke_v', 'f2_ke_chu', 'f2_ke_link', 'f2_ke_county', 'f2_ke_sub_county', 'f2_ke_health']
#     final_dictionary = []
#     for p in prefixes:
#         final_dictionary.append(data_dict.filter(like=p, axis=0)[['select_choices_or_calculations']]
#                                 .apply(remap, axis=1)[['localities']]
#                                 .dropna().to_dict()['localities'])
        
#     # allocating dictionary names for each dictionary 
#     names = ['village_dic', 'chu_dic', 'linkfacility_dic', 'county_dic', 'subcounty_dic', 'healthfacility_dic']
#     data_dict= dict(zip(names,final_dictionary))
    
#     #code to run for all the 3 visits at the same time.
#     for i in range(1, 4):
#         anc_df=pd.read_csv(os.path.join(anc_path, r'Visit{}_v2.csv'.format(i)))        
#         anc_df=set_locality(anc_df, **data_dict)
        
#         #dealing with villages from the 'other_name_column'
#         #subsetting the dataset to get the extracted localities columns and the other name columns
#         df_loca = anc_df[['f2_ke_vfg_other_name','village', 'f2_ke_chu_other_name', 'chu', 'f2_ke_link_facility_other_name', 'linkfacilities', 'f2_ke_county_other_name', 'counties', 'f2_ke_sub_county_other_name', 'subcounties']]

#         #grouping the columns to be merged together in pairs
#         pairs =[df_loca.columns[0:2], df_loca.columns[2:4], df_loca.columns[4:6], df_loca.columns[6:8], df_loca.columns[8:10]]
#         pairs['counties'].replace('Other', np.nan, inplace=True)

#         #applying the merge function to all the pairs
#         df_final = []
#         for item in pairs:
#             df_final.append(df_loca[item].apply(lambda x:" ".join(x.dropna(). astype(str)),axis = 1))
#             localities = pd.DataFrame(df_final).rename(index = {0:'villages', 1:'chu', 2:'link_facilities', 3:'sub_county`s', 4:'county`s' }).T
            
#         anc_df.to_csv(os.path.join(out_path, r'Visit{}_v2.csv'.format(i)))
        

