# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 17:05:11 2022

@author: RetailAdmin
"""

from precise import Precise

gambia=Precise('gambia', 'C:\\Users\\RetailAdmin\\OneDrive\\Documents\\PALS\\PRECISE_Health-Geo_Data\\surveillance_data_090622')

v1=gambia.getData(2)
gambia.dropDuplicates()
v2=gambia.getData(2)

me={1:'one', 2:'two'}

# for i in me:
list(me.keys())