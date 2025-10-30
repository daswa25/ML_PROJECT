import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
import os
import geopandas as gpd
from shapely.geometry import Point
#reading directory
read_dir= os.listdir("data")
print(read_dir)


wq_df=pd.read_csv(r"data/wq_data.csv")
#DATA FRAME 1 WATER QUALITY DATA 
#print(wq_df.head())
print(wq_df.columns)
wq_df.columns=['sample.samplingPoint.notation','sample.samplingPoint.label','']

uw_df=pd.read_excel(r'data/uw_data.xlsx',sheet_name='UWWTPS')

uw_df.to_csv('data/converted_uw_data.csv')

#DATA FRAME 2 URBAN WASTEWATER TREATMENT PLAN
print(uw_df.head())
#DATA FRAME 3 FLOOD DATASET 
flood_df= pd.read_excel(r'data/flood_datalogs.ods')
print(flood_df.head())