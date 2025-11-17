import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
import os
import geopandas as gpd
from shapely.geometry import Point
import datetime as dt 
#reading directory
read_dir= os.listdir("data")
import pandas as pd
import os
import geopandas as gpd
import datetime as dt
import duckdb as db 
def load_data(path, chunk=False):
    ext = os.path.splitext(path)[1]
    
    if ext == '.csv':
        if chunk:
            return pd.read_csv(path, chunksize=50000)  
        return pd.read_csv(path)
        
    elif ext in ['.xls', '.xlsx']:
        return pd.read_excel(path)
        
    elif ext == '.ods':
        return pd.read_excel(path, engine='odf')
        
    else:
        raise ValueError(f"Unsupported file format: {ext}")

#FRAMING THE WATER  QUALITY DATASET
data_paths = [r"data/2024.csv", r"data/flood_datalogs.ods", r"data/uk_climate.csv"]
datasets=[]
text=[]
processed_datasets={}

class preprocessing():
    def __init__(self,data):
        self.data=data
        
    def get_raw_columns(self):
        data=self.data
        
        return  data.columns
    def set_column_clean(self, flag):
        data = self.data
    
        match(flag):
            case 0:
                data = data.drop(['@id','sample.samplingPoint','codedResultInterpretation.interpretation','resultQualifier.notation'], axis=1)
                data.columns = ['UID','Location','datetime_index','Description','Substance_Measurement','Details_of_Measurment','Numerical_range','Measured_value','Measured_Unit','Water_source','Monitoring','east','north']
                data['datetime_index'] = data['datetime_index'].astype(str).str.strip().replace({'0': None, '': None})
                data['datetime_index'] = pd.to_datetime(data['datetime_index'], format='%Y-%m-%dT%H:%M:%S',errors='coerce')
                data['date'] = data['datetime_index'].dt.date
                data['time'] = data['datetime_index'].dt.time
                data.dropna()
                data=data.drop(['datetime_index'],axis=1)
                return data
            
            case 1:
                data.columns = ['datetime','area','Ucode','warning_name','type']
                data['datetime'] = data['datetime'].astype(str).str.strip().replace({'0': None, '': None})
                data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce', dayfirst=True)
                data['date'] = data['datetime'].dt.date
                data['time'] = data['datetime'].dt.time
                #Deals with Missing value in column
                data.dropna()
                # Dropping the datetime column because it can mislead the dataset
                data=data.drop(['datetime'],axis=1)
                return data
            case 2:
                data.columns=['datetime','decade','year','season','month','day','day_in_year','temp','w_speed','precipitation','surface_runoff','dewpoint_temp']
                data=data.drop(['decade','year','month','day','day_in_year'],axis=1)
                data['datetime'] = data['datetime'].astype(str).str.strip().replace({'0': None, '': None})
                data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce', dayfirst=True)
                data['date']=data['datetime'].dt.date
                data.dropna()
                data=data.drop(['datetime'],axis=1)
                return data
            case _:
                
                return "Something Error With the dataset cleaning"


                
                
            
                
        
#loading datasets
for path in data_paths:
    data=load_data(path)
    text.append(f"dataset {path} loading\n{dt.datetime.now()}")
    datasets.append(data)
#preprocessing
for i,data in enumerate(datasets):
    key=f"dataset_{i+1}"
    text.append(f"The flag is{i} and the data is {data.shape}{dt.datetime.now()}\n")
    data_clean=preprocessing(data)
    text.append(f"The dataset executed starts{dt.datetime.now()}\n")
    processed_datasets[key]=data_clean.set_column_clean(i)
    
    
text.append(f"the dataset completed overall at {dt.datetime.now()}\n")
df_1=processed_datasets['dataset_1']
df_2=processed_datasets['dataset_2']
df_3=processed_datasets['dataset_3']
print(df_1.head(10))

with open('log_today.txt','w') as f:
    f.write(f"\n".join(text))