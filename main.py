import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
import os
import geopandas as gpd
from shapely.geometry import Point
import datetime as dt 
#reading directory
read_dir= os.listdir("data")
print(read_dir)
def load_data(path):
    ext= os.path.splitext(path)[1]
    if ext=='.csv':
        df=pd.read_csv(path)
        return df
    elif ext in ['.xls','.xlsx']:
        df=pd.read_excel(path)
        return df
    elif ext=='.ods':
        df=pd.read_excel(path, engine='odf')
        return df
    else:
        raise ValueError("Unsupported file format: {}".format(ext))
    return df
#FRAMING THE WATER  QUALITY DATASET
wq_data=load_data(r"data/2024.csv")
flood_data=load_data(r"data/flood_datalogs.ods")
text=""
text+=f"dataset loading\n{dt.datetime.now()}"

class preprocessing():
    def __init__(self,data):
        self.data=data
        
    def get_raw_columns(self):
        data=self.data
        
        return  data.columns
    def set_column_clean(self, flag):
        data = self.data.copy()
    
        match(flag):
            case 0:
                data = data.drop(['@id','sample.samplingPoint','codedResultInterpretation.interpretation','resultQualifier.notation'], axis=1)
                data.columns = ['UID','Location','datetime_index','Description','Substance_Measurement','Details_of_Measurment','Numerical_range','Measured_value','Measured_Unit','Water_source','Monitoring','east','north']
                data['datetime_index'] = data['datetime_index'].astype(str).str.strip().replace({'0': None, '': None})
                data['datetime_index'] = pd.to_datetime(data['datetime_index'], format='%Y-%m-%dT%H:%M:%S',errors='coerce')
                data['date'] = data['datetime_index'].dt.date
                data['time'] = data['datetime_index'].dt.time
                data=data.drop(['datetime_index'],axis=1)
                return data.head(3)
            
            case 1:
                data.columns = ['datetime','area','Ucode','warning_name','type']
                data['datetime'] = data['datetime'].astype(str).str.strip().replace({'0': None, '': None})
                data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce', dayfirst=True)
                data['date'] = data['datetime'].dt.date
                data['time'] = data['datetime'].dt.time
                data=data.drop(['datetime'],axis=1)
                return data.head(3)
            
            case _:
                return "Something Error With the dataset cleaning"


                
                
            
                
        
#pre-processing
data_process=[wq_data,flood_data]

for i,data in enumerate(data_process):
    text+=f"The flag is{i} and the data is {data}{dt.datetime.now()}\n"
    data_clean=preprocessing(data)
    text+=f"The dataset executed starts{dt.datetime.now()}\n"
    print(data_clean.set_column_clean(i))
    
text+=f"the dataset completed overall at {dt.datetime.now()}\n"
with open('log_today.txt','w') as f:
    f.write(text)