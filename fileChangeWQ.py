import pandas as pd
import numpy as np
class cleanWaterQuality():
    def __init__(self, data_path):
        self.data_path = data_path

    def clean_waterQuality(self):  
        data = pd.read_csv(self.data_path)

        data['sample.sampleDateTime'] = pd.to_datetime(data['sample.sampleDateTime'], errors='coerce')
        data['only_date'] = data['sample.sampleDateTime'].dt.date
        data['result_numeric'] = pd.to_numeric(data['result'], errors='coerce')
        material_col = 'sample.sampledMaterialType.label'
        if material_col in data.columns:
            mat_type = data[material_col].astype(str).str.upper()
            data['is_sewage'] = mat_type.str.contains('SEWAGE|EFFLUENT|DISCHARGE|TRADE', regex=True).astype(int)
        else:
            data['is_sewage'] = 0 
            print(f"WARNING: Column '{material_col}' not found. 'is_sewage' set to 0.")
        nochnge_clmn = [
            'sample.samplingPoint.notation',
            'only_date', 
            'sample.samplingPoint.label', 
            'sample.samplingPoint.easting', 
            'sample.samplingPoint.northing',
            'is_sewage' 
        ]
        top_determinands = data['determinand.label'].value_counts().head(15).index.to_list()
        dataFrameMeasureMents = data[data['determinand.label'].isin(top_determinands)]
        piviotSheet = dataFrameMeasureMents.pivot_table(
            index=['sample.samplingPoint.notation', 'only_date'],
            columns='determinand.label',
            values='result_numeric',
            aggfunc='mean'
        )
        df_metadata = data[nochnge_clmn].drop_duplicates(subset=['sample.samplingPoint.notation', 'only_date'])
        df_final = df_metadata.merge(piviotSheet, on=['sample.samplingPoint.notation', 'only_date'], how='inner')
        return df_final
from genericpath import isfile

def  process_file_water():
    datasets=[r'data/2020.csv',r'data/2021.csv',r'data/2022.csv',r'data/2023.csv',r'data/2024.csv']
    opt_datasets={}
    dataframes=[]
    for index,file in enumerate(datasets):
        opt_data=cleanWaterQuality(file)
        final_head=opt_data.clean_waterQuality()
        opt_datasets[index]=final_head
    for index,convert_csv in opt_datasets.items():
        file_name=f"data/water_quality{index}.csv"
        if   isfile(file_name) :
            dfs=pd.read_csv(file_name)
            dataframes.append(dfs)
            return f"Already the file is exist in the data folder "
        
        else:
            convert_it=convert_csv.to_csv(f"data/water_quality{index}.csv")
            return convert_it

print(process_file_water())
    