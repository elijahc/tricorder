import os
import numpy as np
import pandas as pd
from .compass.utils import load_table, isnum, isthresh, add_hour, add_minute
from .compass.preprocessing import *
from .compass.tables import Table
from .compass_raw import COMPASS_BASE

class TAVR(COMPASS_BASE):
    def __init__(self,root_dir = '/data/compass/raw/TAVR'):
        self.root = root_dir
        self.raw_dir = self.root
        
        self.encounters = Table(os.path.join(self.raw_dir,'Table1_Encounter_Info.csv'),
                                preprocess_func=preprocess_encounters
                               )
        
        self.status = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet_status.csv'),
                            preprocess_func=preprocess_status,
                           )
        
        self.flowsheet = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet.csv'),
                               preprocess_func=preprocess_flowsheet,
                              )
        
        self.labs = Table(os.path.join(self.raw_dir, 'Table3_Lab.csv'),
                          preprocess_func=preprocess_labs
                         )
        
        self.procedures = Table(os.path.join(self.raw_dir,'Table6_Procedure.csv'),
                                preprocess_func=preprocess_procedures
                               )
    
    def pivot_table(self, procedures=None, labs=None, flowsheet=None, time_unit='day'):
        results = self.sel(procedures=procedures, labs=labs, flowsheet=flowsheet)
            
        if time_unit not in results.columns and time_unit not in ['hour', 'minute']:
            raise ValueError('{} not amongst columns: {}'.format(time_unit, str(results.columns.values)))
        elif time_unit not in results.columns and time_unit is 'hour':
            results = add_hour(results)
        elif time_unit not in results.columns and time_unit is 'minute':
            results = add_minute(results)
            
        res_piv = results.pivot_table(values='value', index=['encounter_id',time_unit], columns='name').reset_index()
        exclude = ['hour','day','minute','time','days_from_dob','name','value','unit']
        other_cols = [col for col in results.columns.values if col not in exclude] 
            
        res_piv = res_piv.merge(results[other_cols],on='encounter_id')

        return res_piv
    
class Query(object):
    def __init__(self, dataset):
        self.compass_data = compass_dataset
        self.procedures = procedures
        self.labs = labs
        self.flowsheet = flowsheet