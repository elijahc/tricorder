import os
import numpy as np
import pandas as pd

from .compass.utils import load_table, isnum, isthresh, add_hour, add_minute
from .compass.preprocessing import *
from .compass.tables import Table

class COMPASS_BASE(object):
    def __init__(self,root_dir = '/data/compass'):
        self.root_dir = root_dir
        

    def stratify(self, procedures, name, bins, labels, flowsheet=None, operative_window='pre_op'):
        
        df = self.sel(procedures=procedures, flowsheet=[flowsheet], death_during_encounter=False)
        
        if operative_window == 'pre_op':
            pv = df.query('day < 0').pivot_table(index='encounter_id',columns='name',values='value').dropna()
           
        col_name=operative_window+'_'+name
#         pv[col_name] = 
        out = pd.cut(pv[flowsheet],bins=bins,labels=labels)
        del pv
        del df
        out.name = col_name
        
        return out.reset_index()
    
    def sel(self, procedures=None, labs=None, flowsheet=None, rename_columns=True, demographics=False, death_during_encounter=True):
        enc_df = self.encounters.load_table()
        
        proc_df = None
        procedure_encounter_ids = None
        out_dfs = []
        
        labs_or_flow = labs is not None or flowsheet is not None
        
        demographics_cols = []
        if isinstance(procedures, list) or isinstance(procedures, np.ndarray):
            proc_df = self.procedures.sel(order_name=procedures)
            print('Found {} rows matching procedure filter'.format(len(proc_df)))
            
            if demographics:
                proc_df = proc_df.merge(enc_df, on=['encounter_id','person_id'])
                demographics_cols.extend([c for c in list(enc_df.columns) if c not in ['encounter_id','person_id']])
            elif death_during_encounter:
                proc_df = proc_df.merge(enc_df[['encounter_id','death_during_encounter']], on='encounter_id')
                demographics_cols.extend(['death_during_encounter'])                
            
            if rename_columns and self.procedures.new_columns is not None:
                proc_df = proc_df.astype({'days_from_dob_procstart':np.int}).rename(columns=self.procedures.new_columns)
                        
        lab_df = None
        if isinstance(labs, list) or isinstance(labs, np.ndarray):
            col_order = []
            if proc_df is not None:
                procedure_encounters = proc_df.encounter_id.drop_duplicates().values

                lab_df = self.labs.sel(lab_component_name=labs, encounter_id=procedure_encounters)
                lab_df = lab_df.merge(proc_df, on='encounter_id')
                lab_df['day'] = lab_df.lab_collection_days_since_birth-lab_df.days_from_dob_procstart

            else:
                lab_df = self.labs.sel(lab_component_name=labs)
                lab_df = lab_df.merge(enc_df[['encounter_id','death_during_encounter']], on='encounter_id')
                lab_df['day'] = lab_df.lab_collection_days_since_birth
                        
#             lab_df['lab_collection_time'] = lab_df.lab_collection_time.apply(pd.to_timedelta)
            if rename_columns and self.labs.new_columns is not None:
                lab_df = lab_df.rename(columns = self.labs.new_columns)
            
            print('Found {} rows from labs table matching labs filter'.format(len(lab_df)))
            
            out_dfs.append(lab_df)
            
            
            
        flow_df = None
        if isinstance(flowsheet, list) or isinstance(flowsheet, np.ndarray):
            if proc_df is not None:
                procedure_encounters = proc_df.encounter_id.drop_duplicates().values
                flow_df = self.flowsheet.sel(display_name=flowsheet, encounter_id=procedure_encounters)
                flow_df = flow_df.merge(proc_df, on='encounter_id')
                flow_df['day'] = flow_df.flowsheet_days_since_birth-flow_df.days_from_dob_procstart

            else:
                flow_df = self.flowsheet.sel(display_name=flowsheet)
                flow_df['day'] = flow_df.flowsheet_days_since_birth
                
            if rename_columns and self.flowsheet.new_columns is not None:
                flow_df = flow_df.rename(columns = self.flowsheet.new_columns)
#             flow_df = flow_df.rename(columns={
#                 'flowsheet_days_since_birth' : 'days_from_dob',
#                 'display_name' : 'name',
#                 'flowsheet_value': 'value',
#                 'flowsheet_unit' : 'unit',
#                 'flowsheet_time' : 'time',
#                 })
            print('Found {} rows from flowsheet table matching flowsheet filter'.format(len(flow_df)))
            
            out_dfs.append(flow_df)

        
        # Only procedures
        if labs_or_flow == False and procedures is not None:
            results = proc_df
            id_cols = ['person_id','encounter_id', 'procedure']
            time_cols = []
            result_cols = []
        
        # Only labs or flowsheet
        elif labs_or_flow:
            results = pd.concat(out_dfs, sort=True)
            id_cols = ['person_id','encounter_id']
            time_cols = ['days_from_dob','day','time']
            result_cols = ['name','value']
            if proc_df is not None:
                id_cols.append('procedure')
                
        col_order = []
        for cols in [id_cols,time_cols,result_cols,demographics_cols]:
            col_order.extend(cols)
            
        for df in [proc_df, lab_df, flow_df, enc_df]:
            if isinstance(df, pd.DataFrame):
                del df
        
        return results[col_order]
    
    
from .swan import SWAN
from .tavr import TAVR