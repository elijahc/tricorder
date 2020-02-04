# datasets.compass

import os
import numpy as np
import pandas as pd
from .utils import load_table, isnum, isthresh, search
from .preprocessing import preprocess_encounters, preprocess_procedures, preprocess_labs, preprocess_flowsheet

class SWAN(object):
    def __init__(self,root_dir):
        self.root = root_dir
        self.raw_dir = os.path.join(self.root, 'raw', 'SWAN')
        
        self.encounters = Table(os.path.join(self.raw_dir,'Table1_Encounter_Info.csv'),
                                preprocess_func=preprocess_encounters
                               )

        self.flowsheet = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet.csv'),
                               preprocess_func=preprocess_flowsheet,
                              )
        
        self.labs = Table(os.path.join(self.raw_dir, 'Table3_Lab.csv'),
                          preprocess_func=preprocess_labs
                         )
        
        self.procedures = Table(os.path.join(self.raw_dir,'Table6_Procedures.csv'),
                                preprocess_func=preprocess_procedures
                               )

    def sel(self, encounters=None, procedures=None, labs=None, flowsheet=None):
        enc_df = self.encounters.load_table()
        
        proc_df = None
        procedure_encounter_ids = None
        out_dfs = []
        if isinstance(procedures, list) or isinstance(procedures, np.ndarray):
            proc_df = self.procedures.sel(order_name=procedures)
            print('Found {} rows matching procedure filter'.format(len(proc_df)))
            
            procedure_encounters = proc_df.encounter_id.drop_duplicates().values
            
        lab_df = None
        if isinstance(labs, list) or isinstance(labs, np.ndarray):
            col_order = []
            if proc_df is not None:
                lab_df = self.labs.sel(lab_component_name=labs, encounter_id=procedure_encounters)
                lab_df = lab_df.merge(proc_df, on='encounter_id')
                lab_df['day'] = lab_df.lab_collection_days_since_birth-lab_df.days_from_dob_procstart
                
                lab_df = lab_df.rename(columns={
                    'order_name':'procedure',
#                     'days_from_dob_procstart' : 'days_dob_procedure_start'
                })
                
                id_cols = ['procedure', 'person_id', 'encounter_id']
                col_order.extend(id_cols)
            else:
                lab_df = self.labs.sel(lab_component_name=labs)
                lab_df['day'] = lab_df.lab_collection_days_since_birth
                        
            lab_df = lab_df.rename(columns={
                'lab_collection_days_since_birth' : 'days_from_dob',
                'lab_component_name' : 'name',
                'lab_result_value': 'value',
                'lab_result_unit' : 'unit',
                'lab_collection_time' : 'time',
                })
            
            time_cols = ['days_from_dob','day','time']
            col_order.extend(time_cols)
            
            result_cols = ['name','value','unit']
            col_order.extend(result_cols)
            
            print('Found {} rows from labs table matching labs filter'.format(len(lab_df)))
            
            out_dfs.append(lab_df)
            
        flow_df = None
        if isinstance(flowsheet, list) or isinstance(flowsheet, np.ndarray):
            if proc_df is not None:
                flow_df = self.flowsheet.sel(display_name=flowsheet, encounter_id=procedure_encounters)
                flow_df = flow_df.merge(proc_df, on='encounter_id')
                flow_df['day'] = flow_df.flowsheet_days_since_birth-flow_df.days_from_dob_procstart
                
                flow_df = flow_df.rename(columns={
                    'order_name':'procedure',
#                     'days_from_dob_procstart' : 'days_dob_procedure_start'
                })
                
                id_cols = ['procedure', 'person_id', 'encounter_id']
                col_order.extend(id_cols)
            else:
                flow_df = self.flowsheet.sel(display_name=flowsheet)
                flow_df['day'] = flow_df.flowsheet_days_since_birth
                
                
            flow_df = flow_df.rename(columns={
                'flowsheet_days_since_birth' : 'days_from_dob',
                'display_name' : 'name',
                'flowsheet_value': 'value',
                'flowsheet_unit' : 'unit',
                'flowsheet_time' : 'time',
                })
            
            time_cols = ['days_from_dob','day','time']
            col_order.extend(time_cols)
            
            result_cols = ['name','value','unit']
            col_order.extend(result_cols)
            
            print('Found {} rows from flowsheet table matching flowsheet filter'.format(len(flow_df)))
            
            out_dfs.append(flow_df)
            
        results = pd.concat(out_dfs, sort=True).astype({'value':np.float64,'day':np.int32, 'days_from_dob':np.int32})
        
#         hr_in_days = int(results.time.values.split(':')[0])/24.0
#         results.day = 
        
        if proc_df is None:
            id_cols = ['encounter_id']
        elif lab_df is not None or flow_df is not None:
            id_cols = ['procedure', 'person_id','encounter_id']
            time_cols = ['days_from_dob','day','time']
            result_cols = ['name','value','unit']
        
        results = results.merge(enc_df[['encounter_id','death_during_encounter']], on='encounter_id')
                
        col_order = []
        for cols in [id_cols,time_cols,result_cols]:
            col_order.extend(cols)
            
        col_order.append('death_during_encounter')
            
        for df in [proc_df, lab_df, flow_df, enc_df]:
            if isinstance(df, pd.DataFrame):
                del df
        
        return results[col_order]
        
    def pivot_table(self, procedures=None, labs=None, flowsheet=None, time_unit='day'):
        results = self.sel(procedures=procedures, labs=labs, flowsheet=flowsheet)
            
        if time_unit not in results.columns:
            raise ValueError('{} not amongst columns: {}'.format(time_unit, str(results.columns.values)))
        else:
            res_piv = results.pivot_table(values='value', index=['encounter_id',time_unit], columns='name').reset_index()
        exclude = ['hour','day','minute','time','procedure','days_from_dob','name','value','unit']
        other_cols = [col for col in results.columns.values if col not in exclude] 
            
        res_piv = res_piv.merge(results[other_cols],on='encounter_id')

        return res_piv
    
class Query(object):
    def __init__(self, dataset):
        self.compass_data = compass_dataset
        self.procedures = procedures
        self.labs = labs
        self.flowsheet = flowsheet