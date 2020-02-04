import os
import numpy as np
import pandas as pd
from .utils import load_table, isnum, isthresh, search
from .compass_raw import preprocess_encounters, preprocess_procedures, preprocess_labs

SEARCH_COLS = {
    'Table1_Encounter_Info.csv': None,
    'Table2_Flowsheet.csv' : 'display_name',
    'Table3_Lab.csv' : 'lab_component_name',
    'Table6_Procedures.csv' : 'order_name',
}

class Table(object):
    def __init__(self, raw_path, preprocess_func=None):
        self.file_path = raw_path
        self.data_root, self.table_fn = os.path.split(self.file_path)
        self.default_col = None
        if self.table_fn in SEARCH_COLS.keys():
            self.default_col = SEARCH_COLS[self.table_fn]
        
        self.preprocess = preprocess_func
        self.df = None

    def load_table(self, force_reload=False):
        if self.df is None or force_reload:
            tab = load_table(self.data_root,self.table_fn)
            if self.preprocess is not None:
                tab = self.preprocess(tab)
            self.df = tab
                
        return self.df
        
    def search(self, query, column = None):
        if column is None and self.default_col is None:
            raise ValueError('No default search column specified, must provide column to search in column argument')
        elif column is None and self.default_col is not None:
            column = self.default_col
    
        search_col = self.load_table()[column].drop_duplicates()
            
        return search(query, search_col)
    
    def sel(self, *args, inclusive='all', **kwargs):
        if len(args) == 1 and len(kwargs.keys()) < 1 and self.default_col is None:
            raise ValueError('No default search column specified, must query in form of sel(column=[val1, val2, val3...])')
        elif len(args) == 1 and self.default_col is not None:
            kwargs = {
                self.default_col : args[0]
            }
            
        table = self.load_table()
        
        bin_masks = [table[k].isin(v).values for k,v in kwargs.items()]
        if inclusive == 'all':
            iter_mask = [True]*len(bin_masks[0])
            for m in bin_masks:
                iter_mask = np.logical_and(iter_mask, m)
            mask = iter_mask
        
        return table[mask]
    
    def columns(self):
        return self.load_data().columns
        

class SWAN(object):
    def __init__(self,root_dir):
        self.root = root_dir
        self.raw_dir = os.path.join(self.root, 'raw', 'SWAN')
        
        self.encounters = Table(os.path.join(self.raw_dir,'Table1_Encounter_Info.csv'),
                                preprocess_func=preprocess_encounters
                               )
        self.labs = Table(os.path.join(self.raw_dir, 'Table3_Lab.csv'),
                          preprocess_func=preprocess_labs
                         )
        
        self.flowsheet = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet.csv'))
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
                
            out_dfs.append(flow_df)
            
        results = pd.concat(out_dfs)
        
        results = results.merge(enc_df[['encounter_id','death_during_encounter']], on='encounter_id')
        
        for df in [proc_df, lab_df, flow_df, enc_df]:
            if isinstance(df, pd.DataFrame):
                del df
        
        return results[col_order]
        
        def pivot_table(self, procedures=None, labs=None, flowsheet=None):
            pass
            
            