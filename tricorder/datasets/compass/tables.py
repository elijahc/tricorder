import os
import numpy as np
import pandas as pd
import gc
import multiprocessing
import dask.dataframe as dd
import dask
from .utils import load_table, isnum, isthresh, search
from .preprocessing import preprocess_encounters, preprocess_procedures, preprocess_labs, preprocess_flowsheet

SEARCH_COLS = {
    'Table1_Encounter_Info.csv': None,
    'Table2_Flowsheet.csv' : 'display_name',
    'Table2_Flowsheet_status.csv' : 'display_name',
    'Table2_Flowsheet_numeric.csv' : 'display_name',
    'Table3_Lab.csv' : 'lab_component_name',
    'Table6_Procedure.csv' : 'order_name',
}

RAW_DTYPES = {
    'Table1_Encounter_Info.csv': {
        'encounter_id': np.uint,
        'person_id': np.uint,
#         'gender': str,
        'age': np.uint8,
        'financial_class': str,
        'death_during_encounter': np.bool
    
    },'Table2_Flowsheet.csv' : {
        'encounter_id':np.uint,
        'flowsheet_days_since_birth': np.uint,
#         'flowsheet_time':str,
        'display_name':str,
        
    },'Table2_Flowsheet_status.csv' : {
        'encounter_id':np.uint,
        'flowsheet_days_since_birth': np.uint,
#         'flowsheet_time':str,
        'display_name':str,
    
    }, 'Table3_Lab.csv' : {
        'encounter_id':np.uint,
        'lab_component_name': str,
        'lab_result_value' : str,
#         'lab_result_unit' : str,
#         'lab_collection_days_since_birth': np.uint
    
    }, 'Table6_Procedure.csv' : {
        'encounter_id':np.float,
        'order_name':str,
#         'days_from_dob_procstart':np.uint,

    },
}

NEW_COLUMNS = {
    'Table1_Encounter_Info.csv': None,
    'Table2_Flowsheet_status.csv' : {
        'flowsheet_days_since_birth' : 'days_from_dob',
        'display_name' : 'name',
        'flowsheet_value': 'value',
#         'flowsheet_unit' : 'unit',
        'flowsheet_time' : 'time',
    }, 'Table2_Flowsheet.csv' : {
        'flowsheet_days_since_birth' : 'days_from_dob',
        'display_name' : 'name',
        'flowsheet_value': 'value',
#         'flowsheet_unit' : 'unit',
        'flowsheet_time' : 'time',
    
    }, 'Table3_Lab.csv' : {
        'lab_collection_days_since_birth' : 'days_from_dob',
        'lab_component_name' : 'name',
        'lab_result_value': 'value',
        'lab_result_unit' : 'unit',
        'lab_collection_time' : 'time'
    
    }, 'Table6_Procedure.csv' : {
        'order_name' : 'procedure'
    },
}

class Table(object):
    def __init__(self, raw_path, preprocess_func=None, dtype=None, new_columns=None):
        self.file_path = raw_path
        self.data_root, self.table_fn = os.path.split(self.file_path)
        if self.table_fn in SEARCH_COLS.keys():
            self.default_col = SEARCH_COLS[self.table_fn]
        else:
            self.default_col = None
        
        self.dtype = dtype or RAW_DTYPES[self.table_fn]
        
        self.preprocess = preprocess_func
        self.columns = pd.read_csv(self.file_path, nrows=5).columns.values
        self.new_columns = new_columns or NEW_COLUMNS[self.table_fn]
        self.df = None

    def load_table(self, force_reload=False, rename=False,load_func=pd.read_csv):
        if self.df is None:
            #Check if we've already preprocessed this
            cache_fn = self.table_fn.split('.')[0]+'.parquet'
            cache = os.path.join(self.data_root,cache_fn)
            if not os.path.exists(cache) or force_reload:
                try:
                    tab = load_table(self.data_root,self.table_fn, load_func=load_func, dtype=self.dtype)
                except ValueError as e:
                    print('   Error loading table')
                    print('   {}'.format(str(e)))
                    print('   Loading without dtypes')
                    print()
                    tab = load_table(self.data_root,self.table_fn, load_func=load_func)
                
                if self.preprocess is not None:
                    self.df = self.preprocess(tab)
                else:
                    self.df = tab
                del tab
                gc.collect()
            elif os.path.exists(cache) and force_reload==False:
                self.df = load_table(self.data_root,cache_fn,load_func=pd.read_parquet, engine='pyarrow')
                
        return self.df
        
    def search(self, query, column = None):
        if column is None and self.default_col is None:
            raise ValueError('No default search column specified, must provide column to search in column argument')
        elif column is None and self.default_col is not None:
            column = self.default_col
        
        search_col = self.load_table()[column].drop_duplicates()
            
        return search(query, search_col)
    
    def sel(self, *args, how='all', rename_columns=False, **kwargs):
        if len(args) == 1 and len(kwargs.keys()) < 1 and self.default_col is None:
            raise ValueError('No default search column specified, must query in form of sel(column=[val1, val2, val3...])')
        elif len(args) == 1 and self.default_col is not None:
            kwargs = {
                self.default_col : args[0]
            }
            
        table = self.load_table()
        if isinstance(table,dask.dataframe.core.DataFrame):
            table = table.compute()
        
        bin_masks = [table[k].isin(v).values for k,v in kwargs.items()]
        if how == 'all':
            for k,v in kwargs.items():
                table = table[table[k].isin(v)]
#             iter_mask = [True]*len(bin_masks[0])
#             for m in bin_masks:
#                 iter_mask = np.logical_and(iter_mask, m)
#             mask = iter_mask
        out = table.copy()
        if rename_columns and self.new_columns is not None:
            return table.rename(columns = self.new_columns)
        else:
            return table
        
    def rename(self, df):
        return 

    def columns(self):
        return self.load_table().columns