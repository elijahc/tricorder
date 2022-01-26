import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.csv as csv
from slugify import slugify
from .utils import search, load_table

SEARCH_COLS = {
    'Table1_Encounter_Info.csv': 'encounter_id',
    'Table2_Flowsheet.csv' : 'display_name',
    # 'Table2_Flowsheet_status.csv' : 'display_name',
    # 'Table2_Flowsheet_numeric.csv' : 'display_name',
    'Table3_Lab.csv' : 'lab_component_name',
    'Table6_Procedures.csv' : 'order_name',
    'Table7_DX.csv' : 'CodeDescription',
    'Table5_Blood_Transfusion.csv' : 'transfusion_name',
    'Table4_Administered_Medication.csv' : 'medication_name',
}

SANITIZE_COLS = {
    'Table1_Encounter_Info.csv': None,
    'Table2_Flowsheet.csv' : ['display_name','flowsheet_value'],
    'Table3_Lab.csv' : ['lab_component_name'],
    'Table4_Administered_Medication.csv' : ['medication_name'],
    'Table5_Blood_Transfusion.csv' : ['transfusion_name'],
    'Table6_Procedures.csv' : ['order_name'],
    'Table7_DX.csv' : ['CodeDescription'],
}

RAW_DTYPES = {
    'Table1_Encounter_Info.csv': {
        'encounter_id': np.uint,
        'person_id': np.uint,
#         'gender': str,
        'age': np.uint8,
        'financial_class': str,
        'death_during_encounter': bool
    
    },'Table2_Flowsheet.csv' : {
        'encounter_id':np.uint,
        'flowsheet_days_since_birth': np.uint,
#         'flowsheet_time':str,
        'display_name':str,
        
    }, 'Table3_Lab.csv' : {
        'encounter_id':np.uint,
        'lab_component_name': str,
        'lab_result_value' : str,
#         'lab_result_unit' : str,
        'lab_collection_days_since_birth': np.uint

    }, 'Table4_Administered_Medication.csv' : {
        'encounter_id':np.uint,
        'administered_days_since_birth': int,
        'administered_time': str,
        'medication_name' : str,
        #'dose': np.uint,

    }, 'Table5_Blood_Transfusion.csv' : {
        'encounter_id':np.uint,
        'transfusion_name':str,
#         'days_from_dob_procstart':np.uint,

    }, 'Table6_Procedures.csv' : {
        'encounter_id':np.uint,
        'order_name':str,
#         'days_from_dob_procstart':np.uint,

    }, 'Table7_DX.csv': {
        'person_id': np.uint,
#         'gender': str,
        'CodeDescription':str,
        'Provenance':str,
    },
}

class Table(object):
    def __init__(self, raw_fp):
        self.file_path = raw_fp

        # Extract the file directory and the file name
        self.data_root, self.table_fn = os.path.split(self.file_path)

        if self.table_fn in SEARCH_COLS.keys():
            self.search_col = self.default_col = SEARCH_COLS[self.table_fn]
        else:
            self.default_col = None
        
        self.df = None
        self.default_unique = None

    def sanitize_table(self, tab, column=None):
        column = column or self.default_col
        
        tab = tab.set_column(tab.column_names.index(column),column,pc.utf8_upper(tab[column]))
        return tab

    def _cast_column(self, tab, column, dtype):
        """Casts column defined in tab as dtype

        Parameters
        ----------
        tab : pyarrrow.Table
                 Table to use
        column : str
                 column of tab to cast
        dtype : str
                 dtype to cast column as
        """
        assert column in tab.column_names, '{} not in {}'.format(column,tab.column_names)
        
        return tab.set_column(tab.column_names.index(column),column,pc.cast(tab[column],'string'))

    def partition(self, column=None, overwrite=False):
        """Breaks up dataset into separate files partitioned on column

        Parameters
        ----------
        column : str
                 column of database to partition off of
        overwrite : bool
                 The default value of false will raise errors if there are existing files preventing overwrites
        """
        column = column or self.search_col
        out_dir = self._cache_path('.part')
        if not overwrite and os.path.exists(out_dir):
            raise IOError('cache already exists, use overwrite parameter to overwrite directory')
        
        tab = csv.read_csv(self.file_path)
        tab = tab.set_column(tab.column_names.index(column),column,pc.cast(tab[column],'string'))
        tab = self.sanitize_table(tab,column)

        print('writing partitioned dataset {} sharding on {}'.format(out_dir, column))
        pq.write_to_dataset(tab,root_path=out_dir,partition_cols=[column],)

    def unique(self, column=None, cache=True):
        """Returns unique values from the default column (e.g. order_name from procedures)
        """
        column = None or self.default_col
        if self.default_unique is None:
            self.default_unique = csv.read_csv(self.file_path)[column].to_pandas().astype(str).str.upper().unique()
        return self.default_unique
        
    def search(self, query, column = None):
        """Returns all unique entries of the specified column that match query
        Parameters
        ----------
        query : str
                 string to search column with
        column : str
                 Column to search for using query string, if none provided will search tables default column
        """
        if column is None and self.default_col is None:
            raise ValueError('No default search column specified, must provide column to search in column argument')
        elif column is None and self.default_col is not None:
            series = pd.Series(self.unique())
        elif column is not None:
            series = csv.read_csv(self.file_path)[column].to_pandas()
        
        return search(query, series)
    
    def _filter_table(self, tab, **kwargs):
        for k,v in kwargs.items():
            tab = tab.filter(
                pc.is_in(tab[k],value_set=pa.array(v))
            )

        for n in tab.column_names:
            if 'days_from' in n or 'days_since' in n:
                tab = self._cast_column(tab,n,'string')
                mask = pc.invert(pc.starts_with(tab.column(n),pattern='>'))
                tab = tab.filter(mask)

                mask = pc.utf8_is_numeric(tab.column(n))
                tab = tab.filter(mask)
        return tab

    def load_csv(self,**kwargs):
        tab = csv.read_csv(self.file_path)
        if SANITIZE_COLS[self.table_fn] is not None:
            tab = self.sanitize_table(tab)
        
        # Check if kwargs specifies returning a filtered table
        return_filtered = pd.Series([k in tab.column_names for k in kwargs.keys()]).any()
        if return_filtered:
            return self._filter_table(tab,**kwargs).to_pandas()
        else:
            return tab.to_pandas()

    def partition_load(self,**kwargs):
        if os.path.exists(self._cache_path('.part')):
            tab = pq.read_table(self._cache_path('.part'))
            # for c in STRING_COLS[self.table_fn]:
            #     tab = self.sanitize_table(tab,c)
            return self._filter_table(tab, **kwargs).to_pandas()
            # self.df = load_table(self.data_root,self.table_fn.replace('csv','part'), load_func=pq.read_table()).to_pandas()
            # self.df = load_table(self.data_root,self.table_fn.replace('csv','parquet'), load_func=pd.read_parquet(),engine='pyarrow')

    def sel(self, *args, cache=True, pivot=False, rename_columns=False, **kwargs):
        if len(args) == 1 and len(kwargs.keys()) < 1 and self.default_col is None:
            raise ValueError('No default search column specified, must query in form of sel(column=[val1, val2, val3...])')
        elif len(args) == 1 and self.default_col is not None:
            kwargs = {
                self.default_col : args[0]
            }
        kwargs = {k:v for k,v in kwargs.items() if v is not None}

        if cache and self._cache_exists():
            if os.path.exists(self._cache_path('.part')):
                df = self.partition_load(*args, **kwargs)
            elif os.path.exists(self._cache_path('.parquet')):
                print('Parquet cache detected')
                # table = self.parquet_load(*args, **kwargs)
        else:
            df = self.load_csv(**kwargs)

        try:
            df = df[~df.encounter_id.isna()]
            df = df.astype(RAW_DTYPES[self.table_fn])
        except:
            print('error converting dataframe to RAW_DTYPES')
            pass

        if pivot:
            df.flowsheet_value = df.flowsheet_value.replace({'':np.nan}).astype(float)
            df = df.pivot_table(index=['encounter_id','flowsheet_days_since_birth'],columns='display_name',values='flowsheet_value')
        return df

        # if how == 'all':
        #     for k,v in kwargs.items():
        #         table = table[table[k].isin(v)]
        # out = table.copy()
        # if rename_columns and self.new_columns is not None:
        #     return table.rename(columns = self.new_columns)
        # else:

    def columns(self):
        return csv.read_csv(self.file_path,read_options=csv.ReadOptions(skip_rows_after_names=1500)).column_names

    def _cache_path(self,ext='.parquet'):
        return os.path.join(self.data_root,self.table_fn.split('.')[0]+ext)

    def _cache_exists(self):
        return os.path.exists(self._cache_path('.parquet')) or os.path.exists(self._cache_path('.part'))

    def _cache_paths(self):
        for ext in ['.part','.parquet']:
            cp = self.file_path.replace('.csv',ext)
            if os.path.exists(cp):
                yield cp