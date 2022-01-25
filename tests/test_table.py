import pytest
import os
import numpy as np
import pandas as pd
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa
from tricorder.procedure_codesets import cabg_names
from tricorder.tables import Table, RAW_DTYPES, SEARCH_COLS

def get_table(tab_name):
    datadir = os.getenv('DEV_DATA_DIR') 
    t = Table(os.path.join(datadir,'compass','SWAN','raw',tab_name))
    return t

def test_init():
    for tn in RAW_DTYPES.keys():
        t = get_table(tn)
        assert os.path.exists(t.data_root)
        assert os.path.exists(t.file_path)
        assert isinstance(t.columns(),list)

def test_search():
    for tn in RAW_DTYPES.keys():
        t = get_table(tn)
        assert tn in list(SEARCH_COLS.keys())
        assert SEARCH_COLS[tn] in t.columns()

def test_unqiue():
    for tn in RAW_DTYPES.keys():
        t = get_table(tn)
        assert type(t.unique('encounter_id')[0]) is str

def test_filter_table():
    for tn in RAW_DTYPES.keys():
        t = get_table(tn)
        assert isinstance(t._cache_exists(), bool)
        if t._cache_exists():
            tab = pq.read_table(t._cache_path('.part'))
        else:
            tab = csv.read_csv(t.file_path)
        vals = t.unique()
        query = {SEARCH_COLS[tn]:np.random.choice(vals,1)}
        result = t._filter_table(tab,**query)
        print(result)

        assert isinstance(result,pa.Table)