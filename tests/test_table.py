import pytest
import os
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
        t.unique()
        t._filter_table()
