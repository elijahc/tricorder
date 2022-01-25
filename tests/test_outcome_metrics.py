from importlib.metadata import requires
import pytest
import os
import pandas as pd
from tricorder.swan import SWAN
from tricorder.outcome_metrics import OxygenDelivery
from tricorder.procedure_codesets import cabg_names

datadir = os.getenv('DEV_DATA_DIR') 
swan = SWAN(os.path.join(datadir,'compass','SWAN_20210210'))
pc = swan.create_procedure_cohort(cabg_names)

m = OxygenDelivery(db=swan, encounter_id=pc.eid)

def test_required_methods():
    required_methods = [
        'after_labs_sel',
        'after_flowsheet_sel',
        'db_fetch',
        'db_sample',]
    for f in required_methods:
        assert f in dir(m)

def test_db_sample():
    m = OxygenDelivery(db=swan, encounter_id=pc.eid)
    assert isinstance(m.db_sample(20),pd.DataFrame)
    assert isinstance(m.db_fetch(),pd.DataFrame)