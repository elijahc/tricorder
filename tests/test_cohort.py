import pytest
import pandas as pd
from tableone import TableOne
from tricorder.swan import SWAN, ProcedureCohort
from tricorder.procedure_codesets import cabg_names

def cabg_cohort():
    swan = SWAN('/Users/elijahc/data/compass/SWAN_20210210/')
    return ProcedureCohort(db=swan, procedures=cabg_names)

pc = cabg_cohort()

def table1():
    root = pc.get_post_op_delirium('encounter').reset_index()
    root.post_op_delirium = root.post_op_delirium.replace({False:'No Delirium',True:'Delirium'})
    ops = pc.procedure_info[['encounter_id','order_name']].rename(columns={'order_name':'surgery'})
    ops = ops.replace({k:v for k,v in zip(cabg_names,['CABG']*len(cabg_names))})
    root = root.merge(ops,how='inner',on='encounter_id')
    cols = [
        pc.age,
        pc.gender,
        pc.bmi, 
        pc.dm,
        pc.chf,
        pc.osa,
        pc.mortality,
        pc.mechanical_ventilation_duration,
        pc.icu_los
        ]
    # t1 = root
    vars = cols[0]
    for c in cols[1:]:
        vars = vars.merge(c,how='outer',on='encounter_id')
    t1 = root.merge(vars,how='left',on='encounter_id')
    return t1.drop(columns='index').drop_duplicates()

def test_cohort():

    with swan.cohort(procedures=cabg_names) as lens:
        age = lens.age
        assert isinstance(age, pd.Series)
        print(age)

        mortality = lens.mortality
        assert isinstance(mortality, pd.Series)
        print(mortality)
