import os
import numpy as np
import pandas as pd
import multiprocessing
import datetime
from .utils import load_table, isnum, isthresh

BAD_INTS = ['>89', ">32507"]

def conv_thresh(x):
    if x.strip().startswith('<'):
        return x.split('<')[-1]
    elif x.strip().startswith('>'):
        return x.split('>')[-1]
    else:
        return x

def conv_numeric(x):
    x = str(x)
    if isnum(x):
        return float(x)
    elif isthresh(x):
        return conv_thresh(x)
    else:
        return x
    
def extract_numeric(df,c):
    numeric_rows = df[c].apply(isnum)
#         df[c] = df[c].transform(conv_numeric)
#         numeric_rows = df[c].transform(isnum)
    residuals = df[~numeric_rows]
    out = df[numeric_rows]
        
    return out.astype({c:np.float32}), residuals

def coerce(df, column, input_vals, output):
    replace_dict = {k:output for k in input_vals}
    df[column] = df[column].replace(replace_dict)
    return df

def preprocess_encounters(df):
    df = df[~df.encounter_id.isna()]
#     df['death_during_encounter'] = df.death_during_encounter.replace({0:False,1:True})
    
    if df.gender.dtype.type in [np.int64,int,np.int,np.uint]:
        df['gender'] = df.gender.replace({1:'Male',2:'Female'})
        
    if df.age.isin(['>89']).any():
        print('   coercing >89 -> 95')
        df = coerce(df,'age',['>89'],95)

    return df.astype({'encounter_id':np.uint, 'death_during_encounter':bool})

def preprocess_procedures(df):
    print('cleaning...')
    procs = df.dropna()
    bad_vals = []
    
    # Convert values to numeric
    procs,junk = extract_numeric(procs,'days_from_dob_procstart')
    procs = procs.dropna().astype({'days_from_dob_procstart':np.uint})
    print('   Filtering non-numeric procstart dates')
    
    if procs.days_from_dob_procstart.isin(bad_vals).any():
        # Filter imprecise dates
        print('   Filtering imprecise procstart dates(>32507)')
        procs = procs[procs.days_from_dob_procstart != ">32507"]

    # Filter negative days
    print('   Filtering negative procstart dates')
    procs = procs[procs.days_from_dob_procstart>0]
    out = procs.astype({'encounter_id':np.uint, 'days_from_dob_procstart':np.uint})
#     procs['days_from_dob_procstart'] = procs.days_from_dob_procstart.astype(np.uint)
    del df
    del procs
    return out

def preprocess_labs(df):
    print('cleaning...')
    labs = df.dropna()
    labs = labs[labs.lab_collection_days_since_birth != ">32,507.25"]
    labs = labs[labs.lab_collection_days_since_birth != ">32507"]

    
    labs,junk = extract_numeric(labs,'lab_result_value')
    print('removed {} rows'.format(len(junk)))
    
    labs = coerce(labs,'lab_result_unit',['10^9/L', '10 9/L', '10*9/L'], '10^9/L')
    labs['lab_component_name'] = labs.lab_component_name.apply(lambda s: s.upper()).astype('category')
    
    labs['lab_result_unit'] = labs.lab_result_unit.astype('category')
#     str_to_dt = lambda s: datetime.timedelta(**{k:int(v) for k,v in zip(['hours','minutes','seconds'], s.split(':'))})
    
#     labs['lab_collection_time'] = labs.lab_collection_time.apply(str_to_dt)
    
    del df
    return labs.astype({'encounter_id':np.uint,'lab_collection_days_since_birth':np.uint})

def preprocess_status(df):
    df = df.dropna()
    
    df = coerce(df, 'flowsheet_days_since_birth', ['>32507','>32,507.25'], 34697)
    df.flowsheet_days_since_birth = df.flowsheet_days_since_birth.astype(np.uint64)

    df.display_name = df.display_name.astype('category')

    numeric_idxs = df.flowsheet_value.apply(isnum)
    df = df[~numeric_idxs]
    df = df.dropna()
        
    return df

def preprocess_flowsheet(df, split_numeric=True):
    print('cleaning...')
    #     df['flowsheet_time'] = pd.to_timedelta(df.flowsheet_time)
    
    df = coerce(df, 'flowsheet_days_since_birth', ['>32507','>32,507.25'], 34697)
    df.flowsheet_days_since_birth = df.flowsheet_days_since_birth.astype(np.uint64)
#     df['flowsheet_days_since_birth'] = df[df.flowsheet_days_since_birth != '>32507']
#     df['display_name'] = pd.Categorical(df.display_name,categories=df.display_name.drop_duplicates().values)
    df.display_name = df.display_name.astype('category')

    numeric_idxs = df.flowsheet_value.apply(isnum)
    df = df[numeric_idxs]
#         df,junk = extract_numeric(df,['flowsheet_value'])
#         print('removed {} rows'.format(len(junk)))
    df.flowsheet_value = pd.to_numeric(df.flowsheet_value,errors='coerce')
    df = df.dropna()
    return df




