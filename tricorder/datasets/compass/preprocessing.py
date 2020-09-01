import os
import numpy as np
import pandas as pd
import multiprocessing
import datetime
from .utils import load_table, isnum, isthresh

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
    df['death_during_encounter'] = df.death_during_encounter.replace({0:False,1:True})
    
#     if np.issubdtype(df.gender.dtype,np.dtype(int)):
    df['gender'] = df.gender.replace({1:'Male',2:'Female'})

    return df.astype({'encounter_id':np.uint, 'death_during_encounter':bool})

def preprocess_procedures(df):
    print('cleaning...')
    procs = df.dropna()

    # Filter imprecise dates
    print('   Filtering imprecise procstart dates')
    procs = procs[procs.days_from_dob_procstart != ">32507"]

    # Convert values to numeric
    print('   Filtering non-numeric procstart dates')
    procs,junk = extract_numeric(procs,'days_from_dob_procstart')
    procs = procs.dropna().astype({'days_from_dob_procstart':np.uint})

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

    
    labs,junk = extract_numeric(labs,'lab_result_value')
    print('removed {} rows'.format(len(junk)))
    
    labs = coerce(labs,'lab_result_unit',['10^9/L', '10 9/L', '10*9/L'], '10^9/L')
    labs['lab_component_name'] = labs.lab_component_name.apply(lambda s: s.upper(),meta=('lab_component_name','object')).astype('category')
    
    labs['lab_result_unit'] = labs.lab_result_unit.astype('category')
    str_to_dt = lambda s: datetime.timedelta(**{k:int(v) for k,v in zip(['hours','minutes','seconds'], s.split(':'))})
    
    labs['lab_collection_time'] = labs.lab_collection_time.apply(str_to_dt)
    
    del df
    return labs.astype({'encounter_id':np.uint,'lab_collection_days_since_birth':np.uint})

def preprocess_flowsheet(df):
    print('cleaning...')
    df['flowsheet_value'] = pd.to_numeric(df.flowsheet_value,errors='coerce')
    df['flowsheet_time'] = pd.to_timedelta(df.flowsheet_time)
    df['display_name'] = pd.Categorical(df.display_name,categories=df.display_name.drop_duplicates().values)
#     df,junk = extract_numeric(df,['flowsheet_value'])
#     print('removed {} rows'.format(len(junk)))
    
    return df