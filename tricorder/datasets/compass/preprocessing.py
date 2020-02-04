import os
import numpy as np
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
    
def extract_numeric(df,cols):
    for c in cols:
        df[c] = df[c].transform(conv_numeric)
        numeric_rows = df[c].transform(isnum)
        residuals = df[~numeric_rows].copy()
        df = df[numeric_rows].copy()
        
    return df.astype({c:np.float32 for c in cols}), residuals

def preprocess_encounters(df):
    df = df[~df.encounter_id.isna()]
    df['death_during_encounter'] = df.death_during_encounter.replace({0:False,1:True})
    df['gender'] = df.gender.replace({1:'M',2:'F'})

    return df.astype({'encounter_id':int, 'death_during_encounter':bool})

def preprocess_procedures(df):
    print('cleaning...')
    procs = df[~df.encounter_id.isna()]
    
    # Filter imprecise dates
    print('   Filtering imprecise procstart dates')
    procs = procs[~procs.days_from_dob_procstart.isin([">32507"]).values]
            
    # Convert values to numeric
    print('   Filtering non-numeric procstart dates')
    procs,junk = extract_numeric(procs,['days_from_dob_procstart'])
    procs = procs.dropna()
            
    # Filter negative days
    print('   Filtering negative procstart dates')
    procs = procs[procs.days_from_dob_procstart>0]
#     procs['days_from_dob_procstart'] = procs.days_from_dob_procstart.astype(np.uint)
    return procs.astype({'encounter_id':np.int, 'days_from_dob_procstart':np.uint})

def coerce(df, column, input_vals, output):
    replace_dict = {k:output for k in input_vals}
    df[column] = df[column].replace(replace_dict)
    return df

def preprocess_labs(df):
    print('cleaning...')
    df,junk = extract_numeric(df,['lab_result_value'])
    print('removed {} rows'.format(len(junk)))
    
    df = coerce(df,'lab_result_unit',['10^9/L', '10 9/L', '10*9/L'], '10^9/L')
    df['lab_component_name'] = df.lab_component_name.transform(lambda s: s.upper())
    
    df = df[~df.lab_collection_days_since_birth.isin([">32,507.25"]).values]

    return df

def preprocess_flowsheet(df):
    print('cleaning...')
    df,junk = extract_numeric(df,['flowsheet_value'])
    print('removed {} rows'.format(len(junk)))
    
    return df