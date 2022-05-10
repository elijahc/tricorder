import os
import pandas as pd
import numpy as np
import pyarrow
import pyarrow.parquet as pq
import pyarrow.csv as csv
import tarfile

def unpack(tar_fp,data_root):
    file = tarfile.open(tar_fp)

    file.extractall(data_root)
    file.close()

def check_and_load(fp,load_func=csv.read_csv,**kwargs):
    if os.path.exists(fp):
        print('Loading file: \n','\t{}'.format(fp))
        return load_func(fp,**kwargs)
    else:
        print('File not found: \n',fp)
        raise IOError

check_and_load_csv = check_and_load

def load_table(data_dir,fn,load_func=csv.read_csv,**kwargs):
    fp = os.path.join(data_dir,fn)
    
    return check_and_load_csv(fp,load_func,**kwargs)

def search(q,series):
    idxs = [q in c for c in series.values]
    return series[idxs]

def rebin_time(df, on=None, time_column='time'):
    accepted_binnings = ['hour','q4h','q8h','q12h','day']
    divisors = [1,4,8,12,24]
    assert on in accepted_binnings, 'binning must be one of {}'.format(accepted_binnings)
    on_map = {k:v for k,v in zip(accepted_binnings,divisors)}
    
    t = df.time / np.timedelta64(1,'D') * 24 / on_map[on]

    df['btime'] = t.apply(np.floor) * on_map[on]
    return df.copy()

def tidy_labs(df, hours=False):
    
    get_days = lambda d: pd.to_timedelta(d.lab_collection_days_since_birth-d.lab_collection_days_since_birth.min(),unit='day')

    df = df.rename(columns={'lab_component_name':'name','lab_result_value':'value','lab_collection_time':'time'})
    df.value = pd.to_numeric(df.value,errors='coerce')
    
    days = df.lab_collection_days_since_birth.apply(lambda s: pd.to_timedelta(s,unit='day'))
    
    df.time = pd.to_timedelta(df.time) + days
    df = df.dropna()
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def tidy_flow(df,to_numeric=True):
    df = df.rename(columns={'display_name':'name','flowsheet_value':'value','flowsheet_time':'time'})
    get_days = lambda d: pd.to_timedelta(d.flowsheet_days_since_birth-d.flowsheet_days_since_birth.min(),unit='day')


    if to_numeric:
        df.value = pd.to_numeric(df.value,errors='coerce')
    
    days = df.flowsheet_days_since_birth.apply(lambda s: pd.to_timedelta(s,unit='day'))
    
    df.time = pd.to_timedelta(df.time) + days
    df = df.dropna()
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def tidy_meds(df):
    df = df.rename(columns={'medication_name':'name','dose':'value','administered_time':'time'})

    df.value = pd.to_numeric(df.value,errors='coerce')
    df.encounter_id = pd.to_numeric(df.encounter_id,errors='coerce')
    
    df.administered_days_since_birth = pd.to_numeric(df.administered_days_since_birth, errors='coerce')
    
    days = df.administered_days_since_birth.apply(lambda s: pd.to_timedelta(s,unit='day'))
    
    df.time = pd.to_timedelta(df.time) + days
    df = df.dropna()
    return df[['encounter_id','time','name','value']].sort_values(['encounter_id','time'],ascending=True)

def pivot_tidy(df,t='time'):
    return df.pivot_table(index=['encounter_id',t],values='value', aggfunc='mean', columns='name')

def melt_tidy(df,t='hour'):
    return pd.melt(df.reset_index(),id_vars=['encounter_id',t],value_vars=df.reset_index().columns.tolist(),var_name='name')
