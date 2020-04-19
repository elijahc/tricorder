import os
import numpy as np
import pandas as pd

tdelta = lambda x: td(x,'flowsheet')
to_dt = lambda row: row['DOB']+tdelta(row)

tdelta = lambda x,table: pd.Timedelta('{} days {} hours {} min {} sec'.format(x['{}_days_since_birth'.format(table)], *x['{}_time'.format(table)].split(':')))

def isFloat(string):
    try:
        float(str(string).strip())
        return True
    except ValueError:
        return False

isnum = lambda x: str(x).isnumeric() or isFloat(str(x))
isthresh = lambda x: str(x).startswith('>') or str(x).startswith('<')

def check_and_load(fp,load_func=pd.read_csv,**kwargs):
    if os.path.exists(fp):
        print('Loading file: \n','\t{}'.format(fp))
        return load_func(fp,**kwargs)
    else:
        print('File not found: \n',fp)
        raise ValueError

check_and_load_csv = check_and_load

def load_table(data_dir,fn,load_func=pd.read_csv,**kwargs):
    fp = os.path.join(data_dir,fn)
    
    return check_and_load_csv(fp,load_func,**kwargs)

def add_day(df):
    for c in df.columns:
        if str(c).endswith('days_since_birth'):
            df['day'] = df.groupby('encounter_id')[c].transform(lambda x: x-x.min())
    return df

def add_fake_datetime(df, dobs, rel_year=2018):
    df = df.merge(dobs,on='encounter_id')
    df['datetime']=df.apply(to_dt,axis=1)

    return df[['encounter_id','datetime','display_name','flowsheet_value','day']]

def add_hour(df):
    time_cols = [c for c in df.columns if str(c).endswith('time')]
    if len(time_cols) is not 1:
        print('Missing or too many time cols')

        raise

    elif 'day' not in df.columns:
        df = add_day(df)

    time_col = time_cols[0]

    df['hour'] = df[time_col].transform(lambda t: int(str(t).split(':')[0]))
    df['hour'] = df['hour']+(24*df.day.astype(int))
    return df

def add_minute(df):
    time_cols = [c for c in df.columns if str(c).endswith('time')]
    if len(time_cols) is not 1:
        print('Missing or too many time cols')

        raise

    elif 'day' not in df.columns:
        df = add_day(df)
    elif 'hour' not in df.columns:
        df = add_hour(df)

    time_col = time_cols[0]

    minutes = np.array(df[time_col].transform(lambda t: int(str(t).split(':')[1])))
    df['minute'] = minutes+(24*60*df.day.astype(int)).values+(60*df.hour).values
    return df

def search(q,series):
    idxs = [q in c for c in series.values]
    return series[idxs]
