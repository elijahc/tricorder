import os
import numpy as np
from .utils import load_table, isnum, isthresh

# for dset in ['TAVR','SWAN']:
# raw_path = os.path.join(compass_path,'raw','TAVR')

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
        df[c] = df[c].astype(float)
        
    return df,residuals

def preprocess_encounters(df):
    df = df[~df.encounter_id.isna()]
    df['encounter_id']=df['encounter_id'].astype(int)
    df['death_during_encounter'] = df.death_during_encounter.replace({0:False,1:True})
    return df

def preprocess_procedures(df):
    print('cleaning...')
    procs = df[~df.encounter_id.isna()]
    procs['encounter_id']=procs['encounter_id'].astype(int)
    
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
    procs['days_from_dob_procstart'] = procs.days_from_dob_procstart.astype(np.uint)
    return procs

def coerce(df, column, input_vals, output):
    replace_dict = {k:output for k in input_vals}
    df[column] = df[column].replace(replace_dict)
    return df

def preprocess_labs(df):
    print('cleaning...')
    df,junk = extract_numeric(df,['lab_result_value'])
    print('removed {} rows'.format(len(junk)))
    
    df = coerce(df,'lab_result_unit',['10^9/L', '10 9/L', '10*9/L'], '10^9/L')
    return df

class SWAN(object):
    def __init__(self,root_dir):
        self.root = root_dir
    
    def encounters(self,preprocess=True):
        enc = load_table(os.path.join(self.root,'raw','SWAN'), 'Table1_Encounter_Info.csv')
        if preprocess:
            return preprocess_encounters(enc)
        else:
            return enc
    
    def labs(self,preprocess=True,drop_na=True):
        dtypes = {
#             'lab_collection_time':np.datetime64,
#             'lab_collection_days_since_birth':np.int,
        }
        labs = load_table(
            os.path.join(self.root,'raw','SWAN'),
            'Table3_Lab.csv',
#             parse_dates=['lab_collection_time'],
        )
        if drop_na:
            labs = labs.dropna()
        if preprocess:
            preprocess_labs(labs)
        else:
            return labs
    
    def procedures(self,preprocess=True):
        procs = load_table(os.path.join(self.root,'raw','SWAN'),'Table6_Procedures.csv',
                           dtype={
#                                'encounter_id':int,
                               'person_id':int,
                               'order_name':str,
#                                'days_from_dob_procstart':int,
                                  })
        if preprocess:
            return preprocess_procedures(procs)
        else:
            return procs
    
    def flowsheet(self,preprocess=True):
        flow = load_table(os.path.join(self.root,'raw','SWAN'), 'Table2_Flowsheet.csv')
        if preprocess:
            print('cleaning...')
            flow,junk = extract_numeric(flow,['flowsheet_value'])
            print('removed {} rows'.format(len(junk)))
            return flow
        else:
            return flow
        
    
class TAVR(object):
    def __init__(self,root_dir):
        self.root = root_dir
    
    def encounters(self):
        return load_table(os.path.join(self.root,'raw','TAVR'), 'Table1_Encounter_Info.csv')
    
    def labs(self):
        return load_table(os.path.join(self.root,'raw','TAVR'), 'Table3_Lab.csv')
    
    def procedures(self):
        return load_table(os.path.join(self.root,'raw','TAVR'), 'Table6_Procedures.csv')
    
    def flowsheet(self):
        return load_table(os.path.join(self.root,'raw','TAVR'), 'Table2_Flowsheet.csv')

