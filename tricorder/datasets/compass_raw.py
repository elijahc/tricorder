import os
import numpy as np
from .utils import load_table, isnum, isthresh

# for dset in ['TAVR','SWAN']:
# raw_path = os.path.join(compass_path,'raw','TAVR')

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
        flow = load_table(os.path.join(self.root,'raw','SWAN'),'Table2_Flowsheet.csv')
        if preprocess:
            flow = preprocess_flowsheet(flow)
        
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

