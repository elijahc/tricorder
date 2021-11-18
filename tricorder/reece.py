import os
import pandas as pd

class DB(object):
    names = {
        'primary':'Primary Root_238.csv',
        'redo': 'ReRoot_68.csv'
    }
    def __init__(self, data_dir='/data/reece'):
        self.data_dir = data_dir
        
    def primary(self):
        fp = os.path.join(self.data_dir,self.names['primary'])        
        return pd.read_csv(fp)
    
    def redo(self):
        fp = os.path.join(self.data_dir,self.names['redo'])        
        return pd.read_csv(fp)