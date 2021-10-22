import os
import pandas as pd
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