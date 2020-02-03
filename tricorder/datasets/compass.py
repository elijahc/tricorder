import os
import numpy as np
from .utils import load_table, isnum, isthresh
from .compass_raw import preprocess_encounters

class Table(object):
    def __init__(self, raw_path, preprocess_func=None):
        self.file_path = raw_path
        self.preprocess = preprocess_func
        self.df = None

    def load_table(self):
        tab = load_table(self.file_path)
        if self.preprocess is not None:
            return preprocess(tab)
        else:
            return tab

class SWAN(object):
    def __init__(self,root_dir):
        self.root = root_dir

    def sel(encounters=None, procedures=None, labs=None, flowsheet=None):
        if encounters is not None and isinstance(encounters, list):
            pass

        if procedures is not None and isinstance(procedures, list):
            pass
        

