import os
from contextlib import contextmanager
import numpy as np
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import tarfile
from .tables import Table
from .cohort import ProcedureCohort

NEW_COLUMNS = {
    'Table1_Encounter_Info.csv': None,
    'Table2_Flowsheet_status.csv' : {
        'flowsheet_days_since_birth' : 'days_from_dob',
        'display_name' : 'name',
        'flowsheet_value': 'value',
#         'flowsheet_unit' : 'unit',
        'flowsheet_time' : 'time',
    }, 'Table2_Flowsheet.csv' : {
        'flowsheet_days_since_birth' : 'days_from_dob',
        'display_name' : 'name',
        'flowsheet_value': 'value',
#         'flowsheet_unit' : 'unit',
        'flowsheet_time' : 'time',
    
    }, 'Table3_Lab.csv' : {
        'lab_collection_days_since_birth' : 'days_from_dob',
        'lab_component_name' : 'name',
        'lab_result_value': 'value',
        'lab_result_unit' : 'unit',
        'lab_collection_time' : 'time'
    
    }, 'Table6_Procedures.csv' : {
        'order_name' : 'procedure'
    },
}


class TAVR():
    def __init__(self, root_dir = '/data/compass/TAVR'):
        self.root = root_dir
        self.raw_dir = os.path.join(self.root,'raw')
        
        self.encounters = Table(os.path.join(self.raw_dir,'Table1_Encounter_Info.csv'))

        self.flowsheet = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet.csv'))
        
        self.labs = Table(os.path.join(self.raw_dir, 'Table3_Lab.csv'))
        
        self.procedures = Table(os.path.join(self.raw_dir,'Table6_Procedures.csv'))

        self.diagnosis = Table(os.path.join(self.raw_dir,'Table7_DX.csv'))

        self.transfusion = Table(os.path.join(self.raw_dir,'Table5_Blood_Transfusion.csv'))

        self.medications = Table(os.path.join(self.raw_dir,'Table4_Administered_Medications.csv'))

    def create_procedure_cohort(self, procedures, **kwargs):
        """Creates a ProcedureCohort object
        Parameters
        ----------
        procedures : list
            list of order_names to select from procedures table
            
        Returns
        ----------
        ProcedureCohort
        """
        return ProcedureCohort(db=self, procedures=procedures, **kwargs)    
    
    def sel(self, procedures, labs=None, flowsheet=None, encounter_id=None):

        output_col_order = [
            'person_id','encounter_id','procedure',
            'days_from_dob_procstart', 'days_from_dob','day','time',
            'name','value']
        out_dfs = []
        # Grab list of encounters from procedures, then fetch labs and flowsheet data from those encounters
        proc_df = self.procedures.sel(order_name=list(procedures))
        proc_df.days_from_dob_procstart = pd.to_numeric(proc_df.days_from_dob_procstart, errors='coerce')
        proc_df = proc_df.rename(columns=NEW_COLUMNS[self.procedures.table_fn])
        if encounter_id is not None:
            proc_df = proc_df[proc_df.encounter_id.isin(encounter_id)]

        if labs is not None:
            lab_df = self.labs.sel(lab_component_name=labs,encounter_id=proc_df.encounter_id.unique())
            lab_df = lab_df.merge(proc_df, on='encounter_id')
            lab_df['day'] = lab_df.lab_collection_days_since_birth-lab_df.days_from_dob_procstart
            lab_df = lab_df.rename(columns=NEW_COLUMNS[self.labs.table_fn])
            out_dfs.append(lab_df)

        if flowsheet is not None:
            flow_df = self.flowsheet.sel(display_name=flowsheet, encounter_id=proc_df.encounter_id.unique())
            flow_df = flow_df.merge(proc_df, on='encounter_id')
            flow_df['day'] = flow_df.flowsheet_days_since_birth-flow_df.days_from_dob_procstart
            flow_df = flow_df.rename(columns=NEW_COLUMNS[self.flowsheet.table_fn])
            out_dfs.append(flow_df)

        out_df = pd.concat(out_dfs, sort=True)
        return out_df[output_col_order]
    
class TAVRDemo(TAVR):
    def __init__(self, root_dir = '/datasets/tavr_demo'):
        self.root = root_dir
        self.raw_dir = root_dir
        
        self.encounters = Table(os.path.join(self.raw_dir,'Table1_Encounter_Info.csv'))

        self.flowsheet = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet.csv'))
        
        self.labs = Table(os.path.join(self.raw_dir, 'Table3_Lab.csv'))
        
        self.procedures = Table(os.path.join(self.raw_dir,'Table6_Procedures.csv'))

        self.diagnosis = Table(os.path.join(self.raw_dir,'Table7_DX.csv'))

        self.transfusion = Table(os.path.join(self.raw_dir,'Table5_Blood_Transfusion.csv'))

        self.medications = Table(os.path.join(self.raw_dir,'Table4_Administered_Medications.csv'))