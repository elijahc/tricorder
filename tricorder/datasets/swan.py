import os
import numpy as np
import pandas as pd
from .compass.utils import load_table, isnum, isthresh, add_hour, add_minute
from .compass.preprocessing import *
from .compass.tables import Table
from .compass.procedure_codesets import *
from ..datasets import COMPASS_BASE

room_RN = lambda x: [s.split('ROOM CHG')[-1] for s in x.values]
room_type = lambda x: [s.split('ROOM CHG')[0].split('HB ')[-1] for s in x.values]

class SWAN(COMPASS_BASE):
    def __init__(self,root_dir = '/data/compass/raw/SWAN'):
        self.root = root_dir
        self.raw_dir = self.root
        
        self.encounters = Table(os.path.join(self.raw_dir,'Table1_Encounter_Info.csv'),
                                preprocess_func=preprocess_encounters
                               )

        self.flowsheet = Table(os.path.join(self.raw_dir, 'Table2_Flowsheet.csv'),
                               preprocess_func=preprocess_flowsheet,
                              )
        
        self.labs = Table(os.path.join(self.raw_dir, 'Table3_Lab.csv'),
                          preprocess_func=preprocess_labs
                         )
        
        self.procedures = Table(os.path.join(self.raw_dir,'Table6_Procedure.csv'),
                                preprocess_func=preprocess_procedures
                               )

    def stratify(self, procedures, name, bins, labels, flowsheet=None, operative_window='pre_op'):
        
        df = self.sel(procedures=procedures, flowsheet=[flowsheet], death_during_encounter=False)
        
        if operative_window == 'pre_op':
            pv = df.query('day < 0').pivot_table(index='encounter_id',columns='name',values='value').dropna()
           
        col_name=operative_window+'_'+name
#         pv[col_name] = 
        out = pd.cut(pv[flowsheet],bins=bins,labels=labels)
        del pv
        del df
        out.name = col_name
        
        return out.reset_index()
    
    def sel(self, procedures=None, labs=None, flowsheet=None, rename_columns=True, demographics=False, death_during_encounter=True):
        enc_df = self.encounters.load_table()
        
        proc_df = None
        procedure_encounter_ids = None
        out_dfs = []
        
        labs_or_flow = labs is not None or flowsheet is not None
        
        demographics_cols = []
        if isinstance(procedures, list) or isinstance(procedures, np.ndarray):
            proc_df = self.procedures.sel(order_name=procedures)
            print('Found {} rows matching procedure filter'.format(len(proc_df)))
            
            if demographics:
                proc_df = proc_df.merge(enc_df, on=['encounter_id','person_id'])
                demographics_cols.extend([c for c in list(enc_df.columns) if c not in ['encounter_id','person_id']])
            elif death_during_encounter:
                proc_df = proc_df.merge(enc_df[['encounter_id','death_during_encounter']], on='encounter_id')
                demographics_cols.extend(['death_during_encounter'])                
            
            if rename_columns and self.procedures.new_columns is not None:
                proc_df = proc_df.astype({'days_from_dob_procstart':np.int}).rename(columns=self.procedures.new_columns)
                        
        lab_df = None
        if isinstance(labs, list) or isinstance(labs, np.ndarray):
            col_order = []
            if proc_df is not None:
                procedure_encounters = proc_df.encounter_id.drop_duplicates().values

                lab_df = self.labs.sel(lab_component_name=labs, encounter_id=procedure_encounters)
                lab_df = lab_df.merge(proc_df, on='encounter_id')
                lab_df['day'] = lab_df.lab_collection_days_since_birth-lab_df.days_from_dob_procstart

            else:
                lab_df = self.labs.sel(lab_component_name=labs)
                lab_df = lab_df.merge(enc_df[['encounter_id','death_during_encounter']], on='encounter_id')
                lab_df['day'] = lab_df.lab_collection_days_since_birth
                        
#             lab_df['lab_collection_time'] = lab_df.lab_collection_time.apply(pd.to_timedelta)
            if rename_columns and self.labs.new_columns is not None:
                lab_df = lab_df.rename(columns = self.labs.new_columns)
            
            print('Found {} rows from labs table matching labs filter'.format(len(lab_df)))
            
            out_dfs.append(lab_df)
            
            
            
        flow_df = None
        if isinstance(flowsheet, list) or isinstance(flowsheet, np.ndarray):
            if proc_df is not None:
                procedure_encounters = proc_df.encounter_id.drop_duplicates().values
                flow_df = self.flowsheet.sel(display_name=flowsheet, encounter_id=procedure_encounters)
                flow_df = flow_df.merge(proc_df, on='encounter_id')
                flow_df['day'] = flow_df.flowsheet_days_since_birth-flow_df.days_from_dob_procstart

            else:
                flow_df = self.flowsheet.sel(display_name=flowsheet)
                flow_df['day'] = flow_df.flowsheet_days_since_birth
                
            if rename_columns and self.flowsheet.new_columns is not None:
                flow_df = flow_df.rename(columns = self.flowsheet.new_columns)
#             flow_df = flow_df.rename(columns={
#                 'flowsheet_days_since_birth' : 'days_from_dob',
#                 'display_name' : 'name',
#                 'flowsheet_value': 'value',
#                 'flowsheet_unit' : 'unit',
#                 'flowsheet_time' : 'time',
#                 })
            print('Found {} rows from flowsheet table matching flowsheet filter'.format(len(flow_df)))
            
            out_dfs.append(flow_df)

        
        # Only procedures
        if labs_or_flow == False and procedures is not None:
            results = proc_df
            id_cols = ['person_id','encounter_id', 'procedure']
            time_cols = []
            result_cols = []
        
        # Only labs or flowsheet
        elif labs_or_flow:
            results = pd.concat(out_dfs, sort=True)
            id_cols = ['person_id','encounter_id']
            time_cols = ['days_from_dob','day','time']
            result_cols = ['name','value']
            if proc_df is not None:
                id_cols.append('procedure')
                
        col_order = []
        for cols in [id_cols,time_cols,result_cols,demographics_cols]:
            col_order.extend(cols)
            
        for df in [proc_df, lab_df, flow_df, enc_df]:
            if isinstance(df, pd.DataFrame):
                del df
        
        return results[col_order]
    
@pd.api.extensions.register_dataframe_accessor("q")
class CompassAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.encounter_ids = self._obj.encounter_id.values
        self.public_methods = [m for m in dir(self) if not m.startswith('_')]
    
    def icu_days(self, swan, agg=None):
        enc_ids = self.encounter_ids
        df = swan.procedures.sel(encounter_id=enc_ids, order_name=icu_codes).astype({'order_name':str})
        icu_days = df.groupby(['encounter_id','days_from_dob_procstart','order_name']).count().reset_index()
        icu_days = icu_days.sort_values(['encounter_id','days_from_dob_procstart']).rename(columns={'person_id':'icu_days'})
        if agg is not None:
            return icu_days.groupby('encounter_id')['icu_days'].agg(agg)
        else:
            return icu_days
        
    def vent_days(self, swan, agg=None):
        enc_ids = self.encounter_ids
        df = swan.procedures.sel(encounter_id=enc_ids, order_name=['MECHANICAL VENTILATION'])
        vent_days = df.groupby(['encounter_id','days_from_dob_procstart','order_name']).count().reset_index()
        vent_days = vent_days.sort_values(['encounter_id','days_from_dob_procstart']).rename(columns={'person_id':'vent_days'})
        if agg is not None:
            vent_days = vent_days.groupby('encounter_id')['vent_days'].agg(agg)
    
        return vent_days
    
    def add(self, func, *args, **kwargs):
        if 'on' in kwargs.keys():
            merge_on = kwargs['on']
            del kwargs['on']
        else:
            merge_on = 'encounter_id'
        
        if isinstance(func, str) and func in self.public_methods:
            func_name = func
            f = getattr(self, func)
            if callable(f):
                to_merge = f(*args, **kwargs)
            else:
                to_merge = f
        else:
            func_name = str(func)
            to_merge = func(*args, **kwargs)
        
        if isinstance(to_merge, np.ndarray) or isinstance(to_merge, list):
            to_merge = pd.Series(data=to_merge, index=self._obj.encounter_id,name=func_name)
            to_merge = to_merge.to_frame()
            
        elif isinstance(to_merge, pd.Series):
            to_merge = to_merge.to_frame()
                
        return self._obj.merge(to_merge, on=merge_on)
    
    @property
    def encounter_days(self):
        if 'hour' not in self._obj.columns.tolist():
            self._obj.pipe(add_hour)
            
        enc_duration = self._obj.groupby('encounter_id').hour.agg(
            min_hour='min',
            max_hour='max'
        )

        enc_duration['duration_days'] = (enc_duration.max_hour-enc_duration.min_hour)/24

#         self._obj = self._obj.merge(enc_duration, on='encounter_id')
        
        return enc_duration.duration_days
    
    def pivot_table(self, values='value', index=['encounter_id'], columns='name', time_unit='day'):
        if time_unit not in self._obj.columns and time_unit not in ['hour', 'minute']:
            raise ValueError('{} not amongst columns: {}'.format(time_unit, str(self._obj.columns.values)))
        elif time_unit not in self._obj.columns and time_unit is 'hour':
            self._obj = self._obj.pipe(add_hour)
        elif time_unit not in self._obj.columns and time_unit is 'minute':
            self._obj = self._obj.pipe(add_minute)
        
        index = index + [time_unit]
        
        exclude = ['hour','day','minute','time','days_from_dob','name','value','unit']
        other_cols = [col for col in self._obj.columns.values if col not in exclude]
        
        return self._obj.astype({'name':str}).pivot_table(values=values, index=index, columns=columns)
        
    def rooms(self,swan):
        enc_ids = self.encounter_ids
        df = swan.procedures.sel(encounter_id=enc_ids, order_name=room_codes).sort_values('days_from_dob_procstart')
        df = df.replace({'HB OR-MINUTES OPERATING ROOM COMPLEX': 'HB OPERATING ROOM ROOM CHG'})
        r = df.order_name.transform({'room':room_type, 'RN_level':room_RN})
        df['room']=r.room.values
        df['RN_level']=r.RN_level.values
        return df.drop(columns=['order_name'])

    def stage(self, swan):
        enc_ids = self.encounter_ids
        order_codes = room_codes+OR_start
        df = swan.procedures.sel(encounter_id=enc_ids, order_name=order_codes)

        return df.sort_values(['encounter_id','days_from_dob_procstart'])
    
    def flowsheet(self, swan):
        return swan.flowsheet.sel(encounter_id=self.encounter_ids.tolist())
    
    @property
    def hour(self):
        time_cols = [c for c in self._obj.columns if str(c).endswith('time')]
        if len(time_cols) is not 1:
            print('Missing or too many time cols')

            raise

        time_col = time_cols[0]

        hour = self._obj[time_col].transform(lambda t: int(str(t).split(':')[0]))
        hour = hour+(24*self._obj.day.astype(int)).values
        return hour.values
    
    @property
    def minute(self):
        time_cols = [c for c in self._obj.columns if str(c).endswith('time')]
        if len(time_cols) is not 1:
            print('Missing or too many time cols')

            raise

        elif 'hour' not in self._obj.columns:
            self._obj = self.add('hour')

        time_col = time_cols[0]

        minutes = self._obj[time_col].transform(lambda t: int(str(t).split(':')[1]))
        minutes = minutes+(24*60*self._obj.day.astype(int)).values+(60*self._obj.hour).values
        return minutes.values
    