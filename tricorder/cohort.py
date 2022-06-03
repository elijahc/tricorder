import numpy as np
import pandas as pd
from tqdm.auto import tqdm
from tableone import TableOne
import os
import json
from .utils import tidy_labs, tidy_flow, tidy_procs
from .outcome_utils import mpog_aki,aki_code_map
from .codesets_ICD10 import cad,stroke
from .tables import SEARCH_COLS

blood_product_names = [
    'TRANSFUSE RBC: 1 UNITS',
    'TRANSFUSE RBC: 2 UNITS',
    'TRANSFUSE RBC: 3 UNITS',
    'TRANSFUSE RBC: 4 UNITS',
    'TRANSFUSE PLASMA: 1 UNITS',
    'TRANSFUSE PLASMA: 2 UNITS',
    'TRANSFUSE PLASMA: 3 UNITS',
    'TRANSFUSE PLASMA: 4 UNITS',
    'TRANSFUSE PLATELETS: 1 UNITS',
    'TRANSFUSE PLATELETS: 2 UNITS',
]

def cohort_tableone(co,missing=True,overall=True,**kwargs):
    kwargs['missing'] = missing
    kwargs['overall'] = overall
    cols = [
        co.age,
        co.gender,
        co.bmi,
        co.mortality,
        co.icu_los,
    ]
    t1 = cols[0]
    for c in cols[1:]:
        t1 = t1.merge(c,how='outer',on='encounter_id')
    
    tab1 = TableOne(t1,columns=t1.columns[1:].tolist(),**kwargs)
    return tab1

class CohortMetrics(object):
    def __init__(self):
        self.metrics = []
    
    def __str__(self):
        return str(self.metrics)
    
    def __repr__(self):
        print('CohortMetrics')
        for m in self.metrics:
            print('- {}'.format(str(m.__class__.classname)))
        return self.__str__()

class CohortOutcomes(object):
    def __init__(self):
        self._outcomes = []
    
    def __str__(self):
        return str(self._outcomes)
    
    def __repr__(self):
        print('CohortOutcomes')
        for o in self._outcomes:
            print('- {}'.format(str(o.__class__.classname)))
        return self.__str__()

class EncounterCohort(object):
    def __init__(self, db, encounter_id, person_id=None, metrics=[]):
        self.db = db
        self.encounter_id = encounter_id
        self.encounter_info = self.db.encounters.sel(
            encounter_id = self.encounter_id
        )

        self.eid = self.encounter_info.encounter_id.unique()
        self.pid = self.encounter_info.person_id.unique()
        self.encounters = self.eid

class ProcedureCohort(object):
    def __init__(self, db, procedures, person_id=None, encounter_id=None, metrics=[]):
        self.db = db
        self.procedures = procedures

        self.procedure_info = self.db.procedures.sel(order_name=self.procedures,encounter_id=encounter_id)
        enc = self.db.encounters.sel(
            encounter_id=self.procedure_info.encounter_id.unique(),
        )
        self.encounter_info = self.procedure_info.drop(columns=['days_from_dob_procstart','person_id']).merge(
            enc,on='encounter_id',how='left').dropna().astype({'person_id':int})

        # self.procedure_info = self.procedure_info[self.procedure_info.encounter_id.isin(self.encounter_info.encounter_id.unique())]
        self.eid = self.encounter_info.encounter_id.unique()
        self.pid = self.encounter_info.person_id.unique()
        self.encounters = self.eid

        self._offset = self._enc_series(self.procedure_info, 'days_from_dob_procstart','offset').astype(int)
        self.offset = self._offset
        self.metrics = CohortMetrics()
        
    def _find_required_data(self):
        from tricorder.procedure_codesets import icu_codes, room_codes
        proc_codes = icu_codes+room_codes+self.procedures

        required = {
            'labs':['CREATININE SERUM','TROPONIN I'],
            'flowsheet':[],
            'procedures':proc_codes,
        }
        
        for m in self.metrics.metrics:
            required_vars = m.__class__.REQUIRES
            for k,v in required_vars.items():
                if hasattr(self.db,k):
                    required[k].extend(required_vars[k])
            
        required = {k:pd.Series(v).unique().tolist() for k,v in required.items()}
        return required
        
    def set_offset(self, df):
        assert 'encounter_id' in df.columns
        assert 'offset' in df.columns
        
        self.offset = df[['encounter_id','offset']]
        print('updated offset to:')
        return self.offset.head()
    
    def _dump_tables(self, dir_path, encounter_id=None):
        if encounter_id is None:
            encounter_id = self.eid
            
        required_data = self._find_required_data()
        tables = {
            self.db.encounters.table_fn : self.encounter_info[self.encounter_info.encounter_id.isin(encounter_id)],
        }
        for k,v in required_data.items():
            if hasattr(self.db,k):
                t = getattr(self.db,k)
                query = {
                    'encounter_id':encounter_id,
                    t.search_col : v
                    
                }
                tables[t.table_fn] = t.sel(**query)
        
        for k,v in tables.items():
            fp = os.path.join(dir_path,k)
            print('writing {}'.format(fp))
            v.to_csv(fp, index=False)
            
        return [os.path.join(dir_path,k) for k in tables.keys()]

    def _enc_series(self, df, col, name=None):
        if name is None:
            name = col
        return df[['encounter_id',col]].drop_duplicates().rename(columns={col:name})
    
    def add_continuous_metric(self, metric):
        """Add a custom timevarying metric to the cohort
        """
        m = metric(db=self.db, encounter_id=self.eid)
        self.metrics.metrics.append(m)
        m.__attach__(self.metrics)
        return self.metrics
    
    def align_metric(self,df,time_column='time',events=None):
        """Aligns the timevalues a dataframe according to an event
        Parameters
        ----------
        df : pd.DataFrame
            Dataframe of data to align
        offset : pd.DataFrame, optional
            Dataframe of events to align with, must have a encounter_id and offset columns, defaults to procedure day
        time_column : str
                 Column name of time values
        """
        if events is None:
            events = self.offset
        assert 'encounter_id' in list(events.columns), 'events missing "encounter_id" column'
        assert 'offset' in list(events.columns), 'events missing "offset" column'
        
        df = df.merge(events[['encounter_id','offset']], on='encounter_id', how='left')
        df.time = df.time - pd.to_timedelta(df.offset,unit='day')
        return df
    
    @property
    def age(self):
        s = self.encounter_info
        return self._enc_series(s,'age')
        
    def labs(self,names,dropna=True):
        labs = self.db.labs.sel(lab_component_name=names,encounter_id=self.eid)

        return tidy_labs(labs)

    def flowsheet(self,names, dropna=True, to_numeric=True):
        tab = self.db.flowsheet.sel(display_name=names,encounter_id=self.eid)

        return tidy_flow(tab, to_numeric=to_numeric)
    
    def procs(self, names, dropna=True):
        tab = self.db.procedures.sel(order_name=names,encounter_id=self.eid)

        return tidy_procs(tab)
        
    def preop_labs(self,names):
        labs = self.db.labs.sel(lab_component_name=names)
        labs = tidy_labs(labs)
        labs = self.align_metric(labs)
        labs = labs[labs.time/np.timedelta64(1,'D') <= 0]
        # .query('day <= 0')
        
        # Get most recent labs from people already in he hospital
        
        return labs
    
    @property
    def mortality(self):
        return self._enc_series(self.encounter_info,'death_during_encounter', 'death')

    @property
    def gender(self):
        s = self.encounter_info.groupby('encounter_id').apply(
            lambda r: r.gender.replace({1:'Male',2:'Female'})
        ).reset_index()
        return self._enc_series(s,'gender')

    @property
    def male_gender(self):
        s = self.encounter_info.groupby('encounter_id').apply(
            lambda r: r.gender.replace({1:'Male',2:np.nan})
        ).reset_index()
        return self._enc_series(s,'gender','male gender')
    
    def postop_aki(self,method='mpog'):
        """Postop AKI
        Generates labels for whether or not the patient had AKI out to 7days postop
        """
        scr = self.labs(['CREATININE SERUM']).query('value <= 25 and value >= 0.2').reset_index()
        scr = self.align_metric(scr)
        grouper  = scr.groupby('encounter_id')
        pbar = tqdm(grouper)
        aki = []
        pbar.set_description('postop_aki')
        for _,e in pbar:
            result = mpog_aki(e,result_col='value')
            aki.append(result)
        aki = pd.DataFrame({'encounter_id':scr.encounter_id.unique(),'value':aki})
        aki['desc'] = aki.value.apply(lambda d: aki_code_map[d])
        return aki
    
    def postop_troponin(self, max_days=3):
        """Postop Troponin (I or T)
        Returns peak troponin value in the post op period parameterized by max_days
        """
        trp = self.labs(['TROPONIN I'])
        
        # Select labs between 0 and 72hrs and keep highest
        mask = (trp.time < np.timedelta64(72,'h')) & (trp.time > np.timedelta64(0,'h'))
        trp_hi = trp[mask].reset_index().groupby(['encounter_id','offset']) \
            .apply(lambda d: d[d.value==d.value.max()][['day','value']]).reset_index()[['encounter_id','offset','day','value']]
        trp_hi = self.offset.merge(trp_hi,on=['encounter_id','offset'],how='left')
        return trp_hi
    
    def postop_los(self):
        """Postop length of stay
        """
        
        last = self.room_changes.groupby('encounter_id').days_from_dob_procstart.apply(lambda s:s.max()).rename('last_day')
        c = self.offset.merge(last.astype(int).reset_index(),on='encounter_id',how='left')
        c['postop_los'] = c.last_day-c.offset
        return self._enc_series(c,'postop_los')
    
    @property
    def delirium(self):
        c = self.db.flowsheet.sel(display_name=['CAM ICU'],encounter_id=self.eid)
        return tidy_flow(c, to_numeric=False)

    def get_post_op_delirium(self, detail='full', clean=True):
        # c = self.delirium.query('day >= 0.0')
        c = self.delirium
        c = self.align_metric(c)
        c['days'] = c.time / np.timedelta64(1,'D')
        c = c.query('days >= 0.0')
        # c = c[c.time > c.time/np.timedelta64(c.time,'D')]
        if clean:
            c = c[~c.value.isin(['Unable to assess', ''])]

        if detail == 'encounter':
            c = c.groupby('encounter_id')['value'].apply(
                lambda r: (r=='Delirious- CAM+').any()
                ).rename('post_op_delirium').to_frame().reset_index()
            return self._enc_series(c.drop_duplicates(),'post_op_delirium')
        elif detail == '72hr':
            c = c.groupby('encounter_id').apply(
                lambda r: ((r.value=='Delirious- CAM+') & (r.days<=3)).any()
                ).rename('post_op_delirium').to_frame().reset_index()
            
            return self._enc_series(c.drop_duplicates(),'post_op_delirium')
        elif detail == 'full':
            c.value = c.value.apply(lambda s: s.split('-')[0])
            return c
            # return self._enc_series(c,'post_op_delirium')
        else:
            raise ValueError("for detail use either 'full', '72hr', or 'encounter'")

    def or_days(self):
        from tricorder.procedure_codesets import or_room_codes
        rooms = self.db.procedures.sel(order_name=or_room_codes,encounter_id=self.eid)
        rooms = rooms.rename(columns={'days_from_dob_procstart':'or_day'})
        rooms.or_day = pd.to_numeric(rooms.or_day,errors='coerce')
        rooms = rooms[['encounter_id','or_day','order_name']].sort_values(by=['encounter_id','or_day'])
        return rooms
    
    @property
    def room_changes(self):
        from tricorder.procedure_codesets import room_codes
        rooms = self.db.procedures.sel(order_name=room_codes,encounter_id=self.eid)
        rooms = rooms.pivot_table(index=['encounter_id','days_from_dob_procstart'],columns='order_name',values='person_id',
                                  aggfunc='count')
        rooms = rooms.reset_index().astype({'days_from_dob_procstart':int})
        return rooms        
    
    @property
    def reexploration_for_bleed(self):
        re_exp_procedures = [
                'CONTROL BLEEDING IN MEDIASTINUM, OPEN APPROACH',
                'CONTROL BLEEDING IN CHEST WALL, OPEN APPROACH',
                'POST OPERATIVE BLEEDING HEART  NO BYPASS',
                'POST OP BLEEDING HEART ON BYPASS'
            ]

        rooms = self.room_changes
    
        # determine which pateints had >1 OR day and one of the above procedures
        cidx = rooms.columns[rooms.columns.str.contains('OPERATING ROOM')]
        multior = rooms.groupby('encounter_id')[cidx].transform(lambda s: s.sum())>1.0
        long_eids = rooms.encounter_id[multior.values.flatten()]

        # combine to determine which patients have study, >1 OR day and re-operation procedure
        re_exp = self.db.procedures.sel(order_name=re_exp_procedures,encounter_id=long_eids)
        re_exp['reexploration_for_bleed'] = True
        re_exp = rooms[['encounter_id','person_id']].merge(re_exp,how='left',on=['encounter_id','person_id'])
        
        return self._enc_series(re_exp,'reexploration_for_bleed')
    
    @property
    def bmi(self):
        c = self.db.flowsheet.sel(
            encounter_id=self.eid,
            pivot=True,
            display_name=['HEIGHT','WEIGHT']
        ).reset_index().drop(columns=['flowsheet_days_since_birth']).drop_duplicates()
        c = c.groupby('encounter_id').apply(np.mean).drop(columns='encounter_id').reset_index()
        c['BMI'] = c['WEIGHT']/c['HEIGHT']
        c = self.encounter_info.merge(c[['encounter_id','BMI']],how='left',on='encounter_id')
        return self._enc_series(c, 'BMI')

    def post_op_icu_days(self):
        from tricorder.procedure_codesets import icu_codes
        c = self.db.procedures.sel(encounter_id=self.eid,order_name=icu_codes)
        # c = c.merge(self.offset,how='left').drop_duplicates()
        return c.groupby(['encounter_id','days_from_dob_procstart']).count()

    @property
    def ckd(self):
        l = self.db.labs.sel(lab_component_name=self.db.labs.search('GFR').values, encounter_id=self.eid)
        l = co.align_metric(l)
        l['days'] = l.time/np.timedelta64(1,'D')
        ckd = l.query('days < 1 & days > -3').groupby('encounter_id').apply(lambda d: (d.value<60).any()).rename('ckd').reset_index()
        
        return ckd
    @property
    def icu_start(self):
        from tricorder.procedure_codesets import icu_codes
        c = self.db.procedures.sel(encounter_id=self.eid,order_name=icu_codes)
        c = c.merge(self.offset,how='left').astype({'offset':int})
        c['day'] = c.days_from_dob_procstart.astype(int) - c.offset
        c = c[c.day>=0]
        c = self.encounter_info[['encounter_id','order_name']].merge(c[['encounter_id','offset']],how='left',on='encounter_id')
        c = c.drop_duplicates().dropna().astype({'offset':int})
        return c.groupby('encounter_id')['offset'].min().reset_index()

    @property
    def icu_los(self):
        from tricorder.procedure_codesets import icu_codes
        c = self.db.procedures.sel(encounter_id=self.eid,order_name=icu_codes)
        c = c.merge(self.offset,how='left')
        c['ICU LOS'] = c.days_from_dob_procstart.astype(int) - c.offset
        c = self.encounter_info.merge(c[['encounter_id','ICU LOS']],how='left',on='encounter_id')
        return c.groupby('encounter_id')['ICU LOS'].max().sort_index().to_frame().reset_index()
        
    def blood_products(self,kind='RBC'):
        bp = self.db.transfusion.sel(
            transfusion_name=self.db.transfusion.search('TRANSFUSE {}:'.format(kind)), 
            encounter_id=self.eid)
        bp.number_of_units = bp.number_of_units.apply(lambda s: s.split(' ')[0]).astype(int)
        bp = bp.rename(columns={
            'number_of_units':'UNITS {}'.format(kind),
            # 'transfusion_name':'TRANSFUSE {}'.format(kind)
        })
        return self._enc_series(bp,'UNITS {}'.format(kind))
    
    @property
    def mechanical_ventilation_duration(self):
        c = self.db.flowsheet.sel(
            encounter_id=self.eid,
            display_name=['VENT MODE','$ VENT MODE **REQUIRED** '])
        c.display_name = 'VENT MODE'
#         c = c.merge(self.offset,how='left').drop_duplicates()
        c = tidy_flow(c,to_numeric=False)
        c = self.align_metric(c)
        c['days'] = c.time/np.timedelta64(1,'D')
        return c.groupby('encounter_id')['days'].max().sort_index().to_frame().reset_index()

    @property
    def osa(self, code_type='ICD-10'):
        osa_codes = ['UNSPECIFIED SLEEP APNEA', 'SLEEP APNEA, UNSPECIFIED']
        c = self.db.diagnosis.sel(
            person_id=self.pid,
            CodeType=[code_type+'-CM'],
        )
        c = c.groupby('person_id').CodeDescription.apply(lambda s:s.isin(osa_codes).any()).reset_index()
        c = self.encounter_info.merge(c[['person_id','CodeDescription']],how='left',on='person_id').drop_duplicates()
        return self._enc_series(c,'CodeDescription','OSA')

    @property
    def cad(self, code_type='ICD-10-CM'):
        c = self.db.diagnosis.sel(person_id=self.pid)
        c = c.groupby('person_id').Code.apply(lambda s: s.isin(cad).any()).rename('value')
        c = self.encounter_info.merge(c.reset_index(),how='left',on='person_id').drop_duplicates().astype({'value':int})
        return self._enc_series(c,'value','CAD')
    
    @property
    def chf(self, code_type='ICD-10'):
        chf_codes = self.db.diagnosis.search('HEART FAILURE').unique()
        code_map = {k:v for k,v in zip(chf_codes,[True]*len(chf_codes))}
        c = self.db.diagnosis.sel(
            person_id=self.pid,
            CodeType=[code_type+'-CM'],
        )
        c = c.groupby('person_id').CodeDescription.apply(lambda s:s.isin(chf_codes).any()).reset_index()
        c = self.encounter_info.merge(c[['person_id','CodeDescription']],how='left',on='person_id').drop_duplicates()
        return self._enc_series(c,'CodeDescription','CHF')

    @property
    def dm(self, code_type='ICD-10'):
        dm_codes = self.db.diagnosis.search('DIABETES').unique()
        code_map = {k:v for k,v in zip(dm_codes,[True]*len(dm_codes))}
        c = self.db.diagnosis.sel(
            person_id=self.pid,
            CodeType=[code_type+'-CM'],
        )
        c = c.groupby('person_id').CodeDescription.apply(lambda s:s.isin(dm_codes).any()).reset_index()
        c = self.encounter_info.merge(c[['person_id','CodeDescription']],how='left',on='person_id').drop_duplicates()
        return self._enc_series(c,'CodeDescription','DM')
    
    @property
    def stroke(self):
        c = self.db.diagnosis.sel(person_id=self.pid)
        c = c.groupby('person_id').Code.apply(lambda s: s.isin(stroke).any()).rename('value')
        c = self.encounter_info.merge(c.reset_index(),how='left',on='person_id').drop_duplicates().astype({'value':int})
        return self._enc_series(c,'value','stroke')
        
    def export(self, path, **kwargs):
        pass