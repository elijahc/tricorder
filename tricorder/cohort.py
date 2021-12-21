import numpy as np
import pandas as pd
from .outcome_utils import mpog_aki
from .codesets_ICD10 import cad,stroke

class ProcedureCohort(object):
    def __init__(self, db, procedures, person_id=None, encounter_id=None):
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

        self.offset = self._enc_series(self.procedure_info, 'days_from_dob_procstart','offset').astype(int)

    def _enc_series(self, df, col, name=None):
        if name is None:
            name = col
        return df[['encounter_id',col]].drop_duplicates().rename(columns={col:name})

    @property
    def age(self):
        s = self.encounter_info
        return self._enc_series(s,'age')

    def labs(self,names,dropna=True):
        labs = self.db.labs.sel(lab_component_name=names,encounter_id=self.eid)
        labs = labs.rename(columns={'lab_result_value':'value',
                                    'lab_collection_days_since_birth':'day_from_dob',
                                    'lab_collection_time':'time',
                                    'lab_component_name':'name'})
        # Filter erroneous values
        labs.value = pd.to_numeric(labs['value'],errors='coerce')
        labs = labs.dropna()
        
        labs.day_from_dob = pd.to_numeric(labs.day_from_dob,errors='coerce')
        
        # Get day relative to first OR day
        proc_day = self.offset.groupby('encounter_id').apply(lambda d: d.offset.min()).rename('offset').reset_index()
        labs = labs.merge(proc_day, how='left',on='encounter_id')
        labs['day'] = pd.to_numeric(labs.day_from_dob-labs.offset,errors='coerce')
        labs.time = pd.to_timedelta(labs.time)
        labs = labs.dropna()
        labs.time = [t + np.timedelta64(24*int(d),'h') for t,d in zip(labs.time,labs.day)]
        labs = labs[['encounter_id','offset','day','time','value','lab_result_unit','name',]]
        return labs
    
    def preop_labs(self,names):
        labs = self.labs(names).query('day <=0')
        
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
        aki = scr.groupby(['encounter_id','offset']).apply(mpog_aki,result_col='value').reset_index()
        return aki.sort_values(by=['encounter_id','offset'])
    
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
        c = self.db.sel(procedures=self.procedures,flowsheet=['CAM ICU'],encounter_id=self.eid)
        return c

    def get_post_op_delirium(self, detail='full', clean=True):
        c = self.delirium.query('day >= 0.0')
        if clean:
            c = c[~c.value.isin(['Unable to assess', ''])]

        if detail == 'encounter':
            c = c.groupby('encounter_id')['value'].apply(
                lambda r: (r=='Delirious- CAM+').any()
                ).rename('post_op_delirium').to_frame().reset_index()
            return self._enc_series(c.drop_duplicates(),'post_op_delirium')
        elif detail == 'full':
            return self._enc_series(c,'post_op_delirium')
        else:
            raise ValueError("for detail use either 'full' or 'encounter'")

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

    @property
    def mechanical_ventilation_duration(self):
        c = self.db.procedures.sel(
            encounter_id=self.eid,
            order_name=['MECHANICAL VENTILATION'])
        c = c.merge(self.offset,how='left').drop_duplicates()
        c['VENT DUR'] = c.days_from_dob_procstart.astype(int) - c.offset
        c = self.encounter_info.merge(c[['encounter_id','VENT DUR']],how='left',on='encounter_id')
        return c.groupby('encounter_id')['VENT DUR'].max().sort_index().to_frame().reset_index()

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