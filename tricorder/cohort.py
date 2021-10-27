import numpy as np
import pandas as pd

class ProcedureCohort(object):
    def __init__(self, db, procedures, person_id=None, encounter_id=None):
        self.db = db
        self.procedures = procedures

        self.procedure_info = self.db.procedures.sel(order_name=self.procedures,encounter_id=encounter_id)
        enc = self.db.encounters.sel(
            encounter_id=self.procedure_info.encounter_id.unique(),
        )
        self.encounter_info = self.procedure_info.drop(columns=['days_from_dob_procstart','person_id']).merge(
            enc,on='encounter_id',how='left').dropna()

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