from tqdm.auto import tqdm
import pandas as pd
import seaborn as sns
import numpy as np
from scipy.interpolate import interp1d
from .utils import tidy_labs, tidy_flow, tidy_meds, pivot_tidy, melt_tidy

class Metric(object):
    REQUIRES = {}
    classname = 'Metric'
    shortname = '_default_'
    
    def __repr__(self):
        return str(self.__class__.classname)
    
    def __init__(self, db, encounter_id=None):
        self.__db__ = db
        self.encounter_id = encounter_id
        self._checked_ids = False
    
    def __attach__(self, obj):
        # Attach self to object passed in `obj`
        setattr(obj, '{}'.format(self.__class__.classname), self)
    
    def db_sample(self, n):
        """Fetch a random sample of required data from self.db
        Parameters
        ----------
        n : int
             Fetch data from `n` unique encounters.
        """
        if not self._checked_ids:
            df = self.db_fetch()
            self.encounter_id = df.encounter_id.unique().tolist()
            
            return df[df.encounter_id.isin(df.encounter_id.sample(n))].reset_index(drop=True)
        else:
            # Sample from known encounter ids
            eids = np.random.choice(self.encounter_id, size=n, replace=False)
            return self.db_fetch(encounter_id=eids)
        
    def after_labs_sel(self,df):
        return df
    
    def after_flowsheet_sel(self,df):
        return df
    
    def after_medications_sel(self,df):
        return df
    
    def db_fetch(self, encounter_id=None):
        """Fetch required data from self.db
        Parameters
        ----------
        encounter_id : list
            Fetch data from specified encounter id's
            
        Returns
        ----------
        dataframe
            Dataframe of values
        """
        
        components = []
        
        if encounter_id is None:
            encounter_id = self.encounter_id
        
        
        for k,v in self.__class__.REQUIRES.items():
            if k == 'labs':
                c  = self.__db__.labs.sel(lab_component_name=v, encounter_id=encounter_id)
                
                c = self.after_labs_sel(c)
                components.append(tidy_labs(c))
            elif k == 'flowsheet':
                c  = self.__db__.flowsheet.sel(display_name=v, encounter_id=encounter_id)
                
                c = self.after_flowsheet_sel(c)
                components.append(tidy_flow(c))
            elif k == 'medications':
                c = self.__db__.medications.sel(medication_name=v, encounter_id=encounter_id)
                
                c = self.after_medications_sel(c)
                components.append(tidy_meds(c))


        if len(components) > 1:
            keeps = components[0].encounter_id.unique()
            for d in components[1:]:
                keeps = np.intersect1d(keeps, d.encounter_id.unique())
                
            df = pd.concat(components)
            df = df[df.encounter_id.isin(keeps)]
            
        else:
            df = components[0]

        return df
    
    def plot(self, x='day', y='value', hue='name', sample=1):
        """Fetch required data from self.db
        Parameters
        ----------
        x : str
            column to plot on the x-axis
        y : str
            column to plot on the x-axis
        hue : str
            group points by column specified in hue
        sample : int
            Number of encounters to sample
            
        Returns
        ----------
        Figure object
        
        """
        comps = self.db_sample(n=sample)
        comps['day'] = comps.time / np.timedelta64(1,'D')

        return sns.scatterplot(x=x,y=y,hue=hue,data=comps)

    def from_frame(self, df):
        pass
    
    def from_db(db):
        pass
    
def compute_o2_content(hgb,sat,po2=None,x_new=None):
    if x_new is None:
        x_new = sat.time/np.timedelta64(1,'D')
        s = sat['value']
    else:
        st = sat.time/np.timedelta64(1,'D')
        sv = sat['value']
        s = interp1d(x=st, y=sv, bounds_error=False, fill_value=(sv.min(),sv.max()))(x_new)
        
    if len(hgb)>=2:
        ht = hgb.time/np.timedelta64(1,'D')
        hv = hgb['value']
        
        h = interp1d(x=ht, y=hv, bounds_error=False, fill_value=(hv.min(),hv.max()))(x_new)
    elif len(hgb)==1:
        h = list(hgb['value'])[0]
    else:
        h = np.nan
        
    if len(po2)>=2:
        pt = po2.time/np.timedelta64(1,'D')
        pv = po2['value']
        
        p = interp1d(x=pt, y=pv, bounds_error=False, fill_value=(pv.min(),pv.max()))(x_new)
    elif len(po2)==1:
        p = list(po2['value'])[0]
    else:
        p = 0
        
    # Need to incorporate additional PO2 contribution
    c = (1.34 * s/100.0 * h) + (0.003 * p)
    return pd.DataFrame({'day':x_new,'value': c})
                
class OxygenContent(Metric):
    shortname = "O2_content"
    classname = 'OxygenContent'
    REQUIRES = {
        'labs' : [
            'HEMOGLOBIN','HEMOGLOBIN ARTERIAL','HEMOGLOBIN VENOUS',
            'O2SAT ARTERIAL MEASURED','PO2 ARTERIAL',
            'O2SAT VENOUS MEASURED', 'PO2 VENOUS',
        ],
    }
    
    def __init__(self, db, encounter_id=None):
        self.hgb_names = ['HEMOGLOBIN','HEMOGLOBIN ARTERIAL','HEMOGLOBIN VENOUS']
        self.abg_names = ['O2SAT ARTERIAL MEASURED','PO2 ARTERIAL']
        self.vbg_names = ['O2SAT VENOUS MEASURED', 'PO2 VENOUS']
        
        super(OxygenContent,self).__init__(db, encounter_id)

    def compute(self, sample=None):
        if sample is not None and isinstance(sample, int):    
            components = self.db_sample(n=sample).dropna().sort_values(['encounter_id','time'])
        elif sample is not None and isinstance(sample, (list, type(np.array([])), type(pd.Series()))):
            components = self.db_fetch(encounter_id=sample)
        else:
            components = self.db_fetch()
        grouper = components.groupby('encounter_id')
        pbar = tqdm(grouper)
        
        dfs = []
        
        for _,df in pbar:
            hgb = df[df['name'].isin(self.hgb_names)]
            
            enc_dfs = []
            eid = df.encounter_id.values[0]
            pbar.set_description('{}'.format(self.__class__.classname))
            sata = df[df['name'].str.contains('O2SAT ARTERIAL')]
            po2a = df[df['name'].str.contains('PO2 ARTERIAL')]
            aC = compute_o2_content(hgb,sata,po2a)
            aC['name'] = 'CaO2'
            enc_dfs.append(aC)
            
            # pbar.set_description('{}_b'.format(eid))
            satv = df[df['name'].str.contains('O2SAT VENOUS')]
            po2v = df[df['name'].str.contains('PO2 VENOUS')]
            vC = compute_o2_content(hgb,satv,po2v)
            vC['name'] = 'CvO2'
            enc_dfs.append(vC)

            out = pd.concat(enc_dfs)
            out['encounter_id']=eid
            out['hour'] = (out.day*24).round().astype(int)
            out['hour'] = pd.to_timedelta(out.hour-out.hour.min(), unit='hour')
            dfs.append(out)
            
        return pd.concat(dfs)[['value','name','encounter_id','hour']].pivot_table(
            index=['encounter_id','hour'],columns='name',values='value'
        )
    
class OxygenDelivery(Metric):
    shortname = "DO2"
    classname = 'OxygenDelivery'
    units = {
        'CCI'  : 'L/min/m^2',
        'CCO'  : 'L/min',
        'DO2'  : 'mL/min',
        'DO2_I' : 'mL/min/m^2',
        'CaO2' : 'mL/dL',
    }
    
    limits = {'CCI':(0,20),'DO2_I':(0,2500)}
    
    normal_range = {
        'DO2_I' : (550,650),
        'CCI'   : (2.5,3.6),
    }
    
    REQUIRES = {
        'labs' : [
            'HEMOGLOBIN','HEMOGLOBIN ARTERIAL','HEMOGLOBIN VENOUS',
            'O2SAT ARTERIAL MEASURED','PO2 ARTERIAL',
        ],
        'flowsheet' : [
            'CARDIAC OUTPUT','CCO','CCI',
        ]
    }
    
    def __init__(self, db, encounter_id=None):
        self.hgb_names = ['HEMOGLOBIN','HEMOGLOBIN ARTERIAL','HEMOGLOBIN VENOUS']
        self.abg_names = ['O2SAT ARTERIAL MEASURED','PO2 ARTERIAL']
        
        super(OxygenDelivery,self).__init__(db, encounter_id)
    
    def get_components(self, sample=None):
        if sample is not None and isinstance(sample, int):    
            components = self.db_sample(n=sample).dropna().sort_values(['encounter_id','time'])
        elif sample is not None and isinstance(sample, (list, type(np.array([])), type(pd.Series()))):
            components = self.db_fetch(encounter_id=sample)
        else:
            components = self.db_fetch()
        
        components['name'] = components['name'].replace({'CARDIAC OUTPUT': 'CCO'})
        grouper = components.groupby('encounter_id')
        pbar = tqdm(grouper)
        
        dfs = []
        
        for _,df in pbar:
            hgb = df[df['name'].isin(self.hgb_names)]
            
            enc_dfs = []
            eid = df.encounter_id.values[0]
            pbar.set_description('{}'.format(self.__class__.classname))
            sata = df[df['name'].str.contains('O2SAT ARTERIAL')]
            po2a = df[df['name'].str.contains('PO2 ARTERIAL')]
            aC = compute_o2_content(hgb,sata,po2a)
            aC['name'] = 'CaO2'
            enc_dfs.append(aC)
            
            co = df[df['name'].isin(['CCO','CCI'])].reset_index(drop=True)
            co['day'] = co.time / np.timedelta64(1,'D')
            co = co.drop(columns='time')
            enc_dfs.append(co)

            out = pd.concat(enc_dfs)
            if out.name.drop_duplicates().str.contains('CC').any() and 'CaO2' in out.name.unique():
                out['encounter_id']=eid
                out['hour'] = (out.day*24).round().astype(int)
                dfs.append(out)
            
        output = pd.concat(dfs)[['value','name','encounter_id','hour']].pivot_table(
            index=['encounter_id','hour'], columns='name', values='value'
        )
        return output
    
    def compute(self, sample=None, encounter_id=None, with_components=False):
        df = self.get_components(sample=sample)
        co = df['CCO'].interpolate(limit_area='inside')
        ci = df['CCI'].interpolate(limit_area='inside')
        ca = df['CaO2'].interpolate(limit_area='inside')
        df['DO2']   = (10 * co * ca).interpolate(limit_direction='forward')
        df['DO2_I'] = (10 * ci * ca).interpolate(limit_direction='forward')
        if not with_components:
            df = df[['DO2','DO2_I']]
        df = melt_tidy(df, t='hour')
        df = df.rename(columns={'hour':'time'})
        df.time = pd.to_timedelta(df.time,unit='hour')
        return df
    
class OxygenConsumption(Metric):
    shortname = "VO2"
    classname = 'OxygenConsumption'
    REQUIRES = {
        'labs' : [
            'HEMOGLOBIN','HEMOGLOBIN ARTERIAL','HEMOGLOBIN VENOUS',
            'O2SAT ARTERIAL MEASURED','PO2 ARTERIAL',
            'O2SAT VENOUS MEASURED','PO2 VENOUS',
        ],
        'flowsheet' : [
            'CARDIAC OUTPUT','CCO','CCI','SCVO2 (%)','SVO2 (%)',
            
        ]
    }
    
    def __init__(self, db, encounter_id=None):
        labs = pd.Series(self.__class__.REQUIRES['labs'])
        self.hgb_names = ['HEMOGLOBIN','HEMOGLOBIN ARTERIAL','HEMOGLOBIN VENOUS']
        # self.abg_names = labs[labs.str.contains('ARTERIAL')].tolist()
        # self.vbg_names = labs[labs.str.contains('VENOUS')].tolist()
        # self.co2_names = ['TCO2 VENOUS','TCO2 ARTERIAL','PCO2 ARTERIAL','PH ARTERIAL', 'PH VENOUS','PCO2 VENOUS','BASE EXCESS VENOUS']
        
        super(OxygenConsumption,self).__init__(db, encounter_id)
    
    def _prep(self, sample=None):
        if sample is not None and isinstance(sample, int):    
            components = self.db_sample(n=sample).dropna().sort_values(['encounter_id','time'])
        elif sample is not None and isinstance(sample, (list, type(np.array([])), type(pd.Series()))):
            components = self.db_fetch(encounter_id=sample)
        else:
            components = self.db_fetch()
        
        components['name'] = components['name'].replace({
            'CARDIAC OUTPUT': 'CCO',
            # 'O2SAT VENOUS MEASURED':'SVO2 (%)',
        })
        grouper = components.groupby('encounter_id')
        pbar = tqdm(grouper)
        
        dfs = []
        
        for _,df in pbar:
            hgb = df[df['name'].isin(self.hgb_names)]
            
            enc_dfs = []
            eid = df.encounter_id.values[0]
            pbar.set_description('{}_a'.format(eid))
            sata = df[df['name'].str.contains('O2SAT ARTERIAL')]
            po2a = df[df['name'].str.contains('PO2 ARTERIAL')]
            aC = compute_o2_content(hgb,sata,po2a)
            aC['name'] = 'CaO2'
            enc_dfs.append(aC)
            
            satv = df[df['name'].isin(['O2SAT VENOUS MEASURED','SVO2 (%)'])]
            po2v = df[df['name'].str.contains('PO2 VENOUS')]
            vC = compute_o2_content(hgb,satv,po2v)
            vC['name'] = 'CvO2'
            enc_dfs.append(vC)
            
            co = df[df['name'].isin(['CCO','CCI'])].reset_index(drop=True)
            co['day'] = co.time / np.timedelta64(1,'D')
            co = co.drop(columns='time')
            enc_dfs.append(co)

            out = pd.concat(enc_dfs)
            if out.name.drop_duplicates().str.contains('CC').any() and 'CaO2' in out.name.unique() and 'CvO2' in out.name.unique():
                out['encounter_id']=eid
                out['hour'] = (out.day*24).round().astype(int)
                dfs.append(out)
            
        output = pivot_tidy(pd.concat(dfs)[['value','name','encounter_id','hour']],t='hour')
        
        return output
    
    def compute(self, sample=None, encounter_id=None, with_components=False):
        df = self._prep(sample=sample)
        co = df['CCO'].interpolate(limit_area='inside')
        ci = df['CCI'].interpolate(limit_area='inside')
        ca = df['CaO2'].interpolate(limit_area='inside')
        cv = df['CvO2'].interpolate(limit_area='inside')
        cd = ca-cv
        df['VO2']   = (10 * co * cd).interpolate(limit_direction='forward')
        df['VO2_I'] = (10 * ci * cd).interpolate(limit_direction='forward')
        if not with_components:
            df = df[['VO2','VO2_I']]
        df = melt_tidy(df, t='hour')
        df = df.rename(columns={'hour':'time'})
        df.time = pd.to_timedelta(df.time,unit='hour')
        return df

class CardiacPower(Metric):
    shortname = "CPO"
    classname = 'CardiacPower'
    limits = {'CVP':(0,27.12)}
    REQUIRES = {
        'flowsheet' : [
            'A-LINE MAP ', 'A-LINE 2 MAP ','CVP (MMHG)','CCO','CARDIAC OUTPUT',
        ]
    }
    
    def __init__(self, db, encounter_id=None):
        self.map_names = ['A-LINE MAP ', 'A-LINE 2 MAP ']
        
        super(CardiacPower,self).__init__(db, encounter_id)
    
    def _prep(self, sample=None, encounter_id=None, pivot=True):
        if sample is not None and isinstance(sample, int):    
            components = self.db_sample(n=sample).dropna().sort_values(['encounter_id','time'])
        elif sample is not None and isinstance(sample, (list, type(np.array([])), type(pd.Series()))):
            components = self.db_fetch(encounter_id=sample)
        else:
            components = self.db_fetch()
        
        components['name'] = components['name'].replace({
            'CARDIAC OUTPUT': 'CCO',
            'A-LINE MAP '   : 'A-LINE MAP',
            'A-LINE 2 MAP ' : 'A-LINE MAP',
            'CVP (MMHG)'    : 'CVP',
        })
        grouper = components.groupby('encounter_id')
        pbar = tqdm(grouper)
        
        dfs = []
        
        for _,df in pbar:
            enc_dfs = []
            eid = df.encounter_id.values[0]
            pbar.set_description('{}'.format(self.__class__.classname))
            cvp_limits = self.__class__.limits['CVP']
            cvp = df[df.name == 'CVP'].reset_index(drop=True)
            cvp = cvp[cvp.value <= cvp_limits[-1]]
            df = pd.concat([df[df.name != 'CVP'].reset_index(drop=True),cvp])
            # if len(df.name.value_counts())==3:
            dfs.append(df)
    
        output = pd.concat(dfs)[['value','name','encounter_id','time']]   
        if pivot:
            return pivot_tidy(output, t='time')
        else:
            return output
    
    def compute(self, sample=None, with_components=False, encounter_id=None):
        pvdf = self._prep(sample=sample, encounter_id=encounter_id)
        pvdf['Cardiac Power'] = (pvdf['A-LINE MAP'].interpolate(limit_area='inside')-pvdf.CVP.interpolate(limit_area='inside'))
        pvdf['Cardiac Power'] = pvdf['Cardiac Power']*pvdf.CCO / 451
        pvdf['Cardiac Power'] = pvdf['Cardiac Power'].interpolate(limit_area='inside')
        df = melt_tidy(pvdf, t='time')
        # df.time = (df.time/(np.timedelta64(1,'D'))*24).values
        # df.time = pd.to_timedelta(df.time.round().astype(int),unit='hour')
        pvdf = pivot_tidy(df,t='time')
        
        if not with_components:
            out = pvdf[['Cardiac Power']].reset_index().rename(columns={'Cardiac Power':'value'})
            out['name'] = 'Cardiac Power'
            return out
        else:
            return pvdf
        
        
class VentricularStrokeWorkIndex(CardiacPower):
    shortname = "VSWi"
    classname = 'VentricularStrokeWorkIndex'
    limits = {'CVP':(0,27.12)}
    REQUIRES = {
        'flowsheet' : [
            'A-LINE MAP ', 'A-LINE 2 MAP ',
            'CVP (MMHG)','PULSE',
            'CCI','CARDIAC OUTPUT','LVSWI','RVSWI','PAP (MEAN)',
        ]
    }
    
    def __init__(self, db, encounter_id=None):
        self.map_names = ['A-LINE MAP ', 'A-LINE 2 MAP ']
        
        super(VentricularStrokeWorkIndex,self).__init__(db, encounter_id)
    
    def compute(self, sample=None, with_components=False, encounter_id=None):
        pvdf = self._prep(sample=sample, encounter_id=encounter_id)
        pvdf['SVi'] = pvdf.CCI.interpolate(limit_area='inside') / pvdf.PULSE.interpolate(limit_area='inside')
        pvdf['RVSWI'] = (pvdf['PAP (MEAN)'].interpolate(limit_area='inside')-pvdf.CVP)*pvdf.SVi
        pvdf['RVSWI'] = pvdf['RVSWI'].interpolate(limit_area='inside')
        df = melt_tidy(pvdf, t='time')
        # df.time = (df.time/(np.timedelta64(1,'D'))*24).values
        # df.time = pd.to_timedelta(df.time.round().astype(int),unit='hour')
        pvdf = pivot_tidy(df,t='time')
        
        if not with_components:
            out = pvdf[['RVSWI']].reset_index().rename(columns={'RVSWI':'value'})
            out['name'] = 'RVSWI'
            return out
        else:
            return pvdf
# swan.flowsheet.sel(display_name=swan.flowsheet.search('PAP').values[:-1],encounter_id=pc.eid)