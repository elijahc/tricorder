import numpy as np
import pandas as pd

class Outcome(object):
    REQUIRES = {}
    CODE_DESCRIPTION = {}
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
        setattr(obj, 'get_{}'.format(self.__class__.classname.lower()), self)
    
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

class PostOpAKI(Outcome):
    REQUIRES = {
        'labs':['CREATININE SERUM',]
    }
    CODE_DESCRIPTION = {
        -3:'No preoperative creatinine',
        -1:'No postoperative creatinine',
        3 :'Stage 3 AKI',
        2 :'Stage 2 AKI',
        1 :'Stage 1 AKI',
        0 :'No AKI',
    }
    
    def compute(self):
        if sample is not None and isinstance(sample, int):    
            components = self.db_sample(n=sample).dropna().sort_values(['encounter_id','time'])
        elif sample is not None and isinstance(sample, (list, type(np.array([])), type(pd.Series()))):
            components = self.db_fetch(encounter_id=sample)
        else:
            components = self.db_fetch()
        components = components.query('value <= 25 and value >= 0.2').reset_index()
        grouper = components.groupby('encounter_id')
        pbar = tqdm(grouper)
        
        dfs = []
        
        for _,df in pbar:
            code = mpog_aki(df,result_col='value')

def mpog_aki(df, result_col='lab_result_value'):
    df.time = df.time / np.timedelta64(1,'D')
    if not (df.time<=0).any():
        return -3
    if not (df.time >= 1).any():
        return -1
    cr_basl = df.query('time <= 0')[result_col].mean()
    
    cr7dmax = df.query('time > 0 and time <= 7')[result_col].max()
    cr2dmax = df.query('time > 0 and time <= 2')[result_col].max()
    if cr7dmax >= 3*cr_basl or cr7dmax > 4:
        return 3
    elif cr7dmax >= 2*cr_basl:
        return 2
    elif cr7dmax >= 1.5*cr_basl or cr2dmax > cr_basl+0.3:
        return 1
    else:
        return 0