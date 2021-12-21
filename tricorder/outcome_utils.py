import pandas as pd

def mpog_aki(df, result_col='lab_result_value'):
    if not (df.day<=0).any():
        return pd.Series(['No preoperative creatinine',-3],['desc','value'])
    if not (df.day >= 1).any():
        return pd.Series(['No postoperative creatinine',-1],['desc','value'])
    cr_basl = df.query('day <= 0')[result_col].mean()
    
    cr7dmax = df.query('day > 0 and day <= 7')[result_col].max()
    cr2dmax = df.query('day > 0 and day <= 2')[result_col].max()
    if cr7dmax >= 3*cr_basl or cr7dmax > 4:
        return pd.Series(['Stage 3 AKI',3],['desc','value'])
    elif cr7dmax >= 2*cr_basl:
        return pd.Series(['Stage 2 AKI',2],['desc','value'])
    elif cr7dmax >= 1.5*cr_basl or cr2dmax > cr_basl+0.3:
        return pd.Series(['Stage 1 AKI',1],['desc','value'])
    else:
        return pd.Series(['No AKI',0],['desc','value'])