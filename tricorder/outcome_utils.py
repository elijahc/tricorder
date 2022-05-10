import numpy as np
import pandas as pd

aki_code_map = {
    -3:'No preoperative creatinine',
    -1:'No postoperative creatinine',
    3 :'Stage 3 AKI',
    2 :'Stage 2 AKI',
    1 :'Stage 1 AKI',
    0 :'No AKI',
}

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