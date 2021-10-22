circulatory = [
    'Pulse',
    'CVP',
    'MAP (mmHg)',
    'A-line MAP ',
    'A-line 2 MAP ',
    'Cardiac Output',
    'PAP (Mean)'
]

ICU_monitors = [
#     'CVP',
    'Cardiac Output',
#     'MAP (mmHg)',
    'SVO2 (%)',
    'SVR (dyne*sec)/cm5',
    'SVRI (dyne*sec)/cm5',
    'PVRI (dyne*sec)/cm5',
    'PVR (dyne*sec)/cm5',
]

swan_monitors = [
    'SVO2 (%)',
    'CCI',
    'CCO',
    'SQI',
    'SVI',
]

flow_oxygenation = [
    'SPO2',
    'SVO2 (%)'
]

ventilation = [
    'ETCO2',
    'Resp',
    'Vital Capacity (mls)',
    ' Vt (mL/kg)',
    'Set Ve (L/min)',
    'Ve (L/min)',
]

ASA_monitors = [
    'SpO2',
    'Temp',
    'Pulse',
]

lab_oxygenation = [
    'O2SAT Arterial Measured',
    'PCO2 ARTERIAL',
    'PO2 ARTERIAL',
    'PO2 Venous',
    'O2SAT Venous Measured',
]

class Flowsheet(object):
    
    def __init__(self):
        self.asa_monitors = asa_monitors
        self.ventilation = ventilation
        self.oxygenation = flow_oxygenation
        self.circulatory = circulatory
        
class Labs(object):
    def __init__(self):
        self.oxygenation = lab_oxygenation