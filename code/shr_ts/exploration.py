# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 00:30:34 2026

@author: andre
"""

path = r'C:\Users\andre\OneDrive\research\rerouting\data\rerouted_shares'
file = r'data_share_rerouted.dta'

import pandas as pd
import os

df = pd.read_stata(os.path.join(path, file))
df['year'] = [x.year for x in df.modate_exports]

df_tariff = df.groupby(['hs_6dig']).agg(
    tariff_increase = ('tariff_increase', 'max')
)
df_tariff['ind'] = [x > 0 for x in df_tariff.tariff_increase]

df = df.merge(df_tariff, left_on='hs_6dig', right_index=True, how='left')

df.groupby(['ind','year'])['share_rerouted'].describe()