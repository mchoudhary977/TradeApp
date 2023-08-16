# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 14:54:03 2023

Update Scrip Files for Brokers

@author: Mukesh Choudhary
"""

import pandas as pd
import re
import datetime as dt

ic_instrument_df = pd.read_csv('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')

ic_instrument_df['DRV'] = ic_instrument_df[ic_instrument_df['EC']=='NFO']['CD'].apply(lambda s: s.split('-')[0])
ic_instrument_df['STRIKE'] = ic_instrument_df[ic_instrument_df['EC']=='NFO']['CD'].apply(lambda s: float(re.search(r'-(\d+)(?:-[A-Za-z]{2})?$', s).group(1)) if re.search(r'-(\d+)(?:-[A-Za-z]{2})?$', s) is not None else '')
ic_instrument_df['EXPIRY'] = ic_instrument_df[ic_instrument_df['EC']=='NFO']['CD'].apply(lambda s: dt.datetime.strptime(re.search(r'\d{2}-[A-Za-z]{3}-\d{4}', s).group(), '%d-%b-%Y'))
ic_instrument_df['OT'] = ic_instrument_df[ic_instrument_df['EC']=='NFO']['CD'].apply(lambda s: s.split('-')[-1])

dh_instrument_df=pd.read_csv('https://images.dhan.co/api-data/api-scrip-master.csv')

ic_instrument_df.to_csv('icici.csv',index=False)
dh_instrument_df.to_csv('dhan.csv',index=False)
