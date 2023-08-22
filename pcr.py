# from wA_tradeFunctions import *
import pandas as pd
from datetime import datetime,timedelta, time
import time as tm
import os
from trade_modules import *

# upd_dt = datetime.now()
# upd_flag = 'N'
# live_feed = 'N'

while True:
    curr_dt = datetime.now()
    try:
        if os.path.exists('OIPCR.csv') == False or (curr_dt.time() >= time(9,10) and curr_dt.time() < time(15,31) and curr_dt.minute%5 == 0 and curr_dt.second == 5):
            curr_dt = datetime.now()
            print(curr_dt)
            wl = pd.read_csv('WatchList.csv')
            watchlist = wl['Code'].to_list()
            oi_pcr = pd.DataFrame()
            coi_pcr = pd.DataFrame()
            for i in watchlist:
                if i in ('NIFTY','CNXBAN','NIFFIN'):
                    print(i)
                    data = getPCR(symCode=i, spot_px = 19169)
                    # print(data)
                    oi_pcr = pd.concat([oi_pcr, pd.DataFrame(data['PCR_OI'])], ignore_index=True)
                    # oi_pcr['SymbolCode'] = i
                    coi_pcr = pd.concat([coi_pcr, pd.DataFrame(data['PCR_OICHANGE'])], ignore_index=True)
                    # coi_pcr['SymbolCode'] = i
                    tm.sleep(2)

            coi_pcr = coi_pcr.sort_values(by=['Date','Time','Symbol','ExpiryDate','StrikePrice'], ascending=[False,False,True,False,True])
            coi_pcr.to_csv('COIPCR.csv',index=False)

            if os.path.exists('OIPCR.csv'):
                oi_pcr_csv = pd.read_csv('OIPCR.csv')
                if len(oi_pcr_csv) > 0:
                    oi_pcr = pd.concat([oi_pcr_csv, oi_pcr], ignore_index=True)
                    oi_pcr = oi_pcr.sort_values(by=['Date','Time','Symbol','ExpiryDate'], ascending=[False,False,True,False])
                    oi_pcr.to_csv('OIPCR.csv',index=False)
                else:
                    oi_pcr.to_csv('OIPCR.csv',index=False)
            else:
                oi_pcr.to_csv('OIPCR.csv',index=False)

    except Exception as e:
        print(e)

tm.sleep(1)
# getPCR('NIFFIN', spot_px = 19169)
# d1 = pd.read_csv('OIPCR.csv')
# d2 = pd.read_csv('COIPCR.csv')