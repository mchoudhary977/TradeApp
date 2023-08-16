# -*- coding: utf-8 -*-
"""
ICICIDirect - Checking for Order Status

@author: Mukesh Choudhary
"""

# from ic_functions import * 
from dh_functions import *
# from trade_modules import * 
# from kiteconnect import KiteTicker, KiteConnect
import pandas as pd
import time as tm
from datetime import datetime, time
# import datetime as dt
import os
import sys
import numpy as np


strategy_file = 'Strategy.csv'

while True:
    now = datetime.now()
    if now.time() >= time(9,15) and now.time() <= time(15,30):
        strategy_df = pd.read_csv(strategy_file) if os.path.exists(strategy_file) else pd.DataFrame()
        if len(strategy_df) > 0:
            strategy_actv = strategy_df[strategy_df['exit_id'] == None]
            live_px = {}
            if len(strategy_actv) > 0:
                symbols = strategy_actv['symbol'].unique()
                wl = pd.read_csv('WatchList.csv')
                for sym in symbols:
                    live_px[sym] = wl[wl['Code']==sym]['Close'].values[0]
                
                for i in strategy_actv:
                    if i['type'] == 'long':
                        symbol = i['symbol']
                        if live_px[symbol] < i['stoploss']:
                            st = dh_place_mkt_order('NFO',i['securityId'],'sell',50,0)
                            order_id = st['data']['orderId'] if st['status']=='success' else None
                            order_det = dh_get_order_id(order_id)['data']
                            i['exit_id'] = order_det['orderId']
                            i['exit_px'] = order_det['price']
                            print('send exit order ')
                        elif live_px[symbol] > i['target']:
                            i['stoploss'] = i['stoploss'] + i['step']
                            i['target'] = i['target'] + i['step']
                    if i['type'] == 'short':
                        symbol = i['symbol']
                        if live_px[symbol] > i['stoploss']:
                            st = dh_place_mkt_order('NFO',i['securityId'],'sell',50,0)
                            order_id = st['data']['orderId'] if st['status']=='success' else None
                            order_det = dh_get_order_id(order_id)['data']
                            i['exit_id'] = order_det['orderId']
                            i['exit_px'] = order_det['price']
                            print('send exit order ')
                        elif live_px[symbol] < i['target']:
                            i['stoploss'] = i['stoploss'] - i['step']
                            i['target'] = i['target'] - i['step']
                strategy_df.to_csv('Strategy.csv',index=False)
    else:
        break
    tm.sleep(1)
