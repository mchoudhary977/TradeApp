# -*- coding: utf-8 -*-
"""
ICICIDirect - Implementing real time EMA Strategy

@author: Mukesh Choudhary
"""

from ic_functions import * 
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


def rsi(df, n):
    "function to calculate RSI"
    delta = df["close"].diff().dropna()
    u = delta * 0
    d = u.copy()
    u[delta > 0] = delta[delta > 0]
    d[delta < 0] = -delta[delta < 0]
    u[u.index[n-1]] = np.mean( u[:n]) # first value is average of gains
    u = u.drop(u.index[:(n-1)])
    d[d.index[n-1]] = np.mean( d[:n]) # first value is average of losses
    d = d.drop(d.index[:(n-1)])
    rs = u.ewm(com=n,min_periods=n).mean()/d.ewm(com=n,min_periods=n).mean()
    return 100 - 100 / (1+rs)

def signal(row):
    ema_xover = row['9-ema'] - row['15-ema']
    ema_signal = 1 if ema_xover > 0 and abs(ema_xover) >= 1 else (-1 if ema_xover < 0 and abs(ema_xover) >= 1 else 0)
    rsi_signal = 1 if row['rsi'] > 50 else (-1 if row['rsi'] < 50 else 0)
    signal = 'green' if ema_signal == 1 and ema_signal==rsi_signal else (
        'red' if ema_signal == -1 and ema_signal==rsi_signal else None)
    return signal

if __name__ == '__main__':
    sig = {}
    sig['signal'] = None
    sig['active'] = 'Y'
    ticker='NIFTY'
    while True:
        now = datetime.now()
        if now.time() >= time(9,15) and now.time() <= time(15,30):
            if (now.minute % 5 == 0 and now.second == 5):
                df=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=5)['data']
                df['15-ema'] = df['close'].ewm(span=15, adjust=False).mean()
                df['9-ema'] = df['close'].ewm(span=9, adjust=False).mean()
                df['rsi'] = rsi(df,14)
                df['signal'] = df.apply(signal, axis=1)
                df['signal'] = df['signal'].where(df['signal'] != df['signal'].shift())
                sig = df.copy().iloc[-1]
                if np.isnan(sig['signal'])==False and sig['signal'] is not None:
                    sig['entry'] = sig['high'] if sig['signal'] == 'green' else sig['low'] if sig['signal'] == 'red' else 0
                    sig['sl'] = sig['low'] if sig['signal'] == 'green' else sig['high'] if sig['signal'] == 'red' else 0
                    sig['active'] = 'Y'
            
            if np.isnan(sig['signal'])==False and sig['signal'] is not None:
                wl = pd.read_csv('WatchList.csv')
                last_px = wl[wl['Code']==ticker]['Close'].values[0]
                if (sig['active'] == 'Y' and sig['signal'] == 'green' and last_px > sig['entry'] and sig['entry'] > 0):
                    sig['active'] = 'N'
                    opt=ic_option_chain(ticker, underlying_price=last_px, duration=0).iloc[2]
                    st = dh_place_mkt_order('NFO',opt['TK'],'buy',50,0)
                    tm.sleep(2)
                    order_id = st['data']['orderId'] if st['status']=='success' else None
                    order_det = dh_get_order_id(order_id)['data']
                    if order_det['orderStatus']=='TRADED':
                        new_row = {}
                        new_row['name'] = 'Bullish EMA Strategy'
                        new_row['tradingSymbol'] = order_det['tradingSymbol']
                        new_row['securityId'] = order_det['securityId']
                        new_row['type'] = 'long'
                        new_row['quantity'] = order_det['quantity']
                        new_row['entry_id'] = order_det['orderId']
                        new_row['entry_px'] = order_det['price']
                        new_row['exit_id'] = None
                        new_row['exit_px'] = None
                        new_row['symbol'] = ticker
                        new_row['entry'] = sig['entry']
                        new_row['stoploss'] = sig['sl']
                        new_row['target'] = sig['entry'] + (sig['entry'] - sig['sl'])
                        new_row['step'] = sig['entry'] - sig['sl']
                        
                        stg_file = 'Strategy.csv'
                        stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
                        stg_df = pd.concat([stg_df,pd.DataFrame(new_row)],ignore_index=True)
                         
                    print(f"add buy order - {sig['entry']} - {sig['sl']} - {opt['CD']} - {opt['TK']}")
                elif (sig['active'] == 'Y' and sig['signal'] == 'red' and last_px < sig['entry'] and sig['entry'] > 0):
                    sig['active'] = 'N'
                    opt=ic_option_chain(ticker, underlying_price=last_px, option_type="PE", duration=0).iloc[-3]
                    st = dh_place_mkt_order('NFO',opt['TK'],'buy',50,0)
                    tm.sleep(2)
                    order_id = st['data']['orderId'] if st['status']=='success' else None
                    order_det = dh_get_order_id(order_id)['data']
                    if order_det['orderStatus']=='TRADED':
                        new_row = {}
                        new_row['name'] = 'Bullish EMA Strategy'
                        new_row['tradingSymbol'] = order_det['tradingSymbol']
                        new_row['securityId'] = order_det['securityId']
                        new_row['type'] = 'short'
                        new_row['quantity'] = order_det['quantity']
                        new_row['entry_id'] = order_det['orderId']
                        new_row['entry_px'] = order_det['price']
                        new_row['exit_id'] = None
                        new_row['exit_px'] = None
                        new_row['symbol'] = ticker
                        new_row['entry'] = sig['entry']
                        new_row['stoploss'] = sig['sl']
                        new_row['target'] = sig['entry'] + (sig['entry'] - sig['sl'])
                        new_row['step'] = sig['entry'] - sig['sl']
                        
                        stg_file = 'Strategy.csv'
                        stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
                        stg_df = pd.concat([stg_df,pd.DataFrame(new_row)],ignore_index=True)
                    
                    print(f"add sell order - {sig['entry']} - {sig['sl']} - {opt['CD']} - {opt['TK']}")
                    
        else:
            break
        tm.sleep(1)
        
    sys.exit()
    