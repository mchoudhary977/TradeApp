# -*- coding: utf-8 -*-
"""
ICICIDirect - RSI implementation

@author: Mukesh Choudhary
"""
from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 
import sqlite3 
import numpy as np

# generate trading session
icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
# icici.user_id

# get dump of all NSE instruments
instrument_df=pd.read_csv('icici_scrip.csv')

def instrumentLookup(instrument_df, symbol_code):
    try:
        return instrument_df[instrument_df.CD==symbol_code].TK.values[0]
    except:
        return -1 
    
# instrumentLookup(instrument_df, "CNXBAN")

def fetchOHLC(ticker, interval, duration):
    instrument = instrumentLookup(instrument_df, ticker)
    from_date = (dt.datetime.now()-dt.timedelta(duration)).strftime('%Y-%m-%d')+'T00:00:00.000Z'
    to_date = dt.datetime.today().strftime('%Y-%m-%d')+'T23:59:59.000Z'   
    data = pd.DataFrame(icici.get_historical_data_v2(interval,from_date,to_date,ticker,'NSE','Cash')['Success'])
    data = data.rename(columns={'datetime': 'date'})
    data = data[['date','open','high','low','close','volume']]
    data.set_index("date",inplace=True)
    return data 
# data = fetchOHLC("CNXBAN", "2023-01-01", "5minute")



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

ohlc = fetchOHLC("NIFTY","5minute",5)
rsi = rsi(ohlc,14)