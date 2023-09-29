# -*- coding: utf-8 -*-
"""
ICICIDirect - ATR Implementation

@author: Mukesh Choudhary
"""
from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 
import sqlite3 

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



def atr(DF,n):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L']=abs(df['high']-df['low'])
    df['H-PC']=abs(df['high']-df['close'].shift(1))
    df['L-PC']=abs(df['low']-df['close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    df['ATR'] = df['TR'].ewm(com=n,min_periods=n).mean()
    return df['ATR']


ohlc = fetchOHLC("NIFTY","5minute",5)
atr = atr(ohlc,20)