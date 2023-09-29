# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 11:57:51 2023

@author: MUKESH CHOUDHARY 

ICICIDirect : ADX Implementation

ADX is a way of measuring the strength of trend
Values from 0 to 100
    - 0-25  : Absent or weak trend 
    - 25-50 : Strong trend
    - 50-75 : Very Strong trend
    - 75-100: Extremely Strong trend 
ADX is non directional meaning the ADX value makes no inference about the direction of trend.
Only signifies the strength of trend.
Calculation involves both finding positive and negative directional movement
(by comparing successive highs and successive lows) and then calculating the smoothed
average of the difference of these.
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




def adx(DF,n):
    "function to calculate ADX"
    df2 = DF.copy()
    df2['H-L']=abs(df2['high']-df2['low'])
    df2['H-PC']=abs(df2['high']-df2['close'].shift(1))
    df2['L-PC']=abs(df2['low']-df2['close'].shift(1))
    df2['TR']=df2[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    df2['DMplus']=np.where((df2['high']-df2['high'].shift(1))>(df2['low'].shift(1)-df2['low']),df2['high']-df2['high'].shift(1),0)
    df2['DMplus']=np.where(df2['DMplus']<0,0,df2['DMplus'])
    df2['DMminus']=np.where((df2['low'].shift(1)-df2['low'])>(df2['high']-df2['high'].shift(1)),df2['low'].shift(1)-df2['low'],0)
    df2['DMminus']=np.where(df2['DMminus']<0,0,df2['DMminus'])
    TRn = []
    DMplusN = []
    DMminusN = []
    TR = df2['TR'].tolist()
    DMplus = df2['DMplus'].tolist()
    DMminus = df2['DMminus'].tolist()
    for i in range(len(df2)):
        if i < n:
            TRn.append(np.NaN)
            DMplusN.append(np.NaN)
            DMminusN.append(np.NaN)
        elif i == n:
            TRn.append(df2['TR'].rolling(n).sum().tolist()[n])
            DMplusN.append(df2['DMplus'].rolling(n).sum().tolist()[n])
            DMminusN.append(df2['DMminus'].rolling(n).sum().tolist()[n])
        elif i > n:
            TRn.append(TRn[i-1] - (TRn[i-1]/n) + TR[i])
            DMplusN.append(DMplusN[i-1] - (DMplusN[i-1]/n) + DMplus[i])
            DMminusN.append(DMminusN[i-1] - (DMminusN[i-1]/n) + DMminus[i])
    df2['TRn'] = np.array(TRn)
    df2['DMplusN'] = np.array(DMplusN)
    df2['DMminusN'] = np.array(DMminusN)
    df2['DIplusN']=100*(df2['DMplusN']/df2['TRn'])
    df2['DIminusN']=100*(df2['DMminusN']/df2['TRn'])
    df2['DIdiff']=abs(df2['DIplusN']-df2['DIminusN'])
    df2['DIsum']=df2['DIplusN']+df2['DIminusN']
    df2['DX']=100*(df2['DIdiff']/df2['DIsum'])
    ADX = []
    DX = df2['DX'].tolist()
    for j in range(len(df2)):
        if j < 2*n-1:
            ADX.append(np.NaN)
        elif j == 2*n-1:
            ADX.append(df2['DX'][j-n+1:j+1].mean())
        elif j > 2*n-1:
            ADX.append(((n-1)*ADX[j-1] + DX[j])/n)
    df2['ADX']=np.array(ADX)
    return df2['ADX']


ohlc = fetchOHLC("CNXBAN","5minute",5)
# ohlc = fetchOHLC("NIFTY","5minute",5)
adx1 = adx(ohlc,14)