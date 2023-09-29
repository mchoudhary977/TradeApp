# -*- coding: utf-8 -*-
"""
Zerodha Kite Connect - slope of candles to determine trend

@author: Mayank Rasu (http://rasuquant.com/wp/)
"""
from breeze_connect import BreezeConnect
import pandas as pd
import datetime as dt
import os
import numpy as np
import statsmodels.api as sm
import json

# generate trading session
icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
# icici.user_id

#get dump of all NSE instruments
instrument_list = pd.read_csv('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')
# dh_instrument_list = pd.read_csv('https://images.dhan.co/api-data/api-scrip-master.csv')

instrument_df = instrument_list

def instrumentLookup(instrument_df, symbol_code):
    try:
        return instrument_df[instrument_df.CD==symbol_code].TK.values[0]
    except:
        return -1 

# ohlc = fetchOHLC(ticker="NIFTY",interval="5minute",duration=5)


def fetchOHLC(ticker, interval, duration):
    instrument = instrumentLookup(instrument_df, ticker)
    from_date = (dt.datetime.now()-dt.timedelta(duration)).strftime('%Y-%m-%d')+'T00:00:00.000Z'
    to_date = dt.datetime.today().strftime('%Y-%m-%d')+'T23:59:59.000Z'   
    data = pd.DataFrame(icici.get_historical_data_v2(interval,from_date,to_date,ticker,'NSE','Cash')['Success'])
    data = data.rename(columns={'datetime': 'date'})
    data = data[['date','open','high','low','close','volume']]
    data.set_index("date",inplace=True)
    return data 

def slope(ohlc_df,n):
    "function to calculate the slope of regression line for n consecutive points on a plot"
    df = ohlc_df.iloc[-1*n:,:]
    y = ((df["open"] + df["close"])/2).values
    x = np.array(range(n))
    y_scaled = (y - y.min())/(y.max() - y.min())
    x_scaled = (x - x.min())/(x.max() - x.min())
    x_scaled = sm.add_constant(x_scaled)
    model = sm.OLS(y_scaled,x_scaled)
    results = model.fit()
    slope = np.rad2deg(np.arctan(results.params[-1]))
    return slope

def trend(ohlc_df,n):
    "function to assess the trend by analyzing each candle"
    df = ohlc_df.copy()
    df["up"] = np.where(df["low"]>=df["low"].shift(1),1,0)
    df["dn"] = np.where(df["high"]<=df["high"].shift(1),1,0)
    if df["close"][-1] > df["open"][-1]:
        if df["up"][-1*n:].sum() >= 0.7*n:
            return "uptrend"
    elif df["open"][-1] > df["close"][-1]:
        if df["dn"][-1*n:].sum() >= 0.7*n:
            return "downtrend"
    else:
        return None

ohlc = fetchOHLC("NIFTY","5minute",30)
slope(ohlc,7)
trend(ohlc,7)