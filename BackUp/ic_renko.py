# -*- coding: utf-8 -*-
"""
ICICIDirect - Supertrend implementation

@author: Mukesh Choudhary

Renko chart is build using price movement and not price against standardized time intervals 
- This filters out the noise and lets you visualize the trend

Price movements (fixed) are represented as bricks stacked at 45 degrees to each other.
A new brick is added to the chart only when the price moves by a pre determined amount 
in either direction.

Renko charts have a time axis, but the time scale is not fixed. Some bricks may take longer 
to form than others, depending on how long it takes the  price to move the required box size.

Renko charts typically use only the closing price based on the chart time frame chosen.

"""

from breeze_connect import BreezeConnect
import pandas as pd
import datetime as dt
import os
import numpy as np
import json
from stocktrends import Renko


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

# ohlc = fetchOHLC(ticker="INFY",interval="5minute",duration=5)

def fetchOHLC(ticker, interval, duration):
    instrument = instrumentLookup(instrument_df, ticker)
    from_date = (dt.datetime.now()-dt.timedelta(duration)).strftime('%Y-%m-%d')+'T00:00:00.000Z'
    to_date = dt.datetime.today().strftime('%Y-%m-%d')+'T23:59:59.000Z'   
    data = pd.DataFrame(icici.get_historical_data_v2(interval,from_date,to_date,ticker,'NSE','Cash')['Success'])
    data = data.rename(columns={'datetime': 'date'})
    data = data[['date','open','high','low','close','volume']]
    data.set_index("date",inplace=True)
    return data 

def atr(DF,n):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L']=abs(df['high']-df['low'])
    df['H-PC']=abs(df['high']-df['close'].shift(1))
    df['L-PC']=abs(df['low']-df['close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    df['ATR'] = df['TR'].ewm(com=n,min_periods=n).mean()
    #df['ATR'] = df['TR'].ewm(span=n,adjust=False,min_periods=n).mean()
    return df['ATR']

def renko_DF(DF,blk_size):
    "function to convert ohlc data into renko bricks"
    df = DF.copy()
    df.reset_index(inplace=True)
    df2 = Renko(df)
    df2.brick_size = blk_size
    renko_df = df2.get_ohlc_data()
    return renko_df

ohlc = fetchOHLC("NIFTY","5minute",5)
renko = renko_DF(ohlc,20)