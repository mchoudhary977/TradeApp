# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 23:24:26 2023

ICICI Direct - pivot point implementation

@author: Mukesh Choudhary

----------PRICE ACTION------------
Price action is the branch of technical analysis concerning with price bar patterns.
This trading philosophy intertwines asset price action with chart patterns.

Key terms within price action include Support, Resistance, Higher Highs, Lower Lows
, Swing Trading, Momentum Trading, etc.

Price action based indicators are leading indicators meaning they predict the price action.

Some popular price action based candlestick patterns include:-
    1.  Doji
    2.  Harami Cross
    3.  Maru Bozu
    4.  Hammer
    5.  Shooting Star
    
----------SUPPORT & RESISTANCE------------
Support is the price level which is believe to act as floor for the stock price - based on
historical stock price movement, price fall tends to stop and bounce back from this point.

Resistance is the price level which is believe to act as ceiling for the stock price - 
based on historical stock price movement, price rise tends to stop and reverse from this point.

Both support and resistance price levels are assessed by analyzing historical price movements
- These levels are denoted by multiple touches without breaking  the levels.


----------PIVOT POINTS------------
Pivot Point is a leading indicator used to assess directional movement of an asset and 
potential support and resistance levels.

Very simple to implement mathematically as it is based on previos day's high, low and close
prices. Below is the formula of pivot point and formulae for calculating support and resistance
levels based on pivot points.
- Pivot Point (P) = (High + Low + Close)/3
- Support 1 (S1) = (P * 2) - High 
- Resistance 1 (R1) = (P * 2) - Low
- Support 2 (S2) = P - (High - Low)
- Resistance 2 (R2) = P + (High - Low)
- Support 3 (S3) = Low - 2 * (High - P)
- Resistance 3 (R3) = High + 2 * (P - Low)

Pivot Point calculation will change based on the time horizon of your trading 
(intraday vs hourly vs daily candles)
- candle <= 15 minute : Previous day's high, low, close
- 15 minute < candle <= 1 hour : Previous week's high, low, close
- 1 hour < candle <= 1 day : Previous month's high, low, close

"""

from breeze_connect import BreezeConnect
import pandas as pd
import datetime as dt
import os
import numpy as np
import json


# cwd = os.chdir("D:\\Udemy\\Zerodha KiteConnect API\\1_account_authorization")

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

def levels(ohlc_day):    
    """returns pivot point and support/resistance levels"""
    high = round(ohlc_day["high"][-1],2)
    low = round(ohlc_day["low"][-1],2)
    close = round(ohlc_day["close"][-1],2)
    pivot = round((high + low + close)/3,2)
    r1 = round((2*pivot - low),2)
    r2 = round((pivot + (high - low)),2)
    r3 = round((high + 2*(pivot - low)),2)
    s1 = round((2*pivot - high),2)
    s2 = round((pivot - (high - low)),2)
    s3 = round((low - 2*(high - pivot)),2)
    return (pivot,r1,r2,r3,s1,s2,s3)

ohlc_day = fetchOHLC("NIFTY","1day",30)
pp_levels = levels(ohlc_day.iloc[:-1,:])
# pp_levels = levels(ohlc_day)
