# -*- coding: utf-8 -*-
"""
ICICI Direct - hammer candle identification

@author: Mukesh Choudhary

---------------HAMMER CANDLE---------------
Candle in which lower wick is atleast twice the size of the body.

Body can be red or green (although green is preferred).

Upper wick should be small or non-existent.

Viewed as a bullish reversal candlestick when occurring during a downtrend.

"""

from breeze_connect import BreezeConnect
import pandas as pd
import datetime as dt
import os
import json
import numpy as np

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

def hammer(ohlc_df):    
    """returns dataframe with hammer candle column"""
    df = ohlc_df.copy()
    df["hammer"] = (((df["high"] - df["low"])>3*(df["open"] - df["close"])) & \
                   ((df["close"] - df["low"])/(.001 + df["high"] - df["low"]) > 0.6) & \
                   ((df["open"] - df["low"])/(.001 + df["high"] - df["low"]) > 0.6)) & \
                   (abs(df["close"] - df["open"]) > 0.1* (df["high"] - df["low"]))
    return df

ohlc = fetchOHLC("NIFTY","5minute",30)
hammer_df = hammer(ohlc)