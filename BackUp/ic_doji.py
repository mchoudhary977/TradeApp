# -*- coding: utf-8 -*-
"""
ICICI Direct - doji candle identification

@author: Mukesh Choudhary

---------------DOJI CANDLE---------------
Candle in which open and close are virtually the same i.e. body of the candle is a thin strip
or a line.

Looks like a cross or an inverted cross. Size of wicks is generally irrelevant.

Represents indecision/uncertainty on the part of both buyers and sellers.

Can signal both trend reversal or breakout based on the pattern in which it occurs.

"""
from breeze_connect import BreezeConnect
import pandas as pd
import datetime as dt
import os
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

def doji(ohlc_df):    
    """returns dataframe with doji candle column"""
    df = ohlc_df.copy()
    avg_candle_size = abs(df["close"] - df["open"]).median()
    df["doji"] = abs(df["close"] - df["open"]) <=  (0.05 * avg_candle_size)
    return df


ohlc = fetchOHLC("NIFTY","5minute",5)
doji_df = doji(ohlc)