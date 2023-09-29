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

db = sqlite3.connect('./ticks.db')

tickers=['NIFTY','CNXBAN','NIFFIN','INDVIX']

def tokenLookup(symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(icici.get_names('NSE',symbol)['isec_token_level1'])
        # token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list


# tokenLookup(tickers)

def get_hist(ticker,interval,db):
    token = tokenLookup(ticker)[0].split('!',1)[1].replace(' ','')
    data = pd.read_sql('''SELECT * FROM TOKEN%s WHERE ts >=  date() - '7 day';''' %token, con=db)                
    data = data.set_index(['ts'])
    data.index = pd.to_datetime(data.index)
    ticks = data.loc[:, ['price']]   
    df=ticks['price'].resample(interval).ohlc().dropna()
    return df

# ticker=['NIFTY']
# get_hist(['NIFTY'],'15min',db)