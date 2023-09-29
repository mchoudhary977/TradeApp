from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 

# generate trading session
icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
# icici.user_id

tickers=['NIFTY','CNXBAN','NIFFIN','INDVIX']

def tokenLookup(symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(icici.get_names('NSE',symbol)['isec_token_level1'])
        # token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list


# tokenLookup(tickers)

def startLiveMarketFeed(tickers):
    icici.ws_connect()
    tokens = tokenLookup(tickers)
    for token in tokens:
        icici.subscribe_feeds(token)
    
    icici.on_ticks = on_ticks

def endLiveMarketFeed(tickers):
    tokens = tokenLookup(tickers)
    for token in tokens:
        icici.unsubscribe_feeds(token)
    icici.ws_disconnect()

def on_ticks(ticks):
    print("Ticks: {}".format(ticks))

# startLiveMarketFeed(tickers)
# endLiveMarketFeed(tickers)