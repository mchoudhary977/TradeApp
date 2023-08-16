from trade_modules import * 
# from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 
import sqlite3 
import sys

def tokenLookup(symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(icici.get_names('NSE',symbol)['isec_token_level1'])
        # token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list

def subscribeFeed(tokens):
    for token in tokens:
        st = icici.subscribe_feeds(token)
        print(st)

def unsubscribeFeed(tokens):
    for token in tokens:
        st=icici.unsubscribe_feeds(token)
        print(st)

def on_ticks(ticks): 
    # print(f'{ticks["symbol"]}-{ticks["last"]}')
    global livePrices 
    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'CandleTime'] = ticks['ltt']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Close'] = ticks['last']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'High'] = ticks['high']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Low'] = ticks['low']
    else:
        new_row = {'CandleTime': ticks['ltt'], 'Token': ticks['symbol'][4:], 'Close': ticks['last'], 
                   'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low']}
        livePrices=pd.DataFrame(new_row, index = [0]) 
        
    # print(f"Ticks: {ticks['symbol']}-{ticks['stock_name']}-{ticks['ltt']}-{ticks['last']}-{ticks['ltq']}")
    # insert_ticks=db_insert_ticks(ticks)
   
if icici.user_id is None:
    st = createICICISession(icici)

wl_df = pd.read_csv('WatchList.csv')
tokens=tokenLookup(list(wl_df['Code'].values))
subscription_flag = 'N'
livePrices = wl_df

while True:    
    now = dt.datetime.now()       
    if (now.time() >= time(9,14,50) and now.time() < time(15,35,0)):
        if subscription_flag=='N':
            icici.ws_connect()
            icici.on_ticks = on_ticks
            subscribeFeed(tokens)
            subscription_flag = 'Y'    
        else:
            livePrices.to_csv('WatchList.csv',index=False) 
            # print(livePrices)
    if (now.hour >= 15 and now.minute >= 35 and subscription_flag=='Y'):
        unsubscribeFeed(tokens)
        icici.ws_disconnect()
        subscription_flag='N'
        db_delete_ticks(tickers)
        break
    
    if subscription_flag == 'Y':
        tm.sleep(1)
    else:
        tm.sleep(60)

sys.exit()
