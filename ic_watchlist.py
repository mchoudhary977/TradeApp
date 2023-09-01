from ic_functions import *
from wa_notifications import * 
from log_function import * 
import os
from datetime import datetime, time
# import datetime as dt
import time as tm
import pandas as pd
# import json
# import sys

livePrices = pd.DataFrame()
tsymbols_df = pd.DataFrame()

# Subscribe ICICI Tokens
def ic_subscribeFeed(tokens):
    for token in tokens:
        st = icici.subscribe_feeds(token)
        print(st)

# UnSubscribe ICICI Tokens
def ic_unsubscribeFeed(tokens):
    for token in tokens:
        st=icici.unsubscribe_feeds(token)
        # st=icici.unsubscribe_feeds(token)
        print(st)

# On Ticks function
def on_ticks(ticks):
    # print(f'{ticks["symbol"]}-{ticks["last"]}')
    global tsymbols_df
    global livePrices
    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'CandleTime'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Close'] = ticks['last']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'High'] = ticks['high']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Low'] = ticks['low']

    else:
        new_row = {'CandleTime': ticks['ltt'], 'Token': ticks['symbol'][4:], 'Close': ticks['last'],
                   'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low']}
        livePrices=pd.DataFrame(new_row, index = [0])
        
    
    if len(tsymbols_df) > 0:
        tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'Timestamp'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'LivePrice'] = ticks['last']
        # tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        # tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'High'] = ticks['high']
        # tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'Low'] = ticks['low']
    

def ic_subscribe_traded_symbols():
    trade_file = 'Trades.csv'
    if os.path.exists(trade_file) == True:
        trade_df = pd.read_csv(trade_file)
        if len(trade_df) > 0:   
            tsymbols_df = trade_df
            trade_df = trade_df[trade_df['TradeStatus']=='OPEN'][trade_df['LiveFeed']=='N']
            
            traded_tokens=[]            
            for index, row in trade_df.iterrows():
                traded_tokens.append(f"4.1!{row['SecurityID']}") 
                trade_df.loc[index,'LiveFeed'] = 'Y'
                print(trade_df.loc[index,'LiveFeed'])
            if len(traded_tokens) > 0:
                trade_df['LiveFeed'] = 'Y'
                ic_subscribeFeed(traded_tokens)
                tsymbols_df.to_csv(trade_file,index=False)
                return {'status':'success','remarks':'','data':'traded symbols subscribed'}
    return {'status':'success','remarks':'','data':'no symbols to subscribe'}

    
# Main Function Start
if __name__ == '__main__':
    session_id = ic_autologon()
    write_log('ic_watchlist','i',f"ICICI Session ID - {session_id}")
    if os.path.exists('WatchList.csv') == False:
        ic_update_watchlist(mode='C')
    subscription_flag = 'N'
    ic_update_watchlist(mode='C')

    while True:
        now = datetime.now()
        try:
            if now.time() < time(9,0) or now.time() > time(15,40):
                break
            if (now.time() >= time(9,14) and now.time() < time(15,35,0)):
                if subscription_flag=='N':
                    if os.path.exists('WatchList.csv'):
                        print('Subscribed')
                        icici.ws_connect()
                        icici.on_ticks = on_ticks
                        wl_df = pd.read_csv('WatchList.csv')
                        livePrices = wl_df
                        tokens=ic_tokenLookup(list(wl_df['Code'].values))
                        ic_subscribeFeed(tokens['data'])
                        subscription_flag = 'Y'
                        send_whatsapp_msg('Feed Alert','Market Live Feed Started!')
                    else:
                        ic_get_watchlist(mode='C')
                else:
                    livePrices.to_csv('WatchList.csv',index=False)
                    # ic_subscribe_traded_symbols()
                    # if len(tsymbols_df) > 0:
                    #     tsymbols_df.to_csv('Trades.csv',index=False)
            if (now.time() >= time(15,35) and subscription_flag=='Y'):
                ic_unsubscribeFeed(tokens['data'])
                icici.ws_disconnect()
                subscription_flag='N'
                send_whatsapp_msg('Feed Alert','Market Live Feed Stopped!')
                break

            if subscription_flag == 'Y':
                tm.sleep(1)
            else:
                tm.sleep(60)
        except Exception as e:
            pass
        # tm.sleep(1)