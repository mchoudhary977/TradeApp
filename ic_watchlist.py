from ic_functions import *
import logging
import os
from datetime import datetime, time
# import datetime as dt
import time as tm
import pandas as pd
# import json
# import sys

livePrices = pd.DataFrame()

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


# Main Function Start
if __name__ == '__main__':
    if os.path.exists('WatchList.csv') == False:
        ic_update_watchlist(mode='C')
    subscription_flag = 'N'
    ic_update_watchlist(mode='C')

    while True:
        now = datetime.now()
        try:
            if (now.time() >= time(9,14,50) and now.time() < time(15,35,0)):
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
                    else:
                        ic_get_watchlist(mode='C')
                else:
                    livePrices.to_csv('WatchList.csv',index=False)
            if (now.time() >= time(15,35) and subscription_flag=='Y'):
                ic_unsubscribeFeed(tokens['data'])
                icici.ws_disconnect()
                subscription_flag='N'
                break

            if subscription_flag == 'Y':
                tm.sleep(1)
            else:
                tm.sleep(60)
        except Exception as e:
            pass