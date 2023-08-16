from trade_modules import * 
# from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 
import sys

def ic_tokenLookup(symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(icici.get_names('NSE',symbol)['isec_token_level1'])
        # token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list

def ic_subscribeFeed(tokens):
    for token in tokens:
        st = icici.subscribe_feeds(token)
        print(st)

def ic_unsubscribeFeed(tokens):
    for token in tokens:
        st=icici.unsubscribe_feeds(token)
        print(st)

def on_ticks(ticks): 
    # print(f'{ticks["symbol"]}-{ticks["last"]}')
    global livePrices 
    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'CandleTime'] = dt.datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Close'] = ticks['last']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'High'] = ticks['high']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Low'] = ticks['low']
        
    else:
        new_row = {'CandleTime': ticks['ltt'], 'Token': ticks['symbol'][4:], 'Close': ticks['last'], 
                   'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low']}
        livePrices=pd.DataFrame(new_row, index = [0]) 

def ic_get_sym_price(symbol):
    sym = instrument_df[instrument_df['CD']==symbol][['NS','EC','SG','TK','CD','LS']]  
    sym.rename(columns={'NS':'SymbolName','EC':'ExchangeCode','SG':'Segment',
                        'TK':'Token','CD':'Code','LS':'LotSize'}, inplace=True) 
    
    response=iciciGetSymDetail(exchange_code = "NSE",stock_code = symbol,product_type = "Cash",interval = "5minute",
                            from_date = (datetime.now()-timedelta(1)),to_date = (datetime.now()-timedelta(0)))
    if response['status'] == 'SUCCESS':
        if len(response['data']) > 0:
            data = response['data']
            data['date'] = data['datetime']
            data['datetime'] = data['datetime'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
            max_timestamp = data.groupby(data['datetime'].dt.date)['datetime'].max()[-2]
            data = data[data['datetime']>=max_timestamp]
            
            data = data.set_index('datetime')
            data=data.resample('1D').agg({'date':'last','stock_code':'first','open': 'first','high':'max','low':'min','close':'last'}).dropna()               
            sym['Open']=data.iloc[-1]['open']
            sym['High']=data.iloc[-1]['high']
            sym['Low']=data.iloc[-1]['low']
            sym['Close']=data.iloc[-1]['close']
            sym['PrevClose']=data.iloc[-2]['close']
            sym['Difference']=data.iloc[-1]['close'] - data.iloc[-2]['close']
            sym['CandleTime']=data.iloc[-1]['date']
        else:
            sym['Open']=0
            sym['High']=0
            sym['Low']=0
            sym['Close']=0
            sym['PrevClose']=0
            sym['Difference']=0
            sym['CandleTime']=dt.datetime.now()
    else:
        sym['Open']=0
        sym['High']=0
        sym['Low']=0
        sym['Close']=0
        sym['PrevClose']=0
        sym['Difference']=0
        sym['CandleTime']=dt.datetime.now()
    return sym

def ic_get_watchlist(mode='R'):
    resultDict = {}
    symbol_list = json.load(open('config.json', 'r'))['STOCK_CODES']
    wl_df = pd.DataFrame(columns=['SymbolName','ExchangeCode','Segment','Token','Code','LotSize',
                                  'Open','High','Low','Close','PrevClose','Difference','CandleTime'])
    if mode == 'C':  # Create
        for symbol in symbol_list:
            sym=ic_get_sym_price(symbol)
            wl_df = pd.concat([wl_df,sym],ignore_index=True)
        wl_df.to_csv('WatchList.csv',index=False)       
            
    elif mode == 'I':  # Insert
        wl_df = pd.read_csv('WatchList.csv')
        wl = list(wl_df['Code'].values)
        inserted_symbols = [element for element in symbol_list if element not in wl]
        
        if len(inserted_symbols) > 0:
            for i in inserted_symbols:
                sym=ic_get_sym_price(symbol)
                wl_df = pd.concat([wl_df,sym],ignore_index=True)
            wl_df.to_csv('WatchList.csv',index=False) 
    elif mode == 'D':  # Delete
        wl_df = pd.read_csv('WatchList.csv')
        wl = list(wl_df['Code'].values)
        deleted_symbols = [element for element in wl if element not in symbol_list]
        if len(deleted_symbols) > 0:
            wl_df = wl_df[~wl_df['Code'].isin(deleted_symbols)]
            wl_df.to_csv('WatchList.csv',index=False)
    elif mode == 'R':  # Read
        if os.path.exists('WatchList.csv'):
            wl_df = pd.read_csv('WatchList.csv')
        else:
            wl_df = get_watchlist('C')  
        wl_df = wl_df.to_dict(orient='records')
    elif mode == 'N':  # NormalRead
        if os.path.exists('WatchList.csv'):
            wl_df = pd.read_csv('WatchList.csv')
        else:
            wl_df = get_watchlist('C')  
    resultDict['WatchList-DF'] = wl_df
    return resultDict


def ic_start_market_feed():
    if icici.user_id is None:
        st = createICICISession(icici)    
    if os.path.exists('WatchList.csv'):
        wl_df = pd.read_csv('WatchList.csv')
    tokens=tokenLookup(list(wl_df['Code'].values))
    subscribeFeed(tokens)

instrument_list = pd.read_csv('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')
instrument_df = instrument_list
subscription_flag = 'N'
livePrices = wl_df

# wl_df = pd.read_csv('WatchList.csv')
# tokens=tokenLookup(list(wl_df['Code'].values))

while True:    
    now = dt.datetime.now()       
    if (now.time() >= time(9,14,50) and now.time() < time(15,35,0)):
        if subscription_flag=='N':
            icici.ws_connect()
            icici.on_ticks = on_ticks
            ic_start_market_feed()
            # subscribeFeed(tokens)
            subscription_flag = 'Y'    
        else:
            livePrices.to_csv('WatchList.csv',index=False) 
            print(livePrices)
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
