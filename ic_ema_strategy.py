# -*- coding: utf-8 -*-
"""
ICICIDirect - Implementing real time EMA Strategy

@author: Mukesh Choudhary
"""

from ic_functions import * 
from dh_functions import *
from wa_notifications import * 
# from trade_modules import * 
# from kiteconnect import KiteTicker, KiteConnect
import pandas as pd
import time as tm
from datetime import datetime, time
# import datetime as dt
import os
import sys
import numpy as np


# ohlc_df = doji(ohlc_df)
def doji(ohlc_df):    
    """returns dataframe with doji candle column"""
    df = ohlc_df.copy()
    avg_candle_size = abs(df["close"] - df["open"]).median()
    df["doji"] = abs(df["close"] - df["open"]) <=  (0.05 * avg_candle_size)
    return df

# ohlc_df = maru_bozu(ohlc_df)
def maru_bozu(ohlc_df):    
    """returns dataframe with maru bozu candle column"""
    df = ohlc_df.copy()
    avg_candle_size = abs(df["close"] - df["open"]).median()
    df["h-c"] = df["high"]-df["close"]
    df["l-o"] = df["low"]-df["open"]
    df["h-o"] = df["high"]-df["open"]
    df["l-c"] = df["low"]-df["close"]
    df["maru_bozu"] = np.where((df["close"] - df["open"] > 2*avg_candle_size) & \
                               (df[["h-c","l-o"]].max(axis=1) < 0.005*avg_candle_size),"maru_bozu_green",
                               np.where((df["open"] - df["close"] > 2*avg_candle_size) & \
                               (abs(df[["h-o","l-c"]]).max(axis=1) < 0.005*avg_candle_size),"maru_bozu_red",False))
    df.drop(["h-c","l-o","h-o","l-c"],axis=1,inplace=True)
    return df

# ohlc_df = hammer(ohlc_df)
def hammer(ohlc_df):    
    """returns dataframe with hammer candle column"""
    df = ohlc_df.copy()
    df["hammer"] = (((df["high"] - df["low"])>3*(df["open"] - df["close"])) & \
                   ((df["close"] - df["low"])/(.001 + df["high"] - df["low"]) > 0.6) & \
                   ((df["open"] - df["low"])/(.001 + df["high"] - df["low"]) > 0.6)) & \
                   (abs(df["close"] - df["open"]) > 0.1* (df["high"] - df["low"]))
    return df

# ohlc_df = shooting_star(ohlc_df)
def shooting_star(ohlc_df):    
    """returns dataframe with shooting star candle column"""
    df = ohlc_df.copy()
    df["sstar"] = (((df["high"] - df["low"])>3*(df["open"] - df["close"])) & \
                   ((df["high"] - df["close"])/(.001 + df["high"] - df["low"]) > 0.6) & \
                   ((df["high"] - df["open"])/(.001 + df["high"] - df["low"]) > 0.6)) & \
                   (abs(df["close"] - df["open"]) > 0.1* (df["high"] - df["low"]))
    return df

# ohlc_df = levels(ohlc_day)
def levels(ohlc_day):    
    """returns pivot point and support/resistance levels"""
    high = round(ohlc_day["high"].iloc[-1],2)
    low = round(ohlc_day["low"].iloc[-1],2)
    close = round(ohlc_day["close"].iloc[-1],2)
    pivot = round((high + low + close)/3,2)
    r1 = round((2*pivot - low),2)
    r2 = round((pivot + (high - low)),2)
    r3 = round((high + 2*(pivot - low)),2)
    s1 = round((2*pivot - high),2)
    s2 = round((pivot - (high - low)),2)
    s3 = round((low - 2*(high - pivot)),2)
    return (pivot,r1,r2,r3,s1,s2,s3)

# ohlc_df = trend(ohlc_df,n=1)
# n=1
def trend(ohlc_df,n):
    "function to assess the trend by analyzing each candle"
    df = ohlc_df.copy()
    df["up"] = np.where(df["low"]>=df["low"].shift(1),1,0)
    df["dn"] = np.where(df["high"]<=df["high"].shift(1),1,0)
    if df["close"].iloc[-1] > df["open"].iloc[-1]:
        if df["up"].iloc[-1*n:].sum() >= 0.7*n:
            return "uptrend"
    elif df["open"].iloc[-1] > df["close"].iloc[-1]:
        if df["dn"].iloc[-1*n:].sum() >= 0.7*n:
            return "downtrend"
    else:
        return None

# ohlc_df = res_sup(ohlc_df,ohlc_day)
def res_sup(ohlc_df,ohlc_day):
    """calculates closest resistance and support levels for a given candle"""
    level = ((ohlc_df["close"].iloc[-1] + ohlc_df["open"].iloc[-1])/2 + (ohlc_df["high"].iloc[-1] + ohlc_df["low"].iloc[-1])/2)/2
    p,r1,r2,r3,s1,s2,s3 = levels(ohlc_day)
    l_r1=level-r1
    l_r2=level-r2
    l_r3=level-r3
    l_p=level-p
    l_s1=level-s1
    l_s2=level-s2
    l_s3=level-s3
    lev_ser = pd.Series([l_p,l_r1,l_r2,l_r3,l_s1,l_s2,l_s3],index=["p","r1","r2","r3","s1","s2","s3"])
    sup = lev_ser[lev_ser>0].idxmin()
    res = lev_ser[lev_ser<0].idxmax()
    return (eval('{}'.format(res)), eval('{}'.format(sup)))


# ohlc_df = candle_type(ohlc_df)
def candle_type(ohlc_df):    
    """returns the candle type of the last candle of an OHLC DF"""
    candle = None
    if doji(ohlc_df)["doji"].iloc[-1] == True:
        candle = "doji"    
    if maru_bozu(ohlc_df)["maru_bozu"].iloc[-1] == "maru_bozu_green":
        candle = "maru_bozu_green"       
    if maru_bozu(ohlc_df)["maru_bozu"].iloc[-1] == "maru_bozu_red":
        candle = "maru_bozu_red"        
    if shooting_star(ohlc_df)["sstar"].iloc[-1] == True:
        candle = "shooting_star"        
    if hammer(ohlc_df)["hammer"].iloc[-1] == True:
        candle = "hammer"       
    return candle

# ohlc_df = candle_pattern(ohlc_df,ohlc_day)
def candle_pattern(ohlc_df,ohlc_day):    
    """returns the candle pattern identified"""
    pattern = None
    signi = "low"
    avg_candle_size = abs(ohlc_df["close"] - ohlc_df["open"]).median()
    sup, res = res_sup(ohlc_df,ohlc_day)
    
    if (sup - 1.5*avg_candle_size) < ohlc_df["close"].iloc[-1] < (sup + 1.5*avg_candle_size):
        signi = "HIGH"
        
    if (res - 1.5*avg_candle_size) < ohlc_df["close"].iloc[-1] < (res + 1.5*avg_candle_size):
        signi = "HIGH"
    
    if candle_type(ohlc_df) == 'doji' \
        and ohlc_df["close"].iloc[-1] > ohlc_df["close"].iloc[-2] \
        and ohlc_df["close"].iloc[-1] > ohlc_df["open"].iloc[-1]:
            pattern = "doji_bullish"
    
    if candle_type(ohlc_df) == 'doji' \
        and ohlc_df["close"].iloc[-1] < ohlc_df["close"].iloc[-2] \
        and ohlc_df["close"].iloc[-1] < ohlc_df["open"].iloc[-1]:
            pattern = "doji_bearish" 
            
    if candle_type(ohlc_df) == "maru_bozu_green":
        pattern = "maru_bozu_bullish"
    
    if candle_type(ohlc_df) == "maru_bozu_red":
        pattern = "maru_bozu_bearish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "uptrend" and candle_type(ohlc_df) == "hammer":
        pattern = "hanging_man_bearish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "downtrend" and candle_type(ohlc_df) == "hammer":
        pattern = "hammer_bullish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "uptrend" and candle_type(ohlc_df) == "shooting_star":
        pattern = "shooting_star_bearish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "uptrend" \
        and candle_type(ohlc_df) == "doji" \
        and ohlc_df["high"].iloc[-1] < ohlc_df["close"].iloc[-2] \
        and ohlc_df["low"].iloc[-1] > ohlc_df["open"].iloc[-2]:
        pattern = "harami_cross_bearish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "downtrend" \
        and candle_type(ohlc_df) == "doji" \
        and ohlc_df["high"].iloc[-1] < ohlc_df["open"].iloc[-2] \
        and ohlc_df["low"].iloc[-1] > ohlc_df["close"].iloc[-2]:
        pattern = "harami_cross_bullish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "uptrend" \
        and candle_type(ohlc_df) != "doji" \
        and ohlc_df["open"].iloc[-1] > ohlc_df["high"].iloc[-2] \
        and ohlc_df["close"].iloc[-1] < ohlc_df["low"].iloc[-2]:
        pattern = "engulfing_bearish"
        
    if trend(ohlc_df.iloc[:-1,:],7) == "downtrend" \
        and candle_type(ohlc_df) != "doji" \
        and ohlc_df["close"].iloc[-1] > ohlc_df["high"].iloc[-2] \
        and ohlc_df["open"].iloc[-1] < ohlc_df["low"].iloc[-2]:
        pattern = "engulfing_bullish"
       
    return {'significance': signi, 'pattern': pattern}

def rsi(df, n):
    "function to calculate RSI"
    delta = df["close"].diff().dropna()
    u = delta * 0
    d = u.copy()
    u[delta > 0] = delta[delta > 0]
    d[delta < 0] = -delta[delta < 0]
    u[u.index[n-1]] = np.mean( u[:n]) # first value is average of gains
    u = u.drop(u.index[:(n-1)])
    d[d.index[n-1]] = np.mean( d[:n]) # first value is average of losses
    d = d.drop(d.index[:(n-1)])
    rs = u.ewm(com=n,min_periods=n).mean()/d.ewm(com=n,min_periods=n).mean()
    return 100 - 100 / (1+rs)

def signal(row):
    ema_xover = row['9-ema'] - row['15-ema']
    ema_signal = 1 if ema_xover > 0 and abs(ema_xover) >= 1 else (-1 if ema_xover < 0 and abs(ema_xover) >= 1 else 0)
    rsi_signal = 1 if row['rsi'] > 50 else (-1 if row['rsi'] < 50 else 0)
    signal = 'green' if ema_signal == 1 and ema_signal==rsi_signal else (
        'red' if ema_signal == -1 and ema_signal==rsi_signal else None)
    return signal

def update_entry(row):
    if row['signal'] == 'green':
        return row['high']
    elif row['signal'] == 'red':
        return row['low']
    else:
        return 0    
    
if __name__ == '__main__':
    print('---------------- Start EMA Strategy ----------------')
    sig = {}
    ticker='NIFTY'
    while True:
        now = datetime.now()
        if now.time() < time(9,0) or now.time() > time(15,10):
            break
        
        if (now.minute % 5 == 0 and now.second == 5):
            print('EMA Calculation Start')
            st=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=4)           
            df = st['data']
            if st['status'] == 'FAILURE':
                send_whatsapp_msg('Failure Alert', 'Tick data not returned')            
                continue
            df['datetime'] = df['datetime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))         
            df['timestamp'] = pd.to_datetime(df['datetime'])
            df = df[df['timestamp'].dt.time >= pd.Timestamp('09:15:00').time()]
            df['15-ema'] = df['close'].ewm(span=15, adjust=False).mean()
            df['9-ema'] = df['close'].ewm(span=9, adjust=False).mean()
            df['ema_xover'] = df['9-ema'] - df['15-ema']
            df['rsi'] = rsi(df,14)
            df['signal'] = df.apply(signal, axis=1)
            df['signal'] = df['signal'].where(df['signal'] != df['signal'].shift())
            df['entry'] = df.apply(update_entry, axis=1)
            df['active'] = 'N'
            # df = df[df['datetime']<='2023-08-16 12:30:00']          
            sig = df.iloc[-1]
            for index,row in df.iloc[::-1].iterrows():
                if sig['signal'] == 'green':
                    if row['ema_xover'] < 0:
                        sig['stoploss'] = row['low']
                        sig['step'] = abs(sig['entry']-sig['stoploss'])
                        sig['target'] = sig['entry'] + sig['step']
                        sig['active'] = 'Y'
                        break
                if sig['signal'] == 'red':
                    if row['ema_xover'] > 0:
                        sig['stoploss'] = row['high']
                        sig['step'] = abs(sig['entry']-sig['stoploss'])
                        sig['target'] = sig['entry'] - sig['step']
                        sig['active'] = 'Y'
                        break
                    
            if sig['active'] == 'Y':
                print(sig)
                
            ohlc_df = df
            ohlc_day = ohlc_df.copy()             
            ohlc_day.set_index('datetime', inplace=True)
            ohlc_day = ohlc_day.resample('D').agg({'stock_code':'first', 'open': 'first', 'high':'max','low':'min','close':'last'}).iloc[:-1].dropna()
            cdl_pattern = candle_pattern(ohlc_df,ohlc_day)
            
            if cdl_pattern['pattern'] is not None:
                send_whatsapp_msg('Candle Pattern Alert', f"Pattern -> {cdl_pattern['pattern']} :: Significance -> {cdl_pattern['significance']} ")
            
            
            # print(sig)
            # wl = pd.read_csv('WatchList.csv')
            # last_px = wl[wl['Code']==ticker]['Close'].values[0]
            # last_px = 19377
            # send_whatsapp_msg('EMA Alert', mtext=f"price-{last_px}")
            # print(f"price-{last_px}")
        
        if len(sig) == 0:
            continue
        
        if sig['active'] == 'Y':
            wl = pd.read_csv('WatchList.csv')
            last_px = wl[wl['Code']==ticker]['Close'].values[0]
            stg_file = 'Strategy.csv'
            stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
            
            if (sig['signal'] == 'green' and last_px > sig['entry'] and sig['entry'] > 0):
                sig['active'] = 'N'
                opt=ic_option_chain(ticker, underlying_price=last_px, option_type="CE", duration=0).iloc[2]
                
                mtext=f"BUY Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - {opt['TK']}"
                orders = pd.DataFrame(dhan.get_order_list()['data'])
                if len(orders) >= 0:
                    if len(orders[orders['orderStatus'] == 'TRADED'])/2 < 2.5:
                        try:
                            st = dh_place_bo_order(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=5,tg_point=20,sl_price=0)
                            tm.sleep(2)
                            order_id = st['data']['orderId'] if st['status']=='success' else None
                            order_det = dh_get_order_id(order_id)['data']
                            mtext=f"BUY Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - {order_det['tradingSymbol']}"
                        except Exception as e:
                            mtext=f"BUY Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - Order Placement Error"
                    else:
                        mtext=f"BUY Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - Order Not Placed as day count exceeded - {len(orders[orders['orderStatus'] == 'TRADED'])}"
                
                send_whatsapp_msg('EMA Alert', mtext)
                print(f"BUY Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - {opt['TK']}")
                
            elif (sig['signal'] == 'red' and last_px < sig['entry'] and sig['entry'] > 0):
                sig['active'] = 'N'
                opt=ic_option_chain(ticker, underlying_price=last_px, option_type="PE", duration=0).iloc[-3]
            
                mtext=f"SELL Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - {opt['TK']}"
                orders = pd.DataFrame(dhan.get_order_list()['data'])
                if len(orders) >= 0:
                    if len(orders[orders['orderStatus'] == 'TRADED'])/2 < 2.5:
                        try:
                            st = dh_place_bo_order(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=5,tg_point=20,sl_price=0)
                            tm.sleep(2)
                            order_id = st['data']['orderId'] if st['status']=='success' else None
                            order_det = dh_get_order_id(order_id)['data']
                            mtext=f"SELL Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - {order_det['tradingSymbol']}"
                        except Exception as e:
                            mtext=f"SELL Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - Order Placement Error"
                    else:
                        mtext=f"SELL Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - Order Not Placed as day count exceeded - {len(orders[orders['orderStatus'] == 'TRADED'])}"
                
                send_whatsapp_msg('EMA Alert', mtext)
                print(f"SELL Order - {sig['entry']} - {sig['stoploss']} - {opt['CD']} - {opt['TK']}")
                        
        tm.sleep(1)
    
    sys.exit()
                

    #             if (sig['active'] == 'Y' and sig['signal'] == 'green' and last_px > sig['entry'] and sig['entry'] > 0):
    #                 sig['active'] = 'N'
    #                 opt=ic_option_chain(ticker, underlying_price=last_px, duration=0).iloc[2]
    #                 if len(stg_df)>0:
    #                     if len(stg_df[stg_df['securityId'] == opt['TK']]) > 0:
    #                         continue
    #                 st = dh_place_mkt_order('NFO',opt['TK'],'buy',50,0)
    #                 tm.sleep(2)
    #                 order_id = st['data']['orderId'] if st['status']=='success' else None
    #                 order_det = dh_get_order_id(order_id)['data']
    #                 if order_det['orderStatus']=='TRADED':
    #                     new_row = {}
    #                     new_row['name'] = 'Bullish EMA Strategy'
    #                     new_row['tradingSymbol'] = order_det['tradingSymbol']
    #                     new_row['securityId'] = order_det['securityId']
    #                     new_row['type'] = 'long'
    #                     new_row['quantity'] = order_det['quantity']
    #                     new_row['entry_id'] = order_det['orderId']
    #                     new_row['entry_px'] = order_det['price']
    #                     new_row['exit_id'] = None
    #                     new_row['exit_px'] = None
    #                     new_row['symbol'] = ticker
    #                     new_row['entry'] = sig['entry']
    #                     new_row['stoploss'] = sig['sl']
    #                     new_row['target'] = sig['entry'] + (sig['entry'] - sig['sl'])
    #                     new_row['step'] = sig['entry'] - sig['sl']
                        
    #                     stg_df = pd.concat([stg_df,pd.DataFrame(new_row)],ignore_index=True)
                         
    #                 print(f"add buy order - {sig['entry']} - {sig['sl']} - {opt['CD']} - {opt['TK']}")
    #             elif (sig['active'] == 'Y' and sig['signal'] == 'red' and last_px < sig['entry'] and sig['entry'] > 0):
    #                 sig['active'] = 'N'
    #                 opt=ic_option_chain(ticker, underlying_price=last_px, option_type="PE", duration=0).iloc[-3]
    #                 if len(stg_df)>0:
    #                     if len(stg_df[stg_df['securityId'] == opt['TK']]) > 0:
    #                         continue
    #                 st = dh_place_mkt_order('NFO',opt['TK'],'buy',50,0)
    #                 tm.sleep(2)
    #                 order_id = st['data']['orderId'] if st['status']=='success' else None
    #                 order_det = dh_get_order_id(order_id)['data']
    #                 if order_det['orderStatus']=='TRADED':
    #                     new_row = {}
    #                     new_row['name'] = 'Bullish EMA Strategy'
    #                     new_row['tradingSymbol'] = order_det['tradingSymbol']
    #                     new_row['securityId'] = order_det['securityId']
    #                     new_row['type'] = 'short'
    #                     new_row['quantity'] = order_det['quantity']
    #                     new_row['entry_id'] = order_det['orderId']
    #                     new_row['entry_px'] = order_det['price']
    #                     new_row['exit_id'] = None
    #                     new_row['exit_px'] = None
    #                     new_row['symbol'] = ticker
    #                     new_row['entry'] = sig['entry']
    #                     new_row['stoploss'] = sig['sl']
    #                     new_row['target'] = sig['entry'] + (sig['entry'] - sig['sl'])
    #                     new_row['step'] = sig['entry'] - sig['sl']
                        
    #                     stg_df = pd.concat([stg_df,pd.DataFrame(new_row)],ignore_index=True)
                    
    #                 print(f"add sell order - {sig['entry']} - {sig['sl']} - {opt['CD']} - {opt['TK']}")
                    
    #     else:
    #         break
    #     tm.sleep(1)
        
    # sys.exit()
    