# -*- coding: utf-8 -*-
"""
ICICIDirect - Implementing real time EMA Strategy

@author: Mukesh Choudhary
"""

from ic_functions import *
from dh_functions import *
from wa_notifications import *
from log_function import * 
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
# number of candles to be considered for trend identification n = 7
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

    return {'timestamp':ohlc_df["datetime"].iloc[-1], 'significance': signi, 'pattern': pattern}

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

def generate_ema_signal(df):
    df['15-ema'] = round(df['close'].ewm(span=15, adjust=False).mean(),2)
    df['9-ema'] = round(df['close'].ewm(span=9, adjust=False).mean(),2)
    df['ema_xover'] = round(df['9-ema'] - df['15-ema'],2)
    df['rsi'] = round(rsi(df,14),2)
    df = df[df['timestamp'] >= pd.Timestamp('09:15:00')]
    df['signal'] = df.apply(signal, axis=1)
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'] = df['signal'].where(df['signal'] != df['signal'].shift())
    df['entry'] = round(df.apply(update_entry, axis=1),2)
    df['active'] = 'N'

    sig = df.iloc[-1]

    # sig = last_row.copy()
    for index,row in df.iloc[::-1].iterrows():
        if sig['signal'] == 'green':
            if row['ema_xover'] < 0:
                sig['stoploss'] = row['low']
                sig['step'] = abs(sig['entry']-sig['stoploss'])
                sig['target'] = round(sig['entry'] + sig['step'],2)
                sig['active'] = 'Y'
                break
        if sig['signal'] == 'red':
            if row['ema_xover'] > 0:
                sig['stoploss'] = row['high']
                sig['step'] = abs(sig['entry']-sig['stoploss'])
                sig['target'] = round(sig['entry'] - sig['step'],2)
                sig['active'] = 'Y'
                break
    return sig

# ticker='NIFTY' last_px = 19370
def check_ema_signal(ticker, sig, last_px):
    if (sig['signal'] == 'green' and last_px > sig['entry'] and sig['entry'] > 0):
        sig['active'] = 'N'
        opt=ic_option_chain(ticker, underlying_price=last_px, option_type="CE", duration=0).iloc[2]

        mtext=f"BUY -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - {opt['TK']}"
        orders = pd.DataFrame(dh_get_orders()['data'])
        pos = pd.DataFrame(dh_get_positions()['data'])
        pos_actv = len(pos[pos['securityId']==str(opt['TK'])][pos['positionType']!='CLOSED'])
        if pos_actv > 0:
            mtext=f"BUY -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - {opt['TK']} -> Active Position Present - Order Not Placed"

        if len(orders) >= 0 and pos_actv == 0:
            # if len(orders[orders['orderStatus'] == 'TRADED'])/2 < 2.5:
            if len(orders[orders['orderStatus'] == 'TRADED'])>=0:
                try:
                    # print(f"security_id={opt['TK']}")
                    st = dh_place_bo_order(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=10,tg_point=50,sl_price=0)
                    # st = dh_place_bo_order_1(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=10,tg_point=20,sl_price=0)
                    tm.sleep(2)
                    if st['status'] == 'failure':
                        raise ValueError(f"{st['remarks']['message']}")
                    order_id = st['data']['orderId'] if st['status']=='success' else None
                    order_det = dh_get_order_id(order_id)['data']
                    mtext=f"BUY -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} | {order_det['tradingSymbol']} | {order_id}"
                    # write_log('ic_ema_strategy','i',mtext)
                except Exception as e:
                    write_log('ic_ema_strategy','e',str(e))                    
                    mtext=f"BUY -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - Order Placement Error - {str(e)}"
                    # print(str(e))
                    # print(mtext)
            else:
                mtext=f"BUY -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - Order Not Placed as day count exceeded - {len(orders[orders['orderStatus'] == 'TRADED'])}"

        send_whatsapp_msg(f"EMA Alert - {sig['datetime']}", mtext)
        write_log('ic_ema_strategy','i',mtext)
        # print(f"BUY -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - {opt['TK']}")

    elif (sig['signal'] == 'red' and last_px < sig['entry'] and sig['entry'] > 0):
        sig['active'] = 'N'
        opt=ic_option_chain(ticker, underlying_price=last_px, option_type="PE", duration=0).iloc[-3]

        mtext=f"SELL -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - {opt['TK']}"
        orders = pd.DataFrame(dh_get_orders()['data'])
        pos = pd.DataFrame(dh_get_positions()['data'])
        pos_actv = len(pos[pos['securityId']==str(opt['TK'])][pos['positionType']!='CLOSED'])
        if pos_actv > 0:
            mtext=f"SELL -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - {opt['TK']} -> Active Position Present - Order Not Placed"
        if len(orders) >= 0 and pos_actv == 0:
            # if len(orders[orders['orderStatus'] == 'TRADED'])/2 < 2.5:
            if len(orders[orders['orderStatus'] == 'TRADED'])>=0:
                try:
                    # print(f"security_id={opt['TK']}")
                    st = dh_place_bo_order(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=10,tg_point=50,sl_price=0)
                    # st = dh_place_bo_order_1(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=10,tg_point=20,sl_price=0)
                    tm.sleep(2)
                    if st['status'] == 'failure':
                        raise ValueError(f"{st['remarks']['message']}")
                    order_id = st['data']['orderId'] if st['status']=='success' else None
                    order_det = dh_get_order_id(order_id)['data']
                    mtext=f"SELL -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} | {order_det['tradingSymbol']} | {order_id}"
                    # write_log('ic_ema_strategy','i',mtext)
                except Exception as e:
                    mtext=f"SELL -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - Order Placement Error - {str(e)}"
                    write_log('ic_ema_strategy','e',str(e))
            else:
                mtext=f"SELL -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - Order Not Placed as day count exceeded - {len(orders[orders['orderStatus'] == 'TRADED'])}"

        send_whatsapp_msg(f"EMA Alert - {sig['datetime']}", mtext)
        write_log('ic_ema_strategy','i',mtext)
        print(f"SELL -> {ticker} -> {sig['stoploss']} - {sig['entry']} - {sig['target']} -> {opt['CD']} - {opt['TK']}")
    return {'status':'SUCCESS','data':'Signal Check Complete'}


def main():
    sig = {}
    ticker='NIFTY'
    st=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=4)
    if st['status']=='SUCCESS':
        send_whatsapp_msg('EMA Strategy', 'EMA Strategy Active')
    else:
        send_whatsapp_msg('EMA Strategy Failure', 'EMA Strategy Not Started')
    while True:
        try:
            now = datetime.now()
            if now.time() < time(9,0) or now.time() > time(15,10):
                break

            if (now.minute % 5 == 0 and now.second == 5):
                write_log('ic_ema_strategy','i',f'EMA Calculation Start - {now.strftime("%Y-%m-%d %H:%M:%S")}')
                print(f'EMA Calculation Start - {now.strftime("%Y-%m-%d %H:%M:%S")}')
                st=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=4)
                if st['status'] == 'FAILURE':
                    send_whatsapp_msg('Failure Alert', 'Tick data not returned')
                    continue
                df = st['data'].copy()
                df['datetime'] = df['datetime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
                # df = df[df['datetime']<='2023-08-18 13:05:00']
                df['timestamp'] = pd.to_datetime(df['datetime'])
                df = df[df['timestamp'].dt.time >= pd.Timestamp('09:15:00').time()]

                # ema strategy signal generation
                sig = generate_ema_signal(df)
                if sig['active'] == 'Y':
                    print(sig)

                ohlc_df = df
                ohlc_day = ohlc_df.copy()
                ohlc_day.set_index('datetime', inplace=True)
                ohlc_day = ohlc_day.resample('D').agg({'stock_code':'first', 'open': 'first', 'high':'max','low':'min','close':'last'}).iloc[:-1].dropna()
                cdl_pattern = candle_pattern(ohlc_df,ohlc_day)
                num_of_candles = 7
                trend_direction = trend(ohlc_df,num_of_candles)

                if cdl_pattern['pattern'] is not None:
                    msg = f"Symbol : {ticker} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : sideways"
                    if trend_direction is not None:
                        msg = f"Symbol : {ticker} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : {trend_direction}"
                    send_whatsapp_msg(f'CandleStick Alert - {cdl_pattern["timestamp"]}', msg)
                    print(msg)

                # last_px = wl[wl['Code']==ticker]['Close'].values[0]
                # last_px = 19272
            if len(sig) == 0:
                continue

            if sig['active'] == 'Y':
                wl = pd.read_csv('WatchList.csv')
                last_px = wl[wl['Code']==ticker]['Close'].values[0]
                stg_file = 'Strategy.csv'
                stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
                check_ema_signal(ticker, sig, last_px)
        except Exception as e:
            err = str(e)
            write_log('ic_ema_strategy','e',err)
            send_whatsapp_msg(f"EMA Failure Alert - {now.strftime('%Y-%m-%d %H:%M:%S')}", err)
            pass

        tm.sleep(1)

    sys.exit()

if __name__ == '__main__':
    print('---------------- Start EMA Strategy ----------------')
    main()


# function created for test purpose only
# change the second interval to 0 (now.second ==5) as tick data should match filter criteria accordingly
# last_px code update to have a valid matching value for strategy to give positive signals
# test_data = back_test_data()
# ic_autologon()
def back_test_data():
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    tick_data = pd.DataFrame(dhan.intraday_daily_minute_charts(
        security_id='13',
        exchange_segment='IDX_I',
        instrument_type='EQUITY'
    )['data'])
    
    temp_list = []
    for i in tick_data['start_Time']:
        temp = dhan.convert_to_date_time(i)
        temp_list.append(temp)
    tick_data['Date'] = temp_list
    sig = {}
    ticker='NIFTY'

    st=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=4)
    
    return ticker, tick_data, st
# icici.user_id
# st['data'] = df.copy()
def back_test_ema_strategy(test_data):
    ticker = test_data[0]
    tick_data = test_data[1]
    st = test_data[2]

    i=0
    while i < len(tick_data):
        try:
            now = tick_data.iloc[i]['Date']
            if now.time() < time(9,0) or now.time() > time(15,10):
                break

            if (now.minute % 5 == 0 and now.second == 0):
                print(f'EMA Calculation Start - {now.strftime("%Y-%m-%d %H:%M:%S")}')
                # st=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=4)
                df = st['data'].copy()

                if st['status'] == 'FAILURE':
                    send_whatsapp_msg('Failure Alert', 'Tick data not returned')
                    continue
                df['datetime'] = df['datetime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
                # df = df[df['datetime']<=now]
                df['timestamp'] = pd.to_datetime(df['datetime'])
                df = df[df['timestamp'].dt.time >= pd.Timestamp('09:15:00').time()]

                # ema strategy signal generation
                sig = generate_ema_signal(df)
                if sig['active'] == 'Y':
                    print(sig)

                ohlc_df = df
                ohlc_day = ohlc_df.copy()
                ohlc_day.set_index('datetime', inplace=True)
                ohlc_day = ohlc_day.resample('D').agg({'stock_code':'first', 'open': 'first', 'high':'max','low':'min','close':'last'}).iloc[:-1].dropna()
                cdl_pattern = candle_pattern(ohlc_df,ohlc_day)
                num_of_candles = 7
                trend_direction = trend(ohlc_df,num_of_candles)

                if cdl_pattern['pattern'] is not None:
                    msg = f"Symbol : {ticker} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : sideways"
                    if trend_direction is not None:
                        msg = f"Symbol : {ticker} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : {trend_direction}"
                    send_whatsapp_msg(f'CandleStick Alert - {cdl_pattern["timestamp"]}', msg)
                    print(msg)

            if len(sig) == 0:
                continue

            if sig['active'] == 'Y':
                last_px = (sig['entry']+1) if sig['signal'] == 'green' else (sig['entry']-1)
                print(f'Checking for active signal - {now}-{last_px}')
                stg_file = 'Strategy.csv'
                stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
                check_ema_signal(ticker, sig, last_px)
        except Exception as e:
            err = str(e)
            write_log('ic_ema_strategy','e',err)
            send_whatsapp_msg(f"EMA Failure Alert - {now.strftime('%Y-%m-%d %H:%M:%S')}", err)
            pass

        tm.sleep(1)
        i=i+1
    # sys.exit()
	
def dh_place_bo_order_1(exchange,security_id,buy_sell,quantity,sl_point=5,tg_point=20,sl_price=0):
    try:
        dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
        drv_expiry_date=None
        drv_options_type=None
        drv_strike_price=None   
        exchange_segment = dhan.NSE
        tag = f"{buy_sell}-{security_id}-{quantity}"
        security_id = int(security_id)
        if exchange=='NFO':
            instrument = pd.read_csv('dhan.csv')
            instrument = instrument[instrument['SEM_SMST_SECURITY_ID']==security_id]
            # lot_size = instrument['SEM_LOT_UNITS'].values[0]
            drv_expiry_date=instrument['SEM_EXPIRY_DATE'].values[0]
            drv_options_type='PUT' if instrument['SEM_OPTION_TYPE'].values[0] == 'PE' else 'CALL'
            drv_strike_price=int(instrument['SEM_STRIKE_PRICE'].values[0])
            exchange_segment = dhan.FNO
            
        t_type = dhan.BUY if buy_sell == 'buy' else dhan.SELL
        
        # Below line is only for testing purpose, comment when prod live
        # order_st = {'status':'success','data':{'orderId':'52230822492201'}}
        order_st = dhan.place_order(tag='',
                                        transaction_type=t_type,
                                        exchange_segment=exchange_segment,
                                        product_type=dhan.BO,
                                        order_type=dhan.MARKET,
                                        validity='DAY',
                                        security_id=str(security_id),
                                        quantity=quantity,
                                        disclosed_quantity=0,
                                        price=0,
                                        trigger_price=0,
                                        after_market_order=True,
                                        amo_time='OPEN',
                                        bo_profit_value=tg_point,
                                        bo_stop_loss_Value=sl_point,
                                        drv_expiry_date=drv_expiry_date,
                                        drv_options_type=drv_options_type,
                                        drv_strike_price=drv_strike_price  
                                        )

        # print(order_st)
        return order_st 
    except Exception as e:
        err = str(e)
        write_log('dh_place_bo_order','e',err)
        # print(f"error - {err}")