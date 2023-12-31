# -*- coding: utf-8 -*-
"""
ICICIDirect - Implementing real time EMA Strategy

@author: Mukesh Choudhary
"""
import yfinance as yf
from ic_functions import *
from dh_functions import *
from wa_notifications import *
from log_function import *
# from trade_modules import *
# from kiteconnect import KiteTicker, KiteConnect
import pandas as pd
import time as tm
from datetime import datetime, time, timedelta
# import datetime as dt
import os
import sys
import numpy as np
import json

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
    try:
        res = lev_ser[lev_ser<0].idxmax()
    except Exception as e:
        res = lev_ser[lev_ser>0].idxmax()
        pass
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

# row = df.iloc[-1].copy()
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
    curr_dt = datetime.now()
    df['15-ema'] = round(df['close'].ewm(span=15, adjust=False).mean(),2)
    df['9-ema'] = round(df['close'].ewm(span=9, adjust=False).mean(),2)
    df['ema_xover'] = round(df['9-ema'] - df['15-ema'],2)
    df['rsi'] = round(rsi(df,14),2)
    df = df[df['datetime'] >= pd.Timestamp(curr_dt.strftime('%Y-%m-%d'))]
    # df = df[df['datetime'] >= pd.Timestamp('09:25:00')]
    df['signal'] = df.apply(signal, axis=1)
    df['signal'].fillna(method='ffill', inplace=True)
    # df['signal'] = df['signal'].where(df['signal'] != df['signal'].shift())
    df['entry'] = round(df.apply(update_entry, axis=1),2)
    df['active'] = 'N'

    sig = df.iloc[-1].copy()
    sig['stoploss'] = 0
    sig['step'] = 0
    sig['target'] = 0
    sig['active'] = 'N'

    # sig = last_row.copy()
    for index,row in df.iloc[::-1].iterrows():
        if sig['signal'] == 'green':
            if row['ema_xover'] < 0:
                sig['stoploss'] = round(row['low'],2)
                sig['step'] = abs(sig['entry']-sig['stoploss'])
                sig['target'] = round(sig['entry'] + sig['step'],2)
                sig['active'] = 'Y'
                break
        if sig['signal'] == 'red':
            if row['ema_xover'] > 0:
                sig['stoploss'] = round(row['high'],2)
                sig['step'] = abs(sig['entry']-sig['stoploss'])
                sig['target'] = round(sig['entry'] - sig['step'],2)
                sig['active'] = 'Y'
                break
    return sig

# ticker='NIFTY' last_px = 19295
# signal = 'green' entry_px = 19420 sec_id=61755
# signal = 'red' entry_px = 19422
def check_ema_signal(ticker, ema_sig, last_px):
    funct_name = 'check_ema_signal'.upper()
    msg = {'status':'failure', 'remarks':'', 'data':''}

    live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
    signal_time = ema_sig['datetime']
    signal = ema_sig['signal']
    entry_px = ema_sig['entry']
    stop_loss = ema_sig['stoploss']
    target = ema_sig['target']
    opt_type = 'CE' if signal == 'green' else ('PE' if signal =='red' else None)
    side = 'BUY' if signal == 'green' else ('SELL' if signal =='red' else None)
    msg['data'] = f"{side} -> {ticker} -> {stop_loss} - {entry_px} - {target} "

    if ema_sig['active']=='Y' and ((signal == 'green' and last_px > entry_px and ema_sig['entry'] > 0) or (signal == 'red' and last_px < entry_px and ema_sig['entry'] > 0)):
        ema_sig['active']='N'
        # count changed to 4 due to balance constraints
        exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
        opt = ic_option_chain(ticker, underlying_price=last_px, option_type=opt_type, duration=exp_week)
        num = int(json.load(open('config.json', 'r'))[ticker]['OPT#'])
        opt = opt.iloc[num]
        sec_id = opt['TK']
        sec_name = opt['CD']
        exit_msg = ''
        exit_flag = 'N'
        msg['data'] = msg['data'] + f"-> [{sec_id}] {sec_name} "

        if live_order == 'Y':
            pos = dh_get_positions()
            pos = pd.DataFrame(pos['data']) if pos['status'].lower() == 'success' and pos['data'] is not None else None

            if pos is not None and len(pos[pos['securityId']==str(sec_id)][pos['positionType'] !='CLOSED'])>0:
                exit_flag = 'Y'
                msg['remarks'] = f"Active Position Present. "

            orders = dh_get_orders()
            orders = pd.DataFrame(orders['data']) if orders['status'].lower() == 'success' and orders['data'] is not None else None

            allow_order_count = int(json.load(open('config.json', 'r'))['DAILY ORDER COUNT'])

            if orders is not None and len(orders[orders['orderStatus'] == 'TRADED'])>allow_order_count:
                exit_flag = 'Y'
                msg['remarks'] = msg['remarks'] + f"Order Count = {len(orders[orders['orderStatus'] == 'TRADED'])} | Greater than daily limit - Order Placement Restricted. [{sec_id}] => {sec_name}"

            if exit_flag == 'Y':
                # msg['remarks'] = msg['remarks'] + f"-> {exit_msg}"
                send_whatsapp_msg(f"EMA Alert - {signal_time}", msg['remarks'])
                write_log('ic_ema_strategy','i',msg['remarks'])
                # msg['status'] = 'success'
                return msg # {'status':'SUCCESS','data':msg['remarks']}

        try:
            trade = {}
            trail_pts = 5
            qty = 1
            side = 'buy'
            if live_order == 'Y':
                trade = dh_post_exchange_order(ord_type='mkt', exchange='FNO',
                                           security_id=sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=0, sl_pts=0,
                                           amo=False, prod_type='')
                if trade['status'].lower() == 'success':
                    msg['data'] = msg['data'] + f"=> Order Placed [ OrderId - {trade['data']['orderId']} | OrderStatus - {trade['data']['orderStatus']} ]"
                    msg['status'] = 'success'

                else:
                    msg['remarks'] = msg['remarks'] + f"Order Failed - {trade['remarks']}"
            else:
                msg['status'] = 'success'
                msg['data'] = msg['data'] + f"=> Live Order not enabled, place manually!"

        except Exception as e:
            err = str(e)
            msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err}"
            pass

        if msg['status'].lower() == 'success':
            send_whatsapp_msg(f"EMA Alert - {signal_time}", msg['data'])
            write_log('ic_ema_strategy','i',msg['data'])

            ord_id = f"test_{sec_id}"
            ord_status = f"pending"
            if live_order == 'Y':
                ord_id = trade['data']['orderId']
                ord_status = trade['data']['orderStatus']


            trade_write = add_trade_details(strategy = '9-15 EMA',
                                            trade_symbol = sec_name,
                                            security_id = sec_id,
                                            side = side, qty = qty,
                                            order_id = ord_id,
                                            order_status = ord_status,
                                            trail_pts = trail_pts)

        else:
            send_whatsapp_msg(f"EMA Failure Alert - {signal_time}", f"{msg['data']} => {msg['remarks']}")
            write_log('ic_ema_strategy','i',f"{msg['data']} => {msg['remarks']}")
        return msg
    else:
        msg['status'] = 'success'
        msg['data'] = 'Condition Not Matched. Returning...'
        return msg

def main():
    funct_name = 'strategies main'.upper()
    msg = {'status':'failure', 'remarks':'', 'data':''}

    ema_sig = {}
    sig = {}
    ticker = {'YF':'^NSEI', 'ICDH':'NIFTY'}
    strategy_notification = 'N'
    while True:
        try:
            now = datetime.now()
            if now.time() < time(9,0) or now.time() > time(15,10):
                send_whatsapp_msg(f"STRATEGY END - {now.strftime('%Y-%m-%d %H:%M:%S')}", "Strategy Functionality Disabled!!!")
                break
            # if (1==1):
            if (now.time() > time(9,16) and now.minute % 5 == 0 and now.second == 5):
                if strategy_notification == 'N':
                    strategy_notification = 'Y'
                    lo = json.load(open('config.json', 'r'))['LIVE_ORDER']
                    msg = f"Strategy Functionality Enabled with Live Order = {'Yes' if lo=='Y' else 'No'}"
                    send_whatsapp_msg(f"STRATEGY START - {now.strftime('%Y-%m-%d %H:%M:%S')}", msg)
                write_log('ic_ema_strategy','i',f'EMA Calculation START - {now.strftime("%Y-%m-%d %H:%M:%S")}')
                print(f'EMA Calculation Start - {now.strftime("%Y-%m-%d %H:%M:%S")}')
                start_date = (datetime.now() - timedelta(5)).strftime('%Y-%m-%d')
                end_date = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
                st = yf.download(ticker['YF'], start=start_date, end=end_date, interval="5m")
                df = st.copy()
                df['datetime'] = df.index.tz_localize(None)
                df.rename(columns={'Open':'open','High':'high','Low':'low','Adj Close':'close','Volume':'volumne'}, inplace=True)
                df = df[['datetime','open','high','low','close','volumne']].iloc[:-1]
                # df = df.iloc[:-71]
                # df['datetime'].iloc[0].tz_localize(None)
                # print(df)
                # df['datetime'] = df['datetime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
                # # df = df[df['datetime']<='2023-08-18 13:05:00']
                # df['timestamp'] = pd.to_datetime(df['datetime'])
                # df = df[df['timestamp'].dt.time >= pd.Timestamp('09:15:00').time()]

                # ema strategy signal generation
                sig = generate_ema_signal(df)
                if len(ema_sig) == 0:
                    ema_sig = sig
                else:
                     if ema_sig['signal'] != sig['signal']:
                         ema_sig = sig

                if ema_sig['active'] == 'Y':
                    print(ema_sig)

                ohlc_df = df
                ohlc_day = ohlc_df.copy()
                ohlc_day.set_index('datetime', inplace=True)
                ohlc_day = ohlc_day.resample('D').agg({'open': 'first', 'high':'max','low':'min','close':'last'}).iloc[:-1].dropna()
                cdl_pattern = candle_pattern(ohlc_df,ohlc_day)
                num_of_candles = 7
                trend_direction = trend(ohlc_df,num_of_candles)

                if cdl_pattern['pattern'] is not None:
                    text = f"Symbol : {ticker['ICDH']} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : sideways"
                    if trend_direction is not None:
                        text = f"Symbol : {ticker['ICDH']} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : {trend_direction}"
                    send_whatsapp_msg(f'CandleStick Alert - {cdl_pattern["timestamp"]}', text)
                    print(text)

                # last_px = wl[wl['Code']==ticker]['Close'].values[0]
                # last_px = 19272
            if len(ema_sig) == 0:
                continue

            if ema_sig['active'] == 'Y':
                wl = pd.read_csv('WatchList.csv')
                last_px = wl[wl['Code']==ticker['ICDH']]['Close'].values[0]
                # stg_file = 'Strategy.csv'
                # stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
                # last_px = 20137
                # ticker='NIFTY'
                ema_check = check_ema_signal(ticker['ICDH'], ema_sig, last_px)
        except Exception as e:
            err = str(e)
            write_log('ic_ema_strategy','e',err)
            send_whatsapp_msg(f"EMA Failure Alert - {now.strftime('%Y-%m-%d %H:%M:%S')}", err)
            pass

        tm.sleep(1)

    write_log('ic_ema_strategy','i',f'EMA Calculation Process END - {now.strftime("%Y-%m-%d %H:%M:%S")}')
    sys.exit()

# symbol = 'NIFTY 50' last_px = 20103 signal_time=tick_time exp_week=1
def strat_straddle_buy(symbol,last_px,signal_time):
    try:
        funct_name = 'strat_straddle_buy'.upper()
        msg = {'status':'failure', 'remarks':'', 'data':''}

        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        msg['data'] = 'Straddle Strategy'
        if symbol == 'NIFTY 50':
            ticker = 'NIFTY'
            num = int(json.load(open('config.json', 'r'))[ticker]['OPT#'])
            exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
            call_strike = int(json.load(open('config.json', 'r'))[ticker]['CALL_STRIKE'])
            call_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='CE', duration=exp_week)
            call_opt = call_opt[call_opt['STRIKE'] == call_strike]
            # call_opt = call_opt.iloc[num]
            call_sec_id = call_opt['TK']
            call_sec_name = call_opt['CD']

            put_strike = int(json.load(open('config.json', 'r'))[ticker]['PUT_STRIKE'])
            put_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='PE', duration=exp_week)
            put_opt = put_opt[put_opt['STRIKE'] == put_strike]
            # put_opt = put_opt.iloc[num]
            put_sec_id = put_opt['TK']
            put_sec_name = put_opt['CD']

            trade = {}
            sl_pts = 10
            tg_pts = 16.5
            qty = 1
            side = 'buy'

            if live_order == 'Y':
                call_trade = dh_post_exchange_order(ord_type='bo', exchange='FNO',
                                           security_id=call_sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=False, prod_type='')
                put_trade = dh_post_exchange_order(ord_type='bo', exchange='FNO',
                                           security_id=put_sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=False, prod_type='')

                if call_trade['status'].lower() == 'success':
                    msg['data'] = msg['data'] + f"=> CALL Order Placed - {call_sec_name} - [ OrderId - {call_trade['data']['orderId']} | OrderStatus - {call_trade['data']['orderStatus']} ]"
                else:
                    msg['data'] = msg['data'] + f"=> CALL Order Failed - {call_sec_name} - {call_trade['remarks']}"
                if put_trade['status'].lower() == 'success':
                    msg['data'] = msg['data'] + f"=> PUT Order Placed - {put_sec_name} - [ OrderId - {put_trade['data']['orderId']} | OrderStatus - {put_trade['data']['orderStatus']} ]"
                else:
                    msg['data'] = msg['data'] + f"=> {PUT} Order Failed - {put_sec_name} - {put_trade['remarks']}"

                if call_trade['status'].lower() == 'success' and put_trade['status'].lower() == 'success':
                    msg['status'] = 'success'

            else:
                msg['status'] = 'success'
                msg['data'] = msg['data'] + f"=> Live Order not enabled, place manually! - {call_sec_name} - {put_sec_name}"

            send_whatsapp_msg(f"Straddle Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['data'])

    except Exception as e:
        err = str(e)
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err}"
        send_whatsapp_msg(f"Straddle Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])


if __name__ == '__main__':
    print('---------------- Start EMA Strategy ----------------')
    main()

# t1=pd.read_csv('Trades.csv')
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
    ticker = {'YF':'^NSEI', 'ICDH':'NIFTY'}

    start_date = (datetime.now() - timedelta(5)).strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
    st = yf.download(ticker['YF'], start=start_date, end=end_date, interval="5m")



    # st=ic_get_sym_detail(symbol=ticker, interval='5minute',duration=4)

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
                df = st.copy()
                df['datetime'] = df.index.tz_localize(None)
                df.rename(columns={'Open':'open','High':'high','Low':'low','Adj Close':'close','Volume':'volumne'}, inplace=True)
                df = df[['datetime','open','high','low','close','volumne']]

                # df = df[df['datetime'] <= '2023-08-28 14:20:00']
                df = df[df['datetime'] <= '2023-08-28 09:40:00']
                # ema strategy signal generation
                sig = generate_ema_signal(df)
                if sig['active'] == 'Y':
                    print(sig)

                ohlc_df = df
                ohlc_day = ohlc_df.copy()
                ohlc_day.set_index('datetime', inplace=True)
                ohlc_day = ohlc_day.resample('D').agg({'open': 'first', 'high':'max','low':'min','close':'last'}).iloc[:-1].dropna()
                cdl_pattern = candle_pattern(ohlc_df,ohlc_day)
                num_of_candles = 7
                trend_direction = trend(ohlc_df,num_of_candles)

                if cdl_pattern['pattern'] is not None:
                    msg = f"Symbol : {ticker['ICDH']} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : sideways"
                    if trend_direction is not None:
                        msg = f"Symbol : {ticker['ICDH']} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : {trend_direction}"
                    send_whatsapp_msg(f'CandleStick Alert - {cdl_pattern["timestamp"]}', msg)
                    print(msg)

            if len(sig) == 0:
                continue

            if sig['active'] == 'Y':
                last_px = (sig['entry']+1) if sig['signal'] == 'green' else (sig['entry']-1)
                print(f'Checking for active signal - {now}-{last_px}')
                # stg_file = 'Strategy.csv'
                # stg_df = pd.read_csv(stg_file) if os.path.exists(stg_file) else pd.DataFrame()
                check_ema_signal(ticker['ICDH'], sig, last_px)
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