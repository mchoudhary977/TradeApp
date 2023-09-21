from ic_functions import *
from dh_functions import *
from strategies import *
from wa_notifications import *
from log_function import *
import os
from datetime import datetime, time
# import datetime as dt
import time as tm
import pandas as pd
from threading import Thread
# import json
# import sys

# ------------------------- Global Variables -----------------------------
livePrices = pd.DataFrame()
options_df = pd.DataFrame()
strat_trades_df = pd.DataFrame() 
token_list = []
ema_signal = {}

# ------------------------- TradeApp Files --------------------------------
icici_scrips = 'icici.csv'
watchlist_file = 'WatchList.csv'
options_file = 'Options.csv'
oi_pcr_file = 'OIPCR.csv'
trade_file = 'Trades.csv'

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
    sup = ''
    res = ''
    try:
        sup = lev_ser[lev_ser>0].idxmin()
    except Exception as e:
        sup = lev_ser[lev_ser<0].idxmin()
        pass
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

def signal(row):
    ema_xover = row['9-ema'] - row['15-ema']
    # ema_signal = 1 if ema_xover > 0 else (-1 if ema_xover < 0 else 0)
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
    
# generate_ema_signal,args=(tick_symbol, strat_ema_tf))
# def generate_ema_signal(df):
# symbol = 'NIFTY 50' tf = 3
def generate_ema_signal(symbol, tf):
    global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    try:
        ticker = '^NSEI' if symbol == 'NIFTY 50' else symbol
        if now.hour == 9 and now.minute == (15+tf):
            lo = json.load(open('config.json', 'r'))['LIVE_ORDER']
            msg = f"Strategy Functionality Enabled with Live Order = {'Yes' if lo=='Y' else 'No'}"
            send_whatsapp_msg(f"STRATEGY START - {now.strftime('%Y-%m-%d %H:%M:%S')}", msg)
        
        write_log('ic_ema_strategy','i',f'EMA Calculation START - {now.strftime("%Y-%m-%d %H:%M:%S")}')
        start_date = (datetime.now() - timedelta(5)).strftime('%Y-%m-%d')
        end_date = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
        df = yf.download(ticker, start=start_date, end=end_date, interval="1m")
        # df = st.copy()
        sample_tf = f"{tf}T" if tf > 0 else "5T"
        df = df.resample(sample_tf).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Adj Close': 'last', 'Volume':'last'}).dropna()
        
        df['datetime'] = df.index.tz_localize(None)
        df.rename(columns={'Open':'open','High':'high','Low':'low','Adj Close':'close','Volume':'volumne'}, inplace=True)
        df = df[['datetime','open','high','low','close','volumne']].iloc[:-1]
        df['15-ema'] = round(df['close'].ewm(span=15, adjust=False).mean(),2)
        df['9-ema'] = round(df['close'].ewm(span=9, adjust=False).mean(),2)
        df['ema_xover'] = round(df['9-ema'] - df['15-ema'],2)
        df['rsi'] = round(rsi(df,14),2)
        
        ema_df = df.copy()
        ema_df = ema_df[ema_df['datetime'] >= pd.Timestamp(now.strftime('%Y-%m-%d'))]
        # ema_df = ema_df[ema_df['datetime'] <= pd.Timestamp('15:00:00')]
        ema_df['signal'] = ema_df.apply(signal, axis=1)
        # ema_df['signal'].fillna(method='ffill', inplace=True)
        ema_df['signal'].fillna('neutral', inplace=True)
        # ema_df['signal'] = ema_df['signal'].where(ema_df['signal'] != ema_df['signal'].shift())
        ema_df['entry'] = round(ema_df.apply(update_entry, axis=1),2)
        ema_df['active'] = 'N'

        sig = ema_df.iloc[-1].copy()
        sig['stoploss'] = 0
        sig['step'] = 0
        sig['target'] = 0
        sig['active'] = 'N'

        # sig = last_row.copy()
        for index,row in ema_df.iloc[::-1].iterrows():
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
            if sig['signal'] == 'neutral':
                sig['active'] = 'Y'
                break
        if sig['active'] == 'Y':
            if len(ema_signal) == 0:
                ema_signal = sig
            else:
                if ema_signal['signal'] != sig['signal']:
                    ema_signal = sig
        
        ohlc_df = df
        ohlc_day = ohlc_df.copy()
        ohlc_day.set_index('datetime', inplace=True)
        ohlc_day = ohlc_day.resample('D').agg({'open': 'first', 'high':'max','low':'min','close':'last'}).iloc[:-1].dropna()
        cdl_pattern = candle_pattern(ohlc_df,ohlc_day)
        num_of_candles = 7
        trend_direction = trend(ohlc_df,num_of_candles)

        if cdl_pattern['pattern'] is not None:
            text = f"Symbol : {symbol} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : sideways"
            if trend_direction is not None:
                text = f"Symbol : {symbol} -> Pattern : {cdl_pattern['pattern']} -> Significance : {cdl_pattern['significance']} -> Last {num_of_candles} Candles Trend : {trend_direction}"
            send_whatsapp_msg(f'CandleStick Alert - {cdl_pattern["timestamp"]}', text)
            print(text)

    except Exception as e:
        err = str(e)
        write_log('ic_ema_strategy','e',err)
        send_whatsapp_msg(f"EMA Failure Alert - {now.strftime('%Y-%m-%d %H:%M:%S')}", err)
        pass

# ticks = {'ltt':'Mon Sep 20 15:20:10 2023','symbol':'4.1!NIFTY 50', 'last':19967}
# tick_time = datetime.strptime('Mon Sep 20 15:22:10 2023'[4:25], "%b %d %H:%M:%S %Y")
# last_px = 19897.35
# ema_signal['active'] = 'Y'
# check_ema_signal,args=(tick_symbol, tick_px, tick_time)) 
def check_ema_signal(symbol, last_px, tick_time):
    funct_name = 'check_ema_signal'.upper()
    msg = {'status':'failure', 'remarks':'', 'data':''}
    
    global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    
    ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
    time_diff = (tick_time - ema_signal['datetime']).seconds
    if ema_signal['signal'] == 'neutral' or ema_signal['active'] != 'Y' or time_diff > ema_tf*60:
        print('Returning as condition not matched')
        # return    
    ticker = 'NIFTY' if symbol == 'NIFTY 50' else symbol
    live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
    signal_time = ema_signal['datetime']
    signal = ema_signal['signal']
    entry_px = ema_signal['entry']
    stop_loss = ema_signal['stoploss']
    target = ema_signal['target']
    opt_type = 'CE' if signal == 'green' else ('PE' if signal =='red' else None)
    side = 'BUY' if signal == 'green' else ('SELL' if signal =='red' else None)
    msg['data'] = f"{side} -> {symbol} -> {stop_loss} - {entry_px} - {target} "

    if ema_signal['active']=='Y' and ((signal == 'green' and last_px > entry_px and ema_signal['entry'] > 0) or (signal == 'red' and last_px < entry_px and ema_signal['entry'] > 0)):
        ema_signal['active']='N'
        # count changed to 4 due to balance constraints
        exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
        opt = ic_option_chain(ticker, underlying_price=last_px, option_type=opt_type, duration=exp_week)
        num = int(json.load(open('config.json', 'r'))[symbol]['OPT#'])
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
    
            allow_order_count = int(json.load(open('config.json', 'r'))['DAILY_ORDER_COUNT'])
    
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
            tg_pts = 10
            sl_pts = 5
            qty = 1
            side = 'buy'
            if live_order == 'Y':
                trade = dh_post_exchange_order(ord_type='bo', exchange='FNO',
                                            security_id=sec_id, side=side,
                                            qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                            tg_pts=tg_pts, sl_pts=sl_pts,
                                            amo=False, prod_type='')
                # trade = dh_post_exchange_order(ord_type='mkt', exchange='FNO',
                #                             security_id=sec_id, side=side,
                #                             qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                #                             tg_pts=0, sl_pts=0,
                #                             amo=False, prod_type='')
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
                
            signal_row = {'Strategy':'EMA Crossover',
                          'Symbol': symbol,
                          'Signal': ema_signal['signal'],
                          'Entry': ema_signal['entry'],
                          'SymSL': round(ema_signal['low'],2) if ema_signal['signal'] == 'green' else round(ema_signal['high'],2),
                          'Qty': 1,
                          'DervName': sec_name,
                          'DervID': sec_id,
                          'DervPx': 0.0,
                          'EntryID': trade['data']['orderId'],
                          'EntryPx': 0.0,
                          'DervSL': 0.0,
                          'ExitID': ' ',
                          'ExitPx': 0.0,
                          'PnL': 0.0,
                          'CreationTime': ema_signal['datetime'], 
                          'ExpirationTime': ema_signal['datetime'] + timedelta(minutes=ema_tf),
                          'Active': 'Y',
                          'Status': ' '
                          }
            
            strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)
        else:
            send_whatsapp_msg(f"EMA Failure Alert - {signal_time}", f"{msg['data']} => {msg['remarks']}")
            write_log('ic_ema_strategy','i',f"{msg['data']} => {msg['remarks']}")
        return msg
    else:
        msg['status'] = 'success'
        msg['data'] = 'Condition Not Matched. Returning...'
        return msg

# ticks = {'ltt':'Mon Sep 21 09:17:00 2023','symbol':'4.1!NIFTY 50', 'last': 19825.35}
# symbol = 'NIFTY 50' last_px = 19825.35 signal_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
def strat_straddle_buy(symbol,last_px,signal_time):
    global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    try:
        funct_name = 'strat_straddle_buy'.upper()
        msg = {'status':'failure', 'remarks':'', 'data':''}

        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        msg['data'] = 'Straddle Strategy'
        if symbol == 'NIFTY 50':
            ticker = 'NIFTY'
            num = int(json.load(open('config.json', 'r'))[symbol]['OPT#'])
            exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
            call_strike = int(json.load(open('config.json', 'r'))[symbol]['CALL_STRIKE'])
            call_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='CE', duration=exp_week)
            call_opt = call_opt[call_opt['STRIKE'] == call_strike]
            # call_opt = call_opt.iloc[num]
            call_sec_id = call_opt['TK']
            call_sec_name = call_opt['CD']

            put_strike = int(json.load(open('config.json', 'r'))[symbol]['PUT_STRIKE'])
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
                ord_type = 'bo'
                call_trade = dh_post_exchange_order(ord_type=ord_type, exchange='FNO',
                                           security_id=call_sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=False, prod_type='')
                put_trade = dh_post_exchange_order(ord_type=ord_type, exchange='FNO',
                                           security_id=put_sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=False, prod_type='')

                if call_trade['status'].lower() == 'success':
                    signal_row = {'Strategy':'Straddle CALL',
                                  'Symbol': symbol,
                                  'Signal': 'green',
                                  'Entry': last_px,
                                  'SymSL': 0.0,
                                  'Qty': 1,
                                  'DervName': call_sec_name.values[0],
                                  'DervID': call_sec_id.values[0],
                                  'DervPx': 0.0,
                                  'EntryID': call_trade['data']['orderId'],
                                  'EntryPx': 0.0,
                                  'DervSL': 0.0,
                                  'ExitID': ' ',
                                  'ExitPx': 0.0,
                                  'PnL': 0.0,
                                  'CreationTime': now.strftime('%Y-%m-%d %H:%M:%S'), 
                                  'ExpirationTime': 'NA',
                                  'Active': 'Y',
                                  'Status': ' '
                                  }
                    # print(signal_row)
                    
                    strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)
                    msg['data'] = msg['data'] + f"=> CALL Order Placed - {call_sec_name} - [ OrderId - {call_trade['data']['orderId']} | OrderStatus - {call_trade['data']['orderStatus']} ] "
                else:
                    msg['data'] = msg['data'] + f"=> CALL Order Failed - {call_sec_name} - {call_trade['remarks']} "
                if put_trade['status'].lower() == 'success':
                    signal_row = {'Strategy':'Straddle PUT',
                                  'Symbol': symbol,
                                  'Signal': 'green',
                                  'Entry': last_px,
                                  'SymSL': 0.0,
                                  'Qty': 1,
                                  'DervName': put_sec_name.values[0],
                                  'DervID': put_sec_id.values[0],
                                  'DervPx': 0.0,
                                  'EntryID': put_trade['data']['orderId'],
                                  'EntryPx': 0.0,
                                  'DervSL': 0.0,
                                  'ExitID': ' ',
                                  'ExitPx': 0.0,
                                  'PnL': 0.0,
                                  'CreationTime': now.strftime('%Y-%m-%d %H:%M:%S'), 
                                  'ExpirationTime': 'NA',
                                  'Active': 'Y',
                                  'Status': ' '
                                  }    
                    # print(signal_row)
                    strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)
                    msg['data'] = msg['data'] + f"=> PUT Order Placed - {put_sec_name} - [ OrderId - {put_trade['data']['orderId']} | OrderStatus - {put_trade['data']['orderStatus']} ] "
                else:
                    msg['data'] = msg['data'] + f"=> {PUT} Order Failed - {put_sec_name} - {put_trade['remarks']} "

                if call_trade['status'].lower() == 'success' and put_trade['status'].lower() == 'success':
                    msg['status'] = 'success'

            else:
                msg['status'] = 'success'
                msg['data'] = msg['data'] + f"=> Live Order not enabled, place manually! - {call_sec_name} - {put_sec_name} "

            send_whatsapp_msg(f"Straddle Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['data'])

    except Exception as e:
        err = str(e)
        # print(err)
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err} "
        send_whatsapp_msg(f"Straddle Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])
    
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

# ticks = {'ltt':'Mon Sep 21 09:17:00 2023','symbol':'4.1!NIFTY 50', 'last': 19825.35}
# On Ticks function
def on_ticks(ticks):
    global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    # print(f'{ticks["symbol"]}-{ticks["last"]}')

    tick_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    tick_symbol = ticks['symbol'][4:]
    tick_px = ticks['last']

    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'CandleTime'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Close'] = ticks['last']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'High'] = ticks['high']
        livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'Low'] = ticks['low']
        
        if tick_time.time() > (9,15) and tick_time.time() < (14,5):
            livePrices.loc[livePrices['Token'] == ticks['symbol'][4:], 'PrevClose'] = ticks['close']
    
    else:
        new_row = {'CandleTime': ticks['ltt'], 'Token': ticks['symbol'][4:], 'Close': ticks['last'],
                   'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low']}
        livePrices=pd.DataFrame(new_row, index = [0])
        
    if tick_symbol in ['NIFTY 50','NIFTY BANK','NIFTY FIN SERVICE']:
        if tick_symbol == 'NIFTY 50':
            # Straddle buy strategy for Nifty to initiate at 9:17 AM
            if tick_time.hour == 9 and tick_time.minute == 17 and tick_time.second == 0:
                # print('Straddle Initiating')
                straddle_thread = Thread(target=strat_straddle_buy,args=(tick_symbol,tick_px,tick_time))
                straddle_thread.start()  
            
            strat_ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
            if tick_time.minute % strat_ema_tf == 0 and tick_time.second == 5:
                print(f"EMA Strategy Execution - Timeframe - {strat_ema_tf}")
                ema_strat_thread = Thread(target=generate_ema_signal,args=(tick_symbol, strat_ema_tf)) 
                ema_strat_thread.start()
            
            monitor_ema_thread = Thread(target=check_ema_signal,args=(tick_symbol, tick_px, tick_time)) 
            monitor_ema_thread.start()
    
    if len(strat_trades_df) > 0:
        strat_trades_df.low[strat_trades_df['DervID'] == tick_symbol, 'DervPx'] = tick_px

    # if len(options_df) > 0:
    #     options_df.loc[options_df['SecID'] == ticks['symbol'][4:], 'Timestamp'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    #     options_df.loc[options_df['SecID'] == ticks['symbol'][4:], 'LivePx'] = ticks['last']
        # options_df.loc[options_df['SecurityID'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        # options_df.loc[options_df['SecurityID'] == ticks['symbol'][4:], 'High'] = ticks['high']
        # options_df.loc[options_df['SecurityID'] == ticks['symbol'][4:], 'Low'] = ticks['low']


def ic_subscribe_traded_symbols():
    global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    
    if len(strat_trades_df[strat_trades_df['Active'] == 'Y']) == 0:
        return
    
    opt_token = list(strat_trades_df[strat_trades_df['Active']=='Y']['DervID'])    
    opt_to_subscribe = [f"4.1!{item}" for item in opt_token if item not in token_list]
    
    if len(opt_to_subscribe) > 0:
        ic_subscribeFeed(opt_to_subscribe)
        token_list = token_list + opt_to_subscribe
        return {'status':'success','remarks':'','data':'traded symbols subscribed'}
    return {'status':'success','remarks':'','data':'no symbols to subscribe'}
    


def main():
    global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    
    session_id = ic_autologon()
    write_log('ic_watchlist','i',f"ICICI Session ID - {session_id}")
   
    try:
        if os.path.exists(watchlist_file) == False:
            ic_update_watchlist(mode='C',num=0)
        livePrices = pd.read_csv(watchlist_file)
    except pd.errors.EmptyDataError:
        ic_update_watchlist(mode='C',num=0)
    livePrices = pd.read_csv(watchlist_file)
    
    if len(pd.read_csv(watchlist_file)) > 0:
        for index,row in livePrices.iterrows():
            # if now.date() > datetime.strptime(row['CandleTime'], '%Y-%m-%d %H:%M:%S').date():
            #     livePrices.at[index, 'PrevClose'] = row['Close']
            token_list.append(f"4.1!{row['Token']}")

    while True:
        now = datetime.now()
        try:
            if now.time() < time(9,0) or now.time() > time(15,40):
                break
            if (now.time() >= time(9,14) and now.time() < time(15,35,0)):
                if subscription_flag=='N':
                    if os.path.exists(watchlist_file):
                        # print('Subscribed')
                        icici.ws_connect()
                        icici.on_ticks = on_ticks
                        # wl_df = pd.read_csv(watchlist_file)
                        # livePrices = wl_df
                        # tokens=ic_tokenLookup(list(wl_df['Code'].values))
                        ic_subscribeFeed(token_list)
                        # ic_subscribeFeed(tokens['data'])
                        subscription_flag = 'Y'
                        send_whatsapp_msg('Feed Alert','Market Live Feed Started!')
                    else:
                        ic_get_watchlist(mode='C')
                else:
                    livePrices.to_csv(watchlist_file,index=False)
                    # options_df.to_csv(trade_file,index=False)
                    strat_trades_df.to_csv(trade_file,index=False)
                    if len(strat_trades_df[strat_trades_df['Active'] == 'Y']) > 0:
                        ic_subscribe_traded_symbols()

            if (now.time() >= time(15,35) and subscription_flag=='Y'):
                ic_unsubscribeFeed(token_list)
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
    
# Main Function Start
if __name__ == '__main__':
    main()