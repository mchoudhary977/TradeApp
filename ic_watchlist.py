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

# ------------------------- Global Variables -----------------------------
livePrices = pd.DataFrame()
options_df = pd.DataFrame()
orders_df = pd.DataFrame()
strat_trades_df = pd.DataFrame()
token_list = []
ema_signal = {}
round_strat_flag = 'Y'
strategies = {}

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
    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    try:
        ticker = '^NSEI' if symbol == 'NIFTY 50' else symbol
        if now.hour == 9 and now.minute == (15+tf):
            lo = json.load(open('config.json', 'r'))['LIVE_ORDER']
            msg = f"Strategy Functionality Enabled with Live Order = {'Yes' if lo=='Y' else 'No'}"
            send_whatsapp_msg(f"STRATEGY START - {now.strftime('%Y-%m-%d %H:%M:%S')}", msg)

        printLog('i',f'EMA Calculation START - {now.strftime("%Y-%m-%d %H:%M:%S")}')
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
            printLog('i', text)

    except Exception as e:
        err = str(e)
        printLog('e',err)
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

    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()

    ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
    time_diff = (tick_time - ema_signal['datetime']).seconds
    if ema_signal['signal'] == 'neutral' or ema_signal['active'] != 'Y' or time_diff > ema_tf*60:
        printLog('i', 'Returning as condition not matched')
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
        sec_id = opt['TK'].values[0]
        sec_name = opt['CD'].values[0]
        lot_size = opt['LS'].values[0]
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
                printLog('i',f"ic_ema_strategy - {msg['remarks']}")
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
            printLog('i',f"ic_ema_strategy - {msg['data']}")


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
                          'Qty': (qty * lot_size),
                          'RemQty': (qty * lot_size),
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
            printLog('i',f"ic_ema_strategy - {msg['data']} => {msg['remarks']}")
        return msg
    else:
        msg['status'] = 'success'
        msg['data'] = 'Condition Not Matched. Returning...'
        return msg

def get_entry_option(ticker, last_px, option_type):
    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now() # - timedelta(1)
    exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
    # custom_strike = json.load(open('config.json', 'r'))['CUST_STRIKE']
    opt = ic_option_chain(ticker, underlying_price=last_px, option_type=option_type, duration=exp_week)
    atm_strike = opt[opt['ATM']=='Y']['STRIKE'].values[0]
    lot_size = opt['LS'].values[0]
    strike_step = 50 if (ticker == 'NIFTY' or ticker == 'NIFFIN') else 100

    sel_strike = atm_strike
    if option_type == 'CE':
        sel_strike = atm_strike if (last_px - atm_strike) > 0 else (atm_strike-strike_step)
    elif option_type == 'PE':
        sel_strike = atm_strike if (atm_strike - last_px) > 0 else (atm_strike+strike_step)

    return opt[opt['STRIKE']==sel_strike]
    # return opt[opt['STRIKE']==atm_strike]


def place_strategy_order(strategy='default', symbol='NIFTY 50', last_px=19700,
                         signal_time = datetime.now(), ord_type='mkt',
                         sec_id=1234, sec_name='ABCD', lot_size = 50,
                         amo=False):
    global icici, livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal, round_strat_flag
    now = datetime.now()
    funct_name = 'place_strategy_order'.upper()
    msg = {'status':'failure', 'remarks':'', 'data':''}
    subscribe_token = []
    try:
        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        sl_pts = int(json.load(open('config.json', 'r'))['SL_POINTS'])  # 10
        tg_pts = int(json.load(open('config.json', 'r'))['TG_POINTS']) #16.5
        qty = 1
        side = 'buy'
        signal_row = {'Strategy':strategy,
                      'Symbol': symbol,
                      'Signal': side,
                      'Entry': last_px,
                      'SymSL': 0.0,
                      'Qty': qty*lot_size,
                      'RemQty': (qty * lot_size),
                      'DervName': sec_name,
                      'DervID': sec_id,
                      'DervPx': 0.0,
                      'EntryID': 'test',
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
        if live_order == 'N':
            signal_row['ExitID'] = 'test'
            printLog('i',f"{funct_name} - Place Order Manually!!!")
            msg['data'] = f"{symbol} - {last_px} | {sec_id} => {sec_name} => Live Order Disabled. Place Order Manually!!!"
            send_whatsapp_msg(f"{strategy} Order Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['data'])

        elif live_order == 'Y':
            trade = dh_post_exchange_order(ord_type=ord_type, exchange='FNO',
                                           security_id=sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=amo, prod_type='')
            printLog('i',trade)
            if trade['status'].lower() == 'success':
                entry_id = trade['data']['orderId']
                msg['data'] = f"{symbol} - {last_px} | {sec_id} => {sec_name} => OrderID - {trade['data']['orderId']}"
                send_whatsapp_msg(f"{strategy} Order Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['data'])

                tm.sleep(5)
                # entry_id = 52230927543251
                entry_ord_det = dh_get_order_id(order_id = entry_id)
                if entry_ord_det['status'] == 'success':
                    details = entry_ord_det['data']
                    # if details['orderStatus'].lower() == 'pending':
                    if details['orderStatus'].lower() == 'traded':
                        signal_row['EntryID'] = entry_id
                        signal_row['EntryPx'] = details['price']

                        if ord_type == 'bo':
                            try:
                                orders = dh_get_orders()['data']
                                exit_order = orders[(orders['algoId'] == str(entry_id)) & (orders['legName'] == 'STOP_LOSS_LEG')]
                                signal_row['ExitID'] = exit_order['orderId'].values[0]
                            except Exception as e:
                                printLog('e',f"{funct_name} - {str(e)}")
                                pass
                            strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)
                            return

                        # Place StopLoss Order
                        entry_px = details['price']
                        # entry_px = 70
                        sl_price = entry_px - sl_pts
                        trigger_price = entry_px - (sl_pts - 0.05)
                        # Place Limit Order
                        sl_side = 'sell' if side == 'buy' else 'buy'
                        sl_trade = dh_post_exchange_order(ord_type='sl', exchange='FNO',
                                                       security_id=sec_id, side=sl_side,
                                                       qty=qty, entry_px=sl_price, sl_px=sl_price, trigger_px=trigger_price,
                                                       tg_pts=tg_pts, sl_pts=sl_pts,
                                                       amo=amo, prod_type='')
                        printLog('i',sl_trade)

                # sec_id=85869  sl_side = 'sell' qty=1 sl_price=40 trigger_price=40.05 tg_pts=17 sl_pts=10
                        # lmt_price = details['price'] + (tg_pts+0.5)
                        # lmt_trigger_price = lmt_price + 0.05
                        # lmt_trade = dh_post_exchange_order(ord_type='lmt', exchange='FNO',
                        #                                security_id=sec_id, side=sl_side,
                        #                                qty=qty, entry_px=lmt_price, sl_px=0, trigger_px=lmt_trigger_price,
                        #                                tg_pts=tg_pts, sl_pts=sl_pts,
                        #                                amo=amo, prod_type='')
                        if sl_trade['status'] == 'success':
                            sl_details = sl_trade['data']
                            signal_row['ExitID'] = sl_details['orderId']
                            msg['data'] = f"{symbol} - {last_px} | {sec_id} => {sec_name} => StopLoss OrderID - {sl_trade['data']['orderId']}"
                            send_whatsapp_msg(f"{strategy} Order Alert [SL] - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['data'])

        strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)

    except Exception as e:
        err = str(e)
        printLog('e', f"{funct_name} - {err}")
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err} "
        send_whatsapp_msg(f"{funct_name} Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])
        pass


# ticks = {'ltt':'Mon Oct 03 09:16:20 2023','symbol':'4.1!NIFTY 50','last':19537.25}
# symbol = ticks['symbol'][4:] last_px = ticks['last'] 
# signal_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
def strat_straddle_buy(symbol,last_px,signal_time):
    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal, strategies
    now = datetime.now()
    funct_name = 'strat_straddle_buy'.upper()
    strategy = 'Straddle'
    msg = {'status':'failure', 'remarks':'', 'data':''}
    # exit_strategy = 'N'
    try:
        ticker = 'NIFTY' if symbol == 'NIFTY 50' else symbol
        if strategy not in strategies:
            strategies[strategy] = {}
            
        if symbol not in strategies[strategy]:
            strategies[strategy][symbol] = {'price':0.0, 'entry':0.0, 
                                            'signal':'green', 'ordcnt': 0, 
                                            'call':'N', 'put':'N', 'exit':'N'}
        
        if strategies[strategy][symbol]['exit'] == 'Y':
            return 
        
        if strategies[strategy][symbol]['price'] == 0:
            strategies[strategy][symbol]['price'] = last_px
            
        if signal_time.time() == time(9,16,20):
            live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
            if strategies[strategy][symbol]['ordcnt'] == 0:
                printLog('i', f"{funct_name} - Initiating {strategy} Strategy...")
                send_whatsapp_msg(f"{strategy} Strategy", f"{now.strftime('%Y-%m-%d %H:%M:%S')} - Initiating {strategy} for {symbol}")
                
                if last_px > strategies[strategy][symbol]['price']:
                    call_opt = get_entry_option(ticker, last_px, option_type='CE')
                    call_sec_id = call_opt['TK'].values[0]
                    call_sec_name = call_opt['CD'].values[0]
                    lot_size = call_opt['LS'].values[0]
                    ord_type = json.load(open('config.json', 'r'))['ORDER_TYPE']
                    amo = False
                    call_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                             now, ord_type, call_sec_id, call_sec_name, lot_size,
                                             amo))
                    call_thread.start()
                    strategies[strategy][symbol]['signal'] = 'green'
                    strategies[strategy][symbol]['call'] = 'Y'
                    strategies[strategy][symbol]['entry'] = last_px
                    strategies[strategy][symbol]['ordcnt'] = 1
                    printLog('i',f"{strategies[strategy][symbol]['ordcnt']} - {last_px} - {call_sec_id} - {call_sec_name} - {straddle}")
                
                elif last_px < strategies[strategy][symbol]['price']:
                    put_opt = get_entry_option(ticker, last_px, option_type='PE')
                    put_sec_id = put_opt['TK'].values[0]
                    put_sec_name = put_opt['CD'].values[0]
                    lot_size = put_opt['LS'].values[0]
                    ord_type = json.load(open('config.json', 'r'))['ORDER_TYPE']
                    amo = False
                    put_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                             now, ord_type, put_sec_id, put_sec_name, lot_size,
                                             amo))
                    put_thread.start()
                    strategies[strategy][symbol]['signal'] = 'red'
                    strategies[strategy][symbol]['put'] = 'Y'
                    strategies[strategy][symbol]['entry'] = last_px
                    strategies[strategy][symbol]['ordcnt'] = 1
                    strategies[strategy][symbol]('i',f"{strategies[strategy][symbol]['ordcnt']} - {last_px} - {put_sec_id} - {put_sec_name} - {straddle}")
            
            # Exit trade when expected profit is reached
            if strategies[strategy][symbol]['exit'] == 'N' and strategies[strategy][symbol]['ordcnt'] >= 1:
                if strat_trades_df is not None and len(strat_trades_df) > 0:
                    check_df = strat_trades_df
                    check_df = check_df.to_frame().T if type(check_df) == pd.core.series.Series else check_df
                    check_df = check_df[(check_df['Strategy'] == strategy) & (check_df['Active'] == 'Y')]
                    check_df = check_df.to_frame().T if type(check_df) == pd.core.series.Series else check_df

                    if len(check_df) > 0:
                        checkqty = check_df['Qty'].values[0]
                        running_profit = (check_df['DervPx'].sum() - check_df['EntryPx'].sum()) * checkqty
                        expected_profit = (7 * checkqty)
                        if expected_profit > 0 and running_profit > expected_profit:
                            strategies[strategy][symbol]['exit'] = 'Y'
                            for index, row in check_df.iterrows():
                                order_id = row['ExitID']
                                price = row['DervPx']
                                trigger_px = price + 0.05
                                quantity = row['Qty']
                                if price > 0.0:
                                    printLog('i', f"{funct_name} - StopLoss Order Modified as Req Profit Reached")
                                    mod_st = dh_modify_order(order_id, price, quantity, ord_type = 'sl', trigger_px = trigger_px)
                                    strat_trades_df.loc[(strat_trades_df['Strategy'] == 'Straddle') & (strat_trades_df['Active'] == 'Y'), 'Active'] = 'N'

            # Place second leg of trade if price changes direction
            if strategies[strategy][symbol]['exit'] == 'N' and strategies[strategy][symbol]['ordcnt'] == 1:
                if strategies[strategy][symbol]['call'] == 'Y':
                    if last_px < (strategies[strategy][symbol]['entry'] - 5):
                        put_opt = get_entry_option(ticker, last_px, option_type='PE')
                        put_sec_id = put_opt['TK'].values[0]
                        put_sec_name = put_opt['CD'].values[0]
                        lot_size = put_opt['LS'].values[0]
                        ord_type = json.load(open('config.json', 'r'))['ORDER_TYPE']
                        amo = False
                        put_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                                 now, ord_type, put_sec_id, put_sec_name, lot_size,
                                                 amo))
                        put_thread.start()
                        strategies[strategy][symbol]['ordcnt'] = 2
                        strategies[strategy][symbol]['put'] = 'Y'
                        printLog('i',f"{strategies[strategy][symbol]['ordcnt']} - {last_px} - {put_sec_id} - {put_sec_name} - {strategies[strategy][symbol]}")

                if strategies[strategy][symbol]['put'] == 'Y':
                    if last_px > (strategies[strategy][symbol]['entry'] + 5):
                        call_opt = get_entry_option(ticker, last_px, option_type='CE')
                        call_sec_id = call_opt['TK'].values[0]
                        call_sec_name = call_opt['CD'].values[0]
                        lot_size = call_opt['LS'].values[0]
                        ord_type = json.load(open('config.json', 'r'))['ORDER_TYPE']
                        amo = False
                        call_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                                 now, ord_type, call_sec_id, call_sec_name, lot_size,
                                                 amo))
                        call_thread.start()
                        strategies[strategy][symbol]['ordcnt'] = 2
                        strategies[strategy][symbol]['call'] = 'Y'
                        printLog('i',f"{strategies[strategy][symbol]['ordcnt']} - {last_px} - {call_sec_id} - {call_sec_name} - {strategies[strategy][symbol]}")
           
        strategies[strategy][symbol]['price'] = last_px
    
    except Exception as e:
        err = str(e)
        printLog('e',f"{funct_name} - {err}")
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err} "
        send_whatsapp_msg(f"{funct_name} Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])
        pass


# ticks = {'ltt':'Sat Sep 25 09:17:00 2023','symbol':'4.1!NIFTY 50', 'last': 19825.35}
# symbol = 'NIFTY 50' last_px = 19699.25 signal_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
def strat_round_entries(symbol,last_px,signal_time):
    global icici, livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    funct_name = 'strat_round_entries'.upper()
    strategy = 'Round'
    msg = {'status':'failure', 'remarks':'', 'data':''}
    exit_strategy = 'N'
    round_st = {'price':0.0, 'entry':0.0, 'timestamp':now}

    try:
        while True:
            now = datetime.now()
            round_strat_flag = json.load(open('config.json', 'r'))['ROUND_STRAT_FLAG']

            if round_strat_flag == 'Y':

                if now.time() >= time(9,16,20) and now.time() < time(15,30):
                    ticker = 'NIFTY' if symbol == 'NIFTY 50' else symbol
                    last_px = livePrices[livePrices['SymbolName'] == symbol]['Close'].values[0]
                    round_st['price'] = last_px
                    if symbol == 'NIFTY 50':
                        strike_step = 50 if (ticker == 'NIFTY' or ticker == 'NIFFIN') else 100
                        atm_strike = int(round(last_px/strike_step,0))*strike_step

                        if last_px > (atm_strike-1) and last_px < (atm_strike+1):
                            if len(strat_trades_df) == 0 or (len(strat_trades_df) > 0 and len(strat_trades_df[(strat_trades_df['Strategy'] == strategy) & (strat_trades_df['Active'] == 'Y')]) == 0):
                                if round_st['entry'] != atm_strike or ((now - round_st['timestamp']).seconds/60) > 30:
                                    round_st['entry'] = atm_strike
                                    exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
                                    call_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='CE', duration=exp_week)
                                    call_opt = call_opt[call_opt['STRIKE']==atm_strike]
                                    call_sec_id = call_opt['TK'].values[0]
                                    call_sec_name = call_opt['CD'].values[0]

                                    lot_size = call_opt['LS'].values[0]

                                    put_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='PE', duration=exp_week)
                                    put_opt = put_opt[put_opt['STRIKE']==atm_strike]
                                    put_sec_id = put_opt['TK'].values[0]
                                    put_sec_name = put_opt['CD'].values[0]

                                    ord_type = json.load(open('config.json', 'r'))['ORDER_TYPE']
                                    amo = False
                                    call_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                                             now, ord_type, call_sec_id, call_sec_name, lot_size,
                                                             amo))

                                    put_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                                             now, ord_type, put_sec_id, put_sec_name, lot_size,
                                                             amo))

                                    call_thread.start()
                                    put_thread.start()
                else:
                    break
            else:
                tm.sleep(10)
    except Exception as e:
        err = str(e)
        printLog('e',f"{funct_name} - {err}")
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err} "
        send_whatsapp_msg(f"{funct_name} Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])
        pass


# sleep_tm = 5
def update_trade_info(sleep_tm):
    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    funct_name = 'update_trade_info'.upper()
    try:
        now = datetime.now()
        tm.sleep(sleep_tm)
        od = dh_get_orders()
        if od['status'] == 'success':
            orders_df = dh_get_orders()['data']
        # od = orders_df[orders_df['orderStatus']=='TRADED']
        od = orders_df
        if len(od) > 0:

            for index,row in strat_trades_df.iterrows():
                if row['ExitID'] == ' ':
                    strat_trades_df.loc[index,'ExitID'] = od[(od['orderType'] == 'STOP_LOSS') & (od['orderStatus'] == 'PENDING') & (od['securityId']==str(row['DervID']))]['orderId'].values[0]
                    # strat_trades_df.loc[index,'ExitID'] = od[(od['securityId']==str(row['DervID'])) & (od['legName']=='STOP_LOSS_LEG') & (od['orderStatus']=='PENDING')]['orderId'].values[0]
                    # print(type(row['DervID']))
                if len(od[od['orderId'] == str(row['EntryID'])]) > 0:
                    strat_trades_df.loc[index,'EntryPx'] = od[(od['orderId'] == str(row['EntryID'])) & (od['orderStatus'] == 'TRADED')]['price'].values[0]
                    print(od[od['orderId'] == str(row['EntryID'])]['price'].values[0])
                if len(od[od['orderId'] == str(row['ExitID'])]) > 0:
                    strat_trades_df.loc[index,'ExitPx'] = od[(od['orderId'] == str(row['ExitID'])) & (od['orderStatus'] == 'TRADED')]['price'].values[0]
                    print(od[od['orderId'] == str(row['ExitID'])]['price'].values[0])

            return 'success'
    except Exception as e:
        err = str(e)
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - {funct_name} - {err}")
        pass

def check_strategy(tick_symbol,tick_px,tick_time):
    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal, round_strat_flag
    now = datetime.now()
    funct_name = 'check_strategy'.upper()
    try:
        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        if len(strat_trades_df) > 0:
            if (len(strat_trades_df[strat_trades_df['EntryPx'] <= 0][strat_trades_df['Active']=='Y']) > 0) or (len(strat_trades_df[strat_trades_df['Active']=='Y']) > 0 and tick_time.minute % 5 == 0 and tick_time.second == 10):
                update_trade_info(5)

            straddle_df = strat_trades_df[(strat_trades_df['Strategy']=='Straddle') & (strat_trades_df['Active']=='Y')]
            upd_orders = []

            if live_order == 'Y' and round(straddle_df['PnL'].sum(),2) > 325:
                for index,row in straddle_df.iterrows():
                    sl_order = row['ExitID']
                    sl_price = row['DervPx']
                    qty = row['Qty']/50
                    if len(orders_df[orders_df['orderId'] == sl_order]) > 0:
                        sl = dh_modify_order(order_id=sl_order,price=sl_price,quantity=qty,leg_name='STOP_LOSS_LEG')
                        if sl['status'] == 'success':
                            upd_orders.append(sl_order)
                        # print(sl)
                send_whatsapp_msg("Straddle SL Order Modified",f"Orders Update - {upd_orders}")

        if live_order == 'Y' and tick_time.minute % 5 == 0 and tick_time.second ==5:
            pos = dh_get_positions()['data']
            if pos is not None and len(pos) > 0:
                if len(pos[pos['positionType'] != 'CLOSED']) == 0:
                    round_strat_flag = 'N'

    except Exception as e:
        err = str(e)
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - {funct_name} - {err}")
        pass

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

# icici.on_ticks = on_ticks
# ticks = {'ltt':'Mon Oct 03 09:16:20 2023','symbol':'4.1!NIFTY 50','last':19544.25}
# On Ticks function
def on_ticks(ticks):
    global livePrices
    global strat_trades_df

    tick_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    tick_symbol = ticks['symbol'][4:]
    tick_px = ticks['last']
    close_px = ticks['close']
    # print(f'{tick_symbol}-{tick_time}-{close_px}-{tick_px}')

    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == tick_symbol, 'CandleTime'] = tick_time
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Close'] = tick_px
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Open'] = ticks['open']
        livePrices.loc[livePrices['Token'] == tick_symbol, 'High'] = ticks['high']
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Low'] = ticks['low']

        if tick_time.hour <= 11:
            livePrices.loc[livePrices['Token'] == tick_symbol, 'PrevClose'] = close_px

    else:
        new_row = {'CandleTime': tick_time, 'Token': tick_symbol, 'Close': tick_px,
                   'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low'], 'PrevClose': close_px}
        livePrices=pd.DataFrame(new_row, index = [0])

    if len(strat_trades_df) > 0:
        if tick_symbol.isdigit():
            strat_trades_df.loc[strat_trades_df['DervID'] == str(tick_symbol), 'DervPx'] = tick_px
            strat_trades_df.loc[strat_trades_df['DervID'] == str(tick_symbol), 'PnL'] = (tick_px - strat_trades_df.loc[strat_trades_df['DervID'] == str(tick_symbol), 'EntryPx']) * strat_trades_df.loc[strat_trades_df['DervID'] == str(tick_symbol), 'Qty']
            if len(strat_trades_df[(strat_trades_df['DervID'] == str(tick_symbol)) & (strat_trades_df['EntryID'] == 'test') & (strat_trades_df['EntryPx'] <= 0)]['EntryPx']) > 0:
                strat_trades_df.loc[(strat_trades_df['DervID'] == str(tick_symbol)) & (strat_trades_df['EntryID'] == 'test'), 'EntryPx'] = tick_px

    if tick_symbol in ['NIFTY 50','NIFTY BANK','NIFTY FIN SERVICE']:
        if tick_symbol == 'NIFTY 50':
            # Straddle buy strategy for Nifty to initiate at 9:15:10 AM
            # if tick_time.hour == 13 and tick_time.minute == 34 and tick_time.second == 50:
            if tick_time.time() >= time(9,15,10):
                printLog('i','Straddle Initiating')
                straddle_thread = Thread(target=strat_straddle_buy,args=(tick_symbol,tick_px,tick_time))
                straddle_thread.start()                 

            # if tick_time.time() > time(9,16) and tick_time.time() < time(15,1):
            if tick_time.hour == 9 and tick_time.minute == 15 and tick_time.second == 10:
                round_strat_thread = Thread(target=strat_round_entries,args=(tick_symbol,tick_px,tick_time))
                round_strat_thread.start()


            strat_ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
            if tick_time.minute % strat_ema_tf == 0 and tick_time.second == 5:
                printLog('i', f"EMA Strategy Execution - Timeframe - {strat_ema_tf}")
                ema_strat_thread = Thread(target=generate_ema_signal,args=(tick_symbol, strat_ema_tf))
                ema_strat_thread.start()

            # # run only in local for testing purpose, comment in prod version
            # if (tick_time.second % 5) == 0:
            #     printLog('i',f'{tick_symbol}-{tick_time}-{close_px}-{tick_px}')
            #     livePrices.to_csv(watchlist_file,index=False)
            #     if len(strat_trades_df) > 0:
            #         strat_trades_df.to_csv(trade_file,index=False)
            #     ic_subscribe_traded_symbols()


    #         monitor_ema_thread = Thread(target=check_ema_signal,args=(tick_symbol, tick_px, tick_time))
    #         monitor_ema_thread.start()

            # monitor_thread = Thread(target=check_strategy,args=(tick_symbol,tick_px,tick_time))
            # monitor_thread.start()

def test_strategies():
    strat_trades_df = pd.DataFrame()
    d1 = dh_get_orders()['data']
    for index, row in d1.iterrows():
        if row['legName'] == 'ENTRY_LEG':
            signal_row = {'Strategy':'Straddle',
                          'Symbol': 'NIFTY 50',
                          'Signal': 'green',
                          'Entry': 19709.25,
                          'SymSL': 0.0,
                          'Qty': row['quantity'],
                          'DervName': row['tradingSymbol'],
                          'DervID': row['securityId'],
                          'DervPx': 60.00,
                          'EntryID': row['orderId'],
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
            strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)

# strat_trades_df.loc[strat_trades_df['DervID'] == 85873, 'PnL']

def ic_subscribe_traded_symbols():
    global livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal

    if len(strat_trades_df) == 0:
        return
    if len(strat_trades_df[strat_trades_df['Active'] == 'Y']) == 0:
        return

    opt_token = list(strat_trades_df[strat_trades_df['Active']=='Y']['DervID'])
    opt_to_subscribe = []
    for item in opt_token:
        token = f"4.1!{item}"
        # print(token_list.count(token))
        if token_list.count(token) == 0:
            opt_to_subscribe.append(token)
    if len(opt_to_subscribe) > 0:
        ic_subscribeFeed(opt_to_subscribe)
        token_list = token_list + opt_to_subscribe
        return {'status':'success','remarks':'','data':f'traded symbols subscribed - {opt_to_subscribe}'}
    return {'status':'success','remarks':'','data':'no symbols to subscribe'}


# def main():
if __name__ == '__main__':
    # global livePrices, options_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    printLog('i','Starting Live Feed Process')

    icici_session_id = ic_autologon()
    printLog('i',f"ic_watchlist - ICICI Session ID - {icici_session_id}")
    subscription_flag = 'N'

    msg = f"Started @ {now.strftime('%A %b %d, %Y %H:%M:%S')}"
    send_whatsapp_msg(f"TradeApp Start - {now.strftime('%Y-%m-%d')}",msg)

    try:
        if os.path.exists(watchlist_file) == False:
            ic_update_watchlist(mode='C',num=0)
        livePrices = pd.read_csv(watchlist_file)
    except pd.errors.EmptyDataError:
        ic_update_watchlist(mode='C',num=0)
    livePrices = pd.read_csv(watchlist_file)

    if len(livePrices) == 0:
        symbol_list = json.load(open('config.json', 'r'))['STOCK_CODES']
        ic_instruments = pd.read_csv('icici.csv')
        wl_cols= ['SymbolName','ExchangeCode','Segment','Token','Code','LotSize',
                  'Open','High','Low','Close','PrevClose','CandleTime']

        livePrices = pd.DataFrame(columns=wl_cols)
        for sym in symbol_list:
            sym = ic_instruments[ic_instruments['CD']==sym][['NS','EC','SG','TK','CD','LS']]
            sym.rename(columns={'NS':'SymbolName','EC':'ExchangeCode','SG':'Segment',
                                'TK':'Token','CD':'Code','LS':'LotSize'}, inplace=True)
            sym['Open']=0
            sym['High']=0
            sym['Low']=0
            sym['Close']=0
            sym['PrevClose']=0
            sym['CandleTime']=datetime.now()

            livePrices = pd.concat([livePrices,sym],ignore_index=True)

    if len(livePrices) > 0:
        for index,row in livePrices.iterrows():
            token_list.append(f"4.1!{row['Token']}")

    icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
    icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session_id)

    while True:
        now = datetime.now()
        try:
            if now.time() < time(9,0) or now.time() > time(15,40):
                break
            if (now.time() >= time(9,14) and now.time() < time(15,35,0)):
                if subscription_flag=='N':
                    if len(livePrices) > 0:
                        icici.ws_connect()
                        icici.on_ticks = on_ticks
                        # wl_df = pd.read_csv('WatchList.csv')
                        # livePrices = wl_df
                        tokens=ic_tokenLookup(list(livePrices['Code'].values))
                        # ic_subscribeFeed(tokens['data'])
                        ic_subscribeFeed(token_list)
                        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Subscribed - {token_list}")
                        subscription_flag = 'Y'
                        send_whatsapp_msg('Feed Alert - LIVE',f"Market Live Feed Started @ {now.strftime('%A %b %d, %Y %H:%M:%S')}")
                    else:
                        ic_get_watchlist(mode='C')
                else:
                    livePrices.to_csv(watchlist_file,index=False)
                    if len(strat_trades_df) > 0:
                        strat_trades_df.to_csv(trade_file,index=False)
                    ic_subscribe_traded_symbols()

            if (now.time() >= time(15,35) and subscription_flag=='Y'):
                ic_unsubscribeFeed(token_list)
                icici.ws_disconnect()
                subscription_flag='N'
                send_whatsapp_msg('Feed Alert - STOP',f"Market Live Feed Stopped @ {now.strftime('%A %b %d, %Y %H:%M:%S')}")
                break

            if subscription_flag == 'Y':
                tm.sleep(1)
            else:
                tm.sleep(60)
        except Exception as e:
            printLog('e',f"Main - {str(e)}")
            # print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Main - {str(e)}")
            pass
        # tm.sleep(1)


# t1 = dh_get_trades()['data']
# o1 = dh_get_orders()['data']
# # Main Function Start
# if __name__ == '__main__':
#     main()

# strat_trades_df = pd.read_csv(trade_file)
# icici.user_id