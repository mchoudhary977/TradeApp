import yfinance as yf
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

livePrices = pd.DataFrame()
options_df = pd.DataFrame()
strat_trades_df = pd.DataFrame() 

icici_scrips = 'icici.csv'
watchlist_file = 'WatchList.csv'
options_file = 'Options.csv'
oi_pcr_file = 'OIPCR.csv'
trade_file = 'Trades.csv'


# thread_list = []

def get_option_list(ticker, underlying_px):
    # ticker = 'NIFFIN'
    exchange = 'NFO'
    instrument_list = pd.read_csv(icici_scrips)
    oc_df = instrument_list[instrument_list["SC"]==ticker][instrument_list['EC']==exchange][instrument_list['DRV']=='OPT']
    
    filtered_dates = []
    for date in sorted(pd.to_datetime(oc_df['EXPIRY']).unique()):
        if (date - np.datetime64(dt.datetime.now().date())) >= 0:
            filtered_dates.append(date.astype('datetime64[D]').astype(str))
    filtered_dates = filtered_dates[:2]
    
    oc_df = oc_df[oc_df['EXPIRY'].isin(filtered_dates)]
    
    strike_range = 10 * int(json.load(open('config.json', 'r'))[ticker]['STRIKE_STEP'])
    
    # underlying_px = 20408.2 #46000.85  # 20103
    lower_bound = int((underlying_px-strike_range)/100)*100   #19600
    upper_bound = int((underlying_px+strike_range)/100)*100
    
    oc_df = oc_df[(oc_df['STRIKE'] >= lower_bound) & (oc_df['STRIKE'] <= upper_bound)]
    
    call_df = oc_df[oc_df['OT'] == 'CE']
    put_df = oc_df[oc_df['OT'] == 'PE']
    oc_df = call_df.merge(put_df, on=['SC','SN','EC','SG','LS','NS','TS','DRV','STRIKE', 'EXPIRY'], how='left').dropna()
    
    rn_cols = {'OT_x':'CALL_OT','CD_x':'CALL_CD','TK_x':'CALL_TK','CD_y':'PUT_CD','TK_y':'PUT_TK','OT_y':'PUT_OT'}
    oc_df = oc_df.rename(columns=rn_cols)
    
    oc_df['PCR-COI'] = 0
    oc_df['CALL_COI'] = 0
    oc_df['PUT_COI'] = 0
    oc_df['CALL_PX'] = 0
    oc_df['PUT_PX'] = 0
    oc_df['CALL_CandleTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    oc_df['PUT_CandleTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    oc_df = oc_df[['SC','SN','NS','LS','EXPIRY','PCR-COI','CALL_CandleTime','CALL_OT','CALL_CD','CALL_TK','CALL_COI','CALL_PX','STRIKE','PUT_PX','PUT_COI','PUT_TK','PUT_CD','PUT_OT','PUT_CandleTime']]
    
    oc_df = oc_df.sort_values(by=['EXPIRY', 'STRIKE'], ascending=[True, True])
    oc_df.reset_index(drop=True, inplace=True)
    return oc_df

# oc_df = get_pcr_details(ticker, oc_df)

# df = oc_df.copy() ticker = 'NIFTY'
def get_pcr_details(ticker, df):
    symbol = ticker
    if ticker=='CNXBAN':
        symbol = 'BANKNIFTY'
    elif ticker == 'NIFFIN':
        symbol = 'FINNIFTY'
    pcr_list = []                    
    try:
        url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        headers = {
            'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
            'accept-encoding' : 'gzip, deflate, br',
            'accept-language' : 'en-US,en;q=0.9'
        }
        response = requests.get(url, headers=headers).content
        data = json.loads(response.decode('utf-8'))
        data = data['records']['data']
        
        exp_date_list = list(df['EXPIRY'].unique())
        min_strike = int(df['STRIKE'].min())
        max_strike = int(df['STRIKE'].max())
        
        total_pcr = {}
        for i in exp_date_list:
            total_pcr[i] = {'CODE':'', 'EXPIRY':i, 'CALL OI': 0, 'PUT OI': 0, 'CALL Vol': 0, 'PUT Vol': 0, 'PCR': 0, 'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        for i in data:           
            # print(i['strikePrice'])
            exp_date = str(datetime.strptime(i['expiryDate'], '%d-%b-%Y').strftime('%Y-%m-%d'))
            strike_px = int(i['strikePrice'])
            if exp_date in exp_date_list:
               if strike_px >= min_strike and strike_px <= max_strike:
                   condition = (df['EXPIRY'] == exp_date) & (df['STRIKE'] == strike_px)             
                   df.loc[condition, 'CALL_COI'] = i['CE']['changeinOpenInterest']
                   df.loc[condition, 'PUT_COI'] = i['PE']['changeinOpenInterest']
                   df.loc[condition, 'PCR-COI'] = round(abs(i['PE']['changeinOpenInterest'])/abs(i['CE']['changeinOpenInterest']),3)
                   df.loc[condition, 'CALL_CandleTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                   df.loc[condition, 'PUT_CandleTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
               total_pcr[exp_date]['CODE'] = i['CE']['underlying']
               total_pcr[exp_date]['CALL OI'] = round(total_pcr[exp_date]['CALL OI'] + i['CE']['openInterest'],0)
               total_pcr[exp_date]['PUT OI'] = round(total_pcr[exp_date]['PUT OI'] + i['PE']['openInterest'],0)
               total_pcr[exp_date]['CALL Vol'] = round(total_pcr[exp_date]['CALL Vol'] + i['CE']['totalTradedVolume'],0)
               total_pcr[exp_date]['PUT Vol'] = round(total_pcr[exp_date]['PUT Vol'] + i['PE']['totalTradedVolume'],0)
        
                       
        for key,value in total_pcr.items():
            value['PCR'] = round(value['PUT OI'] / value['CALL OI'],3) if value['CALL OI'] > 0 else 0 
            pcr_list.append(value)
            # print(round(value['total_put_oi'] / value['total_call_oi'],3))
    except Exception as e:
        print(str(e))
        pass
    
    return df,pcr_list
    
    
# df = options_df
# df1 = options_df.copy()
def update_pcr(df):
    global options_df
    ticker_list = list(df['SC'].unique())
    try:
        pcr_strike_df = pd.DataFrame()
        pcr_list =[]
        # i='CNXBAN'
        for i in ticker_list:
            print(i)
            ticker_opt = df[df['SC']==i]
            df1, pcr = get_pcr_details(i, ticker_opt) 
            pcr_list = pcr_list + pcr
            # print(pcr_dict)
            pcr_strike_df = pd.concat([pcr_strike_df,df1], ignore_index=True)
            
        pcr_strike_df = pcr_strike_df[['SC','EXPIRY','STRIKE','PCR-COI','CALL_COI','PUT_COI']]
          
        for index, row in pcr_strike_df.iterrows():
            condition = (options_df['SC'] == row['SC']) & (options_df['EXPIRY'] == row['EXPIRY']) & (options_df['STRIKE'] == row['STRIKE'])
            options_df.loc[condition, 'PCR-COI'] = row['PCR-COI']
            options_df.loc[condition, 'CALL_COI'] = row['CALL_COI']
            options_df.loc[condition, 'PUT_COI'] = row['PUT_COI']         

        # condition = (options_df['SC'] == pcr_strike_df['SC']) & (options_df['EXPIRY'] == pcr_strike_df['EXPIRY']) & (options_df['STRIKE'] == pcr_strike_df['STRIKE'])
        # options_df.loc[condition, 'PCR-COI'] = pcr_strike_df['PCR-COI']
        # options_df.loc[condition, 'CALL_COI'] = pcr_strike_df['CALL_COI']
        # options_df.loc[condition, 'PUT_COI'] = pcr_strike_df['PUT_COI']
        
        oi_pcr_df = pd.DataFrame(pcr_list) 
        
        if os.path.exists(oi_pcr_file):
            oi_pcr_csv = pd.read_csv(oi_pcr_file)
            if len(oi_pcr_csv) > 0:
                oi_pcr_df = pd.concat([oi_pcr_csv, oi_pcr_df], ignore_index=True)
                oi_pcr_df = oi_pcr_df.sort_values(by=['Timestamp','CODE','EXPIRY'], ascending=[False,True,True])
                oi_pcr_df.to_csv(oi_pcr_file,index=False)
            else:
                oi_pcr_df.to_csv(oi_pcr_file,index=False)
        else:
            oi_pcr_df.to_csv(oi_pcr_file,index=False)
       
        return {'status':'success','data':'pcr updated'}   
    except Exception as e:
        err = str(e)
        print(err)
        pass

# signal='green'
# sig_data = strat_trades_df.iloc[-1]
def place_strategy_order(index, symbol, last_px, sig_data):
    sig = sig_data
    strat_index = index
    global options_df, strat_trades_df
    funct_name = 'place_strategy_order'.upper()
    msg = {'status':'failure', 'remarks':'', 'data':''}
    
    ticker = 'NIFTY' if symbol == 'NIFTY 50' else 'TEST'
    try:
        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
        num = int(json.load(open('config.json', 'r'))[ticker]['OPT#'])
        max_prem = int(json.load(open('config.json', 'r'))[ticker]['MAX_PREM'])
        qty = int(json.load(open('config.json', 'r'))[ticker]['Qty_LOT'])
        trail_pts = int(json.load(open('config.json', 'r'))[ticker]['TRAIL_PTS'])
        
        opt_type = 'CE' if sig['Signal'] == 'green' else ('PE' if sig['Signal'] =='red' else None)
        side = 'BUY' if sig['Signal'] == 'green' else ('SELL' if sig['Signal'] =='red' else None)
        msg['data'] = f"{symbol} -> {side} -> {last_px} "
        
        opt = ic_option_chain(ticker, underlying_price=last_px, option_type=opt_type, duration=exp_week)
        # opt = opt.iloc[num]
        sec_id = ''
        sec_name = ''
        opt_prem = 0
        
        order_flag = 'N'
        for index, row in opt.iterrows():
            if sig['Signal'] == 'green':                
                opt_prem = options_df[(options_df['STRIKE']==row['STRIKE']) & (options_df['SC']==row['SC']) & (options_df['EXPIRY']==row['EXPIRY'])]['CALL_PX'].values[0]
            elif sig['Signal'] == 'red':
                opt_prem = options_df[(options_df['STRIKE']==row['STRIKE']) & (options_df['SC']==row['SC']) & (options_df['EXPIRY']==row['EXPIRY'])]['PUT_PX'].values[0]
                
                if opt_prem > 0 and opt_prem < max_prem:
                    sec_id = row['TK']
                    sec_name = row['CD']
                    order_flag = 'Y'
                    break
                
        if order_flag == 'N':
            opt = opt.iloc[num]
            sec_id = opt['TK']
            sec_name = opt['CD']
                
        msg['data'] = msg['data'] + f"-> [{sec_id}] {sec_name} "
        
        trade = {}
        side = 'buy'
           
        if live_order == 'Y':
            pos = dh_get_positions()
            pos = pd.DataFrame(pos['data']) if pos['status'].lower() == 'success' and pos['data'] is not None else None
            
            exit_flag = 'N'

            if pos is not None and len(pos[pos['securityId']==str(sec_id)][pos['positionType'] !='CLOSED'])>0:
                exit_flag = 'Y'
                msg['remarks'] = f"Active Position Present. "
            
            if exit_flag == 'Y':
                send_whatsapp_msg(f"EMA Alert - Failure", msg['remarks'])
                write_log('place_strategy_order','i',msg['remarks'])
                return msg # {'status':'SUCCESS','data':msg['remarks']}
            
            # place bracket order until trailing sl is implemented
            trade = dh_post_exchange_order(ord_type='bo', exchange='FNO',
                                       security_id=sec_id, side=side,
                                       qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                       tg_pts=10, sl_pts=5,
                                       amo=False, prod_type='')
            
            bracket_order = 'Y'  #Comment above trade and set this flag to 'N' for trailing SL order
            
            # trade = dh_post_exchange_order(ord_type='mkt', exchange='FNO',
            #                            security_id=sec_id, side=side,
            #                            qty=qty, entry_px=0, sl_px=0, trigger_px=0,
            #                            tg_pts=0, sl_pts=0,
            #                            amo=False, prod_type='')
            
            if trade['status'].lower() == 'success':      
                entry_id = trade['data']['orderId'] 
                sig['EntryID'] = trade['data']['orderId'] 
                msg['data'] = msg['data'] + f"=> Order Placed [ OrderId - {trade['data']['orderId']} | OrderStatus - {trade['data']['orderStatus']} ]"
                msg['status'] = 'success'
                send_whatsapp_msg(f"EMA Alert - Entry Order", msg['data'])
                
                entry_px = 0.0
                while True:
                    valid_status = ['transit','pending']
                    if trade['data']['orderStatus'].lower() in valid_status:
                        tm.sleep(5)
                        trade = dh_get_order_id(entry_id)
                        if trade['status'].lower() == 'success':
                            # if trade['data']['orderStatus'].lower() == 'pending':   #TEST ONLY, UNCOMMENT BELOW IN PROD
                            if trade['data']['orderStatus'].lower() == 'traded':
                                sig['EntryPx'] = trade['data']['price'] 
                                sig['Status'] = trade['data']['orderStatus']
                                # place SL order
                                # sl_price = 60  #TEST LINE, UNCOMMENT BELOW IN PROD
                                sl_price = sig['EntryPx'] - trail_pts
                                
                                trigger_price = sl_price + 0.05
                                sl_side = 'sell' if side =='buy' else 'sell'
                                
                                sig['DervSL'] = sl_price
                                
                                if bracket_order == 'Y':
                                    break
                                
                                sl_trade = dh_post_exchange_order(ord_type='sl', exchange='FNO',
                                                           security_id=sec_id, side=sl_side,
                                                           qty=qty, entry_px=sl_price, sl_px=0, trigger_px=trigger_price,
                                                           tg_pts=0, sl_pts=0,
                                                           amo=False, prod_type='')
                                if sl_trade['status'].lower() == 'success':
                                    sig['ExitID'] = sl_trade['data']['orderId'] 
                                    sl_msg = f"EntryOrder - {trade['data']['orderId']} => EntryPrice - {sig['EntryPx']} || ExitOrder - {sig['ExitID']} => StopLoss - {sig['DervSL']} "
                                    send_whatsapp_msg(f"EMA Alert - StopLoss Order", sl_msg)                               
                                break
                    elif trade['data']['orderStatus'].lower() in ['rejected','cancelled','expired']:
                        sig['EntryPx'] = 0.0
                        sig['DervSL'] = 0.0
                        sig['ExitID'] = ''
                        sig['Status'] = 'FAILED'
                        
        else:
            sig['EntryID'] = 'TEST_ENTRY_1234'
            sig['EntryPx'] = opt_prem
            sig['DervSL'] = opt_prem - trail_pts
            sig['ExitID'] = 'TEST_EXIT_1234'
            sig['Status'] = 'TRADED'
            msg['data'] = msg['data'] + f"=> Live Order not enabled, place manually!"
            msg['status'] = 'success'
            send_whatsapp_msg(f"EMA Alert - Manual Placement", msg['data'])
            
        strat_trades_df.loc[strat_index, 'Qty'] = qty
        strat_trades_df.loc[strat_index, 'DervName'] = sec_name
        strat_trades_df.loc[strat_index, 'DervID'] = sec_id
        strat_trades_df.loc[strat_index, 'EntryID'] = sig['EntryID']
        strat_trades_df.loc[strat_index, 'EntryPx'] = sig['EntryPx']
        strat_trades_df.loc[strat_index, 'DervSL'] = sig['DervSL']
        strat_trades_df.loc[strat_index, 'ExitID'] = sig['ExitID']
        strat_trades_df.loc[strat_index, 'Status'] = sig['Status']

    except Exception as e:
        err = str(e)
        msg['remarks'] = f"{funct_name} failure - {err}."
        pass
    
    
    


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
    
# symbol = 'NIFTY 50'
# tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
def generate_ema_signal(symbol, tf):
    global strat_trades_df
    if symbol == 'NIFTY 50':
        ticker = '^NSEI'
    else:
        return {'status':'failure','remarks':'Symbol not applicable for ema strategy','data':''}    
    try:
        now = datetime.now()  
        # now = now-timedelta(1) # TEST LINE, COMMENT AFTER TESTING
        start_date = (datetime.now() - timedelta(5)).strftime('%Y-%m-%d')
        end_date = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
        
        st = yf.download(ticker, start=start_date, end=end_date, interval="1m")
        df = st.copy()

        sample_tf = f"{tf}T" if tf > 0 else "5T"
        df = df.resample(sample_tf).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Adj Close': 'last', 'Volume':'last'}).dropna()
        df['datetime'] = df.index.tz_localize(None)
        df.rename(columns={'Open':'open','High':'high','Low':'low','Adj Close':'close','Volume':'volumne'}, inplace=True)
        df = df[['datetime','open','high','low','close','volumne']].iloc[:-1]
        
        df['15-ema'] = round(df['close'].ewm(span=15, adjust=False).mean(),2)
        df['9-ema'] = round(df['close'].ewm(span=9, adjust=False).mean(),2)
        df['ema_xover'] = round(df['9-ema'] - df['15-ema'],2)
        df['rsi'] = round(rsi(df,14),2)
        df = df[df['datetime'] >= pd.Timestamp(now.strftime('%Y-%m-%d'))]
        # df = df[df['datetime'] >= pd.Timestamp('09:25:00')]
        df['signal'] = df.apply(signal, axis=1)
        df['signal'].fillna(method='ffill', inplace=True)
        # df['signal'] = df['signal'].where(df['signal'] != df['signal'].shift())
        df['entry'] = round(df.apply(update_entry, axis=1),2)
        
        # if df.tail(3)['signal'].nunique() > 1:          # TEST LINE, UNCOMMENT BELOW IN PROD
        if df.tail(2)['signal'].nunique() > 1:
            sig = df.iloc[-1].copy()
            
            signal_row = {'Strategy':'EMA Crossover',
                          'Symbol': symbol,
                          'Signal': sig['signal'],
                          'Entry': sig['entry'],
                          'SymSL': round(sig['low'],2) if sig['signal'] == 'green' else round(sig['high'],2),
                          'Qty': 0,
                          'DervName': ' ',
                          'DervID': ' ',
                          'DervPx': 0.0,
                          'EntryID': ' ',
                          'EntryPx': 0.0,
                          'DervSL': 0.0,
                          'ExitID': ' ',
                          'ExitPx': 0.0,
                          'PnL': 0.0,
                          'CreationTime': sig['datetime'], 
                          'ExpirationTime': sig['datetime'] + timedelta(minutes=tf),
                          'Active': 'Y',
                          'Status': ' '
                          }
            
            strat_trades_df = pd.concat([strat_trades_df,pd.DataFrame(signal_row, index=[0])], ignore_index=True)
            
    except Exception as e:
        err = str(e)
        print(err)
        return {'status':'failure','remarks':f"ema failure error - {err}",'data':''}
    
    return {'status':'success','remarks':'','data':'ema generation processed successfully'}

# strat_trades_df = pd.DataFrame()       
# ticks = {'ltt':'Mon Sep 15 15:26:59 2023','symbol':'4.1!NIFTY 50'}
# time_stamp = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
# last_px = 20164
def check_strategies(symbol, last_px, time_stamp):
    global strat_trades_df
    
    condition = (strat_trades_df['Symbol']==symbol) & (strat_trades_df['Active']=='Y')
    
    for index, row in strat_trades_df[condition].iterrows():
        if time_stamp < row['ExpirationTime'] and row['Status'].isspace() == True:
            if row['Entry'] > 0 and ((row['Signal'] == 'red' and last_px < row['Entry']) or (row['Signal'] == 'green' and last_px > row['Entry'])):
                strat_trades_df.loc[index,'Status'] = 'PENDING'
                # place_strategy_order(index, symbol, last_px, row)     #TEST LINE, UNCOMMENT BELOW 2 LINES IN PROD
                order_th = Thread(target=place_strategy_order,args=(index, symbol, last_px, row))                  
                order_th.start()       
                
        elif time_stamp >= row['ExpirationTime'] and row['Status'].isspace() == True:
            strat_trades_df.loc[index,'Active'] = 'N'
            strat_trades_df.loc[index,'Status'] = 'INACTIVE'

                       
                       
# pcr1 = pd.read_csv(oi_pcr_file)   
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

# ticks = {'ltt':'Mon Sep 12 09:17:00 2023','symbol':'4.1!NIFTY 50'}
# On Ticks function
def on_ticks(ticks):
    # print(f'{ticks["symbol"]}-{ticks["last"]}')
    global options_df
    global livePrices
    
    tick_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    tick_symbol = ticks['symbol'][4:]

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
     
    if len(options_df) > 0:
        options_df.loc[options_df['CALL_TK'] == ticks['symbol'][4:], 'CALL_CandleTime'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        options_df.loc[options_df['PUT_TK'] == ticks['symbol'][4:], 'PUT_CandleTime'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        options_df.loc[options_df['CALL_TK'] == ticks['symbol'][4:], 'CALL_PX'] = ticks['last']
        options_df.loc[options_df['PUT_TK'] == ticks['symbol'][4:], 'PUT_PX'] = ticks['last']
    
    if len(strat_trades_df) > 0:
        strat_trades_df.loc[strat_trades_df['DervID'] == ticks['symbol'][4:], 'DervPx'] = ticks['last']
        
    # Straddle buy strategy for Nifty to initiate at 9:17 AM
    if tick_symbol == 'NIFTY 50':
        if tick_time.hour == 9 and tick_time.minute == 17 and tick_time.second == 0:
            send_whatsapp_msg('Straddle Strategy',f"Initiating Straddle for {tick_symbol} - {ticks['last']} - {ticks['ltt']}")
            straddle_thread = Thread(target=strat_straddle_buy,args=(tick_symbol,ticks['last'],tick_time))
            straddle_thread.start()        
            
        if tick_time.minute % 5 == 0 and tick_time.second == 5:
            pcr_thread = Thread(target=update_pcr,args=(options_df))
            pcr_thread.start()
        
        strat_ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
        if tick_time.minute % strat_ema_tf == 0 and tick_time.second == 5:
            ema_strat_thread = Thread(target=generate_ema_signal,args=(tick_symbol, strat_ema_tf)) 
            ema_strat_thread.start()
        
        monitor_thread = Thread(target=check_strategies,args=(tick_symbol, ticks['last'], tick_time)) 
        monitor_thread.start()
        
        
    # if len(tsymbols_df) > 0:
    #     tsymbols_df.loc[tsymbols_df['SecID'] == ticks['symbol'][4:], 'Timestamp'] = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    #     tsymbols_df.loc[tsymbols_df['SecID'] == ticks['symbol'][4:], 'LivePx'] = ticks['last']
        # tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'Open'] = ticks['open']
        # tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'High'] = ticks['high']
        # tsymbols_df.loc[tsymbols_df['SecurityID'] == ticks['symbol'][4:], 'Low'] = ticks['low']
    

def ic_subscribe_traded_symbols():    
    if os.path.exists(trade_file) == True:
        trade_df = pd.read_csv(trade_file)
        if len(trade_df) > 0:   
            tsymbols_df = trade_df         
            if len(tsymbols_df[tsymbols_df['Active']=='Y'][tsymbols_df['LiveFeed']=='N'])<=0:
                return {'status':'success','remarks':'','data':'no symbols to subscribe'}
            # trade_df = trade_df[trade_df['Active']=='Y'][trade_df['LiveFeed']=='N']
            
            traded_tokens=[]
            
            for index, row in tsymbols_df.iterrows():
                if row['LiveFeed'] == 'N':
                    traded_tokens.append(f"4.1!{row['SecID']}") 
                    tsymbols_df.loc[index,'LiveFeed'] = 'Y'
            
            if len(traded_tokens) > 0:
                ic_subscribeFeed(traded_tokens)
                tsymbols_df.to_csv(trade_file,index=False)
                return {'status':'success','remarks':'','data':'traded symbols subscribed'}
    return {'status':'success','remarks':'','data':'no symbols to subscribe'}


# Main Function Start
def main():
    global options_df
    session_id = ic_autologon()
    write_log('ic_watchlist','i',f"ICICI Session ID - {session_id}")  
    print("Watchlist Live Started")
    try:     
        token_list = []
        if os.path.exists(watchlist_file) == False:
            ic_update_watchlist(mode='C',num=0)
        subscription_flag = 'N'
        now = datetime.now()
        wl_df = pd.read_csv(watchlist_file)
        if len(pd.read_csv(watchlist_file)) > 0:
            for index,row in wl_df.iterrows():
                if now.date() > datetime.strptime(row['CandleTime'], '%Y-%m-%d %H:%M:%S').date():
                    wl_df.at[index, 'PrevClose'] = row['Close']
                token_list.append(f"4.1!{row['Token']}")
            # wl_df.to_csv(watchlist_file, index=False)
        print(wl_df) 
        # ticker_dict = {'NIFTY':0.0, 'CNXBAN':0.0}    # TEST LINE, UNCOMMENT BELOW IN PROD
        ticker_dict = {'NIFTY':0.0, 'CNXBAN':0.0, 'NIFFIN':0.0}
        for key,value in ticker_dict.items():
            ticker_dict[key] = wl_df[wl_df['Code']==key]['Close'].values[0]
        print(ticker_dict)    
        
        for key,value in ticker_dict.items():
            options_df = pd.concat([options_df,get_option_list(key, value)])
            # options_df = pd.concat([options_df,get_pcr_details(key,get_option_list(key, value))])
            
        for index,row in options_df.iterrows():
            token_list.append(f"4.1!{row['CALL_TK']}")
            token_list.append(f"4.1!{row['PUT_TK']}")
        print(token_list)
                   
        while True:
            now = datetime.now()
            print(now)
            if now.time() < time(9,0) or now.time() > time(15,40):
                break
            
            if (now.time() >= time(9,14) and now.time() < time(15,35,0)):
                if subscription_flag=='N':
                    if os.path.exists(watchlist_file):
                        print('Subscribed')
                        icici.ws_connect()
                        icici.on_ticks = on_ticks
                        wl_df = pd.read_csv(watchlist_file)
                        livePrices = wl_df
                        
                        ic_subscribeFeed(token_list)
                        subscription_flag = 'Y'
                        send_whatsapp_msg('Feed Alert','Market Live Feed Started!')
                        
                    else:
                        ic_get_watchlist(mode='C')
                else:
                    livePrices.to_csv(watchlist_file, index=False)
                    options_df.to_csv(options_file, index=False)
                    strat_trades_df.to_csv(trade_file, index=False)
                    # if len(tsymbols_df) > 0:
                    #     tsymbols_df.to_csv('Trades.csv',index=False)
                    # ic_subscribe_traded_symbols()
                   
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


def test_function():
    global options_df
    try:     
        token_list = []
        if os.path.exists(watchlist_file) == False:
            ic_update_watchlist(mode='C',num=0)
        subscription_flag = 'N'
        now = datetime.now()
        wl_df = pd.read_csv(watchlist_file)
        if len(pd.read_csv(watchlist_file)) > 0:
            for index,row in wl_df.iterrows():
                if now.date() > datetime.strptime(row['CandleTime'], '%Y-%m-%d %H:%M:%S').date():
                    wl_df.at[index, 'PrevClose'] = row['Close']
                token_list.append(f"4.1!{row['Token']}")
            # wl_df.to_csv(watchlist_file, index=False)
        
        # ticker_dict = {'NIFTY':0.0, 'CNXBAN':0.0}    # TEST LINE
        ticker_dict = {'NIFTY':0.0, 'CNXBAN':0.0, 'NIFFIN':0.0}
        for key,value in ticker_dict.items():
            ticker_dict[key] = wl_df[wl_df['Code']==key]['Close'].values[0]
            
        for key,value in ticker_dict.items():
            options_df = pd.concat([options_df,get_option_list(key, value)])
            # options_df = pd.concat([options_df,get_pcr_details(key,get_option_list(key, value))])
            
        for index,row in options_df.iterrows():
            token_list.append(f"4.1!{row['CALL_TK']}")
            token_list.append(f"4.1!{row['PUT_TK']}")
            
        # update pcr
        update_pcr(options_df)
        
        symbol = 'NIFTY 50'
        tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
        generate_ema_signal(symbol, tf)
        
        # strat_trades_df = pd.DataFrame()       
        ticks = {'ltt':'Mon Sep 15 15:27:00 2023','symbol':'4.1!NIFTY 50'}
        time_stamp = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
        last_px = 20165
        check_strategies(symbol, last_px, time_stamp)
            
    except Exception as e:
        pass
    
if __name__ == '__main__':
    main()