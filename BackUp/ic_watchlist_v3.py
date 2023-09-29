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
import numpy as np
from threading import Thread
# import json
# import sys

# Variables
livePrices = pd.DataFrame()
options_df = pd.DataFrame()
strat_trades_df = pd.DataFrame() 
token_list = []

# Files
icici_scrips = 'icici.csv'
watchlist_file = 'WatchList.csv'
options_file = 'Options.csv'
oi_pcr_file = 'OIPCR.csv'
trade_file = 'Trades.csv'



def get_option_list(ticker, underlying_px):
    # ticker = 'CNXBAN'
    exchange = 'NFO'
    instrument_list = pd.read_csv(icici_scrips)
    oc_df = instrument_list[instrument_list["SC"]==ticker][instrument_list['EC']==exchange][instrument_list['DRV']=='OPT']
    
    # oc_df = instrument_list[instrument_list["SC"]==ticker][instrument_list['EC']=='NSE']['TK']
    
    oc_df['SYM_TK'] = instrument_list[instrument_list["SC"]==ticker][instrument_list['EC']=='NSE']['TK'].values[0]
    
    filtered_dates = []    
    for date in sorted(pd.to_datetime(oc_df['EXPIRY']).unique()):
        if (date - np.datetime64(dt.datetime.now().date())) >= pd.Timedelta(days=0):           
            filtered_dates.append(date.astype('datetime64[D]').astype(str))
            # filtered_dates.append(str(date.date()))
    filtered_dates = filtered_dates[:2]
    # print(filtered_dates)
    oc_df = oc_df[oc_df['EXPIRY'].isin(filtered_dates)]
    
    call_df = oc_df[oc_df['OT'] == 'CE']
    put_df = oc_df[oc_df['OT'] == 'PE']
    oc_df = call_df.merge(put_df, on=['SC','SN','EC','SG','LS','NS','TS','DRV','STRIKE', 'EXPIRY', 'SYM_TK'], how='left').dropna()
    
    rn_cols = {'OT_x':'CALL_OT','CD_x':'CALL_CD','TK_x':'CALL_TK','CD_y':'PUT_CD','TK_y':'PUT_TK','OT_y':'PUT_OT'}
    oc_df = oc_df.rename(columns=rn_cols)
    
    oc_df['PCR-COI'] = 0
    oc_df['CALL_COI'] = 0
    oc_df['PUT_COI'] = 0
    oc_df['CALL_PX'] = 0
    oc_df['PUT_PX'] = 0
    oc_df['CALL_CandleTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    oc_df['PUT_CandleTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    oc_df = oc_df[['SC','SN','NS','SYM_TK','LS','EXPIRY','PCR-COI','CALL_CandleTime','CALL_OT','CALL_CD','CALL_TK','CALL_COI','CALL_PX','STRIKE','PUT_PX','PUT_COI','PUT_TK','PUT_CD','PUT_OT','PUT_CandleTime']]
    
    oc_df = oc_df.sort_values(by=['EXPIRY', 'STRIKE'], ascending=[True, True])
    oc_df.reset_index(drop=True, inplace=True)
    return oc_df

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
                   df.loc[condition, 'PCR-COI'] = round(abs(i['PE']['changeinOpenInterest'])/abs(i['CE']['changeinOpenInterest']),3) if abs(i['CE']['changeinOpenInterest']) > 0 else 0  
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
def update_pcr():
    global options_df
    df = options_df
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
    
# symbol = 'NIFTY 50' price = 20101.95
# symbol = 'NIFTY BANK' price = 45900.85
def check_option_list(symbol, price):
    global options_df, livePrices, token_list
    
    strike_step = sorted(options_df[(options_df['SYM_TK'] == symbol) & (options_df['STRIKE'] > price)]['STRIKE'].unique())[:2]
    strike_step = int(strike_step[1] - strike_step[0])
    strike_range = 5 * strike_step    
    div_num = 10 if strike_step < 100 else 100
        
    lower_bound = int((price-strike_range)/div_num)*div_num
    upper_bound = int((price+strike_range)/div_num)*div_num
    
    exist_tk = list(options_df[options_df['SN'] == symbol]['CALL_TK']) + list(options_df[options_df['SN'] == symbol]['PUT_TK'])   
    updated_tk = list(options_df[(options_df['SN'] == symbol) & (options_df['STRIKE'] >= lower_bound) & (options_df['STRIKE'] <= upper_bound)]['CALL_TK']) + list(options_df[(options_df['SN'] == symbol) & (options_df['STRIKE'] >= lower_bound) & (options_df['STRIKE'] <= upper_bound)]['PUT_TK'])
    
    exist_tk = ['4.1!' + item for item in exist_tk]
    updated_tk = ['4.1!' + item for item in updated_tk]
    
    list_to_subscribe = []
    list_to_unsubscribe = []
    for i in exist_tk:
        if i in updated_tk and i not in token_list:
            list_to_subscribe.append(i)
            token_list.append(i)
        elif i in token_list and i not in updated_tk:
            list_to_unsubscribe.append(i)
            token_list.remove(i)
            
    if len(list_to_subscribe) > 0:
        print(f"Subscribe - {list_to_subscribe}")
        ic_subscribeFeed(list_to_subscribe)
    if len(list_to_unsubscribe) > 0:
        print(f"Unsubscribe - {list_to_unsubscribe}")
        ic_unsubscribeFeed(list_to_unsubscribe)

# symbol = 'NIFTY 50' last_px = 20192.35
def strat_straddle_buy(symbol,last_px,signal_time):
    try:
        global options_df, livePrices, token_list
        funct_name = 'strat_straddle_buy'.upper()
        msg = {'status':'failure', 'remarks':'', 'data':''}
        send_whatsapp_msg(f"Straddle Strategy - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}",f"Initiating Straddle for {symbol} - {last_px} - {signal_time}")
        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        msg['data'] = 'Straddle Strategy'
        
        if symbol in ['NIFTY 50']:
            ticker = livePrices[livePrices['Token']==symbol]['Code'].values[0]
            num = int(json.load(open('config.json', 'r'))[symbol]['OPT#'])
            exp_week = int(json.load(open('config.json', 'r'))['EXP_WEEK'])
            
            call_strike = int(json.load(open('config.json', 'r'))[symbol]['CALL_STRIKE'])
            call_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='CE', duration=exp_week)
            call_opt = call_opt[call_opt['STRIKE'] == call_strike]
            call_sec_id = call_opt['TK'].values[0]
            call_sec_name = call_opt['CD'].values[0]
            
            put_strike = int(json.load(open('config.json', 'r'))[symbol]['PUT_STRIKE'])
            put_opt = ic_option_chain(ticker, underlying_price=last_px, option_type='PE', duration=exp_week)
            put_opt = put_opt[put_opt['STRIKE'] == put_strike]
            put_sec_id = put_opt['TK'].values[0]
            put_sec_name = put_opt['CD'].values[0]
            
            trade = {}
            sl_pts = 10
            tg_pts = 16
            qty = 1
            side = 'buy'
            
            if live_order == 'Y':
                ctrade = dh_post_exchange_order(ord_type='bo', exchange='FNO',
                                           security_id=call_sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=False, prod_type='')
                ptrade = dh_post_exchange_order(ord_type='bo', exchange='FNO',
                                           security_id=put_sec_id, side=side,
                                           qty=qty, entry_px=0, sl_px=0, trigger_px=0,
                                           tg_pts=tg_pts, sl_pts=sl_pts,
                                           amo=False, prod_type='')
                
                
                msg['data'] = msg['data'] + f"CALL - {call_sec_name} => "     
                msg['data'] = (msg['data'] + f"OrderID - {ctrade['data']['orderId']} | Status - {ctrade['data']['orderStatus']}") if ctrade['status'].lower() == 'success' else (msg['data'] + f"Order Failed - {ctrade['remarks']}")
                
                msg['data'] = msg['data'] + f"PUT - {put_sec_name} => "     
                msg['data'] = (msg['data'] + f"OrderID - {ptrade['data']['orderId']} | Status - {ptrade['data']['orderStatus']}") if ptrade['status'].lower() == 'success' else (msg['data'] + f"Order Failed - {ptrade['remarks']}")
                
                if call_trade['status'].lower() == 'success' and put_trade['status'].lower() == 'success':
                    msg['status'] = 'success'
            
            else:
                msg['status'] = 'success'
                msg['data'] = msg['data'] + f"=> Live Order not enabled, place manually! - {call_sec_name} => {put_sec_name}"
            
            send_whatsapp_msg(f"Straddle Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['data'])
    
    except Exception as e:
        err = str(e)
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err}"
        send_whatsapp_msg(f"Straddle Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])

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
        # print(err)
        return {'status':'failure','remarks':f"ema failure error - {err}",'data':''}
    
    return {'status':'success','remarks':'','data':'ema generation processed successfully'}

# tokens = token_list
def ic_subscribeFeed(tokens):
    for token in tokens:
        st = icici.subscribe_feeds(token)
        print(st)
        
def ic_unsubscribeFeed(tokens):
    for token in tokens:
        st=icici.unsubscribe_feeds(token)
        # st=icici.unsubscribe_feeds(token)
        print(st)

# ticks = {'ltt':'Mon Sep 18 09:17:00 2023','symbol':'4.1!NIFTY 50' , 'last':20192.35}
def on_ticks(ticks):
    global options_df, livePrices
    
    tick_symbol = ticks['symbol'][4:]
    tick_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    tick_px = ticks['last']
    
    print(f"{tick_symbol} - {tick_time} - {ticks['last']}")
    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == tick_symbol, 'CandleTime'] = tick_time
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Close'] = tick_px
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Open'] = ticks['open']
        livePrices.loc[livePrices['Token'] == tick_symbol, 'High'] = ticks['high']
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Low'] = ticks['low']

    else:
        new_row = {'CandleTime': ticks['ltt'], 'Token': tick_symbol, 'Close': tick_px,
                   'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low']}
        livePrices=pd.DataFrame(new_row, index = [0])
    
    if len(options_df) > 0:
        options_df.loc[options_df['CALL_TK'] == tick_symbol, 'CALL_CandleTime'] = tick_time
        options_df.loc[options_df['PUT_TK'] == tick_symbol, 'PUT_CandleTime'] = tick_time
        options_df.loc[options_df['CALL_TK'] == tick_symbol, 'CALL_PX'] = tick_px
        options_df.loc[options_df['PUT_TK'] == tick_symbol, 'PUT_PX'] = tick_px
    
    # tick_symbol = 'NIFTY 50' tick_px = 20192.85
    if tick_symbol in ['NIFTY 50','NIFTY BANK','NIFTY FIN SERVICE']:
        if tick_time.second == 10:
            option_check_thread = Thread(target=check_option_list,args=(tick_symbol,tick_px))
            option_check_thread.start()
        
        if tick_time.minute % 5 == 0 and tick_time.second == 5:
            # pcr_thread = Thread(target=update_pcr,args=(options_df))
            pcr_thread = Thread(target=update_pcr)
            pcr_thread.start()
    
        if tick_symbol == 'NIFTY 50':
            if tick_time.hour == 9 and tick_time.minute == 17 and tick_time.second == 0:
                straddle_thread = Thread(target=strat_straddle_buy,args=(tick_symbol,tick_px,tick_time))
                straddle_thread.start()   
        
            strat_ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
            if tick_time.minute % strat_ema_tf == 0 and tick_time.second == 5:
                ema_strat_thread = Thread(target=generate_ema_signal,args=(tick_symbol, strat_ema_tf)) 
                ema_strat_thread.start()
        
            monitor_thread = Thread(target=check_strategies,args=(tick_symbol, tick_px, tick_time)) 
            monitor_thread.start()
    
    # print(ticks)
    

def main():
    global options_df, livePrices, token_list
    session_id = ic_autologon()
    write_log('ic_watchlist','i',f"ICICI Session ID - {session_id}")  
    # print("Watchlist Live Started")
    try:  
        now = datetime.now()
        subscription_flag = 'N'
        token_list = []
        try:
            if os.path.exists(watchlist_file) == False:
                ic_update_watchlist(mode='C',num=0)
            livePrices = pd.read_csv(watchlist_file)
        except pd.errors.EmptyDataError:
            ic_update_watchlist(mode='C',num=0)
        livePrices = pd.read_csv(watchlist_file)
        
        if len(pd.read_csv(watchlist_file)) > 0:
            for index,row in livePrices.iterrows():
                if now.date() > datetime.strptime(row['CandleTime'], '%Y-%m-%d %H:%M:%S').date():
                    livePrices.at[index, 'PrevClose'] = row['Close']
                token_list.append(f"4.1!{row['Token']}")
        
        ticker_dict = {'NIFTY':0.0, 'CNXBAN':0.0, 'NIFFIN':0.0}
        for key,value in ticker_dict.items():
            ticker_dict[key] = livePrices[livePrices['Code']==key]['Close'].values[0]
        
        for key,value in ticker_dict.items():
            options_df = pd.concat([options_df,get_option_list(key, value)])
            
        while True:
            now = datetime.now()
            if now.time() < time(9,0) or now.time() > time(15,40):
                break
            
            if (now.time() >= time(9,14) and now.time() < time(15,35,0)):
                if subscription_flag=='N':
                    if os.path.exists(watchlist_file):
                        print('Subscribed')
                        # icici.user_id()
                        icici.ws_connect()
                        icici.on_ticks = on_ticks
                        # wl_df = pd.read_csv(watchlist_file)
                        # livePrices = wl_df

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
        err = str(e)
        write_log('ic_watchlist','e',f"{err}")  
        pass

if __name__ == '__main__':
    # test1()
    main()
    