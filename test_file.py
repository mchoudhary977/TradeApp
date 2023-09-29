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

dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])

type(dhan)
dhan.get_order_list()

dhan.SLM
# ticks = {'ltt':'Sat Sep 25 09:17:00 2023','symbol':'4.1!NIFTY 50', 'last': 19825.35}
# symbol = 'NIFTY 50' last_px = 19695.25 signal_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
# ticks = {'ltt':'Sat Sep 25 09:17:00 2023','symbol':'4.1!NIFTY 50', 'last': 19825.35}
# symbol = 'NIFTY 50' last_px = 19699.25 signal_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")

def strat_round_entries(symbol,last_px,signal_time):
    global icici, livePrices, options_df, orders_df, strat_trades_df, token_list, icici_scrips, watchlist_file, options_file, oi_pcr_file, trade_file, ema_signal
    now = datetime.now()
    funct_name = 'strat_round_entries'.upper()
    strategy = 'Round'
    msg = {'status':'failure', 'remarks':'', 'data':''}
    exit_strategy = 'N'
    
    try:      
        live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
        round_strat_flag = json.load(open('config.json', 'r'))['ROUND_STRAT_FLAG']
        
        if round_strat_flag == 'Y':
            ticker = 'NIFTY' if symbol == 'NIFTY 50' else symbol
            round_st = {'price':0.0, 'entry':0.0, 'signal':'green', 'ordcnt': 0, 
                        'call':'N', 'put':'N'} 
            round_st['price'] = last_px
            
            if symbol == 'NIFTY 50':
                strike_step = 50 if (ticker == 'NIFTY' or ticker == 'NIFFIN') else 100
                atm_strike = int(round(last_px/strike_step,0))*strike_step
                
                # round_strat_flag = json.load(open('config.json', 'r'))['ROUND_STRAT_FLAG']
                if last_px > (atm_strike-1) and last_px < (atm_strike+1):  
                    if len(strat_trades_df) == 0 or (len(strat_trades_df) > 0 and len(strat_trades_df[(strat_trades_df['Strategy'] == strategy) & (strat_trades_df['Active'] == 'Y')]) == 0):
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
                        
                        ord_type = 'bo'
                        amo = False
                        call_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                                 now, ord_type, call_sec_id, call_sec_name, lot_size,
                                                 amo))
                        
                        put_thread = Thread(target=place_strategy_order,args=(strategy, symbol, last_px,
                                                 now, ord_type, put_sec_id, put_sec_name, lot_size,
                                                 amo))
                        
                        call_thread.start()
                        put_thread.start()
                        
    except Exception as e:
        err = str(e)
        printLog('e',f"{funct_name} - {err}")
        msg['remarks'] = funct_name + ' - ' + msg['remarks'] + f"Order Placement Error - {err} "
        send_whatsapp_msg(f"{funct_name} Failure Alert - {signal_time.strftime('%Y-%m-%d %H:%M:%S')}", msg['remarks'])
        pass

# strat_trades_df['Qty'] = 50