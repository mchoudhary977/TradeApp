# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 16:50:09 2023

@author: mchou
"""

# from ic_functions import *
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


def main():
    funct_name = 'tades main'.upper()
    msg = {'status':'failure', 'remarks':'', 'data':''}
    
    trade_file = 'Trades.csv'
    while True:
        try:
            now = datetime.now()
            if now.time() < time(9,0) or now.time() > time(15,20):
                break            
            if os.path.exists(trade_file) == True:
                trade_df = pd.read_csv(trade_file)
                if len(trade_df) > 0:
                    trade_df = trade_df[trade_df['TradeStatus']=='OPEN']
                    for index, row in trade_df.iterrows():
                        if row['EntryStatus'].lower() == 'traded':
                            print(trade_df.loc[index,'Symbol'])                       
                        if str(row['SLOrderID']).lower() == 'nan':
                            # place sl order 
                            print(trade_df.loc[index,'Symbol'])  
                            sec_id = row['SecurityID']
                            side = 'sell' if row['Side'] == 'BUY' else 'buy'
                            
                            entry_price = row['EntryPrice']
                            
                            if str(entry_price).lower() == 'nan':
                                ord_info = dh_get_trades()
                                ord_info = ord_info['data'] if ord_info['data'] is not None else ''
                                ord_info = ord_info[ord_info['orderId'] == row['EntryOrderID']]
                                # ord_info = ord_info[ord_info['orderId'] == '3223090135751']
                                
                                entry_price = ord_info['tradedPrice'].values[0] if len(ord_info)>0 else 0
                            
                            if entry_price > 0:
                                price = entry_price - row['StopLossPoints'] if side == 'sell' else entry_price + row['StopLossPoints']
                                trigger_price = price + 0.7 if side == 'sell' else price - 0.7
                                sl_order = dh_post_exchange_order(ord_type='sl', exchange='FNO', 
                                                       security_id=sec_id, 
                                                       side='buy',qty=1, entry_px=price, 
                                                       sl_px=0, trigger_px=trigger_price, 
                                                       tg_pts=0, sl_pts=0, 
                                                       amo=False, prod_type='')
                                
                                if sl_order['status'].lower() == 'success':
                                    trade_df.loc[index,'SLOrderID'] = sl_order['data']['orderId']
                                    trade_df.loc[index,'StopLossPrice'] = price
                                    trade_df.loc[index,'SLStatus'] = sl_order['data']['orderStatus']
                        else:
                            print('compare prices')
                    trade_df.to_csv(trade_file,index=False)                           
        except Exception as e:
            err = str(e)
            send_whatsapp_msg(f"Trade Failure Alert - {now.strftime('%Y-%m-%d %H:%M:%S')}", err)
            pass

        tm.sleep(1)