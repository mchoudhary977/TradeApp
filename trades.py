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
                trade_df = trade_df[trade_df['Date']==now.strftime('%Y-%m-%d')]
                
                traded_orders_df = trade_df[trade_df['Active']=='Y'][trade_df['EntrySt'] == 'TRADED']
                pending_orders_df = pd.concat([trade_df[trade_df['Active']=='Y'][trade_df['EntrySt'] == 'TRANSIT'], trade_df[trade_df['Active']=='Y'][trade_df['EntrySt'] == 'PENDING']], ignore_index=True)
                non_active_orders_df = trade_df[trade_df['Active']=='N']
                live_order = json.load(open('config.json', 'r'))['LIVE_ORDER']
                
                stop_loss = 0
                target = 0 
                # traded_orders_df = trade_df
                # traded_orders_df['EntryPx'] = traded_orders_df['EntryPx'] + 50
                if len(traded_orders_df) > 0:
                    for index, row in traded_orders_df.iterrows():  
                        sec_id = row['SecID']
                        side = row['Side'].lower()
                        qty = row['Qty']
                        live_px = row['LivePx']
                        trail_pts = row['TrailPts']
                        if side == 'buy':
                            stop_loss = (row['EntryPx'] - row['TrailPts']) if row['StopLoss'] == 0 else row['StopLoss']
                            target = (row['EntryPx'] + row['TrailPts']) if row['Target'] == 0 else row['Target']
                        
                        elif side == 'sell':
                            stop_loss = (row['EntryPx'] + row['TrailPts']) if row['StopLoss'] == 0 else row['StopLoss']
                            target = (row['EntryPx'] - row['TrailPts']) if row['Target'] == 0 else row['Target']
                            
                        if stop_loss > 0 and target > 0 and ((side == 'buy' and live_px > 0 and live_px > target) or (side == 'sell' and live_px > 0 and live_px < target)):
                            target = target + trail_pts if side == 'buy' else target - trail_pts
                            stop_loss = stop_loss + trail_pts if side == 'buy' else stop_loss - trail_pts
                            
                            traded_orders_df.loc[index,'StopLoss'] = stop_loss
                            traded_orders_df.loc[index,'Target'] = target
                            
                            if live_order == 'Y':
                                if row['ExitID'] == 'XXXX':
                                    sl_side = 'sell' if side == 'buy' else 'buy'
                                    sl_trade = dh_post_exchange_order(ord_type='lmt', exchange='FNO',
                                                               security_id=sec_id, side=sl_side,
                                                               qty=qty, entry_px=stop_loss, sl_px=0, trigger_px=0, 
                                                               tg_pts=0, sl_pts=0, 
                                                               amo=False, prod_type='')
                                    
                                    if sl_trade['status'].lower() == 'success':
                                        traded_orders_df.loc[index,'ExitID'] = sl_trade['data']['orderId']
                                        traded_orders_df.loc[index,'ExitSt'] = sl_trade['data']['orderStatus']
                                else:
                                    sl_order = row['ExitID']
                                    dh_modify_order(sl_order, stop_loss, qty)
                            else:
                                traded_orders_df.loc[index,'ExitID'] = f"test_sl_{sec_id}"
                        
                        if stop_loss > 0 and target > 0 and ((side == 'buy' and live_px > 0 and live_px < stop_loss) or (side == 'sell' and live_px > 0 and live_px > stop_loss)):
                            traded_orders_df.loc[index,'Active'] = 'N'
                
                if len(pending_orders_df) > 0:
                    tm.sleep(20)
                    order_list = dh_get_orders()
                    if order_list['status'] == 'success':
                        order_df = order_list['data']
                        
                        for pindex, prow in pending_orders_df.iterrows():
                            for oindex, orow in order_df.iterrows():
                                if str(prow['EntryID'])==str(orow['orderId']):
                                    pending_orders_df.loc[pindex,'EntrySt'] = orow['orderStatus']
                                
                                if str(prow['ExitID'])==str(orow['orderId']):
                                    pending_orders_df.loc[pindex,'ExitSt'] = orow['orderStatus']
                
                final_df = pd.concat([traded_orders_df, pending_orders_df, non_active_orders_df], ignore_index=True)
                final_df.to_csv(trade_file,index=False)
                        
        except Exception as e:
            err = str(e)
            send_whatsapp_msg(f"Trade Failure Alert - {now.strftime('%Y-%m-%d %H:%M:%S')}", err)
            pass

        tm.sleep(1)