from dhanhq import dhanhq
import json
import pandas as pd 
import os 
from log_function import * 
from datetime import datetime, time, timedelta
import time as tm

# dhan.get_fund_limits()
# dh_place_mkt_order('NFO',52337,'buy',50,0)

def dh_get_positions():
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    st = dhan.get_positions()
    if st['status']=='success':
        if len(st['data']) > 0:
            data = st['data']            
            df = pd.DataFrame(data)   
            df.to_csv('Positions.csv',index=False)
            return {'status':'success', 'data':df}
        else:
            return {'status':'success', 'data':None}
    return {'status':'failure', 'data':None}
    

def dh_get_orders():
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    st = dhan.get_order_list()
    if st['status']=='success':
        if len(st['data']) > 0:
            data = st['data']
            df = pd.DataFrame(data)
            df.to_csv('Orders.csv',index=False)
            return {'status':'success', 'data':df}
        else:
            return {'status':'success', 'data':None}
    return {'status':'failure', 'data':None}
 
# dh_get_order_id(order_id='252308287831')
def dh_get_order_id(order_id):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    st = dhan.get_order_by_id(order_id)
    if st['status']=='success':
        if len(st['data']) > 0:
            data = st['data']
        return {'status':'success','data':data}

# type(opt['TK'])
# dhan.get_order_list()    
# dhan.get_order_by_id('6523081610313')
# order_id = '6523081610313'
# dh_modify_order('6523081610313',price=32,quantity=100)
def dh_modify_order(order_id,price,quantity):  
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    # Modify order given order id
    dhan.modify_order(order_id=order_id,
                      order_type=dhan.LIMIT,
                      leg_name='ENTRY_LEG',
                      quantity=quantity,
                      price=round(price,1),
                      disclosed_quantity=0,
                      trigger_price=round(price,1),
                      validity=dhan.DAY
                      )

# s=dh_cancel_order(order_id = '6523081610313')
def dh_cancel_orders(order_id=None):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    cancelled_orders = []
    if order_id is None:
        orders = dh_get_orders()['data']
        orders = orders[orders['orderStatus'].isin(['TRANSIT','PENDING'])]
        # print(orders)      
        for index, row in orders.iterrows():
            st = dhan.cancel_order(row['orderId'])
            if st['status'].lower() == 'success':
                cancelled_orders.append(st['data']['orderId'])
    else:
        st = dhan.cancel_order(order_id)
        if st['status'].lower() == 'success':
            cancelled_orders.append(st['data']['orderId'])
    return {'status':'success','data':cancelled_orders}


def dh_post_exchange_order(order_type='sl',exchange='FNO',security_id=1234,buy_sell='buy',quantity=50,sl_price=0,tg_point=10,sl_point=20):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    ret_msg = {}
    try:
        if order_type == 'mkt':
            order_info = dh_place_order(dhan,order_type,exchange,security_id,buy_sell,quantity,sl_price=0)
            # order_info = dh_place_mkt_order(exchange,security_id,buy_sell,quantity,sl_price=0)
            if order_info['status'].lower() == 'success':
                tm.sleep(2)
                order_id = order_info['data']['orderId']
                order_detail = dh_get_order_id(order_id)
                if order_detail['status'].lower() == 'success':
                    if order_detail['data']['orderStatus'].lower() == 'traded':
                        ret_msg = {'status':'success','data':order_detail['data']}
                else:
                    ret_msg = {'status':'success','data':{'order_id':order_info['data']['orderId'],'message':'CHECK MARKET ORDER STATUS ON BROKER PORTAL!!!'}}
            else:
                ret_msg = {'status':'failure','data':{'message':order_info['remarks']['message']}}
        elif order_type == 'sl':
            ord_type = 'mkt'
            order_info = dh_place_order(dhan,ord_type,exchange,security_id,buy_sell,quantity,sl_price=0)
            # order_info = dh_place_mkt_order(exchange,security_id,buy_sell,quantity,sl_price=0)
            # print(order_info)
            if order_info['status'].lower() == 'success':
                tm.sleep(2)
                order_id = order_info['data']['orderId']
                order_detail = dh_get_order_id(order_id)
                # print(f"OrderDetail -> {order_detail}")
                if order_detail['status'].lower() == 'success':
                    if order_detail['data']['orderStatus'].lower() == 'pending':
                    # if order_detail['data']['orderStatus'].lower() == 'traded':
                        entry_order = order_detail['data']
                        # place stop loss order 
                        sl_price = entry_order['price'] - sl_point if entry_order['transactionType'] == 'BUY' else entry_order['price'] + sl_point
                        sl_buy_sell = 'sell' if entry_order['transactionType'] == 'BUY' else 'buy'
                        sl_order = dh_place_order(dhan,order_type,exchange,security_id,sl_buy_sell,quantity,sl_price)
                        # print(sl_order)
                        if sl_order['status'].lower() == 'success':
                            entry_order['sl_orderId'] = sl_order['data']['orderId']
                            entry_order['sl_price'] = sl_order['data']['triggerPrice']
                            ret_msg = {'status':'success','data':entry_order}
                        else:
                            ret_msg = {'status':'failure','data':{'message':sl_order['remarks']['message']}}
                    else:
                        order_detail['message']='STOPLOSS ORDER NOT PLACED!!!'
                        ret_msg = {'status':'success','data':order_detail}
                else:
                    ret_msg = {'status':'failure','data':{'order_id':order_info['data']['orderId'],'message':'CHECK ENTRY ORDER AND STOPLOSS ORDER ON BROKER PORTAL!!!'}}
            else:
                ret_msg = {'status':'failure','data':{'message':order_info['remarks']['message']}}
                
        elif order_type == 'bo':
            order_info = dh_place_order(dhan,order_type,exchange,security_id,buy_sell,quantity,sl_price=0,tg_point=10,sl_point=20)
            if order_info['status'].lower() == 'success':
                tm.sleep(2)
                order_id = order_info['data']['orderId']
                order_detail = dh_get_order_id(order_id)
                if order_detail['status'].lower() == 'success':
                    if order_detail['data']['orderStatus'].lower() == 'traded':
                        ret_msg = {'status':'success','data':order_detail['data']}
                else:
                    ret_msg = {'status':'success','data':{'order_id':order_info['data']['orderId'],'message':'CHECK BRACKET ORDER STATUS ON BROKER PORTAL!!!'}}
            else:
                ret_msg = {'status':'failure','data':{'message':order_info['remarks']['message']}}
                
        return ret_msg
    except Exception as e:
        err = str(e)
        return {'status':'failure','data':{'message':err}}

# dh_post_exchange_order(order_type='sl',exchange='FNO',security_id=61547,buy_sell='buy',quantity=50,sl_price=0,tg_point=10,sl_point=20)
# opt=ic_option_chain(ticker='NIFTY', underlying_price=19370, option_type="PE", duration=0).iloc[-3]
# dh_place_mkt_order(exchange='FNO',security_id=61547,buy_sell='buy',quantity=40,sl_price=0)
def dh_place_order(dhan,order_type,exchange,security_id,buy_sell,quantity,sl_price=0,product_type='',tg_point=0,sl_point=0):
    drv_expiry_date=None
    drv_options_type=None
    drv_strike_price=None   		
    exchange_segment = dhan.NSE
    tag_val = f"{buy_sell}-{security_id}-{quantity}"
    security_id = int(security_id)
    order_type = dhan.SLM if order_type=='sl' else dhan.MARKET
    product_type = dhan.BO if order_type == 'bo' else (dhan.CNC if product_type == 'DAY' else dhan.INTRA)
    t_type = dhan.BUY if buy_sell == 'buy' else dhan.SELL
    price = sl_price-1 if order_type == 'sl' else 0 
    trigger_price = sl_price if order_type == 'sl' and buy_sell =='buy' else (sl_price if order_type == 'sl' and buy_sell =='sell' else 0)
   
    if exchange=='FNO':
        instrument = pd.read_csv('dhan.csv')
        instrument = instrument[instrument['SEM_SMST_SECURITY_ID']==security_id]
        lot_size = int(instrument['SEM_LOT_UNITS'].values[0])
        drv_expiry_date=instrument['SEM_EXPIRY_DATE'].values[0]
        drv_options_type='PUT' if instrument['SEM_OPTION_TYPE'].values[0] == 'PE' else 'CALL'
        drv_strike_price=int(instrument['SEM_STRIKE_PRICE'].values[0])
        exchange_segment = dhan.FNO
    
    order_st = dhan.place_order(tag=tag_val,
                                    transaction_type=t_type,
                                    exchange_segment=exchange_segment,
                                    product_type=product_type,
                                    order_type=order_type,
                                    validity='DAY',
                                    security_id=str(security_id),
                                    quantity=quantity,
                                    disclosed_quantity=0,
                                    price=price,
                                    trigger_price=trigger_price,
                                    after_market_order=True,
                                    amo_time='OPEN',
                                    bo_profit_value=tg_point,
                                    bo_stop_loss_Value=sl_point,
                                    drv_expiry_date=drv_expiry_date,
                                    drv_options_type=drv_options_type,
                                    drv_strike_price=drv_strike_price  
                                    )
    return order_st 
        

def dh_place_mkt_order(dhan,exchange,security_id,buy_sell,quantity,sl_price=0,tg_point=0,sl_point=0):
    try:
        # dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
        drv_expiry_date=None
        drv_options_type=None
        drv_strike_price=None   							
        exchange_segment = dhan.NSE
        tag_val = f"{buy_sell}-{security_id}-{quantity}"
        security_id = int(security_id)
        if exchange=='FNO':
            instrument = pd.read_csv('dhan.csv')
            instrument = instrument[instrument['SEM_SMST_SECURITY_ID']==security_id]
            # lot_size = instrument['SEM_LOT_UNITS'].values[0]
            drv_expiry_date=instrument['SEM_EXPIRY_DATE'].values[0]
            drv_options_type='PUT' if instrument['SEM_OPTION_TYPE'].values[0] == 'PE' else 'CALL'
            drv_strike_price=int(instrument['SEM_STRIKE_PRICE'].values[0])
            exchange_segment = dhan.FNO
            
        t_type = dhan.BUY if buy_sell == 'buy' else dhan.SELL
        
        # Below line is only for testing purpose, comment when prod live
        order_st = {'status':'success','data':{'orderId':'test_01234567','tradingSymbol':instrument['SEM_CUSTOM_SYMBOL'].values[0]}}
        order_st = dhan.place_order(tag=tag_val,
                                        transaction_type=t_type,
                                        exchange_segment=exchange_segment,
                                        product_type=dhan.INTRA,
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
        return{'status':'failure','data':{'message':err}}
        write_log('dh_place_mkt_order','e',err)
        # print(f"error - {err}")
        
    

# opt=ic_option_chain(ticker='NIFTY', underlying_price=19370, option_type="PE", duration=1).iloc[-3]
# dh_place_bo_order(exchange='NFO',security_id=61756,buy_sell='buy',quantity=50,sl_point=5,tg_point=20,sl_price=0)
def dh_place_bo_order(dhan,exchange,security_id,buy_sell,quantity,sl_point=10,tg_point=20,sl_price=0):
    try:
        # dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
        drv_expiry_date=None
        drv_options_type=None
        drv_strike_price=None   							
        exchange_segment = dhan.NSE
        tag_val = f"{buy_sell}-{security_id}-{quantity}"
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
        # order_st = {'status':'success','data':{'orderId':'test_01234567'}}
        order_st = dhan.place_order(tag=tag_val,
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
                                        after_market_order=False,
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
        return{'status':'failure','remarks':{'message':err}}
        write_log('dh_place_bo_order','e',err)
        # print(f"error - {err}")

# st = dh_place_bo_order(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=5,tg_point=20,sl_price=0)

