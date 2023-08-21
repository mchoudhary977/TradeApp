from dhanhq import dhanhq
import json
import pandas as pd 
import os 

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
            return {'status':'SUCCESS', 'data':df}
    return {'status':'FAILURE', 'data':'No data returned'}
    

def dh_get_orders():
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    st = dhan.get_order_list()
    if st['status']=='success':
        if len(st['data']) > 0:
            data = st['data']
            df = pd.DataFrame(data)
            df.to_csv('Orders.csv',index=False)
            return {'status':'SUCCESS', 'data':df}
    return {'status':'FAILURE', 'data':'No data returned'}
 
# dh_get_order_id
def dh_get_order_id(order_id):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    st = dhan.get_order_by_id(order_id)
    if st['status']=='success':
        if len(st['data']) > 0:
            data = st['data']
        return {'status':'SUCCESS','data':data}
    
# opt=ic_option_chain(ticker='NIFTY', underlying_price=19370, option_type="PE", duration=0).iloc[-3]
# dh_place_mkt_order(exchange='NFO',security_id=52337,buy_sell='buy',quantity=50,sl_price=0)
def dh_place_mkt_order(exchange,security_id,buy_sell,quantity,sl_price=0):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    drv_expiry_date=None
    drv_options_type=None
    drv_strike_price=None
    exchange_segment = dhan.NSE
    tag = f"{buy_sell}-{security_id}-{quantity}"
    if exchange=='NFO':
        instrument = pd.read_csv('dhan.csv')
        instrument = instrument[instrument['SEM_SMST_SECURITY_ID']==security_id]
        # lot_size = instrument['SEM_LOT_UNITS'].values[0]
        drv_expiry_date=instrument['SEM_EXPIRY_DATE'].values[0]
        drv_options_type='PUT' if instrument['SEM_OPTION_TYPE'].values[0] == 'PE' else 'CALL'
        drv_strike_price=instrument['SEM_STRIKE_PRICE'].values[0]
        exchange_segment = dhan.FNO
    
    t_type = dhan.BUY if buy_sell == 'buy' else dhan.SELL
    order_st = dhan.place_order(tag='',
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
                                    after_market_order=False,
                                    amo_time='OPEN',
                                    bo_profit_value=0,
                                    bo_stop_loss_Value=0,
                                    drv_expiry_date=drv_expiry_date,
                                    drv_options_type=drv_options_type,
                                    drv_strike_price=drv_strike_price  
                                    )
    return order_st 

# opt=ic_option_chain(ticker='NIFTY', underlying_price=19370, option_type="PE", duration=1).iloc[-3]
# dh_place_bo_order(exchange='NFO',security_id=61756,buy_sell='buy',quantity=50,sl_point=5,tg_point=20,sl_price=0)
def dh_place_bo_order(exchange,security_id,buy_sell,quantity,sl_point=5,tg_point=20,sl_price=0):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    drv_expiry_date=None
    drv_options_type=None
    drv_strike_price=None
    security_id = int(security_id)
    exchange_segment = dhan.NSE
    tag = f"{buy_sell}-{security_id}-{quantity}"
    if exchange=='NFO':
        instrument = pd.read_csv('dhan.csv')
        instrument = instrument[instrument['SEM_SMST_SECURITY_ID']==security_id]
        # lot_size = instrument['SEM_LOT_UNITS'].values[0]
        drv_expiry_date=instrument['SEM_EXPIRY_DATE'].values[0]
        drv_options_type='PUT' if instrument['SEM_OPTION_TYPE'].values[0] == 'PE' else 'CALL'
        drv_strike_price=instrument['SEM_STRIKE_PRICE'].values[0]
        exchange_segment = dhan.FNO
    
    t_type = dhan.BUY if buy_sell == 'buy' else dhan.SELL
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
                                    after_market_order=False,
                                    amo_time='OPEN',
                                    bo_profit_value=tg_point,
                                    bo_stop_loss_Value=sl_point,
                                    drv_expiry_date=drv_expiry_date,
                                    drv_options_type=drv_options_type,
                                    drv_strike_price=drv_strike_price  
                                    )
    return order_st 

# st = dh_place_bo_order(exchange='NFO',security_id=opt['TK'],buy_sell='buy',quantity=50,sl_point=5,tg_point=20,sl_price=0)

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
def dh_cancel_order(order_id):
    dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])
    st = dhan.cancel_order(order_id)
    return st 