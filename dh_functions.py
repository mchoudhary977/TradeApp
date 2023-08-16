from dhanhq import dhanhq
import json
import pandas as pd 

dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],
              json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])

# dhan.get_fund_limits()
# dh_place_mkt_order('NFO',52337,'buy',50,0)

def dh_get_positions():
    st = dhan.get_positions()
    if st['status']=='success':
        if len(st['data']) > 0:
            return {'status':'SUCCESS', 'data':st['data']}
    return {'status':'FAILURE', 'data':'No data returned'}

    
def dh_place_mkt_order(exchange,security_id,buy_sell,quantity,sl_price):
    drv_expiry_date=None
    drv_options_type=None
    drv_strike_price=None
    exchange_segment = dhan.NSE
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
                                    after_market_order=True,
                                    amo_time='OPEN',
                                    bo_profit_value=0,
                                    bo_stop_loss_Value=0,
                                    drv_expiry_date=drv_expiry_date,
                                    drv_options_type=drv_options_type,
                                    drv_strike_price=drv_strike_price  
                                    )
    return order_st 

# dhan.get_order_list()    
# dhan.get_order_by_id('6523081610313')
# order_id = '6523081610313'
# ModifyOrder('6523081610313',price=32,quantity=100)
def dh_modify_order(order_id,price,quantity):    
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
    st = dhan.cancel_order(order_id)
    return st 