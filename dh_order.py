from dhanhq import dhanhq
import json
import pandas as pd 

dhan = dhanhq(json.load(open('config.json', 'r'))['DHAN_CLIENT_ID'],
              json.load(open('config.json', 'r'))['DHAN_ACCESS_TK'])

dhan.get_fund_limits()
dhan_scrip=pd.read_csv('https://images.dhan.co/api-data/api-scrip-master.csv')

c1=dhan_scrip[dhan_scrip['SEM_SMST_SECURITY_ID']==52337]
drv_expiry_date=c1['SEM_EXPIRY_DATE'].values[0]
drv_options_type='PUT'
drv_strike_price=c1['SEM_STRIKE_PRICE'].values[0]

st = dhan.place_order(
    tag='',
    transaction_type=dhan.BUY,
    exchange_segment=dhan.FNO,
    product_type=dhan.INTRA,
    order_type=dhan.MARKET,
    validity='DAY',
    security_id='52337',
    quantity=50,
    disclosed_quantity=0,
    price=0,
    trigger_price=0,
    after_market_order=True,
    amo_time='OPEN',
    bo_profit_value=0,
    bo_stop_loss_Value=0,
    drv_expiry_date=None,
    drv_options_type=None,
    drv_strike_price=None    
)

ord1 = dhan.get_order_list()

dhan.cancel_order(order_id='6523081610277')

security_id = 52337
st=placeMKTOrder(exchange='NFO',security_id=52337,buy_sell='buy',quantity=50,sl_price=0)

def placeMKTOrder(exchange,security_id,buy_sell,quantity,sl_price):
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
    
    
    if buy_sell == 'buy':
        t_type=dhan.BUY
        t_type_sl=dhan.SELL
        
        
    c1=dhan_scrip[dhan_scrip['SEM_SMST_SECURITY_ID']==52337]
    drv_expiry_date=c1['SEM_EXPIRY_DATE'].values[0]
    drv_options_type='PUT'
    drv_strike_price=c1['SEM_STRIKE_PRICE'].values[0]
    
    
def placeSLOrder(security_id,buy_sell,quantity,sl_price):    
    # Place an intraday stop loss order on NSE - handles market orders converted to limit orders
    if buy_sell == "buy":
        t_type=dhan.BUY
        t_type_sl=dhan.SELL
    elif buy_sell == "sell":
        t_type=dhan.SELL
        t_type_sl=dhan.BUY
    market_order = dhan.place_order(tag='',
                                    transaction_type=dhan.BUY,
                                    exchange_segment=dhan.NSE,
                                    product_type=dhan.INTRA,
                                    order_type=dhan.MARKET,
                                    validity='DAY',
                                    security_id=52337,
                                    quantity=50,
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
    a = 0
    while a < 10:
        try:
            order_list = dhan.get_order_list()
            break
        except:
            print("can't get orders..retrying")
            a+=1
    for order in order_list:
        if order["orderId"]==market_order["orderId"]:
            if order["status"]=="TRADED":
                dhan.place_order(tag='',
                                 transaction_type=t_type_sl,
                                 exchange_segment=dhan.NSE,
                                 product_type=dhan.INTRA,
                                 order_type=dhan.SL,
                                 validity='DAY',
                                 security_id=security_id,
                                 quantity=quantity,
                                 disclosed_quantity=0,
                                 price=sl_price,
                                 trigger_price=sl_price,
                                 after_market_order=False,
                                 amo_time='OPEN',
                                 bo_profit_value=0,
                                 bo_stop_loss_Value=0,
                                 drv_expiry_date=None,
                                 drv_options_type=None,
                                 drv_strike_price=None  
                                 )
            else:
                dhan.cancel_order(order_id=market_order['orderId'])

def ModifyOrder(order_id,price,quantity):    
    # Modify order given order id
    dhan.modify_order(order_id=order_id,
                      order_type=dhan.SL,
                      leg_name='',
                      quantity=quantity,
                      price=round(price,1),
                      disclosed_quantity=0,
                      trigger_price=round(price,1),
                      validity=dhan.DAY
                      )