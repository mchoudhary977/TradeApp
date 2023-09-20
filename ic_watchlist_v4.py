import http.client
import base64 
import socketio
from datetime import datetime, timedelta 
import json
import time as tm
import pandas as pd
import numpy as np
from threading import Thread

from ic_functions import * 
from dh_functions import * 
from strategies import * 
from wa_notifications import * 
from log_function import * 

# Variables
livePrices = pd.DataFrame()
options_df = pd.DataFrame()
strat_trades_df = pd.DataFrame() 
token_list = []

# Python Socket IO Client
sio = socketio.Client()  

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
            # filtered_dates.append(date.astype('datetime64[D]').astype(str))
            filtered_dates.append(str(date.date()))
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
    now = datetime.now()
    
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
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Options Subscribed - {list_to_subscribe}")
        ic_subscribeFeed(list_to_subscribe)
    # if len(list_to_unsubscribe) > 0:
    #     print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Options UnSubscribed - {list_to_subscribe}")
    #     ic_unsubscribeFeed(list_to_unsubscribe)

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

def parse_market_depth(self, data, exchange):
    depth = []
    counter = 0
    for lis in data:
        counter += 1
        dict = {}
        if exchange == '1':
            dict["BestBuyRate-"+str(counter)] = lis[0]
            dict["BestBuyQty-"+str(counter)] = lis[1]
            dict["BestSellRate-"+str(counter)] = lis[2]
            dict["BestSellQty-"+str(counter)] = lis[3]
            depth.append(dict)
        else:
            dict["BestBuyRate-"+str(counter)] = lis[0]
            dict["BestBuyQty-"+str(counter)] = lis[1]
            dict["BuyNoOfOrders-"+str(counter)] = lis[2]
            dict["BuyFlag-"+str(counter)] = lis[3]
            dict["BestSellRate-"+str(counter)] = lis[4]
            dict["BestSellQty-"+str(counter)] = lis[5]
            dict["SellNoOfOrders-"+str(counter)] = lis[6]
            dict["SellFlag-"+str(counter)] = lis[7]
            depth.append(dict)
    return depth


# parsing logic
def parse_data(data):
    if data and type(data) == list and len(data) > 0 and type(data[0]) == str and "!" not in data[0]:
        order_dict = {}
        order_dict["sourceNumber"] = data[0]                            #Source Number
        order_dict["group"] = data[1]                                   #Group
        order_dict["userId"] = data[2]                                  #User_id
        order_dict["key"] = data[3]                                     #Key
        order_dict["messageLength"] = data[4]                           #Message Length
        order_dict["requestType"] = data[5]                             #Request Type
        order_dict["messageSequence"] = data[6]                         #Message Sequence
        order_dict["messageDate"] = data[7]                             #Date
        order_dict["messageTime"] = data[8]                             #Time
        order_dict["messageCategory"] = data[9]                         #Message Category
        order_dict["messagePriority"] = data[10]                        #Priority
        order_dict["messageType"] = data[11]                            #Message Type
        order_dict["orderMatchAccount"] = data[12]                      #Order Match Account
        order_dict["orderExchangeCode"] = data[13]                      #Exchange Code
        if data[11] == '4' or data[11] == '5':
            order_dict["stockCode"] = data[14]                     #Stock Code
            order_dict["orderFlow"] = tux_to_user_value['orderFlow'].get(str(data[15]).upper(),str(data[15]))                          # Order Flow
            order_dict["limitMarketFlag"] = tux_to_user_value['limitMarketFlag'].get(str(data[16]).upper(),str(data[16]))                    #Limit Market Flag
            order_dict["orderType"] = tux_to_user_value['orderType'].get(str(data[17]).upper(),str(data[17]))                          #OrderType
            order_dict["orderLimitRate"] = data[18]                     #Limit Rate
            order_dict["productType"] = tux_to_user_value['productType'].get(str(data[19]).upper(),str(data[19]))                        #Product Type
            order_dict["orderStatus"] = tux_to_user_value['orderStatus'].get(str(data[20]).upper(),str(data[20]))                        # Order Status
            order_dict["orderDate"] = data[21]                          #Order  Date
            order_dict["orderTradeDate"] = data[22]                     #Trade Date
            order_dict["orderReference"] = data[23]                     #Order Reference
            order_dict["orderQuantity"] = data[24]                      #Order Quantity
            order_dict["openQuantity"] = data[25]                       #Open Quantity
            order_dict["orderExecutedQuantity"] = data[26]              #Order Executed Quantity
            order_dict["cancelledQuantity"] = data[27]                  #Cancelled Quantity
            order_dict["expiredQuantity"] = data[28]                    #Expired Quantity
            order_dict["orderDisclosedQuantity"] = data[29]             # Order Disclosed Quantity
            order_dict["orderStopLossTrigger"] = data[30]               #Order Stop Loss Triger
            order_dict["orderSquareFlag"] = data[31]                    #Order Square Flag
            order_dict["orderAmountBlocked"] = data[32]                 # Order Amount Blocked
            order_dict["orderPipeId"] = data[33]                        #Order PipeId
            order_dict["channel"] = data[34]                            #Channel
            order_dict["exchangeSegmentCode"] = data[35]                #Exchange Segment Code
            order_dict["exchangeSegmentSettlement"] = data[36]          #Exchange Segment Settlement 
            order_dict["segmentDescription"] = data[37]                 #Segment Description
            order_dict["marginSquareOffMode"] = data[38]                #Margin Square Off Mode
            order_dict["orderValidDate"] = data[40]                     #Order Valid Date
            order_dict["orderMessageCharacter"] = data[41]              #Order Message Character
            order_dict["averageExecutedRate"] = data[42]                #Average Exited Rate
            order_dict["orderPriceImprovementFlag"] = data[43]          #Order Price Flag
            order_dict["orderMBCFlag"] = data[44]                       #Order MBC Flag
            order_dict["orderLimitOffset"] = data[45]                   #Order Limit Offset
            order_dict["systemPartnerCode"] = data[46]                  #System Partner Code
        elif data[11] == '6' or data[11] == '7':
            order_dict["stockCode"] = data[14]                         #stockCode
            order_dict["productType"] =  tux_to_user_value['productType'].get(str(data[15]).upper(),str(data[15]))                        #Product Type
            order_dict["optionType"] = tux_to_user_value['optionType'].get(str(data[16]).upper(),str(data[16]))                         #Option Type
            order_dict["exerciseType"] = data[17]                       #Exercise Type
            order_dict["strikePrice"] = data[18]                        #Strike Price
            order_dict["expiryDate"] = data[19]                         #Expiry Date
            order_dict["orderValidDate"] = data[20]                     #Order Valid Date
            order_dict["orderFlow"] = tux_to_user_value['orderFlow'].get(str(data[21]).upper(),str(data[21]))                          #Order  Flow
            order_dict["limitMarketFlag"] = tux_to_user_value['limitMarketFlag'].get(str(data[22]).upper(),str(data[22]))                    #Limit Market Flag
            order_dict["orderType"] = tux_to_user_value['orderType'].get(str(data[23]).upper(),str(data[23]))                          #Order Type
            order_dict["limitRate"] = data[24]                          #Limit Rate
            order_dict["orderStatus"] = tux_to_user_value['orderStatus'].get(str(data[25]).upper(),str(data[25]))                        #Order Status
            order_dict["orderReference"] = data[26]                     #Order Reference
            order_dict["orderTotalQuantity"] = data[27]                 #Order Total Quantity
            order_dict["executedQuantity"] = data[28]                   #Executed Quantity
            order_dict["cancelledQuantity"] = data[29]                  #Cancelled Quantity
            order_dict["expiredQuantity"] = data[30]                    #Expired Quantity
            order_dict["stopLossTrigger"] = data[31]                    #Stop Loss Trigger
            order_dict["specialFlag"] = data[32]                        #Special Flag
            order_dict["pipeId"] = data[33]                             #PipeId
            order_dict["channel"] = data[34]                            #Channel
            order_dict["modificationOrCancelFlag"] = data[35]           #Modification or Cancel Flag
            order_dict["tradeDate"] = data[36]                          #Trade Date
            order_dict["acknowledgeNumber"] = data[37]                  #Acknowledgement Number
            order_dict["stopLossOrderReference"] = data[37]             #Stop Loss Order Reference
            order_dict["totalAmountBlocked"] = data[38]                 # Total Amount Blocked
            order_dict["averageExecutedRate"] = data[39]                #Average Executed Rate
            order_dict["cancelFlag"] = data[40]                         #Cancel Flag
            order_dict["squareOffMarket"] = data[41]                    #SquareOff Market
            order_dict["quickExitFlag"] = data[42]                      #Quick Exit Flag
            order_dict["stopValidTillDateFlag"] = data[43]              #Stop Valid till Date Flag
            order_dict["priceImprovementFlag"] = data[44]               #Price Improvement Flag
            order_dict["conversionImprovementFlag"] = data[45]          #Conversion Improvement Flag
            order_dict["trailUpdateCondition"] = data[45]               #Trail Update Condition
            order_dict["systemPartnerCode"] = data[46]                  #System Partner Code
        return order_dict
    exchange = str.split(data[0], '!')[0].split('.')[0]
    data_type = str.split(data[0], '!')[0].split('.')[1]
    if exchange == '6':
        data_dict = {}
        data_dict["symbol"] = data[0]
        data_dict["AndiOPVolume"] = data[1]
        data_dict["Reserved"] = data[2]
        data_dict["IndexFlag"] = data[3]
        data_dict["ttq"] = data[4]
        data_dict["last"] = data[5]
        data_dict["ltq"] = data[6]
        data_dict["ltt"] = datetime.fromtimestamp(data[7]).strftime('%c')
        data_dict["AvgTradedPrice"] = data[8]
        data_dict["TotalBuyQnt"] = data[9]
        data_dict["TotalSellQnt"] = data[10]
        data_dict["ReservedStr"] = data[11]
        data_dict["ClosePrice"] = data[12]
        data_dict["OpenPrice"] = data[13]
        data_dict["HighPrice"] = data[14]
        data_dict["LowPrice"] = data[15]
        data_dict["ReservedShort"] = data[16]
        data_dict["CurrOpenInterest"] = data[17]
        data_dict["TotalTrades"] = data[18]
        data_dict["HightestPriceEver"] = data[19]
        data_dict["LowestPriceEver"] = data[20]
        data_dict["TotalTradedValue"] = data[21]
        marketDepthIndex = 0
        for i in range(22, len(data)):
            data_dict["Quantity-"+str(marketDepthIndex)] = data[i][0]
            data_dict["OrderPrice-"+str(marketDepthIndex)] = data[i][1]
            data_dict["TotalOrders-"+str(marketDepthIndex)] = data[i][2]
            data_dict["Reserved-"+str(marketDepthIndex)] = data[i][3]
            data_dict["SellQuantity-"+str(marketDepthIndex)] = data[i][4]
            data_dict["SellOrderPrice-"+str(marketDepthIndex)] = data[i][5]
            data_dict["SellTotalOrders-"+str(marketDepthIndex)] = data[i][6]
            data_dict["SellReserved-"+str(marketDepthIndex)] = data[i][7]
            marketDepthIndex += 1
    elif data_type == '1':
        data_dict = {
            "symbol": data[0],
            "open": data[1],
            "last": data[2],
            "high": data[3],
            "low": data[4],
            "change": data[5],
            "bPrice": data[6],
            "bQty": data[7],
            "sPrice": data[8],
            "sQty": data[9],
            "ltq": data[10],
            "avgPrice": data[11],
            "quotes": "Quotes Data"
        }
        # For NSE & BSE conversion
        if len(data) == 21:
            data_dict["ttq"] = data[12]
            data_dict["totalBuyQt"] = data[13]
            data_dict["totalSellQ"] = data[14]
            data_dict["ttv"] = data[15]
            data_dict["trend"] = data[16]
            data_dict["lowerCktLm"] = data[17]
            data_dict["upperCktLm"] = data[18]
            data_dict["ltt"] = datetime.fromtimestamp(
                data[19]).strftime('%c')
            data_dict["close"] = data[20]
        # For FONSE & CDNSE conversion
        elif len(data) == 23:
            data_dict["OI"] = data[12]
            data_dict["CHNGOI"] = data[13]
            data_dict["ttq"] = data[14]
            data_dict["totalBuyQt"] = data[15]
            data_dict["totalSellQ"] = data[16]
            data_dict["ttv"] = data[17]
            data_dict["trend"] = data[18]
            data_dict["lowerCktLm"] = data[19]
            data_dict["upperCktLm"] = data[20]
            data_dict["ltt"] = datetime.fromtimestamp(
                data[21]).strftime('%c')
            data_dict["close"] = data[22]
    else:
        data_dict = {
            "symbol": data[0],
            "time": datetime.fromtimestamp(data[1]).strftime('%c'),
            "depth": parse_market_depth(data[2], exchange),
            "quotes": "Market Depth"
        }
    if exchange == '4' and len(data) == 21:
        data_dict['exchange'] = 'NSE Equity'
    elif exchange == '1':
        data_dict['exchange'] = 'BSE'
    elif exchange == '13':
        data_dict['exchange'] = 'NSE Currency'
    elif exchange == '4' and len(data) == 23:
        data_dict['exchange'] = 'NSE Futures & Options'
    elif exchange == '6':
        data_dict['exchange'] = 'Commodity'
    return data_dict
        
def ic_get_session_details():
    session_id = ic_autologon()
    app_key = json.load(open('config.json', 'r'))['ICICI_API_KEY']
    conn = http.client.HTTPSConnection("api.icicidirect.com")
    payload = "{\r\n    \"SessionToken\": " + f"\"{session_id}\",\r\n    \"AppKey\": \"{app_key}\"" + "\r\n}"
    # payload = "{\r\n    \"SessionToken\": \"22084547\",\r\n    \"AppKey\": \"67891rY8775zC8w226!07&837w71@99(\"\r\n}"
    headers = {
                "Content-Type": "application/json"
            }
    conn.request("GET", "/breezeapi/api/v1/customerdetails", payload, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data.decode('utf-8'))
    # print(data.decode("utf-8"))
    return data

# tokens = token_list
def ic_subscribeFeed(tokens):
    global sio
    for token in tokens:
        script_code = token #Subscribe more than one stock at a time
        sio.emit('join', script_code)
         
def ic_unsubscribeFeed(tokens):
    global sio
    for token in tokens:
        script_code = token #Subscribe more than one stock at a time
        sio.emit("leave", script_code)
        
# CallBack functions to receive feeds
def on_ticks(ticks):
    global options_df, livePrices
    ticks = parse_data(ticks)
    tick_symbol = ticks['symbol'][4:]
    tick_time = datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y")
    tick_px = ticks['last']
    
    
    # print(f"{tick_symbol} - {tick_time} - {ticks['close']} - {ticks['last']}")
    if len(livePrices) > 0:
        livePrices.loc[livePrices['Token'] == tick_symbol, 'CandleTime'] = tick_time
        livePrices.loc[livePrices['Token'] == tick_symbol, 'Close'] = tick_px
        livePrices.loc[livePrices['Token'] == tick_symbol, 'PrevClose'] = ticks['close']
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
            print('Check option levels')
            option_check_thread = Thread(target=check_option_list,args=(tick_symbol,tick_px))
            option_check_thread.start()
    
        if tick_symbol == 'NIFTY 50':
            # print(f"{tick_symbol} - {tick_time} - {ticks['close']} - {ticks['last']}")
            
            if tick_time.hour == 9 and tick_time.minute == 17 and tick_time.second == 0:
                print('Straddle Initiating')
                straddle_thread = Thread(target=strat_straddle_buy,args=(tick_symbol,tick_px,tick_time))
                straddle_thread.start()   
        
    #         strat_ema_tf = int(json.load(open('config.json', 'r'))['STRAT_EMA_TF'])
    #         if tick_time.minute % strat_ema_tf == 0 and tick_time.second == 5:
    #             print(f"EMA Strategy Execution - Timeframe - {strat_ema_tf}")
    #             ema_strat_thread = Thread(target=generate_ema_signal,args=(tick_symbol, strat_ema_tf)) 
    #             ema_strat_thread.start()
                
    #         if tick_time.minute % 5 == 0 and tick_time.second == 5:
    #             print('Calculate PCRs')
    #             # pcr_thread = Thread(target=update_pcr,args=(options_df))
    #             pcr_thread = Thread(target=update_pcr)
    #             pcr_thread.start()
                
        
            # monitor_thread = Thread(target=check_strategies,args=(tick_symbol, tick_px, tick_time)) 
            # monitor_thread.start()
            
def main():
    global options_df, livePrices, token_list, sio
    now = datetime.now()
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Live Market Feed Process Start")
    
    try:  
        subscription_flag = 'N'
        data = ic_get_session_details()
        session_key = data['Success']['session_token']
    
        user_id, session_token = base64.b64decode(session_key.encode('ascii')).decode('ascii').split(":")
        
        auth = {"user": user_id, "token": session_token} 
        sio.connect("https://livestream.icicidirect.com", headers={"User-Agent":"python-socketio[client]/socket"}, 
                        auth=auth, transports="websocket", wait_timeout=3)
        channel_name = 'stock'
        tux_to_user_value = dict()
  
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
                        # icici.user_id()
                        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Symbols Subscribed - {token_list}")
                        sio.on(channel_name, on_ticks)
                        ic_subscribeFeed(token_list)
                        subscription_flag = 'Y'
                        send_whatsapp_msg('Feed Alert','Market Live Feed Started!')
                        
                    else:
                        ic_get_watchlist(mode='C')
                else:
                    livePrices.to_csv(watchlist_file, index=False)
                    options_df.to_csv(options_file, index=False)
                    strat_trades_df.to_csv(trade_file, index=False)
                   
            if (now.time() >= time(15,35) and subscription_flag=='Y'):
                print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Symbols UnSubscribed - {token_list}")
                ic_unsubscribeFeed(token_list)
                sio.emit("disconnect", "transport close")
                subscription_flag='N'
                send_whatsapp_msg('Feed Alert','Market Live Feed Stopped!')
                break

            if subscription_flag == 'Y':
                tm.sleep(1)
            else:
                tm.sleep(60)
    
    except Exception as e:
        err = str(e)
        print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Live Stream Error - {err}")
        write_log('ic_watchlist','e',f"{err}")  
        pass

if __name__ == '__main__':
    # test1()
    main()
    
    
    
