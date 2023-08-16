# Main TradeApp Start
from flask import Flask, render_template, flash, redirect, url_for, request, jsonify
from flask_cors import CORS
from flask_sslify import SSLify
import traceback
from datetime import datetime as dt,timedelta, time
import pytz
import pandas as pd
from threading import Thread
import subprocess
import json
import ssl
from trade_modules import *
import os

app = Flask(__name__)
CORS(app)

instrument_list = pd.read_csv('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')
instrument_df = instrument_list

# sslify = SSLify(app, permanent=True, keyfile='key.pem', certfile='cert.pem')
def get_sym_price(symbol):
    sym = instrument_df[instrument_df['CD']==symbol][['NS','EC','SG','TK','CD','LS']]  
    sym.rename(columns={'NS':'SymbolName','EC':'ExchangeCode','SG':'Segment',
                        'TK':'Token','CD':'Code','LS':'LotSize'}, inplace=True) 
    
    response=iciciGetSymDetail(exchange_code = "NSE",stock_code = symbol,product_type = "Cash",interval = "5minute",
                            from_date = (datetime.now()-timedelta(1)),to_date = (datetime.now()-timedelta(0)))
    if response['status'] == 'SUCCESS':
        if len(response['data']) > 0:
            data = response['data']
            data['datetime'] = data['datetime'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
            max_timestamp = data.groupby(data['datetime'].dt.date)['datetime'].max()[-2]
            data = data[data['datetime']>=max_timestamp]
            data['date'] = data['datetime']
            data = data.set_index('datetime')
            data=data.resample('1D').agg({'date':'last','stock_code':'first','open': 'first','high':'max','low':'min','close':'last'}).dropna()               
            sym['Open']=data.iloc[-1]['open']
            sym['High']=data.iloc[-1]['high']
            sym['Low']=data.iloc[-1]['low']
            sym['Close']=data.iloc[-1]['close']
            sym['PrevClose']=data.iloc[-2]['close']
            sym['Difference']=data.iloc[-1]['close'] - data.iloc[-2]['close']
            sym['CandleTime']=data.iloc[-1]['date']
        else:
            sym['Open']=0
            sym['High']=0
            sym['Low']=0
            sym['Close']=0
            sym['PrevClose']=0
            sym['Difference']=0
            sym['CandleTime']=dt.datetime.now()
    else:
        sym['Open']=0
        sym['High']=0
        sym['Low']=0
        sym['Close']=0
        sym['PrevClose']=0
        sym['Difference']=0
        sym['CandleTime']=dt.datetime.now()
    return sym

@app.route('/get_watchlist')
def get_watchlist(mode='R'):
    resultDict = {}
    symbol_list = json.load(open('config.json', 'r'))['STOCK_CODES']
    wl_df = pd.DataFrame(columns=['SymbolName','ExchangeCode','Segment','Token','Code','LotSize',
                                  'Open','High','Low','Close','PrevClose','Difference','CandleTime'])
    if mode == 'C':  # Create
        for symbol in symbol_list:
            sym=get_sym_price(symbol)
            wl_df = pd.concat([wl_df,sym],ignore_index=True)
        wl_df.to_csv('WatchList.csv',index=False)       
            
    elif mode == 'I':  # Insert
        wl_df = pd.read_csv('WatchList.csv')
        wl = list(wl_df['Code'].values)
        inserted_symbols = [element for element in symbol_list if element not in wl]
        
        if len(inserted_symbols) > 0:
            for i in inserted_symbols:
                sym=get_sym_price(symbol)
                wl_df = pd.concat([wl_df,sym],ignore_index=True)
            wl_df.to_csv('WatchList.csv',index=False) 
    elif mode == 'D':  # Delete
        wl_df = pd.read_csv('WatchList.csv')
        wl = list(wl_df['Code'].values)
        deleted_symbols = [element for element in wl if element not in symbol_list]
        if len(deleted_symbols) > 0:
            wl_df = wl_df[~wl_df['Code'].isin(deleted_symbols)]
            wl_df.to_csv('WatchList.csv',index=False)
    elif mode == 'R':  # Read
        if os.path.exists('WatchList.csv'):
            wl_df = pd.read_csv('WatchList.csv')
        else:
            wl_df = get_watchlist('C')  
        wl_df = wl_df.to_dict(orient='records')
    elif mode == 'NR':  # NormalRead
        if os.path.exists('WatchList.csv'):
            wl_df = pd.read_csv('WatchList.csv')
        else:
            wl_df = get_watchlist('C')  
    resultDict['WatchList-DF'] = wl_df
    return resultDict

# w1=get_watchlist('R')    
        
@app.route('/')
def home():
    resultDict = get_data()
    html_dict = {}

    return render_template('index.html', tdate=datetime.now().strftime("%B %d, %Y %H:%M"),
                           resultDict=resultDict)

@app.route('/get_data')
def get_data():
    curr_dt = datetime.now()
    resultDict = {}
    data_list = ['WatchList','Funds','Positions','Orders','Holdings','Strategy','PCR']
    resultDict['WatchList'] = pd.DataFrame(columns=['No Stocks in Watchlist'])
    resultDict['WatchList']=get_watchlist(mode='NR')
    # if os.path.exists('WatchList.csv'):
    #     resultDict['WatchList'] = pd.read_csv('WatchList.csv')

    oipcr = pd.read_csv('OIPCR.csv')
    unique_symbols = oipcr['SymbolCode'].unique()
    oipcr_dict = {}
    for i in unique_symbols:
        oipcr_dict[i] = oipcr.loc[oipcr['SymbolCode']==i].groupby('Symbol').head(5)

    coipcr = pd.read_csv('COIPCR.csv')
    unique_symbols = coipcr['SymbolCode'].unique()
    coipcr_dict = {}
    wl = resultDict['WatchList']
    for i in unique_symbols:
        spot_px = wl.loc[wl['Code']==i].sort_values(by=['Date'], ascending=[False]).head(1)['Close'].item()
        print(spot_px)
        strike_step = 100 if i == 'CNXBAN' else 50
        atm_strike = int(round(spot_px/50,0)*50) if i != 'CNXBAN' else int(round(spot_px/100,0)*100)
        strike_begin = atm_strike - (2*strike_step)
        strike_end = (2*strike_step) + atm_strike
        coipcr_dict[i] = coipcr.loc[coipcr['SymbolCode']==i][coipcr['StrikePrice']>=strike_begin][coipcr['StrikePrice']<=strike_end]

    resultDict['OIPCR'] = oipcr_dict
    resultDict['COIPCR'] = coipcr_dict
    resultDict['ICICI_SESSION_URL'] = getConfig('ICICI_SESSION_URL')

    return resultDict


# @app.route('/get_watchlist')
# def get_watchlist():
#     resultDict = {}
#     resultDict['WatchList'] = pd.DataFrame(columns=['No Stocks in Watchlist'])
#     if os.path.exists('WatchList.csv'):
#         resultDict['WatchList'] = pd.read_csv('WatchList.csv')

#     resultDict['WatchList'] = resultDict['WatchList'].to_dict(orient='records')

#     return resultDict
#     # return "Hello World"


    
@app.route('/restart')
def startWebApp():
    script_path = '/home/ubuntu/webApp/startWebApp.sh'
    send_whatsapp_msg(mtitle='TA ALERT',mtext='Restarting App')
    subprocess.call([script_path])
    return home()

# Route to handle form submission
@app.route('/#', methods=['POST'])
def submit_form():
    global icici_api,dhan
    icici_session_id = request.form.get('icici_session_id')
    dhan_token = request.form.get('dhan_token')
    wa_token = request.form.get('wa_token')
    live_order_flag = request.form.get('live_order_flag')

    with open("config.json","r") as f:
        json_data = json.load(f)

    if len(live_order_flag) > 0:
        if live_order_flag =='Y' or live_order_flag =='N':
            json_data["LIVE_ORDER"] = live_order_flag

    if len(dhan_token) > 0:
        print(f'token dhan - {dhan_token}')
        json_data["DHAN_ACCESS_TK"] = dhan_token

    if len(wa_token) > 0:
        print(f'token WA - {wa_token}')
        json_data["WA_TKN"] = wa_token

    if len(icici_session_id) > 0:
        print("Updating ICICI Session Token Details")
        # st = iciciUpdSessToken(icici_session_id)
        st = icici_upd_sess_config(icici_session_id)
        if st['status'] == 'SUCCESS':
            startWebApp()

    with open("config.json", "w") as file:
        json.dump(json_data, file, indent=4)


    return home()

@app.route('/admin')
def admin():
    if icici_api.user_id is not None:
        return f"Session Connection - SUCCESS \nUserID: {icici_api.user_id}"
    else:
        st = createICICISession(icici_api)
        if st['status'] == 'SUCCESS':
            return f"Session Connection - SUCCESS \nUserID: {icici_api.user_id}"
        else:
            return f"Session Connection - FAILED \n Error: {st['data']}"
    # script_path = '/home/ubuntu/webApp/startWebApp.sh'
    # send_whatsapp_msg(mtitle='TA ALERT',mtext='Restarting App')
    # subprocess.call([script_path])
    return home()

@app.route('/ac')
def createAccountFiles():
    global icici_api,dhan
    get_watchList()
    pd.DataFrame(columns=['Error Fetching Details!']).to_csv('Funds.csv', index=False)
    pd.DataFrame(columns=['No Open Positions']).to_csv('Positions.csv', index=False)
    pd.DataFrame(columns=['No Orders for Today']).to_csv('Orders.csv', index=False)
    pd.DataFrame(columns=['No Trades for Today']).to_csv('Trades.csv', index=False)
    pd.DataFrame(columns=['No Holdings']).to_csv('Holdings.csv', index=False)
    pd.DataFrame(columns=['No Active Strategy']).to_csv('Strategy.csv', index=False)


    dhan_resp = requests.get('https://images.dhan.co/api-data/api-scrip-master.csv')
    with open('dhan_scrip.csv', 'wb') as f:
        f.write(dhan_resp.content)

    icici_resp = requests.get('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')
    with open('icici_scrip.csv', 'wb') as f:
        f.write(icici_resp.content)
    opt_scrip = pd.read_csv('dhan_scrip.csv')
    opt_scrip.loc[opt_scrip['SEM_INSTRUMENT_NAME']=='OPTIDX'].to_csv('dhan_option_scrip.csv', index=False)

    # d1 = json.loads(response.content)
    # dh=dh.loc[dh['SEM_INSTRUMENT_NAME']=='OPTIDX']

    t1=Thread(target=updateWatchList,args=(icici_api,dhan))
    t1.start()
    t2=Thread(target=get_acct_funds,args=(icici_api,dhan))
    t2.start()
    t3=Thread(target=get_acct_positions,args=(icici_api,dhan))
    t3.start()
    t4=Thread(target=get_acct_orders,args=(icici_api,dhan))
    t4.start()
    t5=Thread(target=get_acct_holdings,args=(icici_api,dhan))
    t5.start()
    t6=Thread(target=process_market,args=(icici_api,dhan))
    t6.start()

    return 'SUCCESS - Files Created!!!'

@app.route('/funds')
def funds():
    global icici_api,dhan
    get_acct_funds(icici_api,dhan)


@app.route('/webhook', methods=['POST'])
def get_webhook():
    global icici_api,dhan
    if request.method == 'POST':
        print("received data: ", request.json)
        print(request.json)
        msg = request.json
        # with open('new_file.txt', 'w') as file:
        #     # Write the data to the file
        #     file.write(msg)
        msg_title = 'TRADING-VIEW Alert'
        if 'title' in msg:
            msg_title = msg['title']

        # msg = msg['text']
        msg = f"{msg['exchange']} - {msg['tradingSymbol']} - {msg['text']}"

        send_whatsapp_msg(mtitle=msg_title,mtext=msg)
        # send_whatsapp_msg(msg=msg['text'])
        return 'success', 200

# def main():
    

if __name__ == '__main__':
    get_watchlist(mode='C')
    # createICICISession(icici_api)
    # createAccountFiles()
    # get_watchList()
    # updateWL = threading.Thread(target=updateWatchList)
    # updateWL.start()
    # app.run(ssl_context=('cert.pem', 'key.pem'))
    app.run(host='0.0.0.0')
    # ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # ssl_context.load_cert_chain('cert.pem', 'key.pem')
    # app.run(host='0.0.0.0',ssl_context=ssl_context)