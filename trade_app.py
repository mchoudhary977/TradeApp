# Main TradeApp Start
from flask import Flask, render_template, flash, redirect, url_for, request, jsonify
from flask_cors import CORS
# from flask_sslify import SSLify
import traceback
# from datetime import datetime as dt,timedelta, time
import pytz
import pandas as pd
from threading import Thread
import subprocess
import json
import ssl
# from trade_modules import *
from dh_functions import *
from wa_notifications import *
from log_function import *

# from ic_watchlist import ic_get_watchlist
import os

app = Flask(__name__)
CORS(app)

# ------------------------- TradeApp Files --------------------------------
icici_scrips = 'icici.csv'
watchlist_file = 'WatchList.csv'
options_file = 'Options.csv'
oi_pcr_file = 'OIPCR.csv'
coi_pcr_file = 'COIPCR.csv'
trade_file = 'Trades.csv'
pos_file = 'Positions.csv'

# sslify = SSLify(app, permanent=True, keyfile='key.pem', certfile='cert.pem')
@app.route('/get_watchlist')
def get_watchlist():
    resultDict = {}
    try:
        if os.path.exists(trade_file) == True:
            resultDict['WatchList'] = pd.read_csv(watchlist_file)
            resultDict['WatchList'] = resultDict['WatchList'].to_dict(orient='records')
            return resultDict
    # except pd.errors.EmptyDataError:
    except Exception as e:
        printLog('e',f"Error while fetching data from file - {watchlist_file} - {str(e)}")

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
    wl = pd.DataFrame(columns=['No Stocks in Watchlist'])
    try:
        if os.path.exists(trade_file) == True:
            wl = pd.read_csv(watchlist_file)
    except pd.errors.EmptyDataError:
        wl = pd.DataFrame(columns=['Error in getting Watchlist, please check!'])
    resultDict['WatchList'] = wl

    # Trades Details
    trades = pd.DataFrame(columns=['No Trades for the day!'])
    try:
        if os.path.exists(trade_file) == True:
            trades = pd.read_csv(trade_file)
            t1 = trades[trades['CreationTime'] >= f"{curr_dt.strftime('%Y-%m-%d')} 00:00:00"]
            trades = t1 if len(t1) > 0 else pd.DataFrame(columns=['No Trades for the day!'])
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame(columns=['No Trades for the day!'])

    trades = trades.to_html(index=False)
    trades = trades.replace('<table border="1" class="dataframe">','<table border="1" class="dataframe" style="border-collapse: collapse; width: 100%;">')
    trades = trades.replace('<thead>','<thead style="background-color: #00008B; color: white;">')
    trades = trades.replace("text-align: right;","text-align: center; border: 1px solid white;")
    trades = trades.replace("<th>",'<th style="border: 1px solid black; width: 100%; white-space: nowrap;">')
    trades = trades.replace("<td>",'<td style="border: 1px solid black; width: 100%; white-space: nowrap;">')
    resultDict['Trades'] = trades

    resultDict['Positions'] = get_position_details()
    # trade = resultDict['Trades'].to_html(index=False)

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
        spot_px = wl.loc[wl['Code']==i].sort_values(by=['CandleTime'], ascending=[False]).head(1)['Close'].item()
        # spot_px = wl.loc[wl['Code']==i].sort_values(by=['Date'], ascending=[False]).head(1)['Close'].item()
        # printLog('i',spot_px)
        strike_step = 100 if i == 'CNXBAN' else 50
        atm_strike = int(round(spot_px/50,0)*50) if i != 'CNXBAN' else int(round(spot_px/100,0)*100)
        strike_begin = atm_strike - (2*strike_step)
        strike_end = (2*strike_step) + atm_strike
        coipcr_dict[i] = coipcr.loc[coipcr['SymbolCode']==i][coipcr['StrikePrice']>=strike_begin][coipcr['StrikePrice']<=strike_end]

    resultDict['OIPCR'] = oipcr_dict
    resultDict['COIPCR'] = coipcr_dict
    resultDict['ICICI_SESSION_URL'] = json.load(open('config.json', 'r'))['UC']['ICICI_SESSION_URL']
    return resultDict

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
    try:
        icici_session_id = request.form.get('icici_session_id')
        dhan_token = request.form.get('dhan_token')
        wa_token = request.form.get('wa_token')
        live_order_flag = request.form.get('live_order_flag')
        nifty_opt_select = request.form.get('nifty_opt_select')
        expiry_week_selection = request.form.get('expiry_week_selection')
        daily_order_count = request.form.get('daily_order_count')

        nifty_call_select = request.form.get('nifty_call_select')
        nifty_put_select = request.form.get('nifty_put_select')

        msg_title = f"Configuration Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        msg_body = ''

        with open("config.json","r") as f:
            json_data = json.load(f)

        if len(live_order_flag) > 0:
            if live_order_flag =='Y' or live_order_flag =='N':
                json_data['TC']["LIVE_ORDER"] = live_order_flag
                msg_body = msg_body + f"Live Order Status Change = {live_order_flag}. "
                # msg = f"Live Order Status Change = {live_order_flag}"
                # send_whatsapp_msg(f"LIVE ORDER - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", msg)

        if len(daily_order_count) > 0:
            json_data['TC']["DAILY_ORDER_COUNT"] = int(daily_order_count)
            msg_body = msg_body + f"Daily Order Limit = {daily_order_count}. "

        if len(nifty_opt_select) > 0:
            if nifty_opt_select.upper().find("SELECT") != -1:
                print('select present')
            else:
                json_data['SC']["NIFTY"]["OPT#"] = int(nifty_opt_select)
                msg_body = msg_body + f"Nifty Option Selected = {nifty_opt_select}. "

        if len(nifty_call_select) > 0:
            json_data['SC']["NIFTY"]["CALL_STRIKE"] = int(nifty_call_select)
            msg_body = msg_body + f"NIFTY CALL STRIKE Selected = {nifty_call_select}. "

        if len(nifty_put_select) > 0:
            json_data['SC']["NIFTY"]["PUT_STRIKE"] = int(nifty_put_select)
            msg_body = msg_body + f"NIFTY PUT STRIKE Selected = {nifty_put_select}. "

        if len(expiry_week_selection) > 0:
            if expiry_week_selection.upper().find("SELECT") != -1:
                print('select present')
            else:
                json_data['TC']["EXP_WEEK"] = int(expiry_week_selection)
                msg_body = msg_body + f"Expiry Week Selected = {expiry_week_selection}. "

        if len(dhan_token) > 0:
            print(f'token dhan - {dhan_token}')
            json_data['UC']["DHAN_ACCESS_TK"] = dhan_token
            msg_body = msg_body + f"Dhan Token Updated. "

        if len(wa_token) > 0:
            print(f'token WA - {wa_token}')
            json_data['UC']["WA_TKN"] = wa_token
            msg_body = msg_body + f"WhatsApp Token Updated. "

        if len(icici_session_id) > 0:
            print("Updating ICICI Session Token Details")
            # st = iciciUpdSessToken(icici_session_id)
            st = icici_upd_sess_config(icici_session_id)
            if st['status'] == 'SUCCESS':
                startWebApp()

        with open("config.json", "w") as file:
            json.dump(json_data, file, indent=4)

        send_whatsapp_msg(msg_title, msg_body)

        return config_data()
    except Exception as e:
        return str(e)


# Route to handle form submission
@app.route('/config')
def config_data():
    with open("config.json","r") as f:
        json_data = json.load(f)

    html_code = '<h2>Trade Configurations</h2>'
    html_code += pd.DataFrame(json_data['TC']).T.to_html()

    return html_code


# Route to handle form submission
@app.route('/update_config', methods=['POST'])
def submit_form_1():
    global icici_api,dhan
    icici_session_id = request.form.get('icici_session_id')
    dhan_token = request.form.get('dhan_acct_token')
    wa_token = request.form.get('whatsapp_token')
    live_order = request.form.get('live_order')
    nifty_opt_select = request.form.get('nifty_opt_select')
    expiry_week_selection = request.form.get('expiry_week_selection')

    msg_title = f"Configuration Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    msg_body = ''

    with open("config.json","r") as f:
        json_data = json.load(f)

    if len(live_order) > 0:
        if live_order =='Y' or live_order =='N':
            json_data['TC']["LIVE_ORDER"] = live_order
            msg_body = f"Live Order Status Change = {live_order}. "
            # send_whatsapp_msg(f"LIVE ORDER - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", msg)

    if len(nifty_opt_select) > 0:
        json_data['SC']["NIFTY"]["OPT#"] = int(nifty_opt_select)
        msg_body = f"Nifty Option Selected = {nifty_opt_select}. "



    if len(expiry_week_selection) > 0:
        json_data['TC']["EXP_WEEK"] = int(expiry_week_selection)
        msg_body = f"Expiry Week Selected = {expiry_week_selection}. "

    if len(dhan_token) > 0:
        print(f'token dhan - {dhan_token}')
        json_data['UC']["DHAN_ACCESS_TK"] = dhan_token
        msg_body = f"Dhan Token Updated. "

    if len(wa_token) > 0:
        print(f'token WA - {wa_token}')
        json_data['UC']["WA_TKN"] = wa_token

    if len(icici_session_id) > 0:
        print("Updating ICICI Session Token Details")
        # st = iciciUpdSessToken(icici_session_id)
        st = icici_upd_sess_config(icici_session_id)
        if st['status'] == 'SUCCESS':
            startWebApp()

    with open("config.json", "w") as file:
        json.dump(json_data, file, indent=4)

    send_whatsapp_msg(msg_title, msg_body)


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

@app.route('/orders')
def get_order_details():
    resultDict={}
    orders = dh_get_orders()
    no_orders = ''
    if orders['status'].lower() == 'success':
        if  orders['data'] is not None:
            orders = orders['data']
            selected_cols = ['orderId', 'tradingSymbol','securityId','orderStatus',
                       'orderType','productType','validity','transactionType',
                       'quantity','filled_qty','price','legName','exchangeTime'
                       ]
            orders = orders[selected_cols]
            orders = orders.sort_values(by=['orderStatus', 'exchangeTime'], ascending=[False, False])
            orders['#'] = orders.reset_index(drop=True).index + 1
            # Reorder columns
            last_column = orders.columns[-1]
            new_order = [last_column] + [col for col in orders.columns if col != last_column]
            orders = orders[new_order]
        else:
            no_orders = f'No Orders for the day - {datetime.now().strftime("%B %d, %Y")}'
            orders = pd.DataFrame(columns=[no_orders])
    else:
        no_orders = 'Order Information Not Returned...'
        orders = pd.DataFrame(columns=[no_orders])
    orders = orders.to_html(index=False)
    orders = orders.replace('<table border="1" class="dataframe">','<table border="1" class="dataframe" style="border-collapse: collapse; width: 100%;">')
    orders = orders.replace('<thead>','<thead style="background-color: #00008B; color: white;">')
    orders = orders.replace("text-align: right;","text-align: center; border: 1px solid white;")
    orders = orders.replace("<th>",'<th style="border: 1px solid black; width: 100%; white-space: nowrap;">')
    orders = orders.replace("<td>",'<td style="border: 1px solid black; width: 100%; white-space: nowrap;">')

    resultDict['Orders'] = orders

    return render_template('orders.html', tdate=datetime.now().strftime("%B %d, %Y %H:%M"),
                           resultDict=resultDict)

@app.route('/positions')
def get_position_details():
    resultDict = {}
    trades = pd.DataFrame()
    positions = pd.DataFrame(columns=['Position Information Not Returned...'])
    fetch_pos = 'Y'
    # try:
    #     if os.path.exists(trade_file) == True:
    #         trades = pd.read_csv(trade_file)
    # except pd.errors.EmptyDataError:
    #     pass

    # try:
    #     if os.path.exists(pos_file) == True:
    #         positions = pd.read_csv(pos_file)
    # except pd.errors.EmptyDataError:
    #     pass

    # if len(trades) > 0 and len(positions) > 0:
    #     tr_agg = trades.groupby('DervID')['Qty'].sum().reset_index()
    #     pos_agg = positions.groupby('securityId')['buyQty'].sum().reset_index()
    #     pos_agg = pos_agg.rename(columns = {'securityId':'DervID','buyQty':'Qty'})
    #     pos_agg['DervID'] = pd.to_numeric(pos_agg['DervID'], errors='coerce').astype(type(tr_agg['DervID'].values[0]))

    #     if tr_agg.equals(pos_agg):
    #         fetch_pos = 'N'
    # elif len(trades) == 0:
    #     fetch_pos = 'N'

    if fetch_pos == 'Y':
        pos = dh_get_positions()
        if pos['status'].lower() == 'success':
            if pos['data'] is not None:
                positions = pos['data']

    if len(positions) > 0:
        selected_cols = ['tradingSymbol','securityId','positionType',
                   'productType','realizedProfit','buyQty','sellQty',
                   'buyAvg','sellAvg','dayBuyValue','daySellValue',
                   'costPrice']
        positions = positions[selected_cols]
        positions['realizedProfit'] = round(positions['daySellValue'] - positions['dayBuyValue'],2)
        positions = positions.sort_values(by=['positionType', 'productType'], ascending=[False, True])
        positions['#'] = positions.reset_index(drop=True).index + 1
        positions['#'] = positions['#'].astype(int)
        # Reorder columns
        last_column = positions.columns[-1]
        new_order = [last_column] + [col for col in positions.columns if col != last_column]
        positions = positions[new_order]
        total_value = positions['realizedProfit'].sum()
        total_row = pd.DataFrame({'tradingSymbol':'Total','realizedProfit': [total_value]}, index=['Total'])
        positions = pd.concat([positions,total_row])
        positions.iloc[-1] = positions.iloc[-1].fillna('')
    else:
        no_positions = f'No Positions for the day - {datetime.now().strftime("%B %d, %Y")}'
        positions = pd.DataFrame(columns=[no_positions])

    positions = positions.to_html(index=False)
    positions = positions.replace('<table border="1" class="dataframe">','<table border="1" class="dataframe" style="border-collapse: collapse; width: 100%;">')
    positions = positions.replace('<thead>','<thead style="background-color: #00008B; color: white;">')
    positions = positions.replace("text-align: right;","text-align: center; border: 1px solid white;")
    positions = positions.replace("<th>",'<th style="border: 1px solid black; width: 100%; white-space: nowrap;">')
    positions = positions.replace("<td>",'<td style="border: 1px solid black; width: 100%; white-space: nowrap;">')

    # resultDict['Positions'] = positions

    return positions
    # return render_template('positions.html', tdate=datetime.now().strftime("%B %d, %Y %H:%M"),
    #                        resultDict=resultDict)

if __name__ == '__main__':
    # ic_get_watchlist(mode='C')
    # app.run(ssl_context=('cert.pem', 'key.pem'))
    now = datetime.now()
    send_whatsapp_msg("TradeApp START", f"Web Application Started @ {now.strftime('%Y-%m-%d %H:%M:%S')}")
    app.run(host='0.0.0.0')
    # ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # ssl_context.load_cert_chain('cert.pem', 'key.pem')
    # app.run(host='0.0.0.0',ssl_context=ssl_context)