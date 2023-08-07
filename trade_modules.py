# customfunctions Module
import requests
import http.client
import json
import hashlib
from datetime import datetime, timedelta, time
from dateutil.relativedelta import relativedelta
import pytz
import time as tm
import pandas as pd
import traceback
from breeze_connect import BreezeConnect
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from pyotp import TOTP

# Global Variables
config_file="config.json"


def getConfig(parameter):
    with open(config_file) as f:
        configparam = json.load(f)
        retValue = configparam[parameter]
        return retValue


icici_api = BreezeConnect(api_key = getConfig('ICICI_API_KEY'))
livePrices = pd.DataFrame()

# ICICI Auto Logon 
def icici_autologon():
    # icici_api = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
    icici_session_url = json.load(open('config.json', 'r'))['ICICI_SESSION_URL']
    
    service = webdriver.chrome.service.Service('./chromedriver')
    service.start()
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    
    driver = webdriver.Chrome(options=options)
    driver.get(icici_session_url)
    driver.implicitly_wait(10)
    username = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[1]/input')
    password = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[3]/div/input')
    
    icici_uname = json.load(open('config.json', 'r'))['ICICI_USER_NAME']
    icici_pwd = json.load(open('config.json', 'r'))['ICICI_PWD']
    username.send_keys(icici_uname)
    password.send_keys(icici_pwd)
    
    driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[4]/div/input').click()
    driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[5]/input').click()
    
    tm.sleep(10)
    totp = TOTP(json.load(open('config.json', 'r'))['ICICI_GOOGLE_AUTHENTICATOR'])
    token = totp.now()
    
    t1 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input')
    t2 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[2]/input')
    t3 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[3]/input')
    t4 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[4]/input')
    t5 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[5]/input')
    t6 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[6]/input')
    
    t1.send_keys(token[0])
    t2.send_keys(token[1])
    t3.send_keys(token[2])
    t4.send_keys(token[3])
    t5.send_keys(token[4])
    t6.send_keys(token[5])
    
    driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[4]/input[1]').click()
    
    tm.sleep(10)
    
    session_id = driver.current_url.split('apisession=')[1]
    json_data = json.load(open('config.json', 'r'))
    json_data['ICICI_API_SESSION'] = session_id
    with open('config.json', 'w') as the_file:
        json.dump(json_data, the_file, indent=4)
    driver.quit()
    return session_id

# Create ICICI Session Function
def createICICISession(icici_api):
    try:
        icici_api.generate_session(api_secret=getConfig('ICICI_API_SECRET_KEY'), session_token=getConfig('ICICI_API_SESSION'))
        msg=f'ICICI Session Created for UserID - {icici_api.user_id}'
        send_whatsapp_msg(mtitle='ICICI ALERT',mtext=msg)
        return{'status':'SUCCESS','data':msg}

    except Exception as e:
        err = str(e)
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        print(f'createICICISession :: {line_number} : {err}')
        if 'AUTH' in str(e).upper():
            session_id = icici_autologon()
            if session_id is not None:
                createICICISession(icici_api)
            else:
                return{'status':'FAILURE','data':f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - {str(e)} - Update ICICI Session Token - {getConfig("ICICI_SESSION_URL")}'}
            # send_whatsapp_msg(mtitle='ERROR',mtext=f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - Update ICICI Session Token - {readConfig("ICICI_SESSION_URL")}')
            # send_whatsapp_msg(msg=f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - Update ICICI Session Token - {readConfig("ICICI_SESSION_URL")}')
        return{'status':'FAILURE','data':f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - {str(e)}'}

# ICICI Session update function
def iciciUpdSessToken(icici_session_id):
    print("Updating session details - {}".format(icici_session_id))
    app_key = getConfig('ICICI_API_KEY')
    conn = http.client.HTTPSConnection("api.icicidirect.com")
    payload = str({ "SessionToken": str(icici_session_id), "AppKey": str(app_key) })
    headers = {
                "Content-Type": "application/json"
            }
    conn.request("GET", "/breezeapi/api/v1/customerdetails", payload, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data.decode("utf-8"))

    with open("config.json","r") as f:
        json_data = json.load(f)
    if data['Status'] == 200:
        print('updating token through function - iciciUpdSessToken')
        json_data["ICICI_SESSION_TOKEN"] = data['Success']['session_token']
        json_data["ICICI_API_SESSION"] = icici_session_id

        with open("config.json", "w") as file:
            json.dump(json_data, file, indent=4)

        return{'status':'SUCCESS', 'data':'Session Updated Successfully.'}
    else:
        return{'status':'FAILURE', 'data':data['Error']}

# Update only the ICICI session ID
def icici_upd_sess_config(icici_session_id):
    with open("config.json","r") as f:
        json_data = json.load(f)
    json_data["ICICI_API_SESSION"] = icici_session_id
    with open("config.json", "w") as file:
        json.dump(json_data, file, indent=4)
    return {"status":"SUCCESS", "data":"Session Updated"}


# Get Symbol Data From ICICI API
def iciciGetSymDetail(exchange_code = "NSE",stock_code = "NIFTY",product_type = "Cash",interval = "30minute",
                        from_date = (datetime.now()-timedelta(1)),to_date = (datetime.now()-timedelta(0))):
    try:
        if icici_api.user_id is None:
            st = createICICISession(icici_api)
            if st['status'] != 'SUCCESS':
                raise ValueError(st['data'])

        i=0
        while i < 10:
            from_date = from_date-timedelta(i)
            if from_date.weekday() not in (5,6) and from_date.strftime('%Y-%m-%d') not in getConfig('HOLIDAY_LIST'):
                break
            i=i+1
        from_date = from_date.strftime('%Y-%m-%d')+'T00:00:00.000Z'
        to_date = to_date.strftime('%Y-%m-%d')+'T23:59:59.000Z'
        change = 'N'
        i=0
        df_tick = icici_api.get_historical_data_v2(interval=interval,
                    from_date= from_date,
                    to_date= to_date,
                    stock_code=stock_code,
                    # stock_code='CNXBAN',
                    exchange_code=exchange_code,
                    # exchange_code='NSE',
                    product_type=product_type)

        if df_tick['Status']==200:
            if len(df_tick['Success']) > 0:
                df_tick = pd.DataFrame(df_tick['Success'])
                return {"status": "SUCCESS", "data": df_tick}
    except Exception as e:
        err = str(e)
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        print(f'iciciGetSymDetail :: {line_number} : {err}')
        return {"status": "FAILURE", "data": err}





# Get Next Expiry Date
def get_next_expiry_date(e_day='thursday'):
    curr_dt = datetime.now().astimezone(pytz.timezone('Asia/Kolkata'))
    # Uncomment below function when dev done
    # if curr_dt.weekday() in (5,6): # and curr_dt.time() < time(9,25,10) and curr_dt.time() > time(15,10):
    #     return 'SUCCESS'
    exp_day = {'monday':0,'tuesday':1,'wednesday':2,'thursday':3,'friday':4}
    d1 = exp_day[e_day]
    today = datetime.now()
    holidays = getConfig('HOLIDAY_LIST')
    days_until_eday = (d1 - today.weekday() + 7) % 7
    next_expiry_date = today + timedelta(days=((d1 - today.weekday() + 7) % 7))
    i=0
    while i<10:
        next_expiry_date=next_expiry_date-timedelta(i)
        #print(next_expiry_date)
        if str(next_expiry_date) not in holidays:
            next_expiry_date = next_expiry_date.strftime('%Y-%m-%d')
            # print ('Yes')
            break
        else:
            i=i+1
    return next_expiry_date

# GET PCR VALUES
def getPCR(symCode='NIFTY', spot_px = 19169):
    curr_dt = datetime.now().astimezone(pytz.timezone('Asia/Kolkata'))
    oi_pcr_dict = {}
    oi_change_pcr_dict = {'Date':[], 'Time': [], 'Symbol':[], 'SymbolCode':[], 'ExpiryDate': [], 'StrikePrice': [], 'CALL COI': [], 'PUT COI': [], 'PCR-COI': [] }
    atm_strike = int(round(spot_px/50,0)*50) if symCode == 'NIFTY' else int(round(spot_px/100,0)*100)
    strike_step = 100 if symCode == 'BANKNIFTY' else 50
    strike_begin = atm_strike - (4*strike_step)
    strike_end = (5*strike_step) + atm_strike
    eday = 'tuesday' if symCode == 'NIFFIN' else 'thursday'
    symbol = symCode
    if symCode=='CNXBAN':
        symbol = 'BANKNIFTY'
    elif symCode == 'NIFFIN':
        symbol = 'FINNIFTY'
    # if curr_dt.time() > time(8,15) and curr_dt.time() < time(23,31):
    #     if True: #curr_dt.minute%5 == 0 and curr_dt.second == 5:
    k=0
    oi_pcr_dict['Date'] = [curr_dt.replace(second=0).strftime('%Y-%m-%d')]
    oi_pcr_dict['Time'] = [curr_dt.replace(second=0).strftime('%H:%M')]
    oi_pcr_dict['Symbol'] = [symbol]
    oi_pcr_dict['SymbolCode'] = [symCode]
    oi_pcr_dict['ExpiryDate'] = [datetime.strptime(get_next_expiry_date(eday), '%Y-%m-%d').strftime('%d-%b-%Y')]
    print ('Calculating PCR Values')
    # url = f"https://www.nseindia.com/api/option-chain-indices?symbol={pcr_dict['Symbol'][k]}"
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    headers = {
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'accept-encoding' : 'gzip, deflate, br',
        'accept-language' : 'en-US,en;q=0.9'
    }
    response = requests.get(url, headers=headers).content
    data = json.loads(response.decode('utf-8'))
    len(data['filtered']['data'])
    pe_change_in_oi = 0
    ce_change_in_oi = 0
    pe_oi = 0
    ce_oi = 0
    #exp_date = datetime.strptime(get_next_expiry_date(), '%Y-%m-%d').strftime('%d-%b-%Y')
    exp_date = oi_pcr_dict['ExpiryDate'][k]
    i=0
    j=0
    strike_px = strike_begin
    while i < len(data['filtered']['data']):
        dct = data['filtered']['data'][i]
        # if dct['strikePrice'] >= strike_begin and dct['strikePrice'] <= strike_end and dct['strikePrice'] == strike_px:
        oi_change_pcr_dict['Date'].append(curr_dt.replace(second=0).strftime('%Y-%m-%d'))
        oi_change_pcr_dict['Time'].append(curr_dt.replace(second=0).strftime('%H:%M'))
        oi_change_pcr_dict['Symbol'].append(dct['CE']['underlying'])
        oi_change_pcr_dict['SymbolCode'].append(symCode)
        oi_change_pcr_dict['ExpiryDate'].append(dct['expiryDate'])
        oi_change_pcr_dict['StrikePrice'].append(dct['strikePrice'])
        oi_change_pcr_dict['CALL COI'].append(dct['CE']['changeinOpenInterest'])
        oi_change_pcr_dict['PUT COI'].append(dct['PE']['changeinOpenInterest'])
        if dct['CE']['changeinOpenInterest']>0:
            oi_change_pcr = round(dct['PE']['changeinOpenInterest']/abs(dct['CE']['changeinOpenInterest']),3)
        else:
            oi_change_pcr = 0
        oi_change_pcr_dict['PCR-COI'].append(oi_change_pcr)
            # j=j+1
            # strike_px = strike_begin + (j * strike_step)
        if 'PE' in dct:
            if dct['PE']['expiryDate'] == exp_date:
                pe_change_in_oi = pe_change_in_oi + dct['PE']['changeinOpenInterest']
                #pe_oi = pe_oi + dct['PE']['openInterest']
        if 'CE' in dct:
            if dct['CE']['expiryDate'] == exp_date:
                ce_change_in_oi = ce_change_in_oi + dct['CE']['changeinOpenInterest']
                #ce_oi = ce_oi + dct['CE']['openInterest']
        i=i+1

    oi_pcr_dict['PUT OI'] = [data['filtered']['PE']['totOI']]
    oi_pcr_dict['CALL OI'] = [data['filtered']['CE']['totOI']]

    if abs(oi_pcr_dict['CALL OI'][k]) > 0:
        oi_pcr_dict['PCR-OI']= [round(oi_pcr_dict['PUT OI'][k]/oi_pcr_dict['CALL OI'][k],3)]
    else:
        oi_pcr_dict['PCR-OI']= [0]

    # if abs(ce_change_in_oi) > 0:
    #     oi_pcr_dict['PCR-OI'] = [round(pe_change_in_oi/ce_change_in_oi, 3)]
    # else:
    #     oi_pcr_dict['PCR-OI'] = [0]

    pcr_dict = {'PCR_OI': oi_pcr_dict, 'PCR_OICHANGE': oi_change_pcr_dict}
    return pcr_dict #oi_change_pcr_dict #oi_pcr_dict
# # # # # # # # # # # # # # # # # # # # # # # # # # # PCR CALCULATION END # # # # # # # # # # # # # # # # # # # # # # # # # # #


# WhatsApp Meesage Module
def send_whatsapp_msg(mtitle='TRADE-APP', mtext='Welcome to TradeApp!'):
    tkn = 'Bearer ' + getConfig('WA_TKN')
    url = 'https://graph.facebook.com/v16.0/108228668943284/messages'
    headers = {
        'Authorization': tkn,
        'Content-Type': 'application/json'
    }
    # payload = {"messaging_product": "whatsapp", "to": "919673843177", "type": "template", "template": { "name": "hello_world", "language": { "code": "en_US" } } }


    payload = {
        "messaging_product":"whatsapp",
        "recipient_type":"individual",
        "to":"919673843177",
        "type":"template",
        "template":{
            "name":"app_msg",
            "language":{
                "code":"en"
                },
            "components":[
                {
                    "type":"header",
                    "parameters":[
                        {
                            "type":"text",
                            "text":mtitle
                            }
                        ]
                    },
                {
                    "type":"body",
                    "parameters":[
                        {
                            "type":"text",
                            "text":mtext
                            }
                        ]
                    }
                ]
            }
        }

    # Send the POST request
    response = requests.post(url, headers=headers, json=payload)

    # Check the response
    if response.status_code == 200:
        # return render_template('index.html',alert_msg = 'WhatsApp Alert Trigerred')
        return {'status':'SUCCESS','msg':response.json()}
    else:
        # return render_template('index.html',alert_msg = response.text)
        return {'status':'ERROR','msg':response.text}