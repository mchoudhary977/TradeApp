# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 14:50:56 2023

@author: Mukesh Choudhary
"""
from breeze_connect import BreezeConnect
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
from pyotp import TOTP
import json
import platform 
import time as tm
import datetime as dt
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os 

icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])


# ICICI Auto Logon 
def ic_autologon():
    # icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
    icici_session_url = json.load(open('config.json', 'r'))['ICICI_SESSION_URL']
    
    service = webdriver.chrome.service.Service('./chromedriver.exe' if platform.system()=='Windows' else './chromedriver')
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
def ic_create_session(icici):
    try:
        icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
        icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
        msg=f'ICICI Session Created for UserID - {icici.user_id}'
        # send_whatsapp_msg(mtitle='ICICI ALERT',mtext=msg)
        return{'status':'SUCCESS','data':msg}
    except Exception as e:
        err = str(e)
        if 'AUTH' in str(e).upper():
            session_id = ic_autologon()
            if session_id is not None:
                st=ic_create_session(icici)
                return st
            else:
                return{'status':'FAILURE','data':f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - {err} - Update ICICI Session Token - {json.load(open("config.json", "r"))["ICICI_SESSION_URL"]}'}
            # send_whatsapp_msg(mtitle='ERROR',mtext=f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - Update ICICI Session Token - {readConfig("ICICI_SESSION_URL")}')
            # send_whatsapp_msg(msg=f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - Update ICICI Session Token - {readConfig("ICICI_SESSION_URL")}')
        return{'status':'FAILURE','data':f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - {err}'}


# ICICI TokenLookup
def ic_tokenLookup(symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    if icici.user_id is None:
        st = ic_create_session(icici)
        if st['status'] != 'SUCCESS':
            return{'status':'FAILURE','data':'Session Initiation Failed'} 
    token_list = []
    for symbol in symbol_list:
        token_list.append(icici.get_names('NSE',symbol)['isec_token_level1'])
        # token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return{'status':'SUCCESS','data':token_list} 
     
        
# Get Symbol Data From ICICI API - ic_get_sym_detail()
def ic_get_sym_detail(exch_code='NSE',symbol='NIFTY',prod_type='Cash',interval='5minute',
                      duration=2):
    try:
        if icici.user_id is None:
            st = ic_create_session(icici)
            if st['status'] != 'SUCCESS':
                raise ValueError(st['data'])
        i=0 
        holiday_list = json.load(open('config.json', 'r'))['HOLIDAY_LIST']
        fdate=(datetime.now()-timedelta(duration))
        tdate=(datetime.now()-timedelta(0))
        while i<10:
            fdate = fdate-timedelta(i)
            if fdate.weekday() not in (5,6) and fdate.strftime('%Y-%m-%d') not in holiday_list:
                break
            i=i+1
        fdate = fdate.strftime('%Y-%m-%d')+'T00:00:00.000Z'
        tdate = tdate.strftime('%Y-%m-%d')+'T23:59:59.000Z'
        df_tick = icici.get_historical_data_v2(interval=interval, from_date = fdate,
                                               to_date = tdate, stock_code = symbol,
                                               exchange_code = exch_code,
                                               product_type = prod_type)
        if df_tick['Status']==200:
            if len(df_tick['Success']) > 0:
                df_tick = pd.DataFrame(df_tick['Success'])
                return {"status": "SUCCESS", "data": df_tick}
        else:
            return {"status": "FAILURE", "data": "Data Not Returned"}
                        
    except Exception as e:
        err = str(e)
        if 'AUTH' in str(e).upper():
            st=ic_create_session(icici)
            return st
        return{'status':'FAILURE','data':f'{datetime.now().strftime("%B %d, %Y %H:%M:%S")} - {err}'}
        print(f'iciciGetSymDetail :: {err}')
        return {"status": "FAILURE", "data": err} 
    

# Update Watchlist data
def ic_update_watchlist(mode='R'):
    symbol_list = json.load(open('config.json', 'r'))['STOCK_CODES']
    wl_file = 'WatchList.csv'
    ic_instruments = pd.read_csv('icici.csv')
    wl_cols= ['SymbolName','ExchangeCode','Segment','Token','Code','LotSize',
              'Open','High','Low','Close','PrevClose','CandleTime']
    wl_df = pd.read_csv(wl_file) if os.path.exists(wl_file) else pd.DataFrame(columns=wl_cols)
    if mode == 'C' or len(wl_df)==0:
        wl_df = pd.DataFrame(columns=wl_cols)
        for sym in symbol_list:
            response = ic_get_sym_detail(symbol=sym,interval='5minute')           
            sym = ic_instruments[ic_instruments['CD']==sym][['NS','EC','SG','TK','CD','LS']] 
            sym.rename(columns={'NS':'SymbolName','EC':'ExchangeCode','SG':'Segment',
                                'TK':'Token','CD':'Code','LS':'LotSize'}, inplace=True)
            sym['Open']=0
            sym['High']=0
            sym['Low']=0
            sym['Close']=0
            sym['PrevClose']=0
            sym['CandleTime']=datetime.now()
            
            if response['status']=='SUCCESS':
                data = pd.DataFrame(response['data'])
                data['datetime'] = data['datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
                data['date'] = data['datetime']
                data = data.set_index('datetime')
                data=data.resample('1D').agg({'date':'last','stock_code':'first','open': 'first','high':'max','low':'min','close':'last'}).dropna()[-2:]
                sym['Open']=data.iloc[-1]['open']
                sym['High']=data.iloc[-1]['high']
                sym['Low']=data.iloc[-1]['low']
                sym['Close']=data.iloc[-1]['close']
                sym['PrevClose']=data.iloc[-2]['close']
                sym['CandleTime']=data.iloc[-1]['date']
            wl_df = pd.concat([wl_df,sym],ignore_index=True)
        wl_df.to_csv('WatchList.csv',index=False)   
    else:
        inserted_symbols = [element for element in symbol_list if element not in wl_df]
        deleted_symbols = [element for element in wl_df if element not in symbol_list]
        
        if len(inserted_symbols) > 0:
            for sym in inserted_symbols:
                response = ic_get_sym_detail(symbol=sym,interval='5minute')           
                sym = ic_instruments[ic_instruments['CD']==sym][['NS','EC','SG','TK','CD','LS']] 
                sym.rename(columns={'NS':'SymbolName','EC':'ExchangeCode','SG':'Segment',
                                    'TK':'Token','CD':'Code','LS':'LotSize'}, inplace=True)
                sym['Open']=0
                sym['High']=0
                sym['Low']=0
                sym['Close']=0
                sym['PrevClose']=0
                sym['CandleTime']=datetime.now()
                
                if response['status']=='SUCCESS':
                    data = pd.DataFrame(response['data'])
                    data['datetime'] = data['datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
                    data['date'] = data['datetime']
                    data = data.set_index('datetime')
                    data=data.resample('1D').agg({'date':'last','stock_code':'first','open': 'first','high':'max','low':'min','close':'last'}).dropna()[-2:]
                    sym['Open']=data.iloc[-1]['open']
                    sym['High']=data.iloc[-1]['high']
                    sym['Low']=data.iloc[-1]['low']
                    sym['Close']=data.iloc[-1]['close']
                    sym['PrevClose']=data.iloc[-2]['close']
                    sym['CandleTime']=data.iloc[-1]['date']
                wl_df = pd.concat([wl_df,sym],ignore_index=True)
            wl_df.to_csv('WatchList.csv',index=False) 
            
        if len(deleted_symbols) > 0:
            wl_df = wl_df[~wl_df['Code'].isin(deleted_symbols)]
            wl_df.to_csv('WatchList.csv',index=False)
    return {"status": "SUCCESS", "data": wl_df} 



# function to extract all option contracts for a given ticker 
def ic_option_contracts(ticker, option_type="CE", exchange="NFO"):
    instrument_list = pd.read_csv('icici.csv')
    option_contracts = instrument_list[instrument_list["SC"]==ticker][instrument_list['EC']==exchange][instrument_list['OT']==option_type]
    return option_contracts  # pd.DataFrame(option_contracts)


# function to extract the closest expiring option contracts
def ic_option_contracts_closest(ticker, duration = 0, option_type="CE", exchange="NFO"):
    #duration = 0 means the closest expiry, 1 means the next closest and so on
    df_opt_contracts = ic_option_contracts(ticker,option_type)
    df_opt_contracts["time_to_expiry"] = (pd.to_datetime(df_opt_contracts["EXPIRY"]) - dt.datetime.now()).dt.days
    # df_opt_contracts["time_to_expiry"] = (pd.to_datetime(df_opt_contracts["EXPIRY"]) - datetime.now()).days
    min_day_count = np.sort(df_opt_contracts["time_to_expiry"].unique())[duration]
    
    return (df_opt_contracts[df_opt_contracts["time_to_expiry"] == min_day_count]).reset_index(drop=True)

# df_opt_contracts = ic_option_contracts_closest("CNXBAN",1)

#function to extract closest strike options to the underlying price
def ic_option_contracts_atm(ticker, underlying_price, duration = 0, option_type="CE", exchange="NFO"):
    #duration = 0 means the closest expiry, 1 means the next closest and so on
    df_opt_contracts = ic_option_contracts_closest(ticker,duration,option_type)
    return df_opt_contracts.iloc[np.array(abs(df_opt_contracts["STRIKE"] - underlying_price)).argmin()]

# atm_contract = ic_option_contracts_atm("CNXBAN",underlying_price=43946, duration=1)

#function to extract n closest options to the underlying price
def ic_option_chain(ticker, underlying_price, duration = 0, num = 7, option_type="CE", exchange="NFO"):
    #duration = 0 means the closest expiry, 1 means the next closest and so on
    #num =5 means return 5 option contracts closest to the market
    df_opt_contracts = ic_option_contracts_closest(ticker,duration,option_type)
    df_opt_contracts.sort_values(by=["STRIKE"],inplace=True, ignore_index=True)
    atm_idx = np.array(abs(df_opt_contracts["STRIKE"] - underlying_price)).argmin()
    up = int(num/2)
    dn = num - up
    return df_opt_contracts.iloc[atm_idx-up:atm_idx+dn]
    
# opt_chain = ic_option_chain("CNXBAN", underlying_price=43946, duration=0,option_type="PE")

# ticker='CNXBAN'