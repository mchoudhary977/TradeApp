import json
import requests
import pandas as pd
import datetime as dt
import time as tm
from datetime import timedelta, time


# (ticker, option_type="CE", exchange="NFO"):
def nse_option_contracts_closest(opt_data):
    opt_data = data['records']
    for i in opt_data['expiryDates']:
        if dt.datetime.strptime(i, '%d-%b-%Y').date() >= now.date():
            expiry_date = dt.datetime.strptime(i, '%d-%b-%Y')
            break
    
    expiry_date = expiry_date.strftime('%d-%b-%Y')
    opt_list = []
    for opt in opt_data['data']:
        if opt['expiryDate'] == expiry_date:
            opt_list.append(opt)
    expiry_date = dt.datetime.strptime(i, '%d-%b-%Y')
    
    ce_open_interest = 0
    pe_open_interest = 0
    ce_volume = 0
    pe_volume = 0
    for i in opt_list:
        for key, value in i.items():
            if key == 'CE':
                ce_open_interest = ce_open_interest + value['openInterest']
                # ce_volume = ce_volume + value['totalBuyQuantity'] + value['totalSellQuantity']
                ce_volume = ce_volume + value['totalTradedVolume']
            
            if key == 'PE':
                pe_open_interest = pe_open_interest + value['openInterest']
                # pe_volume = pe_volume + value['totalBuyQuantity'] + value['totalSellQuantity']
                pe_volume = pe_volume + value['totalTradedVolume']
    
    opt_dict = {}
    opt_dict['CE'] = {'totOI':ce_open_interest, 'totVol':ce_volume}
    opt_dict['PE'] = {'totOI':pe_open_interest, 'totVol':pe_volume}
    opt_dict['data'] = opt_list
    return opt_dict, expiry_date

def nse_pcr_calculation(ticker): 
    sym_code = 'NIFTY' if ticker == 'NIFTY' else ('BANKNIFTY' if ticker == 'CNXBAN' else ('FINNIFTY' if ticker == 'NIFFIN' else  None))
    if sym_code is None:
        return
        # print('continuing')     
    url = f'https://www.nseindia.com/api/option-chain-indices?symbol={sym_code}'
    headers = {
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'accept-encoding' : 'gzip, deflate, br',
        'accept-language' : 'en-US,en;q=0.9'
    }
    response = requests.get(url, headers=headers).content
    
    data = json.loads(response.decode('utf-8'))
    if len(data['filtered']) <= 0:
        return
    
    filtered_opt_data =  data['filtered']
    expiry_date  = dt.datetime.strptime(filtered_opt_data['data'][0]['expiryDate'], '%d-%b-%Y')
    
    if now.date() > expiry_date.date():
        filtered_opt_data, expiry_date = nse_option_contracts_closest(data)
    
    
    oi_pcr = round(filtered_opt_data['PE']['totOI']/filtered_opt_data['CE']['totOI'],2)
    
    opt_dict = {'underlying':[], 'underlyingValue':[], 'strikePrice':[],
           'expiryDate':[], 'ce_lastPrice':[], 'pe_lastPrice':[], 
           'ce_openInterest':[], 'pe_openInterest':[], 
           'ce_changeinOpenInterest':[], 'pe_changeinOpenInterest':[], 'coi_pcr':[]}
    for i in filtered_opt_data['data']:
        coi_pcr = round(i['PE']['changeinOpenInterest']/(1 if i['CE']['changeinOpenInterest'] == 0 else i['CE']['changeinOpenInterest']),2)
        
        opt_dict['underlying'].append(i['CE']['underlying'])
        opt_dict['underlyingValue'].append(i['CE']['underlyingValue'])
        opt_dict['strikePrice'].append(i['strikePrice'])
        
        opt_dict['expiryDate'].append(i['expiryDate'])
        opt_dict['ce_lastPrice'].append(i['CE']['lastPrice'])
        opt_dict['pe_lastPrice'].append(i['PE']['lastPrice'])
        opt_dict['ce_openInterest'].append(i['CE']['openInterest'])
        opt_dict['pe_openInterest'].append(i['PE']['openInterest'])
        opt_dict['ce_changeinOpenInterest'].append(i['CE']['changeinOpenInterest'])
        opt_dict['pe_changeinOpenInterest'].append(i['PE']['changeinOpenInterest'])
        opt_dict['coi_pcr'].append(coi_pcr)
    
    opt_df = pd.DataFrame(opt_dict)    
    opt_df['oi_pcr'] = oi_pcr
    opt_df['datetime'] = now.replace(second=0,microsecond=0)
    opt_df['total_ce_oi'] = filtered_opt_data['CE']['totOI']
    opt_df['total_pe_oi'] = filtered_opt_data['PE']['totOI']

    return opt_df

    
# sym_code = 'NIFTY'
# now = dt.datetime.now()
tickers = ['NIFTY','CNXBAN','NIFFIN']
while True:
    now = dt.datetime.now()
    try:
        if now.time() >= time(9,15) and now.time() <= time(15,30) and now.minute == 5 and now.second == 5:
        # if now.minute == 5 and now.second == 5:
        # if now.time() >= time(9,15) and now.time() <= time(15,30):
            print(now)
            for ticker in tickers:
                
                opt_df = nse_pcr_calculation(ticker)
                
                ce_most_oi = opt_df.sort_values(by='ce_openInterest', ascending=False).head(3)
                pe_most_oi = opt_df.sort_values(by='pe_openInterest', ascending=False).head(3)
                
                ce_most_coi = opt_df.sort_values(by='ce_changeinOpenInterest', ascending=False).head(3)
                pe_most_coi = opt_df.sort_values(by='pe_changeinOpenInterest', ascending=False).head(3)
                
                print (f"{ticker} -> PCR = {opt_df['oi_pcr'].iloc[0]} -> Expiry Date = {opt_df['expiryDate'].iloc[0]}")
        else:
            tm.sleep(1)
    except Exception as e:
        err = str(e)
        print(err)
        pass