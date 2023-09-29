from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 

# generate trading session
icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
# icici.user_id

# get dump of all NSE instruments
instrument_df=pd.read_csv('icici_scrip.csv')

def instrumentLookup(instrument_df, symbol_code):
    try:
        return instrument_df[instrument_df.CD==symbol_code].TK.values[0]
    except:
        return -1 
    
# instrumentLookup(instrument_df, "CNXBAN")

def fetchOHLC(ticker, interval, duration):
    instrument = instrumentLookup(instrument_df, ticker)
    from_date = (dt.datetime.now()-dt.timedelta(duration)).strftime('%Y-%m-%d')+'T00:00:00.000Z'
    to_date = dt.datetime.today().strftime('%Y-%m-%d')+'T23:59:59.000Z'   
    data = pd.DataFrame(icici.get_historical_data_v2(interval,from_date,to_date,ticker,'NSE','Cash')['Success'])
    data = data.rename(columns={'datetime': 'date'})
    data = data[['date','open','high','low','close','volume']]
    data.set_index("date",inplace=True)
    return data 
# data = fetchOHLC("CNXBAN", "2023-01-01", "5minute")

def fetchOHLCExtended(ticker='CNXBAN', inception_date='2023-01-01', interval='30minute'):
    instrument = instrumentLookup(instrument_df, ticker)
    from_date = dt.datetime.strptime(inception_date,"%Y-%m-%d")
    to_date = dt.datetime.today()
    data=pd.DataFrame(columns=['date','open','high','low','close','volume'])
    while True:
        if from_date.date() >= (dt.date.today() - dt.timedelta(100)):
            data1 = pd.DataFrame(icici.get_historical_data_v2(interval,
                                                             from_date.strftime('%Y-%m-%d')+'T00:00:00.000Z',
                                                             dt.datetime.today().strftime('%Y-%m-%d')+'T23:59:59.000Z',
                                                             ticker,'NSE','Cash')['Success'])
            data1 = data1.rename(columns={'datetime': 'date'})
            data1 = data1[['date','open','high','low','close','volume']]
            data=data.append(data1)
            break
        else:
            to_date = from_date + dt.timedelta(100)
            data1 = pd.DataFrame(icici.get_historical_data_v2(interval,
                                                             from_date.strftime('%Y-%m-%d')+'T00:00:00.000Z',
                                                             to_date.strftime('%Y-%m-%d')+'T23:59:59.000Z',
                                                             ticker,'NSE','Cash')['Success'])
            data1 = data1.rename(columns={'datetime': 'date'})
            data1 = data1[['date','open','high','low','close','volume']]
            data=data.append(data1)
            from_date = to_date
    
    data.set_index("date",inplace=True)
    return data 
# data = fetchOHLCExtended("CNXBAN", "2022-12-01", "30minute")
