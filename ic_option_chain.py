# -*- coding: utf-8 -*-
"""
ICICIDirect - Option chain

@author: Mukesh Choudhary
"""

from breeze_connect import BreezeConnect
import yfinance as yf
import logging 
import os 
import datetime as dt 
import pandas as pd 
import numpy as np
import json 
import sqlite3 
import re

# generate trading session
icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
# icici.user_id
# customer=icici.get_customer_details()

instrument_list = pd.read_csv('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')
# dh_instrument_list = pd.read_csv('https://images.dhan.co/api-data/api-scrip-master.csv')


instrument_list[instrument_list['TK']=='58715']
# x=dh_instrument_list[dh_instrument_list['SEM_SMST_SECURITY_ID']==35014]

#get dump of all NFO instruments
# instrument_list = pd.read_csv('icici_scrip.csv')
instrument_list['derivative'] = instrument_list[instrument_list['EC']=='NFO']['CD'].apply(lambda s: s.split('-')[0])
instrument_list['strike'] = instrument_list[instrument_list['EC']=='NFO']['CD'].apply(lambda s: float(re.search(r'-(\d+)(?:-[A-Za-z]{2})?$', s).group(1)) if re.search(r'-(\d+)(?:-[A-Za-z]{2})?$', s) is not None else '')
instrument_list['expiry'] = instrument_list[instrument_list['EC']=='NFO']['CD'].apply(lambda s: dt.datetime.strptime(re.search(r'\d{2}-[A-Za-z]{3}-\d{4}', s).group(), '%d-%b-%Y'))
instrument_list['opt_type'] = instrument_list[instrument_list['EC']=='NFO']['CD'].apply(lambda s: s.split('-')[-1])
instrument_list.rename(columns={'LS':'lot_size','TK':'token'}, inplace=True)

# function to extract all option contracts for a given ticker 
def option_contracts(ticker, option_type="CE", exchange="NFO"):
    option_contracts = instrument_list[instrument_list["SC"]==ticker][instrument_list['EC']==exchange][instrument_list['opt_type']==option_type]
    return option_contracts  # pd.DataFrame(option_contracts)


# function to extract the closest expiring option contracts
def option_contracts_closest(ticker, duration = 0, option_type="CE", exchange="NFO"):
    #duration = 0 means the closest expiry, 1 means the next closest and so on
    df_opt_contracts = option_contracts(ticker)
    df_opt_contracts["time_to_expiry"] = (pd.to_datetime(df_opt_contracts["expiry"]) - dt.datetime.now()).dt.days
    min_day_count = np.sort(df_opt_contracts["time_to_expiry"].unique())[duration]
    
    return (df_opt_contracts[df_opt_contracts["time_to_expiry"] == min_day_count]).reset_index(drop=True)

df_opt_contracts = option_contracts_closest("CNXBAN",1)

#function to extract closest strike options to the underlying price
hist_data = yf.download("^NSEBANK", period='5d')
underlying_price = round(hist_data["Adj Close"].iloc[-1],0)

def option_contracts_atm(ticker, underlying_price, duration = 0, option_type="CE", exchange="NFO"):
    #duration = 0 means the closest expiry, 1 means the next closest and so on
    df_opt_contracts = option_contracts_closest(ticker,duration)
    return df_opt_contracts.iloc[np.array(abs(df_opt_contracts["strike"] - underlying_price)).argmin()]

atm_contract = option_contracts_atm("CNXBAN",underlying_price, duration=1)

#function to extract n closest options to the underlying price
def option_chain(ticker, underlying_price, duration = 0, num = 7, option_type="CE", exchange="NFO"):
    #duration = 0 means the closest expiry, 1 means the next closest and so on
    #num =5 means return 5 option contracts closest to the market
    df_opt_contracts = option_contracts_closest(ticker,duration)
    df_opt_contracts.sort_values(by=["strike"],inplace=True, ignore_index=True)
    atm_idx = np.array(abs(df_opt_contracts["strike"] - underlying_price)).argmin()
    up = int(num/2)
    dn = num - up
    return df_opt_contracts.iloc[atm_idx-up:atm_idx+dn]
    
opt_chain = option_chain("CNXBAN", underlying_price, 0)
