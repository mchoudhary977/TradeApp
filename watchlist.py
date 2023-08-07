from trade_modules import *

import os


def startLiveMarketFeed(wl):
    try:
        if icici_api.user_id is None:
            st = createICICISession(icici_api)
            if st['status'] != 'SUCCESS':
                raise ValueError(st['data'])      
        icici_api.ws_connect()
        i=0
        while i < len(wl):
            tk = f"4.1!{wl.iloc[i]['TK']}"
            print(f'Live Feed Subscribed - {tk}')
            icici_api.subscribe_feeds(tk)
            i=i+1    
        icici_api.on_ticks = on_ticks
        msg = f'Live Market Feed Started - {datetime.now().strftime("%B %d, %Y %H:%M:%S")}'
        send_whatsapp_msg(mtitle='ICICI Alert',mtext=msg)
        return {"status": "SUCCESS", "data": msg}
    except Exception as e:
        err = str(e)
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        print(f'startLiveMarketFeed :: {line_number} : {err}')
        return {"status": "FAILURE", "data": err}


def endLiveMarketFeed(wl):
    try:
        if icici_api.user_id is None:
            st = createICICISession(icici_api)
            if st['status'] != 'SUCCESS':
                raise ValueError(st['data'])    
        i=0
        while i < len(wl):
            tk = f"4.1!{wl.iloc[i]['TK']}"
            print(f'Live Feed UnSubscribed - {tk}')
            icici_api.unsubscribe_feeds(tk)
            i=i+1    
        icici_api.ws_disconnect()
        msg = f'Live Market Feed Ended - {datetime.now().strftime("%B %d, %Y %H:%M:%S")}'
        send_whatsapp_msg(mtitle='ICICI Alert',mtext=msg)    
        return {"status": "SUCCESS", "data": msg}
    except Exception as e:
        err = str(e)
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        print(f'endLiveMarketFeed :: {line_number} : {err}')
        return {"status": "FAILURE", "data": err}


def on_ticks(ticks):
    try:
        global livePrices 
        if len(livePrices) > 0:
            livePrices.loc[livePrices['TK'] == ticks['symbol'][4:], 'CandleTime'] = ticks['ltt']
            livePrices.loc[livePrices['TK'] == ticks['symbol'][4:], 'Close'] = ticks['last']
            livePrices.loc[livePrices['TK'] == ticks['symbol'][4:], 'Open'] = ticks['open']
            livePrices.loc[livePrices['TK'] == ticks['symbol'][4:], 'High'] = ticks['high']
            livePrices.loc[livePrices['TK'] == ticks['symbol'][4:], 'Low'] = ticks['low']
        else:
            new_row = {'CandleTime': ticks['ltt'], 'Symbol': ticks['symbol'][4:], 'Close': ticks['last'], 
                       'Open': ticks['open'], 'High': ticks['high'], 'Low': ticks['low']}
            livePrices=pd.DataFrame(new_row, index = [0]) 
        
        # if ticks['symbol'][4:] == 'NIFTY 50':
        #     check_strategy_scalping_5ema(ticks['last'])    
        # # if ticks['symbol'][4:] == 'NIFTY 50':
        #     stgy_15Min(ticks['last'])
    except Exception as e:
        err = str(e)
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        print(f'on_ticks :: {line_number} : {err}')
        return {"status": "FAILURE", "data": err}
    
live_feed = 'N'
update_flag = 'N'
update_date = datetime.now()
update_icici_session_notify = 'N'
# ICICI_SESS_ID_DATE = datetime.now().date()
# livePrices = pd.DataFrame()

while True:
    try:
        global livePrices
        curr_dt = datetime.now()
        watchlist = getConfig('STOCK_CODES')
        file_path = 'WatchList.csv'
        wl = pd.DataFrame()
        scrip = pd.DataFrame()
        
        if os.path.exists(file_path):
            wl = pd.read_csv('WatchList.csv')
            if (len(wl) != len(watchlist)) or (curr_dt.time() > time(8,30) and curr_dt.time() < time(16,35) and curr_dt.minute%5== 0):
                update_flag = 'Y'
        else:
            update_flag = 'Y'
            
        
        if live_feed == 'N' and update_flag == 'Y':
            print(f"{curr_dt} - Updating WatchList")
            
            if os.path.exists('icici_scrip.csv')==False:
                csv_url = "https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv"
                response = requests.get(csv_url)
                csv_data = response.text
                csv_file = StringIO(csv_data)
                scrip = pd.read_csv(csv_file)
                scrip.to_csv('icici_scrip.csv',index=False)
            else:
                scrip = pd.read_csv('icici_scrip.csv')
            
            wl=pd.DataFrame()
            for i in watchlist:
                wl = pd.concat([wl, scrip.loc[scrip['SC']==i].loc[scrip['SM']==i]])
                # if len(wl)>0:
                #     wl = pd.concat([wl, scrip.loc[scrip['SC']==i].loc[scrip['SM']==i]])
                # else:
                #     wl = scrip.loc[scrip['SC']==i].loc[scrip['SM']==i]
            data_list = []
            
            for i in watchlist:
                data = iciciGetSymDetail(from_date=(datetime.now()-timedelta(1)), stock_code=str(i),interval="5minute")
                if data['status'] == 'SUCCESS':
                    update_date = curr_dt
                    data = data['data']
                    data['date']=data['datetime']
                    data.set_index('datetime', inplace=True)
                    data.index = pd.to_datetime(data.index)
                    data = data[data.index.time >= time(9,15)]
                    data = data.resample('1D').agg({'date':'last','stock_code':'first','open': 'first','high':'max','low':'min','close':'last'})
                    data_list.append(data.iloc[-1])
                else:
                    raise ValueError(data['data'])
            update_flag = 'N'
            if len(data_list)>0:  
                df = pd.DataFrame(data_list)
                df = df.rename(columns={'stock_code':'CD'})
                wl['Date'] = datetime.now().strftime('%Y-%m-%d')
                wl['Time'] = datetime.now().strftime('%H:%M:%S')
                wl['CandleTime'] = datetime.now()
                wl['Close'] = 0
                wl['Open'] = 0
                wl['High'] = 0
                wl['Low'] = 0
                for index, row in df.iterrows():
                    id_val = row['CD']
                    wl.loc[wl['CD'] == id_val, 'CandleTime'] = row['date']
                    wl.loc[wl['CD'] == id_val, 'Close'] = row['close']
                    wl.loc[wl['CD'] == id_val, 'Open'] = row['open']
                    wl.loc[wl['CD'] == id_val, 'High'] = row['high']
                    wl.loc[wl['CD'] == id_val, 'Low'] = row['low']  
                    
                # wl = pd.merge(wl,df, on='CD', how="left")
                # print(wl)
                wl.to_csv('WatchList.csv',index=False)
            
            if live_feed == 'N' and curr_dt.strftime('%Y-%m-%d') not in getConfig('HOLIDAY_LIST') and curr_dt.time() > time(9, 15) and curr_dt.time() < time(15, 35) and curr_dt.weekday() < 5:
                print ('In Live Feed section')
                live_feed = 'Y'
                livePrices = wl
                startLiveMarketFeed(wl)
        else:
            if live_feed == 'Y':
                livePrices.to_csv('WatchList.csv',index=False)                    
                if curr_dt.time() > time(15, 35):
                    live_feed = 'N'
                    endLiveMarketFeed(wl)    
    except Exception as e:
        err = str(e)
        tb = traceback.extract_tb(e.__traceback__)
        line_number = tb[-1].lineno
        if 'Invalid User' in err or 'AUTH' in err.upper():
            if update_date.date() != curr_dt.date():
                send_whatsapp_msg('ICICI Error', err)
                update_date != curr_dt
                # update_icici_session_notify = 'Y'
            session_id = icici_autologon()
            if session_id is not None:
                script_path = './startWebApp.sh'
                send_whatsapp_msg(mtitle='TA ALERT',mtext='Restarting App')
                subprocess.call([script_path])
                createICICISession(icici_api)
                
        print(f'watchlist.py :: {line_number} : {err}')
        
    if live_feed == 'Y':
        tm.sleep(1)
    else:
        tm.sleep(60)

# Start Live Feed from ICICI Direct
