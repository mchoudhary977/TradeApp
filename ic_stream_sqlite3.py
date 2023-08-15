from trade_modules import * 
# from breeze_connect import BreezeConnect
import logging 
import os 
import datetime as dt 
import pandas as pd 
import json 
import sqlite3 

# generate trading session
# icici_session = json.load(open('config.json', 'r'))['ICICI_API_SESSION']
# icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
# icici.generate_session(api_secret=json.load(open('config.json', 'r'))['ICICI_API_SECRET_KEY'], session_token=icici_session)
# icici.user_id

if icici.user_id is None:
    st = createICICISession(icici)
    
db = sqlite3.connect('./ticks.db')

def create_tables(tokens):
    # db = sqlite3.connect('./ticks.db')
    c=db.cursor()
    for i in tokens:
        tk=i.split('!',1)[1].replace(' ','')
        # print(tk)
        c.execute("CREATE TABLE IF NOT EXISTS TOKEN{} (ts datetime primary key,price real(15,5), volume integer)".format(tk))
        # c.execute("CREATE TABLE IF NOT EXISTS TOKEN{} (ts varchar(100) primary key,price real(15,5), volume integer)".format(tk))
    try:
        db.commit()
    except:
        db.rollback()

def db_insert_ticks(ticks):
    # print(f"Insert Operation")
    db = sqlite3.connect('./ticks.db')
    c=db.cursor()
    try:
        tok = "TOKEN"+str(ticks['symbol'].split('!',1)[1].replace(' ',''))
        # print(f"{tok} Inserting")
        vals = [dt.datetime.strptime(ticks['ltt'][4:25], "%b %d %H:%M:%S %Y"),ticks['last'], ticks['ltq']]
        # vals = [ticks['ltt'],ticks['last'], ticks['ltq']]
        query = "INSERT INTO {}(ts,price,volume) VALUES (?,?,?)".format(tok)
        c.execute(query,vals)
    except:
        pass
    try:
        db.commit()
    except:
        db.rollback()    

def db_delete_ticks(tickers):
    db = sqlite3.connect('./ticks.db')
    c=db.cursor()
    try:
        tokens = tokenLookup(tickers)
        for token in tokens:
            tk = "TOKEN"+str(token.split('!',1)[1].replace(' ',''))
            df = pd.DataFrame(c.execute(f'''SELECT * FROM {tk} order by ts desc''').fetchall(), columns=['ts','price','volume'])
            df['Difference'] = df['price']-(df['price'].shift(-1))
            index_first_true = df.index[df['Difference'] != 0].min()
            first_occurrence = df.iloc[index_first_true]
            df['ts'] = df['ts'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
            max_timestamp = df.groupby(df['ts'].dt.date)['ts'].max()
            if isinstance(max_timestamp, dt.datetime) != True and len(max_timestamp) > 2:
                print(type(max_timestamp))
                max_timestamp = max_timestamp[1]
            else:
                max_timestamp = dt.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            # print(first_occurrence)
            # print(f'''DELETE FROM {tk} WHERE ts > '{first_occurrence["ts"]}'  or ts < '{max_timestamp}' ''')
            c.execute(f'''DELETE FROM {tk} WHERE ts > '{first_occurrence["ts"]}' or ts < '{max_timestamp}' ''')
            try:
                db.commit()
            except:
                db.rollback()    
    except:
        pass


# df
# df['ltt'] = df.index
# df['ts'] = df['ts'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))

def tokenLookup(symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(icici.get_names('NSE',symbol)['isec_token_level1'])
        # token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list


def on_ticks(ticks):
    # print(f"Ticks: {ticks['symbol']}-{ticks['stock_name']}-{ticks['ltt']}-{ticks['last']}-{ticks['ltq']}")
    insert_ticks=db_insert_ticks(ticks)
    

def subscribeFeed(tokens):
    for token in tokens:
        st = icici.subscribe_feeds(token)
        print(st)

def unsubscribeFeed(tokens):
    for token in tokens:
        st=icici.unsubscribe_feeds(token)
        print(st)

def get_hist(ticker,interval,db):
    token = tokenLookup(ticker)[0].split('!',1)[1].replace(' ','')
    data = pd.read_sql('''SELECT * FROM TOKEN%s WHERE ts >=  date() - '7 day';''' %token, con=db)                
    data = data.set_index(['ts'])
    data.index = pd.to_datetime(data.index)
    ticks = data.loc[:, ['price']]   
    df=ticks['price'].resample(interval).ohlc().dropna()
    return df



tickers = json.load(open('config.json', 'r'))['STOCK_CODES']
# tickers=['NIFTY','CNXBAN','NIFFIN','INDVIX','INFY']
# symbol_list=['NIFTY','CNXBAN','NIFFIN','INDVIX','INFY']
# symbol='NIFTY'
tokens = tokenLookup(tickers)
subscription_flag = 'N'

#create table
create_tables(tokens)

while True:  
    now = dt.datetime.now()   
    if (now.hour >= 9 and now.minute >= 14 and now.second >= 50 and 
        now.hour <= 15 and now.minute <= 30 and 
        subscription_flag=='N'):       
        icici.ws_connect()
        icici.on_ticks = on_ticks
        subscribeFeed(tokens)
        subscription_flag = 'Y'
    if (now.hour >= 15 and now.minute >= 35 and subscription_flag=='Y'):
        unsubscribeFeed(tokens)
        icici.ws_disconnect()
        subscription_flag='N'
        db_delete_ticks(tickers)
        break
         
db.close()
sys.exit()




"""
Testing the changes using dhan api




#get dump of all NSE instruments
instrument_list = pd.read_csv('https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv')
# dh_instrument_list = pd.read_csv('https://images.dhan.co/api-data/api-scrip-master.csv')

instrument_df = instrument_list

def instrumentLookup(instrument_df, symbol_code):
    try:
        return instrument_df[instrument_df.CD==symbol_code].TK.values[0]
    except:
        return -1 

# ohlc = fetchOHLC(ticker="NIFTY",interval="1minute",duration=5)


def fetchOHLC(ticker, interval, duration):
    instrument = instrumentLookup(instrument_df, ticker)
    from_date = (dt.datetime.now()-dt.timedelta(duration)).strftime('%Y-%m-%d')+'T00:00:00.000Z'
    to_date = dt.datetime.today().strftime('%Y-%m-%d')+'T23:59:59.000Z'   
    data = pd.DataFrame(icici.get_historical_data_v2(interval,from_date,to_date,ticker,'NSE','Cash')['Success'])
    data = data.rename(columns={'datetime': 'date'})
    data = data[['date','open','high','low','close','volume']]
    data.set_index("date",inplace=True)
    return data 

tickers=['NIFTY', 'CNXBAN', 'NIFFIN', 'INDVIX']
for ticker in tickers:
    ohlc = fetchOHLC(ticker,interval="1minute",duration=5)
    ohlc['symbol'] = tokenLookup([ticker])[0]
    ohlc['ltt'] = ohlc.index
    ohlc['ltt'] = ohlc['ltt'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%a %b %d %H:%M:%S %Y"))
    ohlc['last'] = ohlc['close']
    ohlc['ltq'] = 0
    
    for index, row in ohlc.iterrows():
        ticks = {}
        ticks['symbol'] = row['symbol']
        ticks['ltt'] = row['ltt']
        ticks['last'] = row['last']
        ticks['ltq'] = row['ltq']
        # print(f"Index: {index}, close: {row['close']}, open: {row['open']}")
        on_ticks(ticks)
        # print(f"{ticks}")
    


def on_ticks(ticks):
    # print(f"Ticks: {ticks['symbol']}-{ticks['stock_name']}-{ticks['ltt']}-{ticks['last']}-{ticks['ltq']}")
    insert_ticks=db_insert_ticks(ticks)
    

# ticker=['NIFTY']
# get_hist(['NIFTY'],'1d',db).iloc[-1]

"""


"""

    
c=db.cursor()
c.execute('SELECT name from sqlite_master where type= "table"')
c.fetchall()

c.execute('''PRAGMA table_info(TOKEN975873)''')
c.fetchall()
a1 = c.execute('''SELECT * FROM TOKENNIFTYFINSERVICE''').fetchall()
c.execute('''DELETE FROM TOKENNIFTYFINSERVICE WHERE ts >= '2023-08-09 16:00:00' ''')

c.execute('''SELECT DISTINCT substr(ts,1,10) FROM TOKENNIFTY50 order by substr(ts,1,10) desc limit 2 ''').fetchall()[1]

for m in c.execute('''SELECT * FROM TOKENNIFTY50'''):
    print(m)
for m in c.execute('''SELECT * FROM TOKENNIFTYBANK'''):
    print(m)
for m in c.execute('''SELECT * FROM TOKENNIFTYFINSERVICE'''):
    print(m)
for m in c.execute('''SELECT * FROM TOKENINDIAVIX'''):
    print(m)
for m in c.execute('''SELECT * FROM TOKEN1594'''):
    print(m)
"""