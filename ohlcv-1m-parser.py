import pandas as pd 
import os
from datetime import datetime, timedelta
from datetime import datetime
import pandas_market_calendars as mcal
import json
import os


def get_ticker_mapping():
    
    path = os.getcwd()
    file_path = os.path.join(path, "metadata.json")

    with open(file_path) as f:
        meta_data = json.load(f)

    mappings = meta_data['symbology']['mappings']
    tickers = mappings.keys()

    new_mapping = {}
    for ticker in tickers:
        for dct in mappings[ticker]:
            
            date_range = pd.date_range(dct['d0'],dct['d1'])
            date_str = [d.to_pydatetime().date().strftime("%Y-%m-%d") for d in date_range]
            date_str.pop(-1)
            
            s = dct['s']
            
            for date in date_str:
                if not date in new_mapping: 
                    new_mapping[date] = {ticker:s}
                else:
                    new_mapping[date][ticker] = s
    
    return new_mapping, tickers
                            
            
                    
def get_date(file_name):
    """
    takes in a file name, outputs the date in the file name as datetime obj
    """
    
    year, month, day = '','',''
    counter = 0
    i = 0
    for i in range(len(file_name)):
        if file_name[i].isdigit():
            if counter < 4:
                year += file_name[i]
            elif counter < 6:
                month += file_name[i]
            elif counter < 8:
                day += file_name[i]
            counter += 1
            
    return datetime(int(year), int(month), int(day)).date().strftime("%Y-%m-%d") 



def parse(target = None, source = None):
    """
    put this file under a folder with supplied name (default to data)
    it will parse the raw databento csv under same directory into readable formats
    """
    if source == None:
        file_path = os.getcwd()
    else:
        file_path = source
        
    if target == None:
        target = file_path
    else:
        target_file_path = target
        
    files = os.listdir(file_path)
    data_files =  []
    
    for file in files: 
        if file.endswith('ohlcv-1m.csv'):
            data_files.append(file)

    if data_files == []: raise ValueError('No file ending in ohlcv-1m.csv found in specified directory')
    mapping, tickers = get_ticker_mapping()
    
    for file in sorted(data_files):
            
            print(f"processing {file}")
            path = os.path.join(file_path,file)
            df = pd.read_csv(path)
            
            d = get_date(file)
                
            cols = ['ts_event','open','high','low','close']
            cleaned = pd.DataFrame()

            for col in cols:
                if df[col].dtypes != float and col != 'ts_event':
                    df[col] = df[col].astype(float)
                cleaned[col] = df[col]/1000000000
                    
            cleaned['timestamp'] = pd.to_datetime(df['ts_event'], unit='ns', origin='unix')
            cleaned['volume'] = df['volume']
            cleaned['product_id'] = df['product_id']
            cleaned['ticker'] = cleaned['product_id']
            d = cleaned.iloc[1]['timestamp'].strftime("%Y-%m-%d")
            d = mapping[d]
            d = dict(zip(d.values(),d.keys()))
            t = []
            for i, row_value in cleaned['product_id'].items():
                t.append(d[str(row_value)])
            cleaned['ticker'] = t
            cleaned = cleaned.drop('ts_event', axis=1)

            date_str = get_date(file)

            for ticker in tickers:
                ticker_data = cleaned[cleaned['ticker'] == ticker]
                if not ticker_data.empty:
                    ticker_data_file_path = os.path.join(target_file_path, ticker)
                    if not os.path.exists(ticker_data_file_path):
                        os.makedirs(ticker_data_file_path)
                    ticker_data.to_csv(os.path.join(ticker_data_file_path,f"{ticker}-{date_str}.csv"))
            del df
            del cleaned
            del ticker_data

import time
start_time = time.time()
parse(  source = '/Users/jialechen/Documents/GitHub/BentoCruncher',
        target = '/Users/jialechen/Documents/GitHub/BentoCruncher/Nasdaq')
print("--- %s seconds ---" % (time.time() - start_time))
    

