import pandas as pd 
import os
from datetime import datetime, timedelta
​
def parse(data_folder_name = 'data', ticker = "MSFT"):
    """
    put this file under a folder with supplied name (default to data)
    it will parse the raw databento csv under same directory into readable formats
    """
    
    directory = os.getcwd()
    file_path = os.path.join(directory, data_folder_name)
    files = os.listdir(file_path)
    data_files =  []
    
    for file in files: 
        
        if file.endswith('-1m.csv'):
            data_files.append(file)
​
    for file in sorted(data_files):
        
            path = os.path.join(file_path,file)
            df = pd.read_csv(path)
            
            d = get_date(file)
                
            cols = ['ts_event','open','high','low','close']
            cleaned = pd.DataFrame()
​
            for col in cols:
                if df[col].dtypes != float and col != 'ts_event':
                    df[col] = df[col].astype(float)
                cleaned[col] = df[col]/1000000000
                    
            cleaned['ts_event'] = pd.to_datetime(df['ts_event'])
            cleaned['volume'] = df['volume']
​
            file_name = f'{d}'+'-{ticker}-1min.csv'
            file_name = os.path.join(file_path,file_name)
            cleaned.to_csv(file_name)
​
            del df
            del cleaned
            
                    
def get_date(file_name):
    """
    takes in a file name, outputs the date in the file name as datetime obj
    """
    
    year, month, day = '','',''
    counter = 0
    
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
​
parse()
