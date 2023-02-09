import pandas as pd
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
            date_range = pd.date_range(dct['d0'], dct['d1'])
            date_str = [d.to_pydatetime().date().strftime("%Y-%m-%d") for d in date_range]
            date_str.pop(-1)
            s = dct['s']
            for date in date_str:
                if not date in new_mapping:
                    new_mapping[date] = {ticker: s}
                else:
                    new_mapping[date][ticker] = s
    return new_mapping, tickers


def parse(target=None, source=None):
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
    data_files = []

    for file in files:
        if file.endswith('ohlcv-1m.csv'):
            data_files.append(file)

    if data_files == []: raise ValueError('No file ending in ohlcv-1m.csv found in specified directory')
    mapping, tickers = get_ticker_mapping()

    for file in sorted(data_files):
        print(f"processing {file}")
        path = os.path.join(file_path, file)
        df = pd.read_csv(path)

        cols = ['ts_event', 'open', 'high', 'low', 'close']
        cleaned = pd.DataFrame()

        for col in cols:
            if df[col].dtypes != float and col != 'ts_event':
                df[col] = df[col].astype(float)
                cleaned[col] = df[col] / 1000000000

        # I was thinking about mapping the actual ticker name to the id but it is not necessary
        '''
        ticker_to_id = {}
        for key in list(mapping.keys()):
            t_to_i = mapping.get(key)
            ticker_to_id = {**ticker_to_id, **t_to_i}
        listof_product_id = list(df['product_id'])
        ticker_names = []
        for product_id in listof_product_id:
            ticker_names.append(get_key(product_id, ticker_to_id))
        cleaned['ticker'] = pd.DataFrame(ticker_names)
        print(cleaned['ticker'])
        '''

        cleaned['timestamp'] = pd.to_datetime(df['ts_event'], unit='ns', origin='unix')
        cleaned['volume'] = df['volume']
        cleaned['product_id'] = df['product_id']
        cleaned['ticker'] = cleaned['product_id']
        cleaned['dates'] = pd.to_datetime(cleaned['timestamp']).dt.date
        cleaned['time'] = pd.to_datetime(cleaned['timestamp']).dt.time

        dates = sorted(list(set(cleaned['dates'])))
        tickers = list(set(cleaned['ticker']))
        for ticker in tickers:
            filter_ticker = cleaned[cleaned['ticker'] == ticker]
            ticker_str = str(ticker)
            ticker_data_file_path = os.path.join(target_file_path, ticker_str)
            for date in sorted(dates):
                filtered_ticker_date = filter_ticker[filter_ticker['dates'] == date]
                date_str = str(date)
                if not os.path.exists(ticker_data_file_path):
                    os.makedirs(ticker_data_file_path)
                if len(filtered_ticker_date) != 0:
                    filtered_ticker_date.to_csv(os.path.join(ticker_data_file_path, f"xnas-itch-{date_str}.ohlcv-1m.csv"))
        del df
        del cleaned
        del filtered_ticker_date



parse(source='/Users/caoyujia/Desktop/databento',
        target='/Users/caoyujia/Desktop/databento')

