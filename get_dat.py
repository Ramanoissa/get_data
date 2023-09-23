import os
import binance.client
from binance.client import Client
import datetime as dt
import pandas as pd
import threading
import tickers as tkr
import pytz 


tickers = tkr.lists

client = Client(api_key="", api_secret="")


csv_folder_path = "csv_data"
if not os.path.exists(csv_folder_path):
    os.makedirs(csv_folder_path)

def save_csv_data(df, ticker):
    csv_file_path = os.path.join(csv_folder_path, f"{ticker}_data.csv")
    df.to_csv(csv_file_path, index=False)

def load_csv_data(ticker):
    csv_file_path = os.path.join(csv_folder_path, f"{ticker}_data.csv")
    if os.path.exists(csv_file_path):
        df = pd.read_csv(csv_file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    else:
        return None

def get_ticker_data(tickers, interval, depth):
    columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume', 'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']
    for ticker in tickers:
        klines = client.get_historical_klines(ticker, interval, depth)
        df = pd.DataFrame(klines)
        if not df.empty:
            df.columns = columns
            df['Date'] = pd.to_datetime(df['Date'], unit='ms')
            local_tz = pytz.timezone("Europe/Berlin")
            df['Date'] = df['Date'].dt.tz_localize(pytz.UTC).dt.tz_convert(local_tz)

    
            save_csv_data(df, ticker)
            print(f"Daten für {ticker} importiert und in CSV gespeichert")
        else:
            print(None)


def boost(callback, tickers, interval, depth):
    thread_list = []
    for ticker in tickers:
        th = threading.Thread(target=callback, args=(ticker, interval, depth))
        thread_list.append(th)
        th.start()
    for thrd in thread_list:
        thrd.join()

boost(get_ticker_data,tickers= tickers, interval="1d",depth= "600 hours ago UTC+2")



