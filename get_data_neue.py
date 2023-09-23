import os
import binance.client
from binance.client import Client
import datetime as dt
import pandas as pd
import threading
import tickers as tkr
import datetime as dt
import pytz 

german_timezone = pytz.timezone('Europe/Berlin')

end_time = dt.datetime.now(german_timezone)

start_time = end_time - dt.timedelta(hours=600)

start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

csv_folder_path = "csv_data"

if not os.path.exists(csv_folder_path):
    os.makedirs(csv_folder_path)

tickers = tkr.lists

client = Client(api_key="", api_secret="")

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

def get_new_data(tickers, interval):
    for ticker in tickers:
        print(f"Abfrage für Ticker: {ticker}")
        existing_data = load_csv_data(ticker)
        
        if existing_data is not None and not existing_data.empty:
            existing_data['Date'] = pd.to_datetime(existing_data['Date'], utc=True)
            existing_data['Date'] = existing_data['Date'].dt.tz_convert(german_timezone)
            latest_date = existing_data['Date'].max()
            start_time = latest_date + pd.Timedelta(seconds=1)
            time_difference = (end_time - latest_date).total_seconds() / 3600
            
            depth = f"{str(int(time_difference))} hours ago UTC+2"
            
            klines = client.get_historical_klines(ticker, interval, str(start_time))
            new_data = [k for k in klines if int(k[0]) > start_time.timestamp() * 1000]
            df = pd.DataFrame(new_data)
            
        else:
            klines = client.get_historical_klines(ticker, interval, depth)
            df = pd.DataFrame(klines)

        if not df.empty:
            columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume', 'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']
            df.columns = columns
            df['Date'] = pd.to_datetime(df['Date'], unit='ms')
            df['Date'] = pd.to_datetime(df['Date'], unit='ms').dt.tz_localize(pytz.UTC).dt.tz_convert(german_timezone)
            
            if existing_data is not None and not existing_data.empty:
                combined_data = pd.concat([existing_data, df], ignore_index=True)
                save_csv_data(combined_data, ticker)
            else:
                save_csv_data(df, ticker)
            
            print(f"Daten für {ticker} aktualisiert und in CSV gespeichert")
        else:
            print(f"Keine neuen Daten für {ticker} gefunden")



def boost_neue_data(callback, tickers, interval):
    thread_list = []
    for ticker in tickers:
        th = threading.Thread(target=callback, args=(ticker, interval))
        thread_list.append(th)
        th.start()
    for thrd in thread_list:
        thrd.join()


# to run it use :
# boost_neue_data(get_new_data, tkr.lists,"1d")