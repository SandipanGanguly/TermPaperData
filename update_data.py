import os
import yfinance as yf
import pandas_datareader.data as pdr
import pandas as pd
from datetime import datetime, timedelta

def update_yfinance_data(ticker, filename, interval="5m"):
    print(f"Checking {ticker}...")
    if os.path.exists(filename):
        old_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        new_df = yf.download(tickers=ticker, interval=interval, period="7d")
        
        if not new_df.empty:
            combined_df = pd.concat([old_df, new_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df.to_csv(filename)
            print(f"➔ Appended new data. {filename} now has {len(combined_df)} rows.\n")
    else:
        print(f"➔ {filename} not found. Downloading initial 60-day batch...")
        df = yf.download(tickers=ticker, interval=interval, period="60d")
        df.to_csv(filename)
        print(f"➔ Created {filename} with {len(df)} rows.\n")

def update_fred_data(ticker, filename):
    print(f"Checking {ticker} from FRED...")
    end_date = datetime.today()
    if os.path.exists(filename):
        old_df = pd.read_csv(filename, index_col="DATE", parse_dates=True)
        start_date = end_date - timedelta(days=30)
        new_df = pdr.DataReader(ticker, "fred", start_date, end_date)
        
        if not new_df.empty:
            combined_df = pd.concat([old_df, new_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df.to_csv(filename)
            print(f"➔ Appended new data. {filename} now has {len(combined_df)} rows.\n")
    else:
        print(f"➔ {filename} not found. Downloading initial batch...")
        start_date = end_date - timedelta(days=60)
        df = pdr.DataReader(ticker, "fred", start_date, end_date)
        df.to_csv(filename)
        print(f"➔ Created {filename} with {len(df)} rows.\n")

update_yfinance_data("^NSEI", "NIFTY50_5min_Data.csv")
update_yfinance_data("^INDIAVIX", "INDIA_VIX_5min_Data.csv")
update_fred_data("DEXINUS", "USD_INR_Daily_FRED.csv")
