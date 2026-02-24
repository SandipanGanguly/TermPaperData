import os
import yfinance as yf
import pandas as pd

def update_yfinance_data(ticker, filename, interval="5m"):
    print(f"Checking {ticker}...")
    if os.path.exists(filename):
        # Load existing data
        old_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        # Fetch the last 7 days of data to append
        new_df = yf.download(tickers=ticker, interval=interval, period="7d")
        
        if not new_df.empty:
            combined_df = pd.concat([old_df, new_df])
            # Drop duplicates based on the exact 5-minute timestamp
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df.to_csv(filename)
            print(f"➔ Appended new data. {filename} now has {len(combined_df)} rows.\n")
        else:
            print(f"➔ No new data found for {ticker} at this time.\n")
    else:
        print(f"➔ {filename} not found. Downloading initial 60-day batch...")
        # Yahoo Finance allows a max of 60 days for 5-minute intervals
        df = yf.download(tickers=ticker, interval=interval, period="60d")
        df.to_csv(filename)
        print(f"➔ Created {filename} with {len(df)} rows.\n")

# 1. NIFTY 50 Index
update_yfinance_data("^NSEI", "NIFTY50_5min_Data.csv")

# 2. INDIA VIX (Volatility Index)
update_yfinance_data("^INDIAVIX", "INDIA_VIX_5min_Data.csv")

# 3. USD/INR Exchange Rate (Replaces FRED)
update_yfinance_data("INR=X", "USDINR_5min_Data.csv")
