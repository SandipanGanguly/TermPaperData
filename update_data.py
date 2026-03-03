import os
import yfinance as yf
import pandas as pd

def fetch_and_clean(ticker, period, interval):
    """Fetches yfinance data and flattens the messy MultiIndex columns."""
    df = yf.download(tickers=ticker, interval=interval, period=period)
    
    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        # yfinance returns multi-level columns (e.g., 'Close', '^NSEI'). 
        # This flattens it to a clean, single row: 'Close', 'High', etc.
        df.columns = df.columns.get_level_values(0)
        
    return df

def update_yfinance_data(ticker, filename, interval="5m"):
    print(f"Checking {ticker}...")
    
    if os.path.exists(filename):
        try:
            # Try to load the existing data
            old_df = pd.read_csv(filename, index_col=0, parse_dates=True)
            new_df = fetch_and_clean(ticker, "7d", interval)
            
            if not new_df.empty:
                combined_df = pd.concat([old_df, new_df])
                # Drop duplicates and sort to keep the timeline perfect
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df.sort_index(inplace=True)
                combined_df.to_csv(filename)
                print(f"➔ Appended new data. {filename} now has {len(combined_df)} rows.\n")
            else:
                print(f"➔ No new data found for {ticker} at this time.\n")
                
        except Exception as e:
            # If the CSV is corrupted (like the 15-column error), it triggers this.
            # It will delete the corrupted file and fetch a fresh 60-day batch!
            print(f"➔ Corrupted data detected for {ticker} ({e}). Fixing...")
            os.remove(filename)
            df = fetch_and_clean(ticker, "60d", interval)
            df.to_csv(filename)
            print(f"➔ Rebuilt clean {filename} with {len(df)} rows.\n")
            
    else:
        print(f"➔ {filename} not found. Downloading initial 60-day batch...")
        df = fetch_and_clean(ticker, "60d", interval)
        df.to_csv(filename)
        print(f"➔ Created {filename} with {len(df)} rows.\n")

# 1. NIFTY 50 Index
update_yfinance_data("^NSEI", "NIFTY50_5min_Data.csv")

# 2. INDIA VIX (Volatility Index)
update_yfinance_data("^INDIAVIX", "INDIA_VIX_5min_Data.csv")

# 3. USD/INR Exchange Rate
update_yfinance_data("INR=X", "USDINR_5min_Data.csv")

# 4. Global Volatility (VIX Futures)
update_yfinance_data("ES=F", "GLOBAL_SPX_FUT_5min_Data.csv")
