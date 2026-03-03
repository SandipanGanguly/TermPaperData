import pandas as pd
import numpy as np
import os

def run_analysis():
    print("Starting timezone alignment and correlation analysis...")
    
    # 1. Load datasets
    try:
        nifty = pd.read_csv("NIFTY50_5min_Data.csv", index_col=0, parse_dates=True)
        spx_fut = pd.read_csv("GLOBAL_SPX_FUT_5min_Data.csv", index_col=0, parse_dates=True)
    except FileNotFoundError as e:
        print(f"Error: Missing data file. {e}")
        return

    # 2. Force both indices to a common timezone (UTC)
    print("Aligning timezones to UTC...")
    if nifty.index.tz is None:
        nifty.index = nifty.index.tz_localize('Asia/Kolkata').tz_convert('UTC')
    else:
        nifty.index = nifty.index.tz_convert('UTC')

    if spx_fut.index.tz is None:
        spx_fut.index = spx_fut.index.tz_localize('America/New_York').tz_convert('UTC')
    else:
        spx_fut.index = spx_fut.index.tz_convert('UTC')

    # 3. Join datasets (Left join on Nifty to keep only Indian trading hours)
    print("Merging datasets...")
    master_df = nifty[['Close']].rename(columns={'Close': 'Nifty_Close'}).join(
        spx_fut[['Close']].rename(columns={'Close': 'SPX_Fut_Close'}),
        how='left'
    ).fillna(method='ffill') # Forward fill any missing ES=F ticks

    # 4. Compute 5-minute Log Returns
    print("Computing log returns...")
    master_df['r_nifty'] = np.log(master_df['Nifty_Close'] / master_df['Nifty_Close'].shift(1))
    master_df['r_spx'] = np.log(master_df['SPX_Fut_Close'] / master_df['SPX_Fut_Close'].shift(1))

    # 5. Drop NaNs and Calculate the Cross-Correlation
    master_df.dropna(inplace=True)
    correlation = master_df['r_nifty'].corr(master_df['r_spx'])

    # 6. Generate the Output Report
    obs_count = len(master_df)
    output_text = (
        f"--- Econometric Correlation Report ---\n"
        f"Observations (5-min ticks): {obs_count}\n"
        f"Cross-Correlation (r_nifty vs r_spx): {correlation:.4f}\n"
        f"--------------------------------------\n"
    )
    
    print(output_text)
    
    # Save to a text file
    with open("correlation_results.txt", "w") as f:
        f.write(output_text)
    print("Saved results to correlation_results.txt")

if __name__ == "__main__":
    run_analysis()
