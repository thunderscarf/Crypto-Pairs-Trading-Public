import ccxt
import pandas as pd
from coinbase.rest import RESTClient
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import coint
from itertools import combinations
from tqdm import tqdm
import requests
import json
import sqlite3
from coinbase import jwt_generator
from datetime import datetime, timedelta
import logging 

def adf_test(series):
    '''
    returns adf statistic, p-value
    '''
    result = adfuller(series)
    return result[0], result[1]

def fetch_price_data_ccxt(symbol, timeframe='1d', since=None):
    '''
    Fetch historical OHLCV data (Open, High, Low, Close, Volume)
    '''
    exchange = ccxt.binance()
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def fetch_price_data_cb(product_id, timeframe='ONE_DAY', start_time = None):
    '''
    timeframe args: ONE_MINUTE , FIVE_MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, TWO_HOUR, SIX_HOUR, ONE_DAY
    '''
    base_url = f"https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/candles"
    params = { "granularity": timeframe }
    if start_time:
        params["start"] = start_time  # Add start time parameter
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {product_id}: {e}")
        return None
    
    df = pd.DataFrame(data)
    output_df = pd.DataFrame(df["candles"].tolist())
    output_df[["open", "high", "low", "close", "start"]] = output_df[["open", "high", "low", "close", "start"]].apply(lambda x: pd.to_numeric(x, errors="coerce"))
    output_df['start'] = pd.to_datetime(output_df['start'], unit='s', utc=True)
    output_df['timestamp'] = output_df['start'].dt.tz_convert('Asia/Singapore')
    output_df = output_df.sort_values(by="timestamp", ascending=True)
    output_df.drop(labels = "start", axis=1 , inplace=True)
    output_df.reset_index(inplace=True, drop=True)
    return output_df

def get_top_perps_by_trade_volume_cb(client, n=10):
    response = client.get_products(product_type = "FUTURE", contract_expiry_type="PERPETUAL")
    # products_data = response.json() 
    # product_list = response.get('products', [])
    market_data = []
    for product in response['products']:  # Replace 'products' with the actual key if different
        market_data.append({
            "price": product["price"],
            "product_id": product["product_id"],
            "volume": product["volume_24h"],
        })

    df = pd.DataFrame(market_data)
    df["price"] = pd.to_numeric(df["price"], errors='coerce')
    df["volume"] = pd.to_numeric(df["volume"], errors='coerce')
    df["volume_mul_price"] = df["volume"]*df["price"]
    df = df.sort_values(by='volume_mul_price', ascending=False)
    df.reset_index(inplace= True, drop= True)
    return df.head(n)["product_id"].tolist()

def get_pairs(top_n_ls, timeframe= "ONE_DAY", p_val_threshold=0.05, is_ccxt = False, is_cb = True ):
    pair_pval_dict = {}

    crypto_pairs = list(combinations(top_n_ls, 2))

    # Convert the combinations to list of tuples
    crypto_pairs = [(pair[0], pair[1]) for pair in crypto_pairs]
    # successful_pairs_ls = []

    for pair in tqdm(crypto_pairs, desc="Processing Pairs"):
        print(pair[0],pair[1])
        if is_ccxt:
            data_1 = fetch_price_data_ccxt(pair[0])
            data_2 = fetch_price_data_ccxt(pair[1])
        elif is_cb:
            data_1 = fetch_price_data_cb(pair[0], timeframe)
            data_2 = fetch_price_data_cb(pair[1], timeframe)

        df = pd.merge(data_1[['timestamp', 'close']], data_2[['timestamp', 'close']], on='timestamp', suffixes=(f'_{pair[0].replace("/", "_")}', f'_{pair[1].replace("/", "_")}'))
        if len(df) < 150:
            print(f"{pair} doesn't have enough data points, moving on...")
            continue
        adf__stat_1, p_val_1 = adf_test(df[f'close_{pair[0].replace("/", "_")}'])
        adf__stat_2, p_val_2 = adf_test(df[f'close_{pair[1].replace("/", "_")}'])
        
        # print(f"p-value for adf test for {pair[0]} is {p_val_1}")
        # print(f"p-value for adf test for {pair[1]} is {p_val_2}")

        if p_val_1 > p_val_threshold and p_val_2 > p_val_threshold:
            score, p_value, _ = coint(df[f'close_{pair[0].replace("/", "_")}'], df[f'close_{pair[1].replace("/", "_")}'])
            # print('Cointegration p-value:', p_value)
            
            if p_value < p_val_threshold:
                print(f"YAYY WE HAVE FOUND A PAIR FOR {pair}")
                pair_pval_dict[pair] = p_value
                # successful_pairs_ls.append(pair)
            else:
                pass
                # print("Moving on to the next pair...")
    
    successful_pairs_df = pd.DataFrame(pair_pval_dict.items(), columns=['pair', 'p-value'])
    successful_pairs_df = successful_pairs_df.sort_values(by="p-value", ascending=True)
    print(successful_pairs_df)
    successful_pairs_ls = successful_pairs_df['pair'].tolist()
    return successful_pairs_ls