import requests
import pandas as pd 
import sqlite3
from datetime import datetime 
import threading
from rich import print



def get_price_history(product_id, timeframe='ONE_DAY', start_time = None):
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
    output_df.dropna(inplace=True)
    output_df.reset_index(inplace=True, drop=True)
    output_df = output_df[['close', 'timestamp']]
    output_df.set_index('timestamp', inplace=True)
    return output_df

def create_price_database(db_name, prices_df):
    con = sqlite3.connect(db_name)
    prices_df.to_sql(db_name, con, if_exists = 'replace')
    print(f'{db_name} created.')

def get_current_price( product):
    base_url = "https://api.coinbase.com/api/v3/brokerage/market/products/"
    response = requests.get(f"{base_url}{product}")
    if response.status_code == 200:
        price = float(response.json()['price'])
        return price
    else:
        print(f"Failed to get price for {product}: {response.status_code}")

def append_to_db( ticker_name, database_name, db_connection):
    #get df to append
    curr_price = get_current_price(ticker_name)
    curr_timestamp = pd.to_datetime(datetime.now()).tz_localize('Asia/Singapore')
    rounded_timestamp = curr_timestamp.replace(second=0, microsecond=0) + pd.Timedelta(minutes=1)
    price_row = pd.DataFrame({'timestamp': rounded_timestamp, 'close': curr_price }, index=[0])
    price_row.set_index('timestamp', inplace=True, drop=True)
    
    price_row.to_sql(database_name, db_connection, if_exists='append')
    print(f'[bold green]| timestamp: {curr_timestamp} | price: {curr_price} | appended to {database_name}![/bold green]')
    return price_row

def append_function( ticker):
    db_name = f'{ticker}.db'
    con = sqlite3.connect(db_name)
    x = append_to_db(ticker, db_name, con)

def job_with_threading( ticker):
    # Run the task in a separate thread
    thread = threading.Thread(target=append_function, args=(ticker,))
    thread.start()