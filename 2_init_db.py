from auth_keys import *
from database_utils import *
import time 
from datetime import datetime
import schedule

pair = ['XRP-PERP-INTX', '1000PEPE-PERP-INTX']

ticker1 = pair[0]
ticker2 = pair[1]
print(f'[bold purple]Starting Database for {ticker1} and {ticker2}...[/bold purple]')

for ticker in pair:
    db_name = f'{ticker}.db'
    trades_df = get_price_history(ticker, timeframe='ONE_MINUTE', start_time = None)
    create_price_database(db_name, trades_df)

schedule.every().minute.at(":58").do(job_with_threading, ticker1)
schedule.every().minute.at(":58").do(job_with_threading, ticker2)
# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)    