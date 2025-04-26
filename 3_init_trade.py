from auth_keys import *
from database_utils import *
from trade_utils import *
import time 
from datetime import datetime
import schedule
import matplotlib.pyplot as plt 
import uuid
from coinbase.rest import RESTClient
import os 
import numpy as np 
import telegram_utils as tele

API_KEY = PERP_API_KEY
API_SECRET = PERP_PRIVATE_KEY

TRADE_AMOUNT = 100
LEVERAGE = 10

trade_pair = ['XRP-PERP-INTX', '1000PEPE-PERP-INTX']
print(f'[bold purple]Monitoring Trading for {trade_pair} ![/bold purple]')

def init_trading(pair):
    ticker1 = pair[0]
    ticker2 = pair[1]

    curr_time = pd.to_datetime(datetime.now()).tz_localize('Asia/Singapore')

    db1_name = f'{ticker1}.db'
    db2_name = f'{ticker2}.db'

    query1 = f'SELECT * from `{db1_name}`'
    query2 = f'SELECT * from `{db2_name}`'

    con1 = sqlite3.connect(db1_name)
    ticker1_df = pd.read_sql(query1, con1)
    ticker1_df = ticker1_df[-300:]
    ticker1_returns = np.log(ticker1_df['close']).diff().dropna()
    ticker1_returns.reset_index(inplace=True,drop=True)
    
    con2 = sqlite3.connect(db2_name)
    ticker2_df = pd.read_sql(query2, con2)
    ticker2_df = ticker2_df[-300:]
    ticker2_returns = np.log(ticker2_df['close']).diff().dropna()
    ticker2_returns.reset_index(inplace=True,drop=True)

    # ticker1 = X, ticker2 = y 
    # spread = y[i] - (beta * X[i] + alpha)

    spread_srs = rolling_reg(ticker1_returns,ticker2_returns, rolling_window=100)
    z_score_srs = compute_z_score(spread_srs['spread'], rolling_window=100)
    z_scores = z_score_srs.to_frame(name='z_score')
    latest_beta = spread_srs['beta'].iloc[-1] 
    latest_alpha = spread_srs['alpha'].iloc[-1]
    ticker1_lp = ticker1_df['close'].iloc[-1]
    ticker2_lp = ticker2_df['close'].iloc[-1]
    last_zscore = z_scores['z_score'].iloc[-1]
    secondlast_zscore = z_scores['z_score'].iloc[-2]
    cb_client = RESTClient(api_key=API_KEY, api_secret=API_SECRET)
    in_Trade = True if len(cb_client.list_perps_positions(PERP_UUID)['positions']) > 0 else False
    pnl_db = 'pnl.db'
    tx_db = 'trades.db'
    if in_Trade:
        position1_pnl = float(cb_client.list_perps_positions(PERP_UUID)['positions'][0]['aggregated_pnl']['value'])
        position2_pnl = float(cb_client.list_perps_positions(PERP_UUID)['positions'][1]['aggregated_pnl']['value'])
        total_pnl = position1_pnl + position2_pnl
        if  (last_zscore >= 0 and secondlast_zscore <= 0) or (last_zscore <= 0 and secondlast_zscore >= 0) or (last_zscore >= -0.2 and last_zscore <= 0.2):
            print(f'timestamp: {curr_time} | \nZ-Score: {last_zscore} | Close {ticker1} at {ticker1_lp} | Close  {ticker2} at {ticker2_lp}')
            
            orderid1 = cb_client.list_orders(product_ids=ticker1)['orders'][0]['order_id']
            orderid2 = cb_client.list_orders(product_ids=ticker2)['orders'][0]['order_id']
            print(f'[bold purple]PnL: {position1_pnl=}, {position2_pnl=}, {total_pnl=}[/bold purple]')

            print(cb_client.close_position(client_order_id=orderid1, product_id=ticker1))
            print(cb_client.close_position(client_order_id=orderid2, product_id=ticker2))

            fills1 = cb_client.list_orders(product_ids=ticker1, limit=1)['orders']
            fills2 = cb_client.list_orders(product_ids=ticker2, limit=1)['orders']
            fill_price1 = float(fills1[0]['average_filled_price'])
            fill_price2 = float(fills2[0]['average_filled_price'])
            fill_size1 = float(fills1[0]['filled_size'])
            fill_size2 = float(fills2[0]['filled_size'])
            trades_dict = {
                'timestamp' : [curr_time, curr_time],
                'side' : ['close', 'close'],
                'ticker' : [ticker1, ticker2],
                'price' : [fill_price1, fill_price2],
                'qty' : [fill_size1, fill_size2]
            }

            trades_df = pd.DataFrame(trades_dict, index = [0,1])
            db_con = sqlite3.connect(tx_db)
            trades_df.to_sql(tx_db, db_con, if_exists='append')
            print(f'[bold green]| {trades_df} appended to {tx_db}![/bold green]')

            pnl_dict = {'timestamp': curr_time, 'pnl' : total_pnl}
            pnl_df = pd.DataFrame(pnl_dict, index = [0])
            pnl_con = sqlite3.connect(pnl_db)
            pnl_df.to_sql('pnl', pnl_con, if_exists='append')
            print(f'[bold green]| {pnl_df} appended to {pnl_db}![/bold green]')

            tele.exit_trade_msg(last_zscore, total_pnl)


        else: 
            print(f'[purple] timestamp: {curr_time} | Z-Score: {last_zscore} | Chill bro we alr in the trade! let it cook [/purple]')
            print(f'x: {ticker1} at {ticker1_lp} | y: {ticker2} at {ticker2_lp}')
            print(f'[bold purple]PnL: {position1_pnl=}, {position2_pnl=}, {total_pnl=}[/bold purple]')

            tele.in_existing_trade(last_zscore, total_pnl)

    elif not in_Trade:
        if last_zscore < -2: 
            print(f'timestamp: {curr_time} | \nZ-Score: {last_zscore} | Beta: {latest_beta} | Alpha: {latest_alpha} |\n[red]Short {ticker1} at {ticker1_lp} | [green]Long  {ticker2} at {ticker2_lp}[/green]')
            #SHORT TICKER 1
            ticker1_info = cb_client.get_product(ticker1)
            short_curr_price = float(ticker1_info['price'])
            short_base_increment = ticker1_info['base_increment']
            
            #LONG TICKER 2 
            ticker2_info = cb_client.get_product(ticker2)
            long_curr_price = float(ticker2_info['price'])
            long_base_increment = ticker2_info['base_increment']
            
            ticker1_size, ticker2_size = calculate_positions(TRADE_AMOUNT, short_curr_price, long_curr_price, short_base_increment, long_base_increment, latest_beta, LEVERAGE)            
            
            print(cb_client.market_order_sell(client_order_id=str(uuid.uuid4()), product_id= ticker1, base_size= str(ticker1_size), leverage=str(LEVERAGE)))
            print(cb_client.market_order_buy(client_order_id=str(uuid.uuid4()), product_id= ticker2, base_size= str(ticker2_size), leverage=str(LEVERAGE)))
            
            fills1 = cb_client.list_orders(product_ids=ticker1, limit=1)['orders']
            fills2 = cb_client.list_orders(product_ids=ticker2, limit=1)['orders']
            fill_price1 = float(fills1[0]['average_filled_price'])
            fill_price2 = float(fills2[0]['average_filled_price'])


            trades_dict = {
                'timestamp' : [curr_time, curr_time],
                'side' : ['short', 'long'],
                'ticker' : [ticker1, ticker2],
                'price' : [fill_price1, fill_price2],
                'qty' : [ticker1_size, ticker2_size]
            }
            trades_df = pd.DataFrame(trades_dict, index = [0,1])
            if not os.path.isfile(tx_db):
                create_price_database(tx_db, trades_df)
            else:
                db_con = sqlite3.connect(tx_db)
                trades_df.to_sql(tx_db, db_con, if_exists='append')
                print(f'[bold green]| {trades_df} appended to {tx_db}![/bold green]')

            tele.enter_trade_msg(ticker2, fill_price2, ticker2_size, ticker1, fill_price1, ticker1_size, last_zscore)
        elif last_zscore > 2:
            print(f'timestamp: {curr_time} | \nZ-Score: {last_zscore} | Beta: {latest_beta} | Alpha: {latest_alpha} |\n[green]Long {ticker1} at {ticker1_lp}[/green] | [red]Short  {ticker2} at {ticker2_lp}[/red]')
            #LONG TICKER 1
            ticker1_info = cb_client.get_product(ticker1)
            long_curr_price = float(ticker1_info['price'])
            long_base_increment = ticker1_info['base_increment']
            
            #SHORT TICKER 2
            ticker2_info = cb_client.get_product(ticker2)
            short_curr_price = float(ticker2_info['price'])
            short_base_increment = ticker2_info['base_increment']
            
            ticker1_size, ticker2_size = calculate_positions(TRADE_AMOUNT, long_curr_price, short_curr_price, long_base_increment, short_base_increment, latest_beta, LEVERAGE)   
            print(cb_client.market_order_buy(client_order_id=str(uuid.uuid4()), product_id= ticker1, base_size= str(ticker1_size), leverage=str(LEVERAGE)))
            print(cb_client.market_order_sell(client_order_id=str(uuid.uuid4()), product_id= ticker2, base_size= str(ticker2_size),leverage=str(LEVERAGE)))

            fills1 = cb_client.list_orders(product_ids=ticker1, limit=1)['orders']
            fills2 = cb_client.list_orders(product_ids=ticker2, limit=1)['orders']
            fill_price1 = float(fills1[0]['average_filled_price'])
            fill_price2 = float(fills2[0]['average_filled_price'])


            trades_dict = {
                'timestamp' : [curr_time, curr_time],
                'side' : ['long', 'short'],
                'ticker' : [ticker1, ticker2],
                'price' : [fill_price1, fill_price2],
                'qty' : [ticker1_size, ticker2_size]
            }
            trades_df = pd.DataFrame(trades_dict, index = [0,1])
            
            if not os.path.isfile(tx_db):
                create_price_database(tx_db, trades_df)
            else:
                db_con = sqlite3.connect(tx_db)
                trades_df.to_sql(tx_db, db_con, if_exists='append')
                print(f'[bold green]| {trades_df} appended to {tx_db}![/bold green]')

            tele.enter_trade_msg(ticker1, fill_price1, ticker1_size, ticker2, fill_price2, ticker2_size, last_zscore)            
        else:
            print(f'Z-Score: {last_zscore} | Beta: {latest_beta} | Alpha: {latest_alpha} | No trades at {curr_time}|')
            print(f'x: {ticker1} at {ticker1_lp} | y: {ticker2} at {ticker2_lp}')

schedule.every().minute.at(":00").do(init_trading, trade_pair)
while True:
    schedule.run_pending()
    time.sleep(1)  