import pandas as pd
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import coint
from sklearn.linear_model import LinearRegression

def rolling_reg(x_returns, y_returns, rolling_window=100):
    spread_ls = []
    beta_ls = []
    alpha_ls = []
    
    for i in range(rolling_window, len(x_returns)):
        # Extract the window of returns
        x_window = x_returns.iloc[i-rolling_window:i].values.reshape(-1, 1)
        y_window = y_returns.iloc[i-rolling_window:i].values
        
        # Fit regression model
        regr = LinearRegression().fit(x_window, y_window)
        beta = regr.coef_[0]
        alpha = regr.intercept_
        
        # Compute spread (residual) for the current observation
        spread = y_returns.iloc[i] - (beta * x_returns.iloc[i] + alpha)
        spread_ls.append(spread)
        beta_ls.append(beta)
        alpha_ls.append(alpha)
    
    # Align index to the end of each window
    idx = x_returns.index[rolling_window:]
    spread_df = pd.DataFrame({'spread': spread_ls, 'beta': beta_ls, 'alpha': alpha_ls}, index=idx)
    
    return spread_df
def compute_z_score(spread, rolling_window=20):
    rolling_mean = spread.rolling(rolling_window).mean()
    rolling_std = spread.rolling(rolling_window).std()
    z_score = (spread - rolling_mean) / rolling_std
    return z_score

def get_position_size(portfolio_amount, leverage, market_price, base_min_size, quote_increment, beta, alpha, toHedge, max_total_trades = 2):
    max_price_per_trade = portfolio_amount / max_total_trades
    notional_value = max_price_per_trade * leverage
    if toHedge:
        notional_value = notional_value * beta + alpha
    size = notional_value/market_price
    
    if '.' in quote_increment:
        dp = len(quote_increment.split('.')[1])
    else:
        dp = 0
    final_size = round(size,dp)
    return max(final_size, base_min_size)

def calculate_positions(trade_capital, x_price, y_price, x_base_increment, y_base_increment, beta, leverage):
    
    total_notional = trade_capital*leverage
    x_notional = total_notional/(1+beta)
    y_notional = beta * x_notional
    
    x_shares = x_notional / x_price
    y_shares = y_notional / y_price
    
    def get_dp(base_increment):
        dp = len(base_increment.split('.')[1]) if '.' in base_increment else 0
        return dp
    x_dp = get_dp(x_base_increment)
    y_dp = get_dp(y_base_increment)

    X_size = round(x_shares,x_dp)
    y_size = round(y_shares, y_dp)
    return X_size, y_size


