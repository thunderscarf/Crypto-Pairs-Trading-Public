from auth_keys import API_KEY_NAME,PRIVATE_KEY,PERP_API_KEY, PERP_PRIVATE_KEY, PERP_UUID
from pairs_utils import *
from coinbase.rest import RESTClient
from json import dumps
from tqdm import tqdm
import pandas as pd

pd.options.display.float_format = '{:.10f}'.format 

# =============================================================================
# Finding Tickers
# =============================================================================

client = RESTClient(api_key=PERP_API_KEY, api_secret=PERP_PRIVATE_KEY)
top_n_ls = get_top_perps_by_trade_volume_cb(client = client, n=10)
successful_pairs_ls = get_pairs(top_n_ls, timeframe= "ONE_MINUTE")
print(successful_pairs_ls)
