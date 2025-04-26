
import requests
#bot api for cryptopairspoopoobot
API_TOKEN = "peepeepoopoo"
CHAT_ID = 69696696969
def send_telegram_message(message):

    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    response = requests.post(url, json=payload)
    return response.json()


def enter_trade_msg(long_ticker, long_price, long_qty, short_ticker, short_price, short_qty, z_score):
    message = f"""
    *Trade Entered!*
    Long: {long_ticker} @ {long_price} with {long_qty} contracts
    Short: {short_ticker} @ {short_price} with {short_qty} contracts
    Z-Score: {z_score:.2f}
    """

    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    r = requests.post(url, json=payload)
    return r

def exit_trade_msg(z_score, pnl):
    if pnl > 0:
        message = f"""
        🚀🚀🚀 *Exited Trade!* 
        PnL: 🟢{pnl:.4f} USDC
        Z-Score: {z_score:.2f}
        """ 
    else:
        message = f"""
        😭😭😭 *Exited Trade!*
        PnL: 🔴{pnl:.4f} USDC
        Z-Score: {z_score:.2f}
        """ 

    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    r = requests.post(url, json=payload)
    return r

def in_existing_trade(z_score, pnl): 
    if pnl > 0:
        message = f"""
        *Still in trade... ඞඞඞ!*
        PnL: 🟢{pnl:.4f} USDC
        Z-Score: {z_score:.2f}
        """
    else: 
        message = f"""
        *Still in trade... ඞඞඞ!*
        PnL: 🔴{pnl:.4f} USDC
        Z-Score: {z_score:.2f}
        """

    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    r = requests.post(url, json=payload)
    return r