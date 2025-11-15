import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

# CMC
COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY', '')
# Retain other CMC config if used elsewhere, otherwise remove
CMC_CONTRACT_CACHE_TTL = int(os.getenv('CMC_CONTRACT_CACHE_TTL', '86400'))
CMC_REQUEST_TIMEOUT = float(os.getenv('CMC_REQUEST_TIMEOUT', '6.0'))
CMC_MAX_RETRIES = int(os.getenv('CMC_MAX_RETRIES', '2'))


# === API KEYS (из твоего .env) ===
BYBIT_API_KEY = os.getenv('BYBIT_REAL_API_KEY', '')
BYBIT_API_SECRET = os.getenv('BYBIT_REAL_SECRET', '')

MEXC_API_KEY = os.getenv('MEXC_REAL_API_KEY', '')
MEXC_API_SECRET = os.getenv('MEXC_REAL_SECRET', '')

GATE_API_KEY = os.getenv('GATE_REAL_API_KEY', '')
GATE_API_SECRET = os.getenv('GATE_REAL_SECRET', '')

BINGX_API_KEY = os.getenv('BINGX_REAL_API_KEY', '')
BINGX_API_SECRET = os.getenv('BINGX_REAL_SECRET', '')

# === EXCHANGES ===
EXCHANGES = {
    'bybit': {'apiKey': BYBIT_API_KEY, 'secret': BYBIT_API_SECRET, 'enabled': True},
    'mexc':  {'apiKey': MEXC_API_KEY,  'secret': MEXC_API_SECRET,  'enabled': True},
    'gate':  {'apiKey': GATE_API_KEY,  'secret': GATE_API_SECRET,  'enabled': True},
    'bingx': {'apiKey': BINGX_API_KEY, 'secret': BINGX_API_SECRET, 'enabled': True},
}

# === SETTINGS ===
LEVERAGE = 3
MAX_CONCURRENT_TRADES = 3
MIN_SPREAD = 3.0
MAX_MIN_NOTIONAL_USD = 4.0

# Arbitrage parameters (retained as they are used by order_manager and advanced_test_bot)
TRADE_AMOUNT = 1.0
CLOSE_SPREAD = 0.5
MAX_ALLOWED_SPREAD = 50.0
SPREAD_SANITY_CHECK = True
TRAILING_STOP_ENABLED = True
MAX_HOLD_TIME = 6000
MAX_DAILY_LOSS = 8.0
RISKY_SYMBOLS = {'0G': 0.5, 'XNL': 0.3, 'MET': 0.7, 'PIGGY': 0.6}
SYMBOL_BLACKLIST = {'AIA'}

# Channels
CHANNELS_TO_MONITOR_STR = os.getenv('CHANNELS_TO_MONITOR', '@futures_spreads_spam')
MONITOR_CHANNELS = [c.strip() for c in CHANNELS_TO_MONITOR_STR.split(',') if c.strip()]

TELEGRAM_CONFIG = {
    'api_id': API_ID,
    'api_hash': API_HASH,
    'session_file': 'telegram_sessions/arbitrage_session',
    'channels': MONITOR_CHANNELS
}

COINMARKETCAP_CONFIG = {
    'api_key': COINMARKETCAP_API_KEY,
    'enabled': True, # Assuming CMC is enabled if API key is provided
    'cache_ttl': CMC_CONTRACT_CACHE_TTL,
    'max_requests_per_minute': 30 # This was hardcoded, keeping it for now
}

# Local Token DB (retained as they are used by advanced_test_bot)
TOKEN_DB_PATH = os.getenv('TOKEN_DB_PATH', 'data/token_contracts.json')
TOKEN_DB_BACKUP_DIR = os.getenv('TOKEN_DB_BACKUP_DIR', 'data/backups')
TOKEN_DB_TTL_DAYS = int(os.getenv('TOKEN_DB_TTL_DAYS', '30'))
TOKEN_DB_HOT_CACHE_SIZE = int(os.getenv('TOKEN_DB_HOT_CACHE_SIZE', '256'))