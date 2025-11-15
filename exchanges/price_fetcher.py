import asyncio
import ccxt.async_support as ccxt
from typing import Dict, List, Optional, Tuple, Any
import logging
import time

from logging_config import get_logger

logger = get_logger(__name__)

class PriceStreamManager:
    """
    Manages real-time WebSocket price streams for multiple symbols across multiple exchanges
    using the built-in capabilities of ccxt.
    """
    def __init__(self, exchanges: Dict[str, ccxt.Exchange]):
        self.exchanges = exchanges
        self.price_streams: Dict[str, Dict[str, float]] = {}  # {symbol: {exchange: price}}
        self.active_subscriptions: set[str] = set()
        self._lock = asyncio.Lock()
        self._tasks: Dict[str, asyncio.Task] = {}
        logger.info("✅ [STREAM] PriceStreamManager initialized.")

    async def subscribe_to_symbols(self, symbols: List[str]):
        """
        Subscribes to ticker streams for a list of symbols on all exchanges.
        """
        async with self._lock:
            new_symbols = [s for s in symbols if s not in self.active_subscriptions]
            if not new_symbols:
                return

            for symbol in new_symbols:
                self.active_subscriptions.add(symbol)
                logger.info(f"🎯 [STREAM] Subscribing to {symbol} across all exchanges.")
                for exchange_name, exchange in self.exchanges.items():
                    task_id = f"{exchange_name}_{symbol}"
                    if task_id not in self._tasks:
                        task = asyncio.create_task(self._watch_loop(exchange, symbol))
                        self._tasks[task_id] = task

    async def _watch_loop(self, exchange: ccxt.Exchange, symbol: str):
        """
        The core watch loop for a single symbol on a single exchange.
        Handles reconnections internally.
        """
        logger.info(f"👂 [STREAM] Starting watch loop for {symbol} on {exchange.id}")
        while symbol in self.active_subscriptions:
            try:
                ticker = await exchange.watch_ticker(symbol)
                if ticker and 'last' in ticker:
                    price = float(ticker['last'])
                    if symbol not in self.price_streams:
                        self.price_streams[symbol] = {}
                    self.price_streams[symbol][exchange.id] = price
                    logger.debug(f"📊 [STREAM] {exchange.id} | {symbol} | ${price:.6f}")
            except asyncio.CancelledError:
                logger.info(f"🛑 [STREAM] Watch loop for {symbol} on {exchange.id} cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ [STREAM] Error in watch loop for {symbol} on {exchange.id}: {type(e).__name__} - {e}")
                await asyncio.sleep(5)

    def get_current_price(self, symbol: str, exchange_name: str) -> Optional[float]:
        """Gets the most recent price from the stream cache."""
        return self.price_streams.get(symbol, {}).get(exchange_name)

    async def unsubscribe_from_all(self):
        """Stops all active subscriptions and cancels running tasks."""
        async with self._lock:
            self.active_subscriptions.clear()
            for task_id, task in self._tasks.items():
                task.cancel()
            self._tasks.clear()
            self.price_streams.clear()
        logger.info("✅ [STREAM] All subscriptions stopped.")


class PriceFetcher:
    """
    Handles all price-related operations, prioritizing WebSocket streams
    with REST fallbacks, using the built-in async support of ccxt.
    """
    def __init__(self, exchanges_config: Dict):
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.setup_exchanges(exchanges_config)
        self.stream_manager = PriceStreamManager(self.exchanges)
        logger.info("✅ PriceFetcher initialized with ccxt.async_support.")

    def setup_exchanges(self, exchanges_config: Dict):
        """
        Initializes exchange instances using ccxt.async_support.
        """
        logger.info("🔧 [CCXT] Setting up exchanges using ccxt.async_support...")

        # Standard ccxt class names
        exchange_classes = {
            'bybit': ccxt.bybit,
            'bingx': ccxt.bingx,
            'gateio': ccxt.gateio, # Note: ccxt uses 'gateio'
            'mexc': ccxt.mexc
        }
        
        # A mapping for the user's config to the correct ccxt id
        name_to_id = {'gate': 'gateio'}

        for name, config in exchanges_config.items():
            ccxt_id = name_to_id.get(name, name)
            if ccxt_id in exchange_classes and config.get('enabled', False):
                try:
                    exchange_class = exchange_classes[ccxt_id]
                    self.exchanges[name] = exchange_class({
                        'apiKey': config.get('api_key', ''),
                        'secret': config.get('api_secret', ''),
                        'enableRateLimit': True,
                        'options': {'defaultType': 'swap'}
                    })
                    logger.info(f"✅ [CCXT] {name.upper()} initialized.")
                except Exception as e:
                    logger.error(f"❌ [CCXT] Failed to initialize {name}: {e}")

    async def find_market_symbol(self, exchange_name: str, base_symbol: str) -> Optional[str]:
        """
        Finds the correct, tradeable market symbol for a given base symbol (e.g., 'BTC' -> 'BTC/USDT').
        """
        exchange = self.exchanges.get(exchange_name)
        if not exchange: return None

        try:
            if not exchange.markets:
                await exchange.load_markets()
        except Exception as e:
            logger.error(f"❌ [MARKETS] Failed to load markets for {exchange_name}: {e}")
            return None

        variants = [f"{base_symbol}/USDT", f"{base_symbol}/USDT:USDT"]
        for variant in variants:
            if variant in exchange.markets:
                return variant
        
        # Fallback for symbols that might already be in the correct format
        if base_symbol in exchange.markets:
            return base_symbol
            
        logger.warning(f"⚠️ [MARKETS] Could not find a valid market for {base_symbol} on {exchange_name}.")
        return None

    async def get_symbol_price_with_cmc(self, exchange_name: str, symbol: str, **kwargs) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        """
        Primary method to get a price, now WebSocket-first.
        The method signature is kept for compatibility with the bot's main logic.
        """
        market_symbol = await self.find_market_symbol(exchange_name, symbol)
        if not market_symbol:
            return None, None, None

        # 1. Try live stream cache
        price = self.stream_manager.get_current_price(market_symbol, exchange_name)
        if price:
            return price, market_symbol, 'swap'

        # 2. One-time WebSocket request
        logger.warning(f"⚠️ [PRICE] No stream data for {market_symbol} on {exchange_name}. Trying one-time WS request.")
        price = await self.watch_ticker_once(exchange_name, market_symbol)
        if price:
            return price, market_symbol, 'swap'

        # 3. Fallback to REST
        logger.warning(f"⚠️ [PRICE] WS request failed for {market_symbol} on {exchange_name}. Falling back to REST.")
        price = await self.fetch_ticker_rest(exchange_name, market_symbol)
        if price:
            return price, market_symbol, 'swap'
            
        return None, None, None

    async def watch_ticker_once(self, exchange_name: str, market_symbol: str, timeout: int = 10) -> Optional[float]:
        """Performs a single, one-off ticker watch to get the current price via WebSocket."""
        exchange = self.exchanges.get(exchange_name)
        if not exchange: return None
        
        try:
            ticker = await asyncio.wait_for(exchange.watch_ticker(market_symbol), timeout=timeout)
            return float(ticker['last']) if ticker and 'last' in ticker else None
        except Exception as e:
            logger.error(f"❌ [WS_ONCE] Error for {market_symbol} on {exchange_name}: {type(e).__name__}")
        return None

    async def fetch_ticker_rest(self, exchange_name: str, market_symbol: str, timeout: int = 10) -> Optional[float]:
        """Fallback method to get a price via a standard REST API call."""
        exchange = self.exchanges.get(exchange_name)
        if not exchange: return None

        try:
            ticker = await asyncio.wait_for(exchange.fetch_ticker(market_symbol), timeout=timeout)
            return float(ticker['last']) if ticker and 'last' in ticker else None
        except Exception as e:
            logger.error(f"❌ [REST_FALLBACK] Error for {market_symbol} on {exchange_name}: {type(e).__name__}")
        return None

    async def find_arbitrage_opportunity_ws(self, base_symbol: str, min_spread: float) -> Optional[Dict]:
        """
        Finds arbitrage opportunities by using the live price streams.
        """
        market_symbol = f"{base_symbol}/USDT"
        logger.info(f"🎯 [WS_ARB] Searching for arbitrage for {market_symbol} (min spread: {min_spread}%)")

        await self.stream_manager.subscribe_to_symbols([market_symbol])
        await asyncio.sleep(2.5) # Allow time for streams to populate

        prices = self.stream_manager.price_streams.get(market_symbol, {})
        
        if len(prices) < 2:
            logger.warning(f"⚠️ [WS_ARB] Not enough price data for {market_symbol}. Found prices on {len(prices)} exchanges.")
            return None

        # Find best buy (min) and sell (max) prices
        min_price, max_price = float('inf'), float('-inf')
        buy_exchange, sell_exchange = None, None

        for exchange, price in prices.items():
            if price < min_price: min_price, buy_exchange = price, exchange
            if price > max_price: max_price, sell_exchange = price, exchange
        
        if buy_exchange and sell_exchange and buy_exchange != sell_exchange and min_price > 0:
            spread = ((max_price - min_price) / min_price) * 100
            if spread >= min_spread:
                logger.info(f"✅ [WS_ARB] Found opportunity for {market_symbol}! Spread: {spread:.2f}%")
                return {
                    'long_exchange': buy_exchange,
                    'short_exchange': sell_exchange,
                    'long_price': min_price,
                    'short_price': max_price,
                    'spread': spread,
                    'symbol': market_symbol
                }

        logger.info(f"📊 [WS_ARB] No arbitrage opportunity found for {market_symbol} meeting the {min_spread}% spread requirement.")
        return None

    async def close_all_connections(self):
        """
        Gracefully closes all active WebSocket connections.
        """
        logger.info("🔌 [CCXT] Closing all exchange connections...")
        await self.stream_manager.unsubscribe_from_all()
        
        tasks = [ex.close() for ex in self.exchanges.values() if hasattr(ex, 'close')]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            name = list(self.exchanges.keys())[i]
            if isinstance(result, Exception):
                logger.error(f"❌ [CCXT] Error closing connection for {name}: {result}")
            else:
                logger.info(f"✅ [CCXT] Connection for {name} closed.")
