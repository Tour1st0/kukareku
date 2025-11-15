# websocket_manager.py
import asyncio
import logging
from typing import Dict, Optional, List
import ccxt.async_support as ccxt

# Configure logging
logger = logging.getLogger(__name__)

class WebSocketManager:
    """
    Manages dynamic WebSocket connections to cryptocurrency exchanges for real-time price updates
    with automatic reconnection and exponential backoff.
    """
    def __init__(self, config: dict):
        """
        Initializes the WebSocketManager.
        Args:
            config: A dictionary containing exchange configurations.
        """
        self.config = config
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.prices: Dict[str, Dict[str, float]] = {}  # Cache: {symbol: {exchange_id: price}}
        self._is_running = False
        self._watch_tasks: Dict[str, List[asyncio.Task]] = {}
        logger.info("WebSocketManager initialized.")

    def _get_exchange_symbol(self, symbol: str, exchange_id: str) -> str:
        """Gets the exchange-specific symbol format from a standard format like 'BTC-USDT'."""
        if exchange_id == 'bybit':
            return f"{symbol.replace('-', '/')}:USDT"
        elif exchange_id == 'mexc':
            return symbol.replace('-', '')
        elif exchange_id == 'gate':
            return symbol.replace('-', '_')
        elif exchange_id == 'bingx':
            return symbol  # BingX uses 'BTC-USDT' format
        else:
            logger.warning(f"No specific symbol format for {exchange_id}, using default.")
            return symbol.replace('-', '/')

    async def start(self):
        """
        Initializes all enabled exchanges from the configuration.
        """
        logger.info("Starting WebSocketManager and initializing exchanges...")
        self._is_running = True
        for exchange_id, params in self.config.items():
            if params.get('enabled', False):
                try:
                    exchange_class = getattr(ccxt, exchange_id)
                    exchange = exchange_class({
                        'apiKey': params.get('apiKey'),
                        'secret': params.get('secret'),
                        'options': {'defaultType': 'swap'},
                    })
                    self.exchanges[exchange_id] = exchange
                    logger.info(f"Initialized {exchange_id} exchange.")
                except Exception as e:
                    logger.error(f"Failed to initialize exchange {exchange_id}: {e}")
        logger.info("WebSocketManager started successfully.")

    async def subscribe(self, symbol: str):
        """
        Creates watcher tasks for a given symbol on all enabled exchanges.
        """
        if not self._is_running:
            logger.error("Cannot subscribe, WebSocketManager is not running. Call start() first.")
            return

        if symbol in self._watch_tasks:
            logger.debug(f"Already subscribed or subscribing to {symbol}. Skipping.")
            return

        logger.info(f"Subscribing to ticker updates for {symbol} on all enabled exchanges.")
        self._watch_tasks[symbol] = []
        for exchange_id, exchange in self.exchanges.items():
            task = asyncio.create_task(self._watch_ticker_loop(exchange, symbol))
            self._watch_tasks[symbol].append(task)

    async def _watch_ticker_loop(self, exchange: ccxt.Exchange, symbol: str):
        """
        Continuously watches the ticker for a symbol, with exponential backoff for reconnection.
        """
        exchange_id = exchange.id
        exchange_symbol = self._get_exchange_symbol(symbol, exchange_id)
        retry_delay = 1  # Initial delay in seconds

        logger.info(f"Starting watcher loop for {exchange_symbol} on {exchange_id}.")
        while self._is_running:
            try:
                ticker = await exchange.watch_ticker(exchange_symbol)
                price = ticker.get('last')
                if price is not None:
                    if symbol not in self.prices:
                        self.prices[symbol] = {}
                    self.prices[symbol][exchange_id] = price
                    logger.debug(f"Price update: {exchange_id} {symbol} = {price}")
                
                # Reset delay after a successful fetch
                retry_delay = 1
            
            except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable) as e:
                logger.warning(f"WS connection issue for {exchange_id} ({symbol}): {type(e).__name__}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 8)  # Exponential backoff up to 8 seconds
            
            except Exception as e:
                logger.error(f"Critical WS error for {exchange_id} ({symbol}): {e}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 8)

    async def get_price(self, symbol: str, exchange: str, timeout: int = 5) -> Optional[float]:
        """
        Retrieves the latest price for a symbol. If not subscribed, it initiates a subscription
        and waits for a specified timeout for the price to become available.
        """
        # 1. Subscribe on demand
        if symbol not in self._watch_tasks:
            await self.subscribe(symbol)
            await asyncio.sleep(1)  # Give a moment for the connection to establish

        # 2. Poll for the price for up to `timeout` seconds
        end_time = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end_time:
            price = self.prices.get(symbol, {}).get(exchange)
            if price is not None:
                return price
            await asyncio.sleep(0.2)

        # 3. If timeout is reached, log a warning and return None
        logger.warning(f"Timeout: Could not fetch price for {symbol} on {exchange} within {timeout}s.")
        return None

    async def close(self):
        """
        Closes all exchange connections and stops all watching tasks.
        """
        logger.info("Closing WebSocketManager...")
        self._is_running = False

        for symbol, tasks in self._watch_tasks.items():
            for task in tasks:
                task.cancel()
            logger.info(f"Cancelled ticker watch tasks for {symbol}.")
        
        # Wait for all tasks to be cancelled
        all_tasks = [task for tasks in self._watch_tasks.values() for task in tasks]
        await asyncio.gather(*all_tasks, return_exceptions=True)

        for exchange_id, exchange in self.exchanges.items():
            try:
                await exchange.close()
                logger.info(f"Closed connection for {exchange_id}.")
            except Exception as e:
                logger.error(f"Error closing connection for {exchange_id}: {e}")
        
        self.exchanges.clear()
        self.prices.clear()
        self._watch_tasks.clear()
        logger.info("WebSocketManager closed successfully.")