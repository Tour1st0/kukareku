# advanced_test_bot.py
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

from config import (
    API_ID, API_HASH, EXCHANGES, MONITOR_CHANNELS, MIN_SPREAD,
    SPREAD_SANITY_CHECK, MAX_ALLOWED_SPREAD
)
from order_manager import OrderManager
from price_fetcher import PriceFetcher
from telegram_monitor import TelegramMonitor
from dashboard import Dashboard

# --- Bot-specific Logging Handler ---
class BufferHandler(logging.Handler):
    """A logging handler that directs logs to the bot's internal buffer for the dashboard."""
    def __init__(self, bot_instance: Any):
        super().__init__()
        self.bot = bot_instance

    def emit(self, record):
        if hasattr(self.bot, 'log_buffer'):
            msg = self.format(record)
            self.bot.log_buffer.append(msg)
            if len(self.bot.log_buffer) > 100:
                self.bot.log_buffer.pop(0)

# --- Main Application ---
logger = logging.getLogger(__name__)

class ArbitrageBot:
    """
    The main orchestrator for the arbitrage bot.
    Initializes and coordinates all modules.
    """
    def __init__(self):
        self.is_running = False
        self.config = __import__('config')
        
        # State for Dashboard
        self.total_balance = 0.0
        self.signal_count = 0
        self.log_buffer = []
        
        self._setup_logging()

        # Initialize components
        self.price_fetcher = PriceFetcher(config=EXCHANGES)
        self.order_manager = OrderManager(price_fetcher=self.price_fetcher)
        self.telegram_monitor = TelegramMonitor(
            api_id=API_ID,
            api_hash=API_HASH,
            channels=MONITOR_CHANNELS
        )
        self.dashboard = Dashboard(self)
        logger.info("All components initialized.")

    def _setup_logging(self):
        handler = BufferHandler(self)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    async def start(self):
        self.is_running = True
        logger.info("Starting bot services...")
        await self.price_fetcher.start()
        await self.order_manager.connect()
        logger.info("Services connected. Starting main tasks...")

        await asyncio.gather(
            asyncio.create_task(self.dashboard.run()),
            asyncio.create_task(self.telegram_monitor.start(self.handle_signal)),
            asyncio.create_task(self.balance_loop())
        )

    async def stop(self):
        if not self.is_running: return
        self.is_running = False
        logger.info("Stopping bot services...")
        await self.telegram_monitor.stop()
        await self.order_manager.close()
        await self.price_fetcher.close()
        logger.info("All services stopped.")

    async def handle_signal(self, symbol: str, spread: float, prices: Dict[str, float]):
        self.signal_count += 1
        logger.info(f"СИГНАЛ #{self.signal_count}: {symbol} | Spread: {spread:.2f}% | Цены: {prices}")

        if spread < MIN_SPREAD: return
        if SPREAD_SANITY_CHECK and spread > MAX_ALLOWED_SPREAD: return

        try:
            low_exch = min(prices, key=prices.get)
            high_exch = max(prices, key=prices.get)
            await self.order_manager.open_arbitrage(symbol, spread, low_exch, high_exch, prices[low_exch], prices[high_exch])
        except Exception as e:
            logger.error(f"Error processing signal for {symbol}: {e}")

    async def balance_loop(self):
        while self.is_running:
            try:
                self.total_balance = await self.order_manager.get_balance()
                logger.debug(f"Total balance updated: ${self.total_balance:.2f}")
            except Exception as e:
                logger.error(f"Error updating total balance: {e}")
            await asyncio.sleep(30)

async def main_orchestrator():
    bot = ArbitrageBot()
    try:
        await bot.start()
    finally:
        await bot.stop()

if __name__ == "__main__":
    retry_delay = 5
    while True:
        try:
            asyncio.run(main_orchestrator())
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user.")
            break
        except Exception as e:
            logger.critical(f"CRITICAL ERROR in main loop: {e}. Restarting in {retry_delay}s...", exc_info=True)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)