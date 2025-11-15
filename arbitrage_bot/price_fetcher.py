# price_fetcher.py
import asyncio
import logging
from typing import Optional, Dict

from websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)

class PriceFetcher:
    """
    A simplified wrapper around WebSocketManager to fetch prices for any symbol on demand.
    """
    def __init__(self, config: Dict):
        """
        Initializes the PriceFetcher.
        Args:
            config: A dictionary containing exchange configurations for WebSocketManager.
        """
        self.ws_manager = WebSocketManager(config)
        logger.info("PriceFetcher initialized.")

    async def start(self):
        """
        Starts the underlying WebSocketManager.
        """
        logger.info("Starting PriceFetcher...")
        await self.ws_manager.start()
        logger.info("PriceFetcher started. Ready to fetch prices on demand.")

    async def get_price(self, symbol: str, exchange: str) -> Optional[float]:
        """
        Fetches the price for a given symbol and exchange using the WebSocketManager.
        This is now an asynchronous method.
        """
        logger.debug(f"Requesting price for {symbol} on {exchange}...")
        price = await self.ws_manager.get_price(symbol, exchange)
        if price is not None:
            logger.debug(f"Successfully fetched price for {symbol} on {exchange}: {price}")
        return price

    async def close(self):
        """
        Closes the underlying WebSocketManager connections.
        """
        logger.info("Closing PriceFetcher...")
        await self.ws_manager.close()
        logger.info("PriceFetcher closed.")

if __name__ == '__main__':
    from config import EXCHANGES  # Assuming config.py has the exchange configs

    async def main():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        fetcher = PriceFetcher(EXCHANGES)
        await fetcher.start()

        try:
            # Fetch prices for multiple symbols dynamically
            symbols_to_test = ['BTC-USDT', 'ETH-USDT']
            for _ in range(10):
                for symbol in symbols_to_test:
                    logger.info(f"--- Fetching prices for {symbol} ---")
                    bybit_price = await fetcher.get_price(symbol, 'bybit')
                    mexc_price = await fetcher.get_price(symbol, 'mexc')
                    gate_price = await fetcher.get_price(symbol, 'gate')
                    bingx_price = await fetcher.get_price(symbol, 'bingx')
                    
                    logger.info(f"Prices for {symbol}: Bybit={bybit_price}, MEXC={mexc_price}, Gate={gate_price}, BingX={bingx_price}")
                
                await asyncio.sleep(5)

        except KeyboardInterrupt:
            logger.info("Test interrupted by user.")
        finally:
            await fetcher.close()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program terminated.")
