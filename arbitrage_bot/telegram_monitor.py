# telegram_monitor.py
import asyncio
import re
import logging
from typing import List, Dict, Optional, Tuple, Callable, Awaitable
from telethon import TelegramClient, events

logger = logging.getLogger(__name__)

class TelegramMonitor:
    def __init__(self, api_id: int, api_hash: str, channels: List[str]):
        self.api_id = api_id
        self.api_hash = api_hash
        self.channel_usernames = channels
        self.client = TelegramClient(f"telegram_session_{api_id}", api_id, api_hash)
        self.is_running = False
        self.channel_entities = []
        logger.info("TelegramMonitor initialized.")

    def _parse_message(self, message: str) -> Optional[Tuple[str, float, Dict[str, float]]]:
        """
        Parses symbol, spread, and prices from various signal formats.
        Returns: (normalized_symbol, spread, {low_exch: price, high_exch: price}) or None
        """
        text = message.upper().replace(' ', '')

        # 1. Find #SYMBOL
        symbol_match = re.search(r'#([A-Z0-9]+)', text)
        if not symbol_match: return None
        raw_symbol = symbol_match.group(1)

        # 2. Find Spread: X.XX%
        spread_match = re.search(r'SPREAD[:]*([\d\.]+)%', text)
        if not spread_match: return None
        try:
            spread = float(spread_match.group(1))
        except (ValueError, IndexError):
            return None

        # 3. Find (COPY: SYMBOL) if it exists
        copy_match = re.search(r'\(COPY:([A-Z0_9]+)\)', text)
        if copy_match:
            raw_symbol = copy_match.group(1)

        # 4. Normalize symbol: WOJAKONX_USDT -> WOJAKONX-USDT
        symbol = raw_symbol.replace('_USDT', '').replace('USDT', '') + '-USDT'

        # 5. Find prices: Short EXCH: $X.XX | Long EXCH: $X.XX
        prices = {}
        short_match = re.search(r'SHORT([A-Z]+)[:]*\$([\d\.]+)', text)
        long_match = re.search(r'LONG([A-Z]+)[:]*\$([\d\.]+)', text)

        if short_match and long_match:
            try:
                prices[long_match.group(1).lower()] = float(long_match.group(2))
                prices[short_match.group(1).lower()] = float(short_match.group(2))
            except (ValueError, IndexError):
                return None # Prices are malformed

        if len(prices) != 2:
            return None

        return symbol, spread, prices

    async def start(self, callback: Callable[[str, float, Dict], Awaitable[None]]):
        self.is_running = True
        logger.info("Connecting to Telegram...")
        try:
            await self.client.start()
            logger.info("Telegram client started successfully.")

            for username in self.channel_usernames:
                try:
                    entity = await self.client.get_entity(username)
                    self.channel_entities.append(entity)
                    logger.info(f"Successfully resolved channel: {username}")
                except Exception as e:
                    logger.error(f"Failed to resolve channel '{username}': {e}. It will be skipped.")
            
            if not self.channel_entities:
                logger.error("No channels could be resolved. Stopping monitor.")
                self.is_running = False
                return

            @self.client.on(events.NewMessage(chats=self.channel_entities))
            async def handler(event):
                result = self._parse_message(event.message.message)
                if result:
                    symbol, spread, prices = result
                    # Create a new task to not block the event handler
                    asyncio.create_task(callback(symbol, spread, prices))

            logger.info(f"Started monitoring {len(self.channel_entities)} channel(s)...")
            await self.client.run_until_disconnected()

        except Exception as e:
            logger.error(f"A critical error occurred in TelegramMonitor: {e}")
        finally:
            self.is_running = False
            logger.info("TelegramMonitor event loop has stopped.")
            await self.stop()

    async def stop(self):
        if self.client and self.client.is_connected():
            logger.info("Disconnecting Telegram client...")
            await self.client.disconnect()
        self.is_running = False
