# order_manager.py
import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import ccxt.async_support as ccxt

from config import (
    LEVERAGE, MAX_DAILY_LOSS, RISKY_SYMBOLS, SYMBOL_BLACKLIST,
    MAX_CONCURRENT_TRADES, TRADE_AMOUNT, CLOSE_SPREAD, MAX_HOLD_TIME,
    TRAILING_STOP_ENABLED, MAX_MIN_NOTIONAL_USD, EXCHANGES
)
from logging_config import get_logger

logger = get_logger(__name__)

# Forward declaration for type hinting
if False:
    from price_fetcher import PriceFetcher

class OrderManager:
    def __init__(self, price_fetcher: 'PriceFetcher'):
        self.price_fetcher = price_fetcher
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.active_trades: Dict[str, Dict] = {}
        self.daily_pnl: float = 0.0
        self.last_pnl_reset = datetime.utcnow()
        self._market_cache: Dict = {}
        logger.info("OrderManager initialized.")

    async def connect(self):
        logger.info("Connecting and configuring exchanges...")
        exchange_classes = {
            "bybit": ccxt.bybit, "bingx": ccxt.bingx,
            "gate": ccxt.gateio, "mexc": ccxt.mexc
        }

        for name, cfg in EXCHANGES.items():
            if not cfg.get("enabled", False):
                continue
            try:
                timeout = 60000 if name == 'gate' else 30000
                options = {"defaultType": "swap"}
                if name == 'gate':
                    logger.info("Disabling fetchCurrencies for Gate.io to prevent timeout.")
                    options['fetchCurrencies'] = False

                params = {
                    "apiKey": cfg.get("apiKey"),
                    "secret": cfg.get("secret"),
                    "enableRateLimit": True,
                    "timeout": timeout,
                    "options": options,
                }
                exchange = exchange_classes[name](params)

                if name == 'gate':
                    logger.info("Applying special connection logic for Gate.io...")
                    # Override fetch_currencies to prevent timeout during load_markets
                    async def dummy_fetch_currencies(self, params={}):
                        logger.warning("Gate.io fetch_currencies call bypassed to prevent timeout.")
                        return {}
                    exchange.fetch_currencies = dummy_fetch_currencies.__get__(exchange, exchange.__class__)
                    
                    await exchange.load_markets()
                    await exchange.fapiPublicGetContracts()
                else:
                    await exchange.load_markets()

                # === УСТАНОВКА РЕЖИМА ПОЗИЦИИ И ПЛЕЧА (EXCHANGE-SPECIFIC) ===
                try:
                    # --- 1. Установка режима хеджирования (Hedge Mode) ---
                    if name in ['bybit', 'bingx', 'mexc', 'gate']:
                        if hasattr(exchange, 'set_position_mode'):
                            # Для Gate.io hedge mode включается на уровне аккаунта, но вызов не повредит
                            await exchange.set_position_mode(hedged=True, symbol=None)
                            logger.info(f"{name.upper()}: Position mode set to 'hedged'.")

                    # --- 2. Установка плеча (Leverage) ---
                    if hasattr(exchange, 'set_leverage'):
                        logger.info(f"Attempting to set leverage to {LEVERAGE}x for {name.upper()}...")
                        
                        if name == 'bybit':
                            # Bybit требует 'category' для Unified аккаунтов
                            params = {'category': 'linear'}
                            await exchange.set_leverage(LEVERAGE, 'ETH/USDT:USDT', params)

                        elif name == 'bingx':
                            # BingX в режиме хеджирования требует установки плеча для LONG и SHORT отдельно
                            await exchange.set_leverage(LEVERAGE, 'ETH-USDT', {'side': 'LONG'})
                            await exchange.set_leverage(LEVERAGE, 'ETH-USDT', {'side': 'SHORT'})

                        elif name == 'gate':
                            # Для Gate.io достаточно установить плечо для символа
                            await exchange.set_leverage(LEVERAGE, 'ETH_USDT')

                        elif name == 'mexc':
                            # MEXC uses market_id and requires openType and positionType for hedge mode
                            market_id = 'ETH_USDT' 
                            params_long = {'openType': 1, 'positionType': 1}  # 1=isolated, 1=long
                            params_short = {'openType': 1, 'positionType': 2} # 1=isolated, 2=short
                            await exchange.set_leverage(LEVERAGE, market_id, params_long)
                            await exchange.set_leverage(LEVERAGE, market_id, params_short)
                        
                        else:
                            # Общий случай для других бирж (если будут добавлены)
                            await exchange.set_leverage(LEVERAGE, 'ETH/USDT')

                        logger.info(f"{name.upper()}: Successfully set leverage to {LEVERAGE}x.")

                except ccxt.ExchangeError as e:
                    if 'leverage not modified' in str(e) or '110043' in str(e) or '40027' in str(e) or 'no modification' in str(e).lower():
                        logger.info(f"{name.upper()}: Leverage was already set to {LEVERAGE}x.")
                    elif '合约不存在' in str(e) or 'contract not found' in str(e).lower():
                        logger.warning(f"{name.upper()}: Could not set leverage, contract not found for test symbol. This is likely okay.")
                    elif 'side' in str(e) and 'BOTH' in str(e):
                         logger.warning(f"{name.upper()}: Could not set leverage. The exchange is likely not in hedge mode.")
                    else:
                        logger.warning(f"{name.upper()}: Failed to set leverage/position mode: {e}")
                except Exception as e:
                    logger.error(f"{name.upper()}: An unexpected error occurred during leverage/position mode setup: {e}", exc_info=True)

                self.exchanges[name] = exchange
                logger.info(f"✅ {name.upper()} connected and configured successfully.")
            except Exception as e:
                logger.error(f"❌ Failed to connect to {name.upper()}: {e}", exc_info=True)

    async def get_balance(self, currency: str = 'USDT') -> float:
        total = 0.0
        balance_tasks = [ex.fetch_balance() for ex in self.exchanges.values()]
        results = await asyncio.gather(*balance_tasks, return_exceptions=True)
        for i, result in enumerate(results):
            exchange_name = list(self.exchanges.keys())[i]
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch balance from {exchange_name}: {result}")
            else:
                total += result.get(currency, {}).get('free', 0.0) or 0.0
        return total

    async def get_market_details(self, exchange_name: str, symbol: str) -> Optional[Dict]:
        if exchange_name not in self.exchanges: return None
        try:
            # Use the exchange's internal market structure
            return self.exchanges[exchange_name].market(symbol)
        except Exception:
            logger.warning(f"Market details not found for {symbol} on {exchange_name}.")
            return None

    async def open_arbitrage(self, symbol: str, spread: float, low_exch: str, high_exch: str, low_price: float, high_price: float):
        if symbol in SYMBOL_BLACKLIST or len(self.active_trades) >= MAX_CONCURRENT_TRADES or self.daily_pnl <= -MAX_DAILY_LOSS:
            logger.warning("Trade opening conditions not met. Aborting.")
            return

        low_mkt, high_mkt = await asyncio.gather(
            self.get_market_details(low_exch, symbol),
            self.get_market_details(high_exch, symbol)
        )
        if not low_mkt or not high_mkt:
            logger.error(f"Could not get market details for {symbol}. Aborting.")
            return

        min_notional = max(low_mkt.get('limits', {}).get('cost', {}).get('min', 0.0) or 0.0,
                           high_mkt.get('limits', {}).get('cost', {}).get('min', 0.0) or 0.0)
        if min_notional > MAX_MIN_NOTIONAL_USD:
            logger.warning(f"Min notional ${min_notional:.2f} exceeds limit of ${MAX_MIN_NOTIONAL_USD}. Aborting.")
            return

        min_qty = max(low_mkt.get('limits', {}).get('amount', {}).get('min', 0.0) or 0.0,
                      high_mkt.get('limits', {}).get('amount', {}).get('min', 0.0) or 0.0)
        quantity = min_qty if min_qty > 0 else (TRADE_AMOUNT * LEVERAGE) / low_price
        if symbol in RISKY_SYMBOLS: quantity *= RISKY_SYMBOLS[symbol]
        
        logger.info(f"ОТКРЫТА СДЕЛКА: {symbol} | Qty: {quantity:.6f}")
        buy_params = {'positionSide': 'long'} if low_exch == 'mexc' else {}
        sell_params = {'positionSide': 'short'} if high_exch == 'mexc' else {}

        buy_order, sell_order = await asyncio.gather(
            self.create_order(low_exch, symbol, 'buy', quantity, low_price, buy_params),
            self.create_order(high_exch, symbol, 'sell', quantity, high_price, sell_params)
        )

        if buy_order and sell_order:
            trade_id = f"{symbol.split('-')[0]}-{int(time.time())}"
            self.active_trades[trade_id] = {
                "symbol": symbol, "entry_spread": spread, "low_exchange": low_exch,
                "high_exchange": high_exch, "entry_buy_price": buy_order['price'],
                "entry_sell_price": sell_order['price'], "quantity": quantity,
                "status": "open", "entry_time": datetime.utcnow(), "max_spread_seen": spread,
            }
            asyncio.create_task(self.monitor_trade(trade_id))
        else:
            logger.error(f"Failed to open full arbitrage for {symbol}. Cleaning up.")
            if buy_order: await self.cancel_order(low_exch, buy_order['id'], symbol)
            if sell_order: await self.cancel_order(high_exch, sell_order['id'], symbol)

    async def monitor_trade(self, trade_id: str):
        trade = self.active_trades.get(trade_id)
        if not trade: return
        while trade['status'] == 'open':
            await asyncio.sleep(5)
            low_px = await self.price_fetcher.get_price(trade['symbol'], trade['low_exchange'])
            high_px = await self.price_fetcher.get_price(trade['symbol'], trade['high_exchange'])
            if not low_px or not high_px: continue
            
            current_spread = (low_px - high_px) / high_px * 100
            duration = (datetime.utcnow() - trade['entry_time']).total_seconds()
            
            close_reason = None
            if duration > MAX_HOLD_TIME: close_reason = "timeout"
            elif current_spread < CLOSE_SPREAD: close_reason = "spread_collapse"
            elif TRAILING_STOP_ENABLED and current_spread < trade['max_spread_seen'] * 0.5: close_reason = "trailing_stop"
            
            if close_reason:
                logger.info(f"Closing {trade_id} due to: {close_reason}")
                await self.close_arbitrage(trade_id, close_reason)
                break

    async def close_arbitrage(self, trade_id: str, reason: str):
        trade = self.active_trades.get(trade_id)
        if not trade or trade['status'] != 'open': return
        trade['status'] = 'closing'
        
        sell_price = await self.price_fetcher.get_price(trade['symbol'], trade['low_exchange'])
        buy_price = await self.price_fetcher.get_price(trade['symbol'], trade['high_exchange'])
        if not sell_price or not buy_price:
            trade['status'] = 'error_closing'
            return

        sell_params = {'positionSide': 'long'} if trade['low_exchange'] == 'mexc' else {}
        buy_params = {'positionSide': 'short'} if trade['high_exchange'] == 'mexc' else {}

        sell_order, buy_order = await asyncio.gather(
            self.create_order(trade['low_exchange'], trade['symbol'], 'sell', trade['quantity'], sell_price, sell_params),
            self.create_order(trade['high_exchange'], trade['symbol'], 'buy', trade['quantity'], buy_price, buy_params)
        )

        if sell_order and buy_order:
            await self._calculate_and_record_pnl(trade, sell_order['price'], buy_order['price'])
        else:
            trade['status'] = 'error_closing'

    async def _calculate_and_record_pnl(self, trade: Dict, exit_sell_price: float, exit_buy_price: float):
        pnl_long = (exit_sell_price - trade['entry_buy_price']) * trade['quantity']
        pnl_short = (trade['entry_sell_price'] - exit_buy_price) * trade['quantity']
        gross_pnl = pnl_long + pnl_short
        commission = (trade['entry_buy_price'] + trade['entry_sell_price'] + exit_sell_price + exit_buy_price) * trade['quantity'] * 0.0007
        net_pnl = gross_pnl - commission
        self.daily_pnl += net_pnl
        trade.update({'status': 'closed', 'net_pnl': net_pnl})
        logger.info(f"PNL for {trade['symbol']}: ${net_pnl:+.4f}. Daily PNL: ${self.daily_pnl:+.2f}")
        self.active_trades.pop(trade_id, None)

    async def create_order(self, ex_name: str, sym: str, side: str, qty: float, px: float, params: Optional[Dict] = None) -> Optional[Dict]:
        if ex_name not in self.exchanges: return None
        try:
            return await self.exchanges[ex_name].create_order(sym, 'limit', side, qty, px, params or {})
        except Exception as e:
            logger.error(f"Order creation failed on {ex_name}: {e}")
            return None

    async def cancel_order(self, exchange_name: str, order_id: str, symbol: str) -> bool:
        if exchange_name not in self.exchanges: return False
        try:
            await self.exchanges[exchange_name].cancel_order(order_id, symbol)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id} on {exchange_name}: {e}")
            return False

    async def close(self):
        logger.info("Closing all exchange connections...")
        await asyncio.gather(*[ex.close() for ex in self.exchanges.values()], return_exceptions=True)
        self.exchanges.clear()