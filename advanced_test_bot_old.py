import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple, Any
import re
import statistics
from telethon import TelegramClient, events
from collections import defaultdict
import json
import os
import time
import requests
import platform
import subprocess

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.text import Text
from rich.align import Align
from rich import box
from config import COINMARKETCAP_CONFIG

from config import (
    TRADE_AMOUNT, LEVERAGE, MAX_CONCURRENT_TRADES, MIN_SPREAD, CLOSE_SPREAD,
    MAX_DAILY_LOSS, RISKY_SYMBOLS, CORRELATED_PAIRS, MIN_VOLUME, MIN_VOLUME_RATIO,
    MAX_TRADE_COST, MAX_TRADE_AMOUNT, MAX_SINGLE_TRADE_AMOUNT, SYMBOL_BLACKLIST, MAX_HOLD_TIME, FORCE_CLOSE_ENABLED,
    TRAILING_STOP_ENABLED, PROFIT_TRAILING_START, TRAILING_STOP_LEVELS,
    COMMISSION_RATES, PRICE_FETCH_TIMEOUT, ASYNC_TIMEOUT, EXCHANGE_KEYS,
    API_ID, API_HASH, MONITOR_CHANNELS, TOKEN_DB_PATH, TOKEN_DB_BACKUP_DIR,
    TOKEN_DB_TTL_DAYS, TOKEN_DB_HOT_CACHE_SIZE
)
from exchanges.price_fetcher import PriceFetcher
from order_manager import OrderManager
from logging_config import get_logger
from exchange_network_logger import network_logger
from services.token_db import TokenDB

logger = get_logger(__name__)
console = Console()

class SmartArbitrageBot:
    def __init__(self):
        self.client = None
        
        # Конфигурация бирж
        exchanges_config = {
            'bybit': {
                'enabled': True,
                'api_key': EXCHANGE_KEYS['bybit']['apiKey'],
                'api_secret': EXCHANGE_KEYS['bybit']['secret']
            },
            'bingx': {
                'enabled': True,
                'api_key': EXCHANGE_KEYS['bingx']['apiKey'],
                'api_secret': EXCHANGE_KEYS['bingx']['secret']
            },
            'gate': {
                'enabled': True,
                'api_key': EXCHANGE_KEYS['gate']['apiKey'],
                'api_secret': EXCHANGE_KEYS['gate']['secret']
            },
            'mexc': {
                'enabled': True,
                'api_key': EXCHANGE_KEYS['mexc']['apiKey'],
                'api_secret': EXCHANGE_KEYS['mexc']['secret']
            }
        }
        
        self.price_fetcher = PriceFetcher(
    exchanges_config, 
    cmc_api_key=COINMARKETCAP_CONFIG.get('api_key') if COINMARKETCAP_CONFIG.get('enabled') else None
)
        self.order_manager = OrderManager(exchanges_config)
        # Local token DB to minimize CMC API usage
        self.token_db = TokenDB(
            db_path=TOKEN_DB_PATH,
            backup_dir=TOKEN_DB_BACKUP_DIR,
            ttl_days=TOKEN_DB_TTL_DAYS,
            hot_cache_size=TOKEN_DB_HOT_CACHE_SIZE,
        )
        
        self.active_trades = {}
        self.real_orders = {}
        self.trade_history = []
        # Инициализация черного списка из config
        self.symbol_blacklist = set(SYMBOL_BLACKLIST)
        self.daily_pnl = 0.0
        self.last_reset = datetime.now()
        self.daily_trade_count = 0
        self.last_signal_time = None
        self.signals_processed = 0
        self.performance_stats = {
            'total_profit': 0.0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_commission': 0.0
        }
        
        # Файл для сохранения балансов
        self.balances_file = 'exchange_balances.json'
        
        # Инициализация реальных балансов (будут обновлены при подключении)
        self.exchange_balances = {
            'bybit': {'total': 0.0, 'available': 0.0, 'locked': 0.0, 'pnl_today': 0.0, 'unrealized_pnl': 0.0, 'initial': 0.0, 'real_data': False},
            'gate': {'total': 0.0, 'available': 0.0, 'locked': 0.0, 'pnl_today': 0.0, 'unrealized_pnl': 0.0, 'initial': 0.0, 'real_data': False},
            'mexc': {'total': 0.0, 'available': 0.0, 'locked': 0.0, 'pnl_today': 0.0, 'unrealized_pnl': 0.0, 'initial': 0.0, 'real_data': False},
            'bingx': {'total': 0.0, 'available': 0.0, 'locked': 0.0, 'pnl_today': 0.0, 'unrealized_pnl': 0.0, 'initial': 0.0, 'real_data': False}
        }
        
        self.total_balance = 0.0
        
        # Кэш для ускорения обработки
        self.symbol_cache = {}
        self.price_cache = {}
        self.limits_cache = {}  # Кэш лимитов бирж
        self.cache_timeout = 2
        self.limits_cache_timeout = 300  # 5 минут для лимитов
        self.last_balance_update = datetime.now()
        
        # Счетчик ошибок для автоматического отключения проблемных бирж
        self.exchange_errors = defaultdict(int)
        self.max_errors_before_disable = 15
        self.health_check_interval = 120
        
        # Счетчик обработанных сообщений для отладки
        self.message_counter = 0
        
        # 🕒 СИНХРОНИЗАЦИЯ ВРЕМЕНИ ПРОЦЕССА
        self.time_offset = 0
        self.last_time_sync = None
        
        logger.info("🤖 Арбитражный бот инициализирован с REAL ORDER MANAGER")

    async def sync_exchange_time(self):
        """Синхронизация времени процесса с биржами без изменения системного времени"""
        try:
            logger.info("🕒 Синхронизация времени процесса с биржами...")
            
            exchanges_time_urls = {
                'bybit': 'https://api.bybit.com/v2/public/time',
                'gate': 'https://api.gateio.ws/api/v4/spot/time',
                'mexc': 'https://api.mexc.com/api/v3/time',
                'bingx': 'https://open-api.bingx.com/openApi/spot/v1/common/time'
            }
            
            time_diffs = []
            
            for exchange, url in exchanges_time_urls.items():
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code == 200:
                        if exchange == 'bybit':
                            exchange_time = int(float(response.json()['time_now']) * 1000)
                        elif exchange == 'gate':
                            exchange_time = response.json()['server_time']
                        elif exchange == 'mexc':
                            exchange_time = response.json()['serverTime']
                        elif exchange == 'bingx':
                            exchange_time = response.json()['data']
                        else:
                            exchange_time = None
                            
                        if exchange_time:
                            local_time = int(time.time() * 1000)
                            time_diff = exchange_time - local_time
                            time_diffs.append(time_diff)
                            logger.info(f"   {exchange.upper()}: расхождение {time_diff} мс")
                            
                except Exception as e:
                    logger.debug(f"⚠️ Не удалось получить время с {exchange}: {e}")
            
            if time_diffs:
                time_diffs.sort()
                median_diff = time_diffs[len(time_diffs) // 2]
                self.time_offset = median_diff
                self.last_time_sync = datetime.now()
                
                logger.info(f"✅ Установлено смещение времени: {self.time_offset} мс")
                
                if abs(self.time_offset) > 5000:
                    logger.warning(f"⚠️ Большое расхождение времени: {self.time_offset} мс")
                return True
            else:
                logger.warning("⚠️ Не удалось синхронизироваться ни с одной биржей")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации времени: {e}")
            return False

    def get_exchange_time(self):
        """Возвращает текущее время, скорректированное под биржи"""
        local_time = int(time.time() * 1000)
        return local_time + self.time_offset

    async def apply_time_offset_to_exchanges(self):
        """Применяет временное смещение к настройкам бирж"""
        try:
            logger.info("🔧 Применение временной коррекции к биржам...")
            
            for exchange_name in self.order_manager.exchanges.keys():
                try:
                    exchange = self.order_manager.exchanges[exchange_name]
                    
                    if exchange_name == 'bybit':
                        exchange.options['recvWindow'] = 60000
                        if hasattr(exchange, 'offset'):
                            exchange.offset = self.time_offset
                            
                    elif exchange_name == 'gate':
                        exchange.options['recvWindow'] = 60000
                        
                    elif exchange_name == 'mexc':
                        exchange.options['recvWindow'] = 60000
                        
                    elif exchange_name == 'bingx':
                        exchange.options['recvWindow'] = 60000
                        
                    logger.info(f"✅ {exchange_name.upper()}: recvWindow=60000 мс, offset={self.time_offset} мс")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось настроить время для {exchange_name}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка применения временной коррекции: {e}")

    async def time_sync_monitor(self):
        """Фоновый мониторинг и коррекция времени"""
        while True:
            try:
                await asyncio.sleep(600)
                
                if not self.last_time_sync or (datetime.now() - self.last_time_sync).total_seconds() > 3600:
                    await self.sync_exchange_time()
                    await self.apply_time_offset_to_exchanges()
                    
                try:
                    response = requests.get('https://api.bybit.com/v2/public/time', timeout=3)
                    if response.status_code == 200:
                        bybit_time = int(float(response.json()['time_now']) * 1000)
                        local_time = int(time.time() * 1000)
                        current_diff = bybit_time - (local_time + self.time_offset)
                        
                        if abs(current_diff) > 3000:
                            logger.warning(f"🕒 Обнаружено расхождение: {current_diff} мс, пересинхронизация...")
                            await self.sync_exchange_time()
                            await self.apply_time_offset_to_exchanges()
                except:
                    pass
                    
            except Exception as e:
                logger.error(f"❌ Ошибка в мониторе времени: {e}")
                await asyncio.sleep(300)

    async def fetch_real_balances(self):
        """Получает РЕАЛЬНЫЕ балансы со всех бирж"""
        real_balances = {}
        
        for exchange_name in ['bybit', 'gate', 'mexc', 'bingx']:
            try:
                if self.exchange_errors[exchange_name] >= self.max_errors_before_disable:
                    logger.warning(f"🚫 Пропускаем {exchange_name} - превышен лимит ошибок")
                    real_balances[exchange_name] = {
                        'total': 0.0, 
                        'available': 0.0, 
                        'locked': 0.0,
                        'real_data': False
                    }
                    continue
                
                balance_data = await self.order_manager.fetch_balance(exchange_name)
                
                if balance_data:
                    free_balance = balance_data.get('free', 0)
                    total_balance = balance_data.get('total', 0)
                    used_balance = balance_data.get('used', 0)
                    
                    real_balances[exchange_name] = {
                        'total': float(total_balance),
                        'available': float(free_balance),
                        'locked': float(used_balance),
                        'real_data': True
                    }
                    
                    logger.info(f"💰 Реальный баланс {exchange_name.upper()}: ${free_balance:.2f} USDT")
                    
                    if self.exchange_errors[exchange_name] > 0:
                        old_errors = self.exchange_errors[exchange_name]
                        self.exchange_errors[exchange_name] = 0
                        logger.info(f"✅ {exchange_name.upper()} восстановлен после {old_errors} ошибок")
                else:
                    real_balances[exchange_name] = {
                        'total': 0.0, 
                        'available': 0.0, 
                        'locked': 0.0,
                        'real_data': False
                    }
                    
            except Exception as e:
                logger.error(f"❌ Ошибка получения баланса с {exchange_name}: {e}")
                real_balances[exchange_name] = {
                    'total': 0.0, 
                    'available': 0.0, 
                    'locked': 0.0,
                    'real_data': False
                }
        
        return real_balances

    async def debug_balance_status(self):
        """Диагностика балансов"""
        logger.info("🔍 ДИАГНОСТИКА БАЛАНСОВ...")
        
        for exchange_name in ['bybit', 'gate', 'mexc', 'bingx']:
            try:
                balance = await self.order_manager.fetch_balance(exchange_name)
                if balance:
                    free = balance.get('free', 0)
                    total = balance.get('total', 0)
                    used = balance.get('used', 0)
                    
                    logger.info(f"💰 {exchange_name.upper()}: свободно=${free:.2f}, занято=${used:.2f}, всего=${total:.2f}")
                    
                    if exchange_name in self.exchange_balances:
                        self.exchange_balances[exchange_name].update({
                            'total': total,
                            'available': free,
                            'locked': used,
                            'real_data': True
                        })
                else:
                    logger.warning(f"⚠️ {exchange_name.upper()}: баланс не получен")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка диагностики баланса {exchange_name}: {e}")
    
        self.total_balance = sum(bal['total'] for bal in self.exchange_balances.values())
        logger.info(f"📊 ОБЩИЙ БАЛАНС: ${self.total_balance:.2f}")

    async def update_real_balances(self):
        """Обновление РЕАЛЬНЫХ балансов с правильной обработкой"""
        while True:
            try:
                real_balances = await self.fetch_real_balances()
                
                for exchange, real_data in real_balances.items():
                    if exchange not in self.exchange_balances:
                        self.exchange_balances[exchange] = {
                            'total': 0.0, 
                            'available': 0.0, 
                            'locked': 0.0,
                            'pnl_today': 0.0,
                            'unrealized_pnl': 0.0,
                            'initial': 0.0,
                            'real_data': False
                        }
                    
                    self.exchange_balances[exchange]['total'] = real_data['total']
                    self.exchange_balances[exchange]['available'] = real_data['available']
                    self.exchange_balances[exchange]['locked'] = real_data['locked']
                    self.exchange_balances[exchange]['real_data'] = real_data['real_data']
                    
                    if (self.exchange_balances[exchange].get('initial', 0) == 0 and 
                        real_data['real_data'] and real_data['total'] > 0):
                        self.exchange_balances[exchange]['initial'] = real_data['total']
                
                total_unrealized_pnl = 0.0
                for trade in self.active_trades.values():
                    try:
                        current_pnl = await self.calculate_current_pnl(trade)
                        total_unrealized_pnl += current_pnl
                        
                        pnl_per_exchange = current_pnl / 2
                        if trade['long_exchange'] in self.exchange_balances:
                            self.exchange_balances[trade['long_exchange']]['unrealized_pnl'] = pnl_per_exchange
                        if trade['short_exchange'] in self.exchange_balances:
                            self.exchange_balances[trade['short_exchange']]['unrealized_pnl'] = pnl_per_exchange
                        
                    except Exception as e:
                        logger.debug(f"⚠️ Ошибка расчета PnL для сделки {trade.get('trade_id', 'unknown')}: {e}")
                
                self.total_balance = sum(bal['total'] for bal in self.exchange_balances.values())
                self.last_balance_update = datetime.now()
                
                if datetime.now().second % 30 == 0:
                    self.save_balances()
                    
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"❌ Ошибка обновления реальных балансов: {e}")
                await asyncio.sleep(30)

    async def force_immediate_balance_update(self):
        """Принудительное немедленное обновление балансов при старте"""
        try:
            logger.info("🚀 ПРИНУДИТЕЛЬНОЕ обновление балансов при старте...")
            
            real_balances = await self.fetch_real_balances()
            
            for exchange, real_data in real_balances.items():
                if exchange not in self.exchange_balances:
                    self.exchange_balances[exchange] = {
                        'total': 0.0, 'available': 0.0, 'locked': 0.0,
                        'pnl_today': 0.0, 'unrealized_pnl': 0.0, 'initial': 0.0, 'real_data': False
                    }
                
                self.exchange_balances[exchange].update({
                    'total': real_data['total'],
                    'available': real_data['available'],
                    'locked': real_data['locked'],
                    'real_data': real_data['real_data']
                })
                
                if real_data['real_data'] and real_data['total'] > 0:
                    self.exchange_balances[exchange]['initial'] = real_data['total']
                    logger.info(f"💰 Установлен initial баланс для {exchange}: ${real_data['total']:.2f}")
            
            self.total_balance = sum(bal['total'] for bal in self.exchange_balances.values())
            self.last_balance_update = datetime.now()
            
            logger.info(f"📊 ПРИНУДИТЕЛЬНОЕ обновление завершено. Общий баланс: ${self.total_balance:.2f}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка принудительного обновления балансов: {e}")

    async def get_real_commission_rates(self, exchange_name: str) -> float:
        """Получает реальные комиссии с биржи"""
        try:
            exchange = self.order_manager.exchanges.get(exchange_name)
            if exchange:
                if hasattr(exchange, 'fetch_trading_fees'):
                    fees = await asyncio.get_event_loop().run_in_executor(
                        None, exchange.fetch_trading_fees
                    )
                    for symbol, fee_info in fees.items():
                        if 'USDT' in symbol and 'taker' in fee_info:
                            return fee_info['taker']
                
                if exchange_name == 'bybit':
                    return 0.0006
                elif exchange_name == 'gate':
                    return 0.0005
                elif exchange_name == 'mexc':
                    return 0.0004
                elif exchange_name == 'bingx':
                    return 0.0004
                    
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить комиссию с {exchange_name}: {e}")
        
        return 0.001

    async def get_exchange_min_quantity(self, exchange_name: str, symbol: str, found_symbol: str = None) -> Tuple[float, float]:
        """
        Получает минимальное количество и precision для символа на бирже
        Возвращает: (min_quantity, precision)
        """
        cache_key = f"{exchange_name}:{symbol}"
        
        # Проверяем кэш
        if cache_key in self.limits_cache:
            cached_data, cached_time = self.limits_cache[cache_key]
            if (datetime.now() - cached_time).total_seconds() < self.limits_cache_timeout:
                return cached_data
        
        try:
            exchange = self.order_manager.exchanges.get(exchange_name)
            if not exchange:
                return (0.0, 0.001)

            market_symbol = found_symbol
            if not market_symbol:
                price_data = await self.price_fetcher.get_symbol_price_with_cmc(exchange_name, symbol)
                market_symbol = price_data[1] if price_data and price_data[1] else f"{symbol}/USDT:USDT"

            market = None
            # Попробуем взять из заранее загруженных маркетов
            try:
                market = exchange.markets.get(market_symbol) if hasattr(exchange, "markets") else None
            except Exception:
                market = None

            # Если не нашли, пробуем запросить детально
            if market is None:
                try:
                    loop = asyncio.get_event_loop()
                    market = await loop.run_in_executor(None, exchange.market, market_symbol)
                except Exception as e:
                    logger.debug(f"🔍 Не удалось получить лимиты для {market_symbol} на {exchange_name}: {e}")
                    market = None
                    
                    if market:
                        min_amount = market.get('limits', {}).get('amount', {}).get('min', 0)
                        amount_precision = market.get('precision', {}).get('amount', 0.001)
                self.limits_cache[cache_key] = ((min_amount, amount_precision), datetime.now())
                return (min_amount, amount_precision)
        except Exception as e:
            logger.debug(f"⚠️ Ошибка получения лимитов для {symbol} на {exchange_name}: {e}")
        
        # Возвращаем значения по умолчанию
        return (0.0, 0.001)
    
    async def calculate_optimal_quantity(self, symbol: str, long_exchange: str, short_exchange: str,
                                        long_price: float, short_price: float,
                                        long_symbol: str = None, short_symbol: str = None) -> Tuple[float, str]:
        """
        Рассчитывает оптимальное количество для арбитража
        Использует максимальный минимальный объем из обеих бирж
        Возвращает: (quantity, reason)
        """
        try:
            # Получаем минимальные объемы с обеих бирж параллельно
            long_result, short_result = await asyncio.gather(
                self.get_exchange_min_quantity(long_exchange, symbol, long_symbol),
                self.get_exchange_min_quantity(short_exchange, symbol, short_symbol),
                return_exceptions=True
            )
            
            # Обработка результатов
            if isinstance(long_result, Exception):
                logger.debug(f"⚠️ Ошибка получения лимитов LONG: {long_result}")
                long_min_qty, long_precision = (0.0, 0.001)
            else:
                long_min_qty, long_precision = long_result
            
            if isinstance(short_result, Exception):
                logger.debug(f"⚠️ Ошибка получения лимитов SHORT: {short_result}")
                short_min_qty, short_precision = (0.0, 0.001)
            else:
                short_min_qty, short_precision = short_result
            
            # Выбираем максимальный минимальный объем (чтобы удовлетворить обе биржи)
            min_quantity = max(long_min_qty, short_min_qty)
            precision = min(long_precision, short_precision) if long_precision > 0 and short_precision > 0 else max(long_precision, short_precision)
            
            if precision <= 0:
                precision = 0.001
            
            # Округляем до precision
            if min_quantity > 0:
                quantity = round(min_quantity / precision) * precision
            else:
                # Если минимумы не найдены, используем базовый расчет
                quantity = TRADE_AMOUNT * LEVERAGE / long_price
                quantity = round(quantity / precision) * precision
            
            # Проверяем стоимость сделки
            trade_cost = quantity * long_price / LEVERAGE  # Стоимость без учета плеча
            
            if trade_cost > MAX_TRADE_COST:
                reason = f"ПРОПУСК: стоимость ${trade_cost:.2f} превышает лимит ${MAX_TRADE_COST}"
                logger.warning(f"❌ {reason} - {symbol}")
                logger.warning(f"   Мин. объем {quantity:.6f} стоит ${trade_cost:.2f} (лимит: ${MAX_TRADE_COST})")
                return (0.0, reason)
            
            logger.info(f"📏 Оптимальный объем для {symbol}: {quantity:.6f} (мин. LONG: {long_min_qty:.6f}, мин. SHORT: {short_min_qty:.6f}, выбрано: {quantity:.6f})")
            logger.info(f"   Стоимость сделки: ${trade_cost:.2f} (лимит: ${MAX_TRADE_COST})")
            
            return (quantity, "")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка расчета оптимального объема для {symbol}: {e}")
            return (0.0, f"Ошибка расчета: {e}")
    
    async def calculate_real_quantity(self, symbol: str, price: float, exchange_name: str) -> float:
        """Рассчитывает количество с учетом реальных лимитов биржи (старая функция для обратной совместимости)"""
        min_qty, precision = await self.get_exchange_min_quantity(exchange_name, symbol)
        
        if min_qty > 0:
            quantity = min_qty
            if precision > 0:
                quantity = round(quantity / precision) * precision
            return quantity
        
        return self.calculate_adaptive_quantity(symbol, price, 0, 1.0)

    async def check_real_margin_availability(self, exchange_name: str, symbol: str, quantity: float) -> bool:
        """Проверяет реальную доступность маржи на бирже"""
        try:
            balance = await self.order_manager.fetch_balance(exchange_name)
            if balance:
                available_balance = balance.get('free', 0)
                required_margin = (quantity / LEVERAGE)
                
                if available_balance >= required_margin:
                    return True
                else:
                    logger.warning(f"⚠️ Недостаточно маржи на {exchange_name}: требуется ${required_margin:.2f}, доступно ${available_balance:.2f}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Ошибка проверки маржи на {exchange_name}: {e}")
        
        return True

    async def check_real_liquidity(self, exchange_name: str, symbol: str) -> Tuple[bool, float]:
        """Проверяет реальную ликвидность через стакан заявок"""
        try:
            orderbook = await self.price_fetcher.get_order_book(exchange_name, symbol)
            if not orderbook or not orderbook['bids'] or not orderbook['asks']:
                return False, 0.0
            
            bid_depth = sum(bid[1] for bid in orderbook['bids'][:3])
            ask_depth = sum(ask[1] for ask in orderbook['asks'][:3])
            
            price_data = await self.price_fetcher.get_symbol_price_with_cmc(exchange_name, symbol)
            if not price_data[0]:
                return False, 0.0
            
            bid_liquidity_usd = bid_depth * price_data[0]
            ask_liquidity_usd = ask_depth * price_data[0]
            min_liquidity = min(bid_liquidity_usd, ask_liquidity_usd)
            
            best_bid = orderbook['bids'][0][0]
            best_ask = orderbook['asks'][0][0]
            spread = (best_ask - best_bid) / best_bid * 100
            
            logger.info(f"📊 Реальная ликвидность {symbol} на {exchange_name}: ${min_liquidity:,.2f}, спред: {spread:.2f}%")
            
            return min_liquidity >= 1000, min_liquidity
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки ликвидности {symbol} на {exchange_name}: {e}")
            return False, 0.0

    async def monitor_liquidation(self, trade: Dict):
        """Мониторинг ликвидации позиций"""
        try:
            trade_id = trade['trade_id']
            
            long_position_closed = await self.check_position_closed(trade['long_exchange'], trade['symbol'], 'long')
            short_position_closed = await self.check_position_closed(trade['short_exchange'], trade['symbol'], 'short')
            
            if long_position_closed and not short_position_closed:
                logger.warning(f"🚨 LONG позиция ликвидирована на {trade['long_exchange']}! Мгновенно закрываем SHORT...")
                await self.emergency_close_single_position(trade, 'short')
                
            elif short_position_closed and not long_position_closed:
                logger.warning(f"🚨 SHORT позиция ликвидирована на {trade['short_exchange']}! Мгновенно закрываем LONG...")
                await self.emergency_close_single_position(trade, 'long')
                
            elif long_position_closed and short_position_closed:
                logger.warning(f"🚨 ОБЕ позиции ликвидированы! Удаляем сделку {trade_id}")
                await self.finalize_liquidated_trade(trade)
                    
        except Exception as e:
            logger.error(f"❌ Ошибка мониторинга ликвидации для {trade.get('trade_id', 'unknown')}: {e}")

    async def emergency_close_single_position(self, trade: Dict, position_to_close: str):
        """Экстренное закрытие одной позиции"""
        try:
            trade_id = trade['trade_id']
            
            if position_to_close == 'long':
                exchange = trade['long_exchange']
                side = 'sell'
                price_data = await self.price_fetcher.get_symbol_price_with_cmc(exchange, trade['symbol'])
                current_price = price_data[0] if price_data[0] else trade['entry_long_price']
                price = current_price * 0.995
            else:
                exchange = trade['short_exchange']
                side = 'buy'
                price_data = await self.price_fetcher.get_symbol_price_with_cmc(exchange, trade['symbol'])
                current_price = price_data[0] if price_data[0] else trade['entry_short_price']
                price = current_price * 1.005
            
            logger.warning(f"🚨 ЭКСТРЕННОЕ ЗАКРЫТИЕ {position_to_close} позиции для {trade_id}")
            logger.warning(f"   Биржа: {exchange}, Символ: {trade['symbol']}")
            logger.warning(f"   Сторона: {side}, Цена: {price:.6f}, Количество: {trade['quantity']}")
            
            order = await self.order_manager.create_limit_order(
                exchange,
                trade['symbol'],
                side,
                trade['quantity'],
                price
            )
            
            if order:
                logger.info(f"✅ УСПЕШНО СОЗДАН ОРДЕР ЭКСТРЕННОГО ЗАКРЫТИЯ: {order['id']}")
                
                if position_to_close == 'long':
                    trade['long_order_id'] = order['id']
                else:
                    trade['short_order_id'] = order['id']
                    
                trade['emergency_close'] = True
                trade['close_reason'] = f'liquidation_{position_to_close}'
                
            else:
                logger.error(f"❌ НЕ УДАЛОСЬ СОЗДАТЬ ОРДЕР ЭКСТРЕННОГО ЗАКРЫТИЯ ДЛЯ {position_to_close}")
                await self.try_market_close(trade, position_to_close)
                
        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ЭКСТРЕННОГО ЗАКРЫТИЯ {position_to_close}: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"📋 Трассировка экстренного закрытия: {traceback.format_exc()}")

    async def check_force_close_conditions(self):
        """Проверяет условия для принудительного закрытия"""
        if not FORCE_CLOSE_ENABLED or not self.active_trades:
            return
            
        current_time = datetime.now()
        for trade_id, trade in list(self.active_trades.items()):
            try:
                if trade.get('long_order_id') or trade.get('short_order_id'):
                    continue
                    
                duration = (current_time - trade['entry_time']).total_seconds()
                
                if duration > MAX_HOLD_TIME:
                    logger.warning(f"⏰ Принудительное закрытие {trade_id} по времени ({duration:.0f} сек)")
                    await self.create_real_orders(trade, "timeout")
                    continue
                    
                await self.check_trailing_stop(trade_id, trade, duration)
                
                await self.monitor_liquidation(trade)
                
            except Exception as e:
                logger.error(f"❌ Ошибка проверки сделки {trade_id}: {e}")

    async def check_trailing_stop(self, trade_id: str, trade: Dict, duration: float):
        """Проверяет условия трейлинг-стопа"""
        if not TRAILING_STOP_ENABLED:
            return
            
        try:
            current_pnl = await self.calculate_current_pnl(trade)
            
            if current_pnl < PROFIT_TRAILING_START:
                return
                
            if trade.get('long_order_id') or trade.get('short_order_id'):
                return
                
            if 'max_pnl' not in trade or current_pnl > trade['max_pnl']:
                trade['max_pnl'] = current_pnl
                return
                
            for time_threshold, keep_ratio in TRAILING_STOP_LEVELS.items():
                if duration >= time_threshold:
                    threshold_pnl = trade['max_pnl'] * keep_ratio
                    if current_pnl <= threshold_pnl:
                        logger.info(f"🎯 Сработал трейлинг-стоп для {trade_id} на {time_threshold}сек (PnL: ${current_pnl:.2f})")
                        await self.create_real_orders(trade, f"trailing_stop_{time_threshold}s")
                        return
                        
        except Exception as e:
            logger.error(f"❌ Ошибка проверки трейлинг-стопа для {trade_id}: {e}")

    async def execute_arbitrage_trade(self, symbol: str, opportunity: Dict):
        """Выполняет арбитражную сделку с ИЗОЛИРОВАННОЙ маржой и проверками безопасности"""
        try:
            # ========== ПРОВЕРКА 1: Черный список ==========
            if symbol.upper() in self.symbol_blacklist:
                reason = f"ПРОПУСК: {symbol} в черном списке"
                logger.warning(f"❌ {reason}")
                return
            
            # ========== ПРОВЕРКА 2: Лимит активных сделок ==========
            if len(self.active_trades) >= MAX_CONCURRENT_TRADES:
                reason = f"ПРОПУСК: уже есть активная сделка (лимит: {MAX_CONCURRENT_TRADES})"
                logger.warning(f"❌ {reason}")
                return
            
            # ========== ПРОВЕРКА 3: Ошибки бирж ==========
            if (self.exchange_errors[opportunity['long_exchange']] >= self.max_errors_before_disable or 
                self.exchange_errors[opportunity['short_exchange']] >= self.max_errors_before_disable):
                reason = f"ПРОПУСК: биржи имеют много ошибок"
                logger.warning(f"❌ {reason} - {symbol}")
                return

            # Получаем найденные символы
            long_symbol = opportunity.get('long_symbol') or symbol
            short_symbol = opportunity.get('short_symbol') or symbol
            
            # ========== ПРОВЕРКА 4: Расчет минимальных параметров сделки ==========
            logger.info(f"🔍 [MINIMAL] Расчет минимальных параметров для {symbol}...")
            logger.info(f"   LONG: {opportunity['long_exchange']} @ ${opportunity['long_price']:.6f}")
            logger.info(f"   SHORT: {opportunity['short_exchange']} @ ${opportunity['short_price']:.6f}")
            
            # Рассчитать минимальные параметры сделки
            trade_params = await self.calculate_minimal_trade_parameters(
                symbol, 
                opportunity['long_exchange'],
                opportunity['short_exchange'],
                opportunity['long_price'], 
                opportunity['short_price'],
                long_symbol,
                short_symbol
            )
            
            # Проверка: если параметры не рассчитаны (превышен лимит $3)
            if trade_params is None:
                reason = f"ПРОПУСК: минимальный объем превышает жесткий лимит ${MAX_SINGLE_TRADE_AMOUNT}"
                logger.warning(f"🚫 {reason} - {symbol}")
                return
            
            quantity_long = trade_params['quantity_long']
            quantity_short = trade_params['quantity_short']
            volume_usdt = trade_params['volume_usdt']
            
            logger.info(f"✅ [MINIMAL] Параметры рассчитаны:")
            logger.info(f"   💰 Объем: ${volume_usdt:.4f}")
            logger.info(f"   🔢 Количество long: {quantity_long:.6f} токенов")
            logger.info(f"   🔢 Количество short: {quantity_short:.6f} токенов")
            
            # Финальная проверка: расчетные объемы не должны превышать $3
            long_volume = quantity_long * opportunity['long_price']
            short_volume = quantity_short * opportunity['short_price']
            
            if long_volume > MAX_SINGLE_TRADE_AMOUNT or short_volume > MAX_SINGLE_TRADE_AMOUNT:
                reason = f"ПРОПУСК: расчетный объем превышает лимит ${MAX_SINGLE_TRADE_AMOUNT} (LONG=${long_volume:.2f}, SHORT=${short_volume:.2f})"
                logger.error(f"💥 {reason} - {symbol}")
                return
            
            if quantity_long <= 0 or quantity_short <= 0:
                reason = f"ПРОПУСК: некорректные количества (long={quantity_long:.6f}, short={quantity_short:.6f})"
                logger.warning(f"❌ {reason} - {symbol}")
                return
            
            logger.info(f"✅ [LIMIT] Финальная проверка пройдена: LONG=${long_volume:.2f}, SHORT=${short_volume:.2f} <= ${MAX_SINGLE_TRADE_AMOUNT}")

            # ========== ПРОВЕРКА 5: Маржа (параллельно) ==========
            logger.info(f"🔍 [MARGIN] Проверка доступности маржи для {symbol}...")
            required_margin_long = quantity_long * opportunity['long_price'] / LEVERAGE
            required_margin_short = quantity_short * opportunity['short_price'] / LEVERAGE
            
            margin_long_task = self.check_real_margin_availability(opportunity['long_exchange'], symbol, quantity_long)
            margin_short_task = self.check_real_margin_availability(opportunity['short_exchange'], symbol, quantity_short)
            
            margin_long_result, margin_short_result = await asyncio.gather(
                margin_long_task, margin_short_task,
                return_exceptions=True
            )
            
            # Обработка результатов маржи
            if isinstance(margin_long_result, Exception):
                margin_ok_long = False
                logger.warning(f"   ⚠️ Ошибка проверки маржи LONG ({opportunity['long_exchange']}): {margin_long_result}")
            else:
                margin_ok_long = margin_long_result
                logger.info(f"   ✅ LONG маржа ({opportunity['long_exchange']}): требуется ${required_margin_long:.2f} ({'OK' if margin_ok_long else 'Недостаточно'})")
            
            if isinstance(margin_short_result, Exception):
                margin_ok_short = False
                logger.warning(f"   ⚠️ Ошибка проверки маржи SHORT ({opportunity['short_exchange']}): {margin_short_result}")
            else:
                margin_ok_short = margin_short_result
                logger.info(f"   ✅ SHORT маржа ({opportunity['short_exchange']}): требуется ${required_margin_short:.2f} ({'OK' if margin_ok_short else 'Недостаточно'})")
            
            if not margin_ok_long or not margin_ok_short:
                reason = f"ПРОПУСК: недостаточно маржи (LONG: {'OK' if margin_ok_long else 'FAIL'}, SHORT: {'OK' if margin_ok_short else 'FAIL'})"
                logger.warning(f"❌ {reason} - {symbol}")
                return

            logger.info(f"✅ Маржа достаточна для обеих бирж")

            # ========== ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ - НАСТРОЙКА БИРЖ ==========
            logger.info(f"🔧 Настройка маржи и плеча для {symbol}...")
            logger.info(f"   LONG символ: {long_symbol} на {opportunity['long_exchange']}")
            logger.info(f"   SHORT символ: {short_symbol} на {opportunity['short_exchange']}")
            
            # Параллельная настройка маржи и плеча для обеих бирж
            margin_long_task = self.order_manager.set_margin_mode(opportunity['long_exchange'], long_symbol, 'isolated')
            leverage_long_task = self.order_manager.set_leverage(opportunity['long_exchange'], long_symbol, LEVERAGE)
            margin_short_task = self.order_manager.set_margin_mode(opportunity['short_exchange'], short_symbol, 'isolated')
            leverage_short_task = self.order_manager.set_leverage(opportunity['short_exchange'], short_symbol, LEVERAGE)
            
            margin_result_long, leverage_result_long, margin_result_short, leverage_result_short = await asyncio.gather(
                margin_long_task, leverage_long_task, margin_short_task, leverage_short_task,
                return_exceptions=True
            )
            
            # Обработка результатов
            margin_result_long = margin_result_long if not isinstance(margin_result_long, Exception) else False
            leverage_result_long = leverage_result_long if not isinstance(leverage_result_long, Exception) else False
            margin_result_short = margin_result_short if not isinstance(margin_result_short, Exception) else False
            leverage_result_short = leverage_result_short if not isinstance(leverage_result_short, Exception) else False
            
            logger.info(f"📊 Результаты настройки: LONG margin={margin_result_long}, leverage={leverage_result_long}")
            logger.info(f"📊 Результаты настройки: SHORT margin={margin_result_short}, leverage={leverage_result_short}")
            
            # Предупреждаем если настройка не удалась, но продолжаем
            if not margin_result_long or not margin_result_short:
                logger.warning(f"⚠️ Не удалось установить режим маржи для одной из бирж. Продолжаем...")
            if not leverage_result_long or not leverage_result_short:
                logger.warning(f"⚠️ Не удалось установить плечо для одной из бирж. Продолжаем...")
            
            # ========== СОЗДАНИЕ ЛИМИТНЫХ ОРДЕРОВ ==========
            trade_id = f"trade_{len(self.trade_history) + 1}_{symbol}"
            trade_cost = volume_usdt / LEVERAGE
            
            logger.info(f"🎯 [EXECUTE] ВХОД В СДЕЛКУ {symbol}")
            logger.info(f"   📊 Объем: ${volume_usdt:.4f}")
            logger.info(f"   💰 Стоимость сделки: ${trade_cost:.2f}")
            logger.info(f"   📈 Расчетный спред: {opportunity['spread']:.2f}%")
            
            # Размещение лимитного ордера на LONG (покупка)
            logger.info(f"🟢 [EXECUTE] Размещение LONG ордера на {opportunity['long_exchange']}: {quantity_long:.6f} токенов по ${opportunity['long_price']:.6f}")
            long_order_task = self.order_manager.create_limit_order(
                opportunity['long_exchange'],
                symbol, 
                'buy', 
                quantity_long,
                opportunity['long_price'],
                found_symbol=long_symbol
            )
            
            # Размещение лимитного ордера на SHORT (продажа)
            logger.info(f"🔴 [EXECUTE] Размещение SHORT ордера на {opportunity['short_exchange']}: {quantity_short:.6f} токенов по ${opportunity['short_price']:.6f}")
            short_order_task = self.order_manager.create_limit_order(
                opportunity['short_exchange'],
                symbol,
                'sell', 
                quantity_short, 
                opportunity['short_price'],
                found_symbol=short_symbol
            )
            
            long_order, short_order = await asyncio.gather(
                long_order_task, short_order_task,
                return_exceptions=True
            )
            
            # Обработка результатов создания ордеров
            long_order = long_order if not isinstance(long_order, Exception) else None
            short_order = short_order if not isinstance(short_order, Exception) else None
            
            if long_order and short_order:
                trade = {
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'entry_time': datetime.now(),
                    'long_exchange': opportunity['long_exchange'],
                    'short_exchange': opportunity['short_exchange'],
                    'entry_long_price': opportunity['long_price'],
                    'entry_short_price': opportunity['short_price'],
                    'quantity_long': quantity_long,
                    'quantity_short': quantity_short,
                    'volume_usdt': volume_usdt,
                    'entry_spread': opportunity['spread'],
                    'long_order_id': long_order['id'],
                    'short_order_id': short_order['id'],
                    'status': 'open',
                    'margin_mode': 'isolated',
                    'leverage': LEVERAGE,
                    'trade_cost': trade_cost
                }
                
                self.active_trades[trade_id] = trade
                self.trade_history.append(trade)
                self.daily_trade_count += 1
                
                # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ УСПЕШНОЙ СДЕЛКИ
                logger.info(f"✅ [EXECUTE] СДЕЛКА ВЫПОЛНЕНА: {symbol}")
                logger.info(f"   📊 Объем: ${volume_usdt:.4f}")
                logger.info(f"   🔢 Количество long: {quantity_long:.6f} токенов")
                logger.info(f"   🔢 Количество short: {quantity_short:.6f} токенов")
                logger.info(f"   💰 Стоимость сделки: ${trade_cost:.2f}")
                logger.info(f"   📈 Расчетный спред: {opportunity['spread']:.2f}%")
                logger.info(f"   LONG: {opportunity['long_exchange']} @ ${opportunity['long_price']:.6f} (ордер: {long_order['id']})")
                logger.info(f"   SHORT: {opportunity['short_exchange']} @ ${opportunity['short_price']:.6f} (ордер: {short_order['id']})")
                logger.info(f"   Плечо: {LEVERAGE}x")
                logger.info(f"   Маржа: ИЗОЛИРОВАННАЯ")
                
            else:
                logger.error(f"❌ НЕ УДАЛОСЬ СОЗДАТЬ ОДИН ИЗ ОРДЕРОВ ДЛЯ {symbol}")
                logger.error(f"   LONG ордер: {'Успех' if long_order else 'Ошибка'}")
                logger.error(f"   SHORT ордер: {'Успех' if short_order else 'Ошибка'}")
                
                # Отменяем созданные ордера
                if long_order:
                    logger.info(f"🔄 Отмена LONG ордера {long_order['id']}...")
                    await self.order_manager.cancel_order(opportunity['long_exchange'], long_order['id'])
                if short_order:
                    logger.info(f"🔄 Отмена SHORT ордера {short_order['id']}...")
                    await self.order_manager.cancel_order(opportunity['short_exchange'], short_order['id'])
            
        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ВЫПОЛНЕНИЯ СДЕЛКИ ДЛЯ {symbol}: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"📋 Трассировка сделки: {traceback.format_exc()}")

    async def check_position_closed(self, exchange_name: str, symbol: str, side: str) -> bool:
        """Проверяет, закрыта ли позиция"""
        try:
            position_exists = await self.order_manager.check_position_exists(exchange_name, symbol, side)
            return not position_exists
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки позиции на {exchange_name}: {e}")
            return False

    async def try_market_close(self, trade: Dict, position_to_close: str):
        """Пытаемся закрыть позицию рыночным ордером"""
        try:
            if position_to_close == 'long':
                exchange = trade['long_exchange']
                side = 'sell'
            else:
                exchange = trade['short_exchange']
                side = 'buy'
                
            price_data = await self.price_fetcher.get_symbol_price_with_cmc(exchange, trade['symbol'])
            current_price = price_data[0] if price_data[0] else (
                trade['entry_long_price'] if position_to_close == 'long' else trade['entry_short_price']
            )
            
            if side == 'sell':
                price = current_price * 0.99
            else:
                price = current_price * 1.01
                
            order = await self.order_manager.create_limit_order(
                exchange,
                trade['symbol'],
                side,
                trade['quantity'],
                price
            )
            
            if order:
                logger.info(f"✅ Резервное закрытие {position_to_close} позиции: {order['id']}")
                if position_to_close == 'long':
                    trade['long_order_id'] = order['id']
                else:
                    trade['short_order_id'] = order['id']
            else:
                logger.error(f"❌ Не удалось закрыть {position_to_close} позицию даже рыночным ордером")
                
        except Exception as e:
            logger.error(f"❌ Ошибка резервного закрытия: {e}")

    async def finalize_liquidated_trade(self, trade: Dict):
        """Финальная обработка полностью ликвидированной сделки"""
        try:
            trade_id = trade['trade_id']
            
            long_liquidation_price = trade['entry_long_price'] * 0.7
            short_liquidation_price = trade['entry_short_price'] * 1.3
            
            long_pnl = (long_liquidation_price - trade['entry_long_price']) * trade['quantity']
            short_pnl = (trade['entry_short_price'] - short_liquidation_price) * trade['quantity']
            gross_pnl = long_pnl + short_pnl
            
            long_commission_rate = await self.get_real_commission_rates(trade['long_exchange'])
            short_commission_rate = await self.get_real_commission_rates(trade['short_exchange'])
            
            long_commission = trade['quantity'] * trade['entry_long_price'] * long_commission_rate
            short_commission = trade['quantity'] * trade['entry_short_price'] * short_commission_rate
            total_commission = long_commission + short_commission
            net_pnl = gross_pnl - total_commission
            
            self.performance_stats['total_profit'] += net_pnl
            self.performance_stats['total_commission'] += total_commission
            if net_pnl > 0:
                self.performance_stats['winning_trades'] += 1
            else:
                self.performance_stats['losing_trades'] += 1
            
            self.daily_pnl += net_pnl
            
            trade.update({
                'exit_time': datetime.now(),
                'exit_long_price': long_liquidation_price,
                'exit_short_price': short_liquidation_price,
                'gross_pnl': gross_pnl,
                'net_pnl': net_pnl,
                'pnl': net_pnl,
                'commission': total_commission,
                'duration_seconds': (datetime.now() - trade['entry_time']).total_seconds(),
                'close_reason': 'both_liquidated',
                'status': 'closed'
            })
            
            logger.info(f"💀 Сделка {trade_id} полностью ликвидирована. Примерный PnL: ${net_pnl:.2f}")
            
            if trade_id in self.active_trades:
                del self.active_trades[trade_id]
                
        except Exception as e:
            logger.error(f"❌ Ошибка финализации ликвидированной сделки {trade.get('trade_id', 'unknown')}: {e}")

    async def create_real_orders(self, trade: Dict, close_reason: str) -> bool:
        """Создает РЕАЛЬНЫЕ ордера на закрытие позиций"""
        try:
            trade_id = trade['trade_id']
            symbol = trade['symbol']
            quantity = trade['quantity']
            
            logger.info(f"🔚 Создание РЕАЛЬНЫХ ордеров на закрытие для {trade_id}. Причина: {close_reason}")
            
            long_current_price = None
            short_current_price = None
            
            try:
                long_price_data = await self.price_fetcher.get_symbol_price_with_cmc(trade['long_exchange'], symbol)
                short_price_data = await self.price_fetcher.get_symbol_price_with_cmc(trade['short_exchange'], symbol)
                
                if long_price_data[0] and short_price_data[0]:
                    long_current_price = long_price_data[0]
                    short_current_price = short_price_data[0]
                else:
                    long_current_price = trade['entry_long_price'] * 0.99
                    short_current_price = trade['entry_short_price'] * 1.01
            except Exception as e:
                logger.warning(f"⚠️ Не удалось получить текущие цены: {e}")
                long_current_price = trade['entry_long_price'] * 0.99
                short_current_price = trade['entry_short_price'] * 1.01
            
            long_execution_price = long_current_price * 0.998
            short_execution_price = short_current_price * 1.002
            
            long_order = await self.order_manager.create_limit_order(
                trade['long_exchange'],
                symbol, 
                'sell',
                quantity,
                long_execution_price
            )
            
            short_order = await self.order_manager.create_limit_order(
                trade['short_exchange'],
                symbol,
                'buy',
                quantity, 
                short_execution_price
            )
            
            if long_order and short_order:
                trade['long_order_id'] = long_order['id']
                trade['short_order_id'] = short_order['id']
                trade['close_reason'] = close_reason
                
                logger.info(f"📝 Созданы РЕАЛЬНЫЕ ордера на закрытие для {trade_id}")
                logger.info(f"   LONG закрытие: {trade['long_exchange']} @ ${long_execution_price:.6f}")
                logger.info(f"   SHORT закрытие: {trade['short_exchange']} @ ${short_execution_price:.6f}")
                
                return True
            else:
                logger.error(f"❌ Не удалось создать один из ордеров для {trade_id}")
                if long_order:
                    await self.order_manager.cancel_order(trade['long_exchange'], long_order['id'])
                if short_order:
                    await self.order_manager.cancel_order(trade['short_exchange'], short_order['id'])
                return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания реальных ордеров для {trade_id}: {e}")
            return False

    async def calculate_current_pnl(self, trade: Dict) -> float:
        """УПРОЩЕННЫЙ РАСЧЕТ PnL"""
        try:
            return self.calculate_simple_pnl(trade)
        except Exception:
            return 0.0

    def calculate_simple_pnl(self, trade: Dict) -> float:
        """ПРОСТОЙ РАСЧЕТ PnL - СИНХРОННАЯ ВЕРСИЯ"""
        try:
            loop = asyncio.get_event_loop()
            
            long_price_data = loop.run_until_complete(
                self.price_fetcher.get_symbol_price_with_cmc(trade['long_exchange'], trade['symbol'])
            )
            short_price_data = loop.run_until_complete(
                self.price_fetcher.get_symbol_price_with_cmc(trade['short_exchange'], trade['symbol'])
            )
            
            current_long = long_price_data[0] if long_price_data[0] else trade['entry_long_price']
            current_short = short_price_data[0] if short_price_data[0] else trade['entry_short_price']
            
            long_pnl = (current_long - trade['entry_long_price']) * trade['quantity']
            short_pnl = (trade['entry_short_price'] - current_short) * trade['quantity']
            gross_pnl = long_pnl + short_pnl
            
            loop = asyncio.get_event_loop()
            long_commission_rate = loop.run_until_complete(self.get_real_commission_rates(trade['long_exchange']))
            short_commission_rate = loop.run_until_complete(self.get_real_commission_rates(trade['short_exchange']))
            
            long_commission = trade['quantity'] * trade['entry_long_price'] * long_commission_rate
            short_commission = trade['quantity'] * trade['entry_short_price'] * short_commission_rate
            total_commission = long_commission + short_commission
            
            return gross_pnl - total_commission
            
        except Exception as e:
            logger.debug(f"⚠️ Ошибка расчета PnL: {e}")
            return 0.0

    def save_balances(self):
        """Сохраняет текущие балансы в файл"""
        try:
            with open(self.balances_file, 'w', encoding='utf-8') as f:
                json.dump(self.exchange_balances, f, indent=2, ensure_ascii=False)
            logger.debug("💾 Балансы сохранены в файл")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения балансов: {e}")

    def print_configuration_diagnostics(self):
        """Выводит диагностическую информацию о конфигурации"""
        logger.info("=" * 80)
        logger.info("🔧 ========== ДИАГНОСТИКА КОНФИГУРАЦИИ ==========")
        logger.info("=" * 80)
        
        # Проверка MIN_SPREAD
        logger.info(f"📊 [CONFIG] Параметры арбитража:")
        logger.info(f"   - MIN_SPREAD: {MIN_SPREAD}%")
        logger.info(f"   - CLOSE_SPREAD: {CLOSE_SPREAD}%")
        logger.info(f"   - TRADE_AMOUNT: ${TRADE_AMOUNT}")
        logger.info(f"   - LEVERAGE: {LEVERAGE}x")
        logger.info(f"   - MAX_CONCURRENT_TRADES: {MAX_CONCURRENT_TRADES}")
        
        # Проверка бирж
        logger.info(f"📡 [CONFIG] Конфигурация бирж:")
        exchanges_config = {
            'bybit': {'enabled': True, 'api_key': EXCHANGE_KEYS['bybit']['apiKey']},
            'bingx': {'enabled': True, 'api_key': EXCHANGE_KEYS['bingx']['apiKey']},
            'gate': {'enabled': True, 'api_key': EXCHANGE_KEYS['gate']['apiKey']},
            'mexc': {'enabled': True, 'api_key': EXCHANGE_KEYS['mexc']['apiKey']}
        }
        
        for ex_name, config in exchanges_config.items():
            api_key = config.get('api_key', '')
            enabled = config.get('enabled', False)
            has_key = bool(api_key and len(api_key) > 10)
            masked_key = f"{api_key[:6]}...{api_key[-4:]}" if has_key else "(отсутствует)"
            status = "✅" if (enabled and has_key) else "❌"
            logger.info(f"   {status} {ex_name.upper()}: enabled={enabled}, API key={masked_key}")
        
        # Проверка Telegram каналов
        logger.info(f"📱 [CONFIG] Telegram каналы для мониторинга:")
        if MONITOR_CHANNELS:
            for channel in MONITOR_CHANNELS:
                logger.info(f"   - {channel}")
        else:
            logger.warning("   ⚠️ Каналы не заданы — будут обрабатываться все входящие сообщения")
        
        # Проверка символов
        logger.info(f"🎯 [CONFIG] Фильтры символов:")
        logger.info(f"   - Черный список: {list(self.symbol_blacklist)}")
        logger.info(f"   - Рисковые символы: {list(RISKY_SYMBOLS.keys())}")
        
        logger.info("=" * 80)

    async def initialize(self):
        """Инициализация бота с НЕМЕДЛЕННЫМ получением балансов и автономным Telegram"""
        try:
            # Очистка кэша символов CMC при старте
            if self.price_fetcher.cmc_resolver:
                logger.info("🧹 Очистка кэша CMC и символов...")
                self.price_fetcher.cmc_resolver.clear_cache()
            self.price_fetcher.symbol_cache_obj.clear()
            
            # Выводим диагностику конфигурации
            self.print_configuration_diagnostics()
            
            logger.info("🕒 Настройка времени процесса...")
            await self.sync_exchange_time()
            await self.apply_time_offset_to_exchanges()
            
            # Инициализация Telegram клиента (автономный режим)
            await self.initialize_telegram_client()
            
            # ЭКСТРЕННАЯ ДИАГНОСТИКА TELEGRAM
            logger.info("🔧 Запуск экстренной диагностики Telegram...")
            await self.emergency_telegram_diagnostic()
            
            # Тестирование обработки сигнала
            await self.test_signal_processing()
            
            # Проверка конфигурации мониторинга
            logger.info("=" * 80)
            logger.info("🔧 ========== КОНФИГУРАЦИЯ МОНИТОРИНГА ==========")
            logger.info("=" * 80)
            logger.info(f"📱 [CONFIG] MONITOR_CHANNELS: {MONITOR_CHANNELS}")
            logger.info(f"📱 [CONFIG] Количество каналов: {len(MONITOR_CHANNELS) if MONITOR_CHANNELS else 0}")
            if MONITOR_CHANNELS:
                for idx, channel in enumerate(MONITOR_CHANNELS, 1):
                    logger.info(f"   {idx}. {channel}")
            else:
                logger.warning("   ⚠️ Каналы не заданы — будут обрабатываться все входящие сообщения")
            logger.info("=" * 80)
                
            # Тестируем подключения ко всем биржам
            logger.info("🔍 Тестирование подключений к биржам...")
            connection_results = await self.order_manager.test_all_connections()
            
            connected_exchanges = [ex for ex, connected in connection_results.items() if connected]
            if len(connected_exchanges) < 2:
                logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Только {len(connected_exchanges)} из {len(connection_results)} бирж подключено!")
                logger.error(f"   Подключено: {connected_exchanges}")
                logger.error(f"   Для арбитража требуется минимум 2 биржи")
                for exchange_name, connected in connection_results.items():
                    if not connected and exchange_name in self.order_manager.connection_status:
                        error = self.order_manager.connection_status[exchange_name].get('error', 'Unknown')
                        logger.error(f"   {exchange_name.upper()}: {error}")
            else:
                logger.info(f"✅ {len(connected_exchanges)} бирж подключено успешно: {connected_exchanges}")
                
            # Дополнительное тестирование через балансы
            await self.test_exchange_connection()
            
            logger.info("🔄 НЕМЕДЛЕННОЕ получение реальных балансов при старте...")
            await self.force_immediate_balance_update()
            
            print("✅ Бот инициализирован и готов к работе в РЕАЛЬНОМ режиме!")
            print(f"💵 Реальные балансы загружены")
            print(f"📊 Размер сделки: ${TRADE_AMOUNT} с {LEVERAGE}x плечом")
            print(f"🎯 Минимальный спред: {MIN_SPREAD}%")
            
            await self.debug_balance_status()
            
            await asyncio.sleep(2)
            await self.debug_balance_status()
            
            # Запускаем фоновые задачи
            asyncio.create_task(self.monitor_force_close())
            asyncio.create_task(self.monitor_daily_limits())
            asyncio.create_task(self.run_interface())
            asyncio.create_task(self.update_real_balances())
            asyncio.create_task(self.connection_watchdog())
            asyncio.create_task(self.monitor_real_orders())
            asyncio.create_task(self.health_check())
            asyncio.create_task(self.time_sync_monitor())
            
            # Запускаем мониторинг Telegram в фоне (не блокирует)
            asyncio.create_task(self.start_monitoring())
            
            logger.info("✅ Все фоновые задачи запущены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            import traceback
            logger.error(f"📋 Трассировка: {traceback.format_exc()}")
            raise
    
    async def emergency_telegram_diagnostic(self):
        """Экстренная диагностика Telegram клиента"""
        logger.info("=" * 80)
        logger.info("🚨 ========== ЭКСТРЕННАЯ ДИАГНОСТИКА TELEGRAM ==========")
        logger.info("=" * 80)
        
        if not self.client:
            logger.error("❌ [DIAG] Telegram клиент не инициализирован!")
            return False
        
        # Проверка 1: Статус подключения
        try:
            is_connected = self.client.is_connected()
            logger.info(f"📡 [DIAG] Статус подключения: {is_connected}")
            if not is_connected:
                logger.warning("⚠️ [DIAG] Telegram клиент НЕ подключен!")
                try:
                    logger.info("🔄 [DIAG] Попытка подключения...")
                    await self.client.connect()
                    is_connected = self.client.is_connected()
                    logger.info(f"📡 [DIAG] Статус после подключения: {is_connected}")
                except Exception as conn_error:
                    logger.error(f"❌ [DIAG] Ошибка подключения: {conn_error}")
                    return False
        except Exception as e:
            logger.error(f"❌ [DIAG] Ошибка проверки статуса подключения: {e}")
            return False
        
        # Проверка 2: Информация о пользователе
        try:
            me = await self.client.get_me()
            if me:
                logger.info(f"👤 [DIAG] Бот авторизован как: {me.first_name} (@{me.username}) (ID: {me.id})")
            else:
                logger.error("❌ [DIAG] Не удалось получить информацию о пользователе")
                return False
        except Exception as e:
            logger.error(f"❌ [DIAG] Ошибка получения информации о боте: {e}")
            import traceback
            logger.error(f"📋 [DIAG] Трассировка: {traceback.format_exc()}")
            return False
        
        # Проверка 3: Поиск канала
        try:
            logger.info(f"🔍 [DIAG] Поиск каналов для мониторинга...")
            logger.info(f"   - MONITOR_CHANNELS: {MONITOR_CHANNELS}")
            
            for channel_name in MONITOR_CHANNELS:
                try:
                    # Убираем @ если есть
                    clean_name = channel_name.replace('@', '')
                    logger.info(f"🔍 [DIAG] Поиск канала: {clean_name}")
                    
                    try:
                        channel = await self.client.get_entity(clean_name)
                        logger.info(f"📢 [DIAG] Канал найден: {channel.title} (ID: {channel.id})")
                        
                        # Проверка 4: Участие в канале
                        try:
                            participants = await self.client.get_participants(channel, limit=1)
                            logger.info(f"👥 [DIAG] Доступ к каналу подтвержден (участников: {len(participants)}+)")
                        except Exception as part_error:
                            logger.warning(f"⚠️ [DIAG] Не удалось получить участников канала: {part_error}")
                        
                        # Проверка 5: Последние сообщения
                        try:
                            messages = await self.client.get_messages(channel, limit=3)
                            logger.info(f"📨 [DIAG] Последние {len(messages)} сообщений из канала '{channel.title}':")
                            for idx, msg in enumerate(messages, 1):
                                msg_text = msg.text[:100] if msg.text else "(медиа/пустое)"
                                logger.info(f"   {idx}. {msg_text}...")
                        except Exception as msg_error:
                            logger.warning(f"⚠️ [DIAG] Не удалось получить сообщения из канала: {msg_error}")
                            
                    except Exception as entity_error:
                        logger.error(f"❌ [DIAG] Канал '{clean_name}' не найден: {entity_error}")
                        
                except Exception as e:
                    logger.error(f"❌ [DIAG] Ошибка обработки канала '{channel_name}': {e}")
                    
        except Exception as e:
            logger.error(f"❌ [DIAG] Ошибка доступа к каналам: {e}")
            import traceback
            logger.error(f"📋 [DIAG] Трассировка: {traceback.format_exc()}")
        
        # Проверка 6: Регистрация обработчиков событий
        try:
            handlers = self.client.list_event_handlers()
            logger.info(f"📋 [DIAG] Зарегистрировано обработчиков: {len(handlers)}")
            if handlers:
                for idx, handler in enumerate(handlers, 1):
                    logger.info(f"   {idx}. {handler}")
            else:
                logger.warning("⚠️ [DIAG] НЕТ зарегистрированных обработчиков событий!")
        except Exception as e:
            logger.warning(f"⚠️ [DIAG] Не удалось получить список обработчиков: {e}")
        
        logger.info("=" * 80)
        return True
    
    async def test_signal_processing(self):
        """Тестирование обработки сигнала"""
        logger.info("=" * 80)
        logger.info("🧪 ========== ТЕСТИРОВАНИЕ ОБРАБОТКИ СИГНАЛА ==========")
        logger.info("=" * 80)
        
        test_message = """📈📈#1 | Spread: 14.11%
📌 1_USDT (COPY: 1)
🔴Short MEXC  : $0.027650000
🟢Long  BINGX : $0.024230000"""
        
        logger.info(f"🧪 [TEST] Тестовое сообщение:")
        logger.info(f"   {test_message}")
        
        # Тест парсинга
        logger.info(f"🧪 [TEST] Тест парсинга символа...")
        symbol = self.parse_symbol_enhanced(test_message)
        logger.info(f"🧪 [TEST] Результат парсинга тестового сообщения: '{symbol}'")
        
        if symbol:
            logger.info(f"✅ [TEST] Парсинг успешен: символ '{symbol}' извлечен")
        else:
            logger.error(f"❌ [TEST] Парсинг НЕУДАЧЕН: символ не извлечен")
        
        # Тест обработки (без реального выполнения сделки)
        logger.info(f"🧪 [TEST] Тест обработки сигнала (без реального выполнения)...")
        logger.info(f"   (Полная обработка будет выполнена при реальных сигналах)")
        
        logger.info("=" * 80)
    
    async def restart_telegram_session(self):
        """Перезапуск сессии Telegram"""
        logger.info("=" * 80)
        logger.info("🔄 ========== ПЕРЕЗАПУСК СЕССИИ TELEGRAM ==========")
        logger.info("=" * 80)
        
        try:
            if self.client:
                if self.client.is_connected():
                    logger.info("🔌 [RESTART] Отключение текущего соединения...")
                    await self.client.disconnect()
                    logger.info("✅ [RESTART] Соединение отключено")
                
                logger.info("🔄 [RESTART] Подключение заново...")
                await self.client.connect()
                logger.info("✅ [RESTART] Подключение восстановлено")
                
                # Проверяем авторизацию
                is_authorized = await self.client.is_user_authorized()
                if is_authorized:
                    me = await self.client.get_me()
                    logger.info(f"✅ [RESTART] Сессия авторизована: {me.first_name} (@{me.username})")
                else:
                    logger.warning("⚠️ [RESTART] Сессия не авторизована после перезапуска")
                    
            else:
                logger.warning("⚠️ [RESTART] Telegram клиент не инициализирован")
                
        except Exception as e:
            logger.error(f"❌ [RESTART] Ошибка перезапуска сессии: {e}")
            import traceback
            logger.error(f"📋 [RESTART] Трассировка: {traceback.format_exc()}")
        
        logger.info("=" * 80)
    
    async def initialize_telegram_client(self):
        """Инициализация Telegram клиента в автономном режиме"""
        try:
            session_dir = 'telegram_sessions'
            if not os.path.exists(session_dir):
                os.makedirs(session_dir)
                logger.info(f"📁 Создана директория для сессий: {session_dir}")
            
            session_file = os.path.join(session_dir, 'arbitrage_session')
            
            # Автономная конфигурация Telegram клиента (без GUI)
            self.client = TelegramClient(
                session_file, 
                API_ID, 
                API_HASH,
                connection_retries=10,
                timeout=60,
                request_retries=10,
                flood_sleep_threshold=120,
                device_model="Arbitrage Bot Persistent",
                system_version="Linux",
                app_version="2.0.0",
                lang_code="en",
                system_lang_code="en",
                # Настройки для автономной работы
                use_ipv6=False,  # Отключаем IPv6 для стабильности
                proxy=None,  # Без прокси
            )
            
            # Включаем автоматическое переподключение
            self.client.auto_reconnect = True
            
            logger.info("🔄 Подключение к Telegram (автономный режим)...")
            
            # Попытка подключения с повторными попытками
            max_connection_attempts = 5
            for attempt in range(max_connection_attempts):
                try:
                    # Подключаемся если не подключены
                    if not self.client.is_connected():
                        logger.info(f"🔌 Подключение к Telegram (попытка {attempt + 1}/{max_connection_attempts})...")
                        await self.client.connect()
                    
                    # Проверяем авторизацию
                    is_authorized = await self.client.is_user_authorized()
                    
                    if not is_authorized:
                        logger.warning("⚠️ Telegram сессия не авторизована")
                        logger.info("📱 Для первой авторизации запустите бота интерактивно")
                        logger.info("   После авторизации бот будет работать автономно")
                        
                        # Проверяем наличие файла сессии
                        if os.path.exists(session_file + '.session'):
                            logger.info("📂 Найдена сохраненная сессия, но она не авторизована")
                            logger.info("   Попробуйте запустить бота с интерактивной авторизацией")
                        else:
                            logger.info("📂 Файл сессии не найден")
                            logger.info("   Запустите бота для первой авторизации")
                        
                        # Если это не первая попытка, продолжаем
                        if attempt < max_connection_attempts - 1:
                            await asyncio.sleep(2)
                            continue
                        else:
                            raise Exception("Telegram сессия не авторизована. Требуется авторизация.")
                    
                    # Проверяем подключение через get_me()
                    try:
                        me = await self.client.get_me()
                        username = me.username if me.username else "N/A"
                        first_name = me.first_name if me.first_name else "User"
                        logger.info(f"✅ Telegram подключен. Авторизован как: {first_name} (@{username})")
                        print(f"✅ Telegram подключен: {first_name} (@{username})")
                        # Старт механизмов поддержания активности (однократно на процесс)
                        if not hasattr(self, 'activity_tasks_started') or not self.activity_tasks_started:
                            await self.start_activity_maintenance()
                            self.activity_tasks_started = True
                    except Exception as get_me_error:
                        logger.warning(f"⚠️ Не удалось получить информацию о пользователе: {get_me_error}")
                        logger.info("   Но сессия авторизована, продолжаем работу...")
                    
                    # В Telethon сессия сохраняется автоматически при использовании файловой сессии
                    # Файл сессии сохраняется автоматически при работе клиента
                    logger.info("💾 Telegram сессия будет сохранена автоматически")
                    logger.info("✅ Telegram клиент готов к работе")
                    
                    return
                    
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    logger.warning(f"⚠️ Попытка подключения {attempt + 1}/{max_connection_attempts} не удалась: {error_type}: {error_msg}")
                    
                    # Если это не последняя попытка, ждем и пробуем снова
                    if attempt < max_connection_attempts - 1:
                        wait_time = min((attempt + 1) * 3, 15)  # Максимум 15 секунд
                        logger.info(f"⏳ Ожидание {wait_time} секунд перед следующей попыткой...")
                        await asyncio.sleep(wait_time)
                        
                        # Пытаемся отключиться перед следующей попыткой
                        try:
                            if self.client.is_connected():
                                await self.client.disconnect()
                        except:
                            pass
                    else:
                        logger.error(f"❌ Не удалось подключиться к Telegram после {max_connection_attempts} попыток")
                        logger.error(f"   Последняя ошибка: {error_type}: {error_msg}")
                        logger.error(f"   Убедитесь что:")
                        logger.error(f"   1. API_ID и API_HASH корректны")
                        logger.error(f"   2. Интернет соединение работает")
                        logger.error(f"   3. Telegram сервис доступен")
                        logger.error(f"   4. Сессия авторизована (запустите бота один раз для авторизации)")
                        raise
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка инициализации Telegram: {e}")
            import traceback
            logger.error(f"📋 Трассировка: {traceback.format_exc()}")
            raise

    async def test_exchange_connection(self):
        """Тестирует подключение к биржам через получение балансов"""
        logger.info("🧪 ДОПОЛНИТЕЛЬНОЕ ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЙ ЧЕРЕЗ БАЛАНСЫ...")
        
        for exchange_name in ['bybit', 'gate', 'mexc', 'bingx']:
            try:
                # Проверяем статус подключения
                if exchange_name in self.order_manager.connection_status:
                    status = self.order_manager.connection_status[exchange_name]
                    if not status.get('connected', False):
                        logger.warning(f"⚠️ {exchange_name.upper()}: не подключен - {status.get('error', 'Unknown error')}")
                        continue
                
                balance = await self.order_manager.fetch_balance(exchange_name)
                
                if balance is not None:
                    free_balance = balance.get('free', 0)
                    used_balance = balance.get('used', 0)
                    total_balance = balance.get('total', 0)
                    logger.info(f"✅ {exchange_name.upper()}: free=${free_balance:.2f}, used=${used_balance:.2f}, total=${total_balance:.2f}")
                    
                    if free_balance > 0 and free_balance < 10:
                        logger.warning(f"⚠️ Низкий баланс на {exchange_name}: ${free_balance:.2f}")
                    
                    # Проверяем что free balance не равен нулю если есть позиции
                    if total_balance > 0 and free_balance == 0 and used_balance == 0:
                        logger.warning(f"⚠️ {exchange_name.upper()}: возможна проблема с парсингом баланса (total={total_balance:.2f}, но free=used=0)")
                else:
                    logger.warning(f"⚠️ {exchange_name.upper()}: не удалось получить баланс")
                    
            except Exception as e:
                error_msg = str(e)[:100]
                logger.error(f"❌ {exchange_name.upper()}: ошибка получения баланса: {error_msg}")
                import traceback
                logger.debug(f"   Трассировка: {traceback.format_exc()}")

    async def health_check(self):
        """Расширенная проверка здоровья бирж"""
        while True:
            try:
                await self.check_exchange_health()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"❌ Ошибка в health check: {type(e).__name__}: {str(e)}")
                await asyncio.sleep(60)

    async def check_exchange_health(self):
        """Проверяет состояние бирж (без постоянных запросов BTC)"""
        exchanges_to_check = ['bybit', 'gate', 'mexc', 'bingx']
        
        for exchange in exchanges_to_check:
            try:
                if self.exchange_errors[exchange] >= self.max_errors_before_disable:
                    # Проверяем отключенные биржи только раз в 5 минут
                    if datetime.now().minute % 5 == 0:
                        logger.info(f"🔄 Проверяем отключенную биржу {exchange}...")
                        # Используем легкую проверку баланса вместо запроса цены BTC
                        try:
                            balance = await self.order_manager.fetch_balance(exchange)
                            if balance and balance.get('free', 0) >= 0:
                                old_errors = self.exchange_errors[exchange]
                                self.exchange_errors[exchange] = 0
                                logger.info(f"✅ Биржа {exchange} ВОССТАНОВЛЕНА через проверку баланса. Ошибок было: {old_errors}")
                            else:
                                logger.debug(f"⚠️ Биржа {exchange} все еще недоступна")
                        except Exception as e:
                            logger.debug(f"⚠️ Биржа {exchange} недоступна: {e}")
                    else:
                        continue
                else:
                    # Для активных бирж просто проверяем счетчик ошибок
                    # Не делаем постоянных запросов - биржа считается активной, если нет ошибок
                    if self.exchange_errors[exchange] > 0:
                        # Сбрасываем счетчик ошибок, если биржа работает нормально
                        # (ошибки сбрасываются при успешных операциях)
                        pass
                        
            except Exception as e:
                logger.debug(f"⚠️ Ошибка проверки биржи {exchange}: {type(e).__name__}: {str(e)}")
                if "NetworkError" in str(type(e)):
                    self.exchange_errors[exchange] += 1
                else:
                    self.exchange_errors[exchange] += 1

    async def monitor_force_close(self):
        """Мониторинг принудительного закрытия сделок"""
        while True:
            try:
                await self.check_force_close_conditions()
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"❌ Ошибка в мониторе принудительного закрытия: {e}")
                await asyncio.sleep(30)

    async def monitor_real_orders(self):
        """Мониторинг реальных ордеров"""
        while True:
            try:
                await self.check_real_orders()
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"❌ Ошибка в мониторе реальных ордеров: {e}")
                await asyncio.sleep(10)

    async def check_real_orders(self):
        """Проверяет исполнение реальных ордеров"""
        if not self.order_manager.active_orders:
            return

        completed_orders = []

        for order_id, order_info in list(self.order_manager.active_orders.items()):
            try:
                status = await self.order_manager.get_order_status(
                    order_info['exchange'], 
                    order_id
                )
                
                if status in ['closed', 'filled', 'canceled']:
                    logger.info(f"✅ Ордер {order_id} исполнен/отменен. Статус: {status}")
                    
                    trade = None
                    for t in self.active_trades.values():
                        if t.get('long_order_id') == order_id or t.get('short_order_id') == order_id:
                            trade = t
                            break
                    
                    if trade:
                        if await self.check_both_orders_filled(trade):
                            await self.finalize_trade_closure(trade, "real_order_filled")
                            completed_orders.append(order_id)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка проверки ордера {order_id}: {e}")

    async def check_both_orders_filled(self, trade: Dict) -> bool:
        """Проверяет, исполнены ли оба ордера сделки"""
        long_status = await self.order_manager.get_order_status(
            trade['long_exchange'], 
            trade.get('long_order_id', '')
        ) if trade.get('long_order_id') else None
        
        short_status = await self.order_manager.get_order_status(
            trade['short_exchange'], 
            trade.get('short_order_id', '')
        ) if trade.get('short_order_id') else None
        
        return (long_status in ['closed', 'filled'] and 
                short_status in ['closed', 'filled'])

    async def finalize_trade_closure(self, trade: Dict, close_reason: str):
        """Финальное закрытие сделки после исполнения ордеров"""
        try:
            long_order_info = await asyncio.get_event_loop().run_in_executor(
                None,
                self.order_manager.exchanges[trade['long_exchange']].fetch_order,
                trade.get('long_order_id', '')
            ) if trade.get('long_order_id') else None
            
            short_order_info = await asyncio.get_event_loop().run_in_executor(
                None,
                self.order_manager.exchanges[trade['short_exchange']].fetch_order,
                trade.get('short_order_id', '')
            ) if trade.get('short_order_id') else None
            
            long_price = long_order_info['average'] if long_order_info else trade['entry_long_price']
            short_price = short_order_info['average'] if short_order_info else trade['entry_short_price']
            
            long_pnl = (long_price - trade['entry_long_price']) * trade['quantity']
            short_pnl = (trade['entry_short_price'] - short_price) * trade['quantity']
            gross_pnl = long_pnl + short_pnl
            
            long_commission_rate = await self.get_real_commission_rates(trade['long_exchange'])
            short_commission_rate = await self.get_real_commission_rates(trade['short_exchange'])
            
            long_commission = trade['quantity'] * trade['entry_long_price'] * long_commission_rate
            short_commission = trade['quantity'] * trade['entry_short_price'] * short_commission_rate
            total_commission = long_commission + short_commission
            net_pnl = gross_pnl - total_commission
            
            self.performance_stats['total_profit'] += net_pnl
            self.performance_stats['total_commission'] += total_commission
            if net_pnl > 0:
                self.performance_stats['winning_trades'] += 1
            else:
                self.performance_stats['losing_trades'] += 1
            
            self.daily_pnl += net_pnl
            
            pnl_per_exchange = net_pnl / 2
            if trade['long_exchange'] in self.exchange_balances:
                self.exchange_balances[trade['long_exchange']]['pnl_today'] += pnl_per_exchange
            if trade['short_exchange'] in self.exchange_balances:
                self.exchange_balances[trade['short_exchange']]['pnl_today'] += pnl_per_exchange
            
            if trade['long_exchange'] in self.exchange_balances:
                self.exchange_balances[trade['long_exchange']]['unrealized_pnl'] = 0.0
                self.exchange_balances[trade['long_exchange']]['locked'] = 0.0
            if trade['short_exchange'] in self.exchange_balances:
                self.exchange_balances[trade['short_exchange']]['unrealized_pnl'] = 0.0
                self.exchange_balances[trade['short_exchange']]['locked'] = 0.0
            
            trade.update({
                'exit_time': datetime.now(),
                'exit_long_price': long_price,
                'exit_short_price': short_price,
                'gross_pnl': gross_pnl,
                'net_pnl': net_pnl,
                'pnl': net_pnl,
                'commission': total_commission,
                'exit_spread': (short_price - long_price) / long_price * 100 if long_price else 0,
                'duration_seconds': (datetime.now() - trade['entry_time']).total_seconds(),
                'close_reason': close_reason,
                'status': 'closed'
            })
            
            logger.info(f"📊 Итоги сделки {trade['trade_id']}: PnL ${net_pnl:.2f}, длительность {trade['duration_seconds']:.0f}сек")
            
            if trade['trade_id'] in self.active_trades:
                del self.active_trades[trade['trade_id']]
                
            await self.update_balances_immediately()
            self.save_balances()
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка финализации сделки {trade['trade_id']}: {e}")

    async def update_balances_immediately(self):
        """Немедленное обновление балансов"""
        try:
            for exchange in self.exchange_balances:
                base_balance = self.exchange_balances[exchange]['initial']
                realized_today = self.exchange_balances[exchange]['pnl_today']
                unrealized = self.exchange_balances[exchange]['unrealized_pnl']
                
                self.exchange_balances[exchange]['total'] = base_balance + realized_today + unrealized
                self.exchange_balances[exchange]['available'] = max(0, 
                    self.exchange_balances[exchange]['total'] - self.exchange_balances[exchange]['locked'])
            
            self.total_balance = sum(bal['total'] for bal in self.exchange_balances.values())
            self.last_balance_update = datetime.now()
            
        except Exception as e:
            logger.error(f"❌ Ошибка немедленного обновления балансов: {e}")

    async def connection_watchdog(self):
        """Мониторит и восстанавливает Telegram соединение (автономный режим)"""
        logger.info("🔍 Запущен мониторинг Telegram соединения...")
        consecutive_failures = 0
        max_failures = 5
        
        while True:
            try:
                # Проверяем подключение
                if not self.client or not self.client.is_connected():
                    consecutive_failures += 1
                    logger.warning(f"🔌 Telegram соединение потеряно (попытка {consecutive_failures}/{max_failures})...")
                    # Пытаемся мягко разорвать текущее соединение
                    try:
                        if self.client: await self.client.disconnect()
                    except Exception:
                        pass
                    # Попытка переподключения
                    await asyncio.sleep(5)
                    await self.client.connect()
                    if await self.client.is_user_authorized():
                        me = await self.client.get_me()
                        logger.info(f"✅ Telegram соединение восстановлено: {me.first_name} (@{me.username})")
                        consecutive_failures = 0
                    else:
                        logger.error("❌ Telegram сессия не авторизована после переподключения")
                        consecutive_failures += 1
                    # Превышение лимита неудач
                    if consecutive_failures >= max_failures:
                        logger.error(f"🚫 Превышен лимит неудачных попыток подключения ({max_failures})")
                        logger.error("   Бот продолжит работу, но Telegram мониторинг приостановлен")
                        await asyncio.sleep(300)
                        consecutive_failures = 0
                else:
                    # Соединение активно, сбрасываем счетчик
                    consecutive_failures = 0
                
                # Проверяем каждые 30 секунд
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в мониторинге Telegram соединения: {e}")
                consecutive_failures += 1
                await asyncio.sleep(60)

    async def monitor_daily_limits(self):
        """Мониторинг дневных лимитов"""
        while True:
            try:
                await self.check_daily_reset()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"❌ Ошибка в мониторе дневных лимитов: {e}")
                await asyncio.sleep(300)

    async def check_daily_reset(self):
        """Проверяет сброс дневных счетчиков"""
        now = datetime.now()
        if now.date() != self.last_reset.date():
            self.daily_pnl = 0.0
            self.daily_trade_count = 0
            self.last_reset = now
            for exchange in self.exchange_balances:
                self.exchange_balances[exchange]['pnl_today'] = 0.0
                self.exchange_balances[exchange]['initial'] = self.exchange_balances[exchange]['total']
            logger.info("🔄 Дневные счетчики сброшены")

    def parse_symbol(self, message: str) -> Optional[str]:
        """Парсит символ из сообщения (старый метод, используется для совместимости)"""
        return self.parse_symbol_enhanced(message)
    
    def parse_symbol_enhanced(self, message: str) -> Optional[str]:
        """УЛУЧШЕННЫЙ парсинг символа из любого формата арбитражного сигнала"""
        try:
            logger.info(f"🔍 [PARSE] Начало парсинга символа (длина сообщения: {len(message)})")
            
            if not message or len(message) < 3:
                logger.warning(f"🔍 [PARSE] Сообщение слишком короткое или пустое")
                return None
            
            # ФИЛЬТР: пропускать сообщения о выравнивании (aligned)
            message_lower = message.lower()
            if 'aligned in' in message_lower or 'aligned' in message_lower:
                logger.info("⏩ [PARSE] ПРОПУСК: сообщение о выравнивании, не арбитражный сигнал")
                return None
            
            # ФИЛЬТР: искать ТОЛЬКО арбитражные сигналы с форматом Spread
            arbitrage_patterns = [
                r'Spread:\s*[\d.]+%',  # Spread: X.XX%
                r'📈📈#\w+\s*\|',      # Эмодзи спреда
                r'🟢Long\s+\w+\s*:',   # Long биржа
                r'🔴Short\s+\w+\s*:'   # Short биржа
            ]
            
            is_arbitrage_signal = any(re.search(pattern, message, re.IGNORECASE) for pattern in arbitrage_patterns)
            if not is_arbitrage_signal:
                logger.info("⏩ [PARSE] ПРОПУСК: не арбитражный сигнал (нет паттернов Spread/Long/Short)")
                return None
            
            # Проверяем кэш
            if message in self.symbol_cache:
                cached_symbol, cache_time = self.symbol_cache[message]
                cache_age = (datetime.now() - cache_time).total_seconds()
                if cache_age < self.cache_timeout:
                    logger.info(f"🔍 [PARSE] Символ найден в кэше: '{cached_symbol}' (возраст: {cache_age:.1f}с)")
                    return cached_symbol
                else:
                    logger.debug(f"🔍 [PARSE] Кэш устарел (возраст: {cache_age:.1f}с), продолжаем парсинг")
            
            message_lower = message.lower()
            message_upper = message.upper()
            
            # Расширенный список ключевых слов для арбитража
            arbitrage_keywords = [
                'спред', 'spread', 'арбитраж', 'arbitrage', 'разница', 'difference',
                'gap', 'дисконт', 'премия', 'premium', 'discount',
                'арб', 'arb', 'спред-сигнал', 'spread signal'
            ]
            
            # Проверяем наличие ключевых слов арбитража
            has_arbitrage_keyword = any(keyword in message_lower for keyword in arbitrage_keywords)
            
            # Детальное логирование для отладки
            logger.info(f"🔍 [PARSE] Проверка ключевых слов арбитража: найдено={has_arbitrage_keyword}")
            if has_arbitrage_keyword:
                found_keywords = [kw for kw in arbitrage_keywords if kw in message_lower]
                logger.info(f"🔍 [PARSE] Найденные ключевые слова: {found_keywords}")
            else:
                logger.warning(f"🔍 [PARSE] Ключевые слова арбитража не найдены")
                logger.warning(f"   Проверяемые слова: {arbitrage_keywords}")
                logger.warning(f"   Сообщение (первые 200 символов): {message[:200]}")
            
            # Если нет ключевых слов арбитража, это не арбитражный сигнал
            if not has_arbitrage_keyword:
                logger.warning(f"🔍 [PARSE] Сообщение пропущено: нет ключевых слов арбитража")
                return None
            
            # Очищаем сообщение для парсинга
            # Исправлено: дефис должен быть в конце или экранирован
            clean_msg = re.sub(r'[^\w\s|/#:\+\-%\.]', ' ', message)
            clean_msg_upper = clean_msg.upper()
            
            # УЛУЧШЕННЫЕ паттерны для извлечения символа (разрешаем короткие символы 1, 2, 3)
            patterns = [
                # Приоритетные форматы (проверяются первыми)
                r'#(\w+)\s*\|',                    # #SYMBOL | (приоритетный)
                r'📌\s*(\w+)_USDT',                # 📌 SYMBOL_USDT
                r'\(COPY:\s*(\w+)\)',              # (COPY: SYMBOL)
                r'(\w+[_\-]USDT)\s*\(COPY:\s*(\w+)\)',  # SYMBOL_USDT (COPY: SYMBOL) - приоритетный
                r'#(\w+[_\-]?USDT)\s*\|.*Spread[:\s]+[\d.]+%',  # #SYMBOL_USDT | Spread: X%
                r'#(\w+)\s*\|.*Spread[:\s]+[\d.]+%',  # #SYMBOL | Spread: X%
                r'#(\w+)\s*\|.*\s+Spread',  # #SYMBOL | ... Spread
                r'#(\w+)\s*Spread',  # #SYMBOL Spread
                r'(\w+)\s*Spread[:\s]+[\d.]+%',  # SYMBOL Spread: X%
                r'SYMBOL[:\s]+(\w+)',  # SYMBOL: TOKEN
                r'TOKEN[:\s]+(\w+)',  # TOKEN: SYMBOL
                r'COIN[:\s]+(\w+)',  # COIN: SYMBOL
                
                # Форматы с арбитражем
                r'(\w+)\s*[-–]\s*Arbitrage',  # SYMBOL - Arbitrage
                r'Arbitrage\s*[-–]\s*(\w+)',  # Arbitrage - SYMBOL
                r'(\w+)\s*Arbitrage',  # SYMBOL Arbitrage
                r'Arbitrage\s*(\w+)',  # Arbitrage SYMBOL
                
                # Форматы со спредом
                r'(\w+)\s*спред',  # SYMBOL спред
                r'спред\s*(\w+)',  # спред SYMBOL
                r'(\w+)\s*Spread',  # SYMBOL Spread
                r'Spread\s*(\w+)',  # Spread SYMBOL
                
                # Форматы с процентами
                r'(\w+)\s*[\d.]+\s*%',  # SYMBOL X.XX%
                r'(\w+)\s*:\s*[\d.]+\s*%',  # SYMBOL: X.XX%
                
                # Форматы с биржами
                r'(\w+)\s*Bybit.*Gate',  # SYMBOL Bybit...Gate
                r'(\w+)\s*Gate.*Bybit',  # SYMBOL Gate...Bybit
                r'(\w+)\s*MEXC.*BingX',  # SYMBOL MEXC...BingX
                
                # Форматы с ценой
                r'(\w+)\s*\$[\d.]+',  # SYMBOL $X.XX
                r'(\w+)\s*USD[T]?\s*[\d.]+',  # SYMBOL USDT X.XX
                
                # Общие паттерны
                r'\b([A-Z]{2,10})\b.*(?:spread|арбитраж|arbitrage)',  # CAPITAL LETTERS near arbitrage keywords
                r'(?:spread|арбитраж|arbitrage).*\b([A-Z]{2,10})\b',  # CAPITAL LETTERS after arbitrage keywords
            ]
            
            # Попытка извлечения символа по паттернам
            logger.info(f"🔍 [PARSE] Проверяю {len(patterns)} паттернов для извлечения символа...")
            for idx, pattern in enumerate(patterns):
                matches = list(re.finditer(pattern, clean_msg_upper, re.IGNORECASE))
                if matches:
                    logger.info(f"🔍 [PARSE] Паттерн #{idx+1} '{pattern[:60]}...' нашел {len(matches)} совпадений")
                for match in matches:
                    symbol = match.group(1).upper().strip()
                    logger.info(f"🔍 [PARSE] Извлечен кандидат '{symbol}' из паттерна #{idx+1}")
                    
                    # Если нашли полное название с суффиксом, извлекаем базовый символ
                    if '_USDT' in symbol or '-USDT' in symbol:
                        # Извлекаем базовый символ (CYPR из CYPR_USDT)
                        base_symbol = symbol.replace('_USDT', '').replace('-USDT', '').replace('USDT', '')
                        if base_symbol and len(base_symbol) >= 2:
                            logger.info(f"🔍 [PARSE] Извлечен базовый символ '{base_symbol}' из полного названия '{symbol}'")
                            symbol = base_symbol
                    
                    # Если паттерн нашел два совпадения (например, "CYPR_USDT (COPY: CYPR)"), используем второе
                    if len(match.groups()) > 1 and match.group(2):
                        symbol = match.group(2).upper().strip()
                        logger.info(f"🔍 [PARSE] Использован символ из второй группы паттерна: '{symbol}'")
                    
                    # Фильтруем известные криптовалюты (если это не арбитражный сигнал специально для них)
                    major_coins = ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOT', 'DOGE', 'LTC', 'BCH', 'LINK', 'SOL', 'MATIC', 'AVAX']
                    if symbol in major_coins and 'major' not in message_lower and 'top' not in message_lower:
                        # Пропускаем только если это не явный арбитражный сигнал
                        if not any(indicator in message for indicator in ['#', 'Spread:', 'SPREAD:', 'сигнал', 'signal']):
                            logger.warning(f"🔍 [PARSE] Символ '{symbol}' пропущен: это известная криптовалюта без явного арбитражного сигнала")
                        continue
                    
                    # Проверяем валидность символа (РАЗРЕШАЕМ КОРОТКИЕ СИМВОЛЫ: 1, 2, 3, etc.)
                    if len(symbol) >= 1 and len(symbol) <= 15:
                        if symbol.isalnum():  # Убрали проверку isdigit() - разрешаем цифры
                            # Дополнительная проверка: символ должен быть упомянут рядом с ключевыми словами
                            symbol_lower = symbol.lower()
                            message_words = message_lower.split()
                            
                            # Ищем символ в сообщении
                            if symbol_lower in message_lower or symbol in message_upper:
                                self.symbol_cache[message] = (symbol, datetime.now())
                                logger.info(f"✅ [PARSE] УСПЕХ! Извлечен символ '{symbol}' из сигнала (паттерн #{idx+1})")
                                return symbol
                            else:
                                logger.warning(f"🔍 [PARSE] Символ '{symbol}' не найден в исходном сообщении")
                        else:
                            logger.warning(f"🔍 [PARSE] Символ '{symbol}' невалиден: не alnum")
                    else:
                        logger.warning(f"🔍 [PARSE] Символ '{symbol}' невалиден: длина {len(symbol)} не в диапазоне 1-15")
            
            # Если паттерны не сработали, попробуем найти любые заглавные слова после ключевых слов
            words = clean_msg_upper.split()
            arbitrage_indices = []
            for i, word in enumerate(words):
                if any(keyword.upper() in word for keyword in arbitrage_keywords):
                    arbitrage_indices.append(i)
            
            # Ищем символы рядом с ключевыми словами
            for idx in arbitrage_indices:
                # Проверяем слова до и после ключевого слова
                for offset in [-2, -1, 1, 2]:
                    check_idx = idx + offset
                    if 0 <= check_idx < len(words):
                        word = words[check_idx].strip('.,;:!?()[]{}#')
                        if len(word) >= 2 and len(word) <= 10:
                            if word.isalnum() and not word.isdigit() and word.isupper():
                                # Исключаем общие слова
                                exclude_words = ['THE', 'AND', 'OR', 'FOR', 'WITH', 'FROM', 'THAT', 'THIS', 'SPREAD', 'ARBITRAGE']
                                if word not in exclude_words:
                                    self.symbol_cache[message] = (word, datetime.now())
                                    logger.info(f"🎯 Извлечен символ '{word}' из контекста арбитража")
                                    return word
            
            # Если ничего не найдено
            self.symbol_cache[message] = (None, datetime.now())
            logger.warning(f"⚠️ Не удалось извлечь символ из сообщения после проверки всех паттернов")
            logger.warning(f"📋 Очищенное сообщение: {clean_msg_upper[:200]}...")
            logger.warning(f"📋 Исходное сообщение: {message[:300]}...")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга символа из '{message[:100] if message else 'None'}...': {e}")
            import traceback
            logger.debug(f"📋 Трассировка парсинга: {traceback.format_exc()}")
            return None

    def extract_reference_price(self, message: str) -> Optional[float]:
        """Извлекает репрезентативную цену из текста сигнала (используем медиану всех значений с $)."""
        try:
            if not message:
                return None
            price_matches = re.findall(r'\$\s*([0-9]+(?:\.[0-9]+)?)', message)
            prices = []
            for raw in price_matches:
                try:
                    value = float(raw)
                    if value > 0:
                        prices.append(value)
                except ValueError:
                    continue
            if not prices:
                return None
            return statistics.median(prices)
        except Exception as e:
            logger.debug(f"⚠️ Ошибка извлечения референсной цены из сообщения: {e}")
            return None

    def calculate_adaptive_quantity(self, symbol: str, price: float, spread: float, volume_ratio: float) -> float:
        """Рассчитывает адаптивное количество с учетом рисков"""
        base_amount = TRADE_AMOUNT
        
        if spread > 7.0:
            base_amount *= 1.8
        elif spread > 5.0:
            base_amount *= 1.5
        elif spread > 4.0:
            base_amount *= 1.2
            
        if volume_ratio < 0.2:
            base_amount *= 0.4
        elif volume_ratio < 0.4:
            base_amount *= 0.7
            
        risk_factor = RISKY_SYMBOLS.get(symbol, 1.0)
        base_amount *= risk_factor
        
        quantity = base_amount * LEVERAGE / price
        
        logger.info(f"📊 Адаптивный объем для {symbol}: ${base_amount:.2f} -> {quantity:.6f} (спред: {spread:.1f}%, объем: {volume_ratio:.1%})")
        return quantity

    def is_correlated_open(self, new_symbol: str) -> bool:
        """Проверяет, нет ли открытых коррелированных пар"""
        open_symbols = [t['symbol'] for t in self.active_trades.values()]
        correlated = CORRELATED_PAIRS.get(new_symbol, [])
        
        if any(sym in correlated for sym in open_symbols):
            logger.info(f"🔗 Пропускаем {new_symbol} - есть коррелированные открытые позиции: {open_symbols}")
            return True
        return False

    async def get_min_order_amount(self, exchange_name: str, symbol: str, found_symbol: str) -> float:
        """
        Получает минимальную сумму ордера в USDT для биржи
        """
        try:
            exchange = self.order_manager.exchanges.get(exchange_name)
            if not exchange:
                logger.warning(f"⚠️ Биржа {exchange_name} не найдена для получения минимального объема")
                return 5.0  # $5 по умолчанию
            
            # Получить информацию о рынке
            try:
                market = exchange.market(found_symbol)
                
                # Минимальная стоимость ордера обычно в limits->cost->min
                if market and 'limits' in market and 'cost' in market['limits']:
                    min_cost = market['limits']['cost'].get('min')
                    if min_cost and min_cost > 0:
                        logger.info(f"📊 Минимальная стоимость ордера для {found_symbol} на {exchange_name}: ${min_cost:.4f}")
                        return float(min_cost)
                
                # Если нет минимальной стоимости, используем минимальное количество
                if market and 'limits' in market and 'amount' in market['limits']:
                    min_amount = market['limits']['amount'].get('min')
                    if min_amount and min_amount > 0:
                        # Получаем текущую цену
                        price_data = await self.price_fetcher.get_symbol_price_with_cmc(exchange_name, symbol)
                        if price_data[0] and price_data[0] > 0:
                            min_cost = min_amount * price_data[0]
                            logger.info(f"📊 Минимальная стоимость (из количества) для {found_symbol} на {exchange_name}: ${min_cost:.4f}")
                            return min_cost
                            
            except Exception as market_error:
                logger.debug(f"⚠️ Ошибка получения market для {found_symbol} на {exchange_name}: {market_error}")
            
            # Дефолтное значение если не найдено
            logger.info(f"📊 Используем дефолтное минимальное значение для {exchange_name}: $5.00")
            return 5.0  # $5 по умолчанию
                
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить минимальный объем для {exchange_name}: {e}")
            return 5.0  # $5 по умолчанию
    
    async def calculate_minimal_trade_parameters(self, symbol: str, exchange_long: str, exchange_short: str, 
                                                 price_long: float, price_short: float,
                                                 long_symbol: str, short_symbol: str) -> Optional[Dict[str, Any]]:
        """
        Рассчитывает минимальные параметры для сделки на основе требований бирж
        С ЖЕСТКИМ ЛИМИТОМ $3 на одну биржу
        """
        logger.info(f"📏 [MINIMAL] Расчет минимальных параметров для {symbol}")
        logger.info(f"🔒 [LIMIT] Жесткий лимит: макс ${MAX_SINGLE_TRADE_AMOUNT} на биржу")
        
        # Получить минимальные объемы для обеих бирж
        min_amount_long = await self.get_min_order_amount(exchange_long, symbol, long_symbol)
        min_amount_short = await self.get_min_order_amount(exchange_short, symbol, short_symbol)
        
        logger.info(f"📊 [MINIMAL] Минимальные объемы: {exchange_long.upper()}=${min_amount_long:.4f}, {exchange_short.upper()}=${min_amount_short:.4f}")
        
        # Взять МАКСИМАЛЬНЫЙ из минимальных объемов (в USDT)
        min_volume_usdt = max(min_amount_long, min_amount_short)
        
        # ЖЕСТКОЕ ОГРАНИЧЕНИЕ: не более $3
        if min_volume_usdt > MAX_SINGLE_TRADE_AMOUNT:
            logger.warning(f"🚫 [LIMIT] МИНИМАЛЬНЫЙ ОБЪЕМ ${min_volume_usdt:.2f} ПРЕВЫШАЕТ ЛИМИТ ${MAX_SINGLE_TRADE_AMOUNT}")
            logger.warning(f"🚫 [LIMIT] Сделка {symbol} ОТМЕНЕНА - минимальный объем слишком большой")
            return None
        
        # Добавить 10% для гарантии исполнения, но не превышать $3
        trade_volume_usdt = min(min_volume_usdt * 1.10, MAX_SINGLE_TRADE_AMOUNT)
        
        logger.info(f"✅ [LIMIT] Объем в пределах лимита: ${trade_volume_usdt:.4f} <= ${MAX_SINGLE_TRADE_AMOUNT}")
        
        # Рассчитать количество токенов для long позиции
        quantity_long = trade_volume_usdt / price_long
        
        # Рассчитать количество токенов для short позиции  
        quantity_short = trade_volume_usdt / price_short
        
        # Проверить что расчетные объемы не превышают $3
        long_volume_check = quantity_long * price_long
        short_volume_check = quantity_short * price_short
        
        if long_volume_check > MAX_SINGLE_TRADE_AMOUNT or short_volume_check > MAX_SINGLE_TRADE_AMOUNT:
            logger.error(f"💥 [LIMIT] ОШИБКА: РАСЧЕТНЫЙ ОБЪЕМ ПРЕВЫШАЕТ ${MAX_SINGLE_TRADE_AMOUNT}")
            logger.error(f"   LONG: ${long_volume_check:.2f}, SHORT: ${short_volume_check:.2f}")
            return None
        
        logger.info(f"📊 [MINIMAL] Параметры сделки {symbol}:")
        logger.info(f"   💰 Объем: ${trade_volume_usdt:.4f} (лимит: ${MAX_SINGLE_TRADE_AMOUNT})")
        logger.info(f"   🔢 Количество long: {quantity_long:.6f} токенов (${long_volume_check:.2f})")
        logger.info(f"   🔢 Количество short: {quantity_short:.6f} токенов (${short_volume_check:.2f})")
        
        return {
            'volume_usdt': trade_volume_usdt,
            'quantity_long': quantity_long,
            'quantity_short': quantity_short
        }

    async def should_trade_symbol(self, symbol: str, exchange1: str, exchange2: str) -> Tuple[bool, float]:
        """УПРОЩЕННАЯ проверка для входа в сделку (БЕЗ ПРОВЕРКИ ЛИКВИДНОСТИ)"""
        logger.info(f"🔍 [SIMPLE] Упрощенная проверка для {symbol}:")
        
        try:
            # Условие 1: Биржи не имеют критических ошибок
            if (self.exchange_errors[exchange1] >= self.max_errors_before_disable or 
                self.exchange_errors[exchange2] >= self.max_errors_before_disable):
                logger.warning(f"   ❌ Биржи имеют много ошибок: {exchange1}({self.exchange_errors[exchange1]}), {exchange2}({self.exchange_errors[exchange2]})")
                return False, 0.0
            logger.info(f"   ✅ Биржи доступны: {exchange1}, {exchange2}")
            
            # Условие 2: Не превышен лимит активных сделок
            trades_ok = len(self.active_trades) < MAX_CONCURRENT_TRADES
            logger.info(f"   {'✅' if trades_ok else '❌'} Лимит сделок: {len(self.active_trades)}/{MAX_CONCURRENT_TRADES}")
            if not trades_ok:
                return False, 0.0
                
            logger.info(f"✅ [SIMPLE] Все условия выполнены для {symbol}")
            return True, 1.0  # Возвращаем 1.0 вместо volume_ratio
            
        except Exception as e:
            logger.error(f"❌ Ошибка упрощенной проверки символа {symbol}: {e}")
            return False, 0.0

    async def handle_arbitrage_signal(self, message: str):
        """Обрабатывает арбитражный сигнал с улучшенной диагностикой"""
        try:
            self.message_counter += 1
            signal_time = datetime.now()
            logger.info(f"📨 ========== ОБРАБОТКА СИГНАЛА #{self.message_counter} ==========")
            logger.info(f"📨 Время: {signal_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"📨 Сообщение: {message[:300]}...")
            
            symbol = self.parse_symbol(message)
            if not symbol:
                logger.warning(f"🚫 ПРОПУСК: Не удалось извлечь символ из сообщения #{self.message_counter}")
                logger.warning(f"📋 Полный текст: {message[:500]}...")
                return
                
            self.last_signal_time = signal_time
            self.signals_processed += 1
            
            logger.info(f"✅ Символ извлечен: {symbol}")
            logger.info(f"🎯 Начинаю обработку арбитражного сигнала для {symbol}")

            reference_price = self.extract_reference_price(message)
            if reference_price:
                logger.info(f"📐 Референсная цена из сигнала: ${reference_price:.6f}")
            else:
                logger.info("📐 Референсная цена из сообщения не определена")
            
            # Быстрые проверки (выполняются до запросов к биржам)
            logger.info(f"🔍 Проверка 1: Черный список символов...")
            if symbol.upper() in self.symbol_blacklist:
                logger.warning(f"❌ ПРОПУСК: {symbol} в черном списке")
                return
            logger.info(f"✅ {symbol} не в черном списке")
                
            logger.info(f"🔍 Проверка 2: Коррелированные позиции...")
            if self.is_correlated_open(symbol):
                logger.warning(f"❌ ПРОПУСК: {symbol} - есть коррелированные открытые позиции")
                return
            logger.info(f"✅ Нет коррелированных позиций")
                
            logger.info(f"🔍 Проверка 3: Лимит активных сделок...")
            logger.info(f"   Активных сделок: {len(self.active_trades)}/{MAX_CONCURRENT_TRADES}")
            if len(self.active_trades) >= MAX_CONCURRENT_TRADES:
                logger.warning(f"❌ ПРОПУСК: уже есть активная сделка (лимит: {MAX_CONCURRENT_TRADES})")
                if self.active_trades:
                    logger.warning(f"   Активные сделки: {list(self.active_trades.keys())}")
                return
            logger.info(f"✅ Лимит активных сделок не превышен")
            
            logger.info(f"🔍 Проверка 4: Дневной лимит убытков...")
            logger.info(f"   Текущий PnL: ${self.daily_pnl:.2f}, Лимит: ${-MAX_DAILY_LOSS:.2f}")
            if self.daily_pnl <= -MAX_DAILY_LOSS:
                logger.warning(f"❌ ПРОПУСК: достигнут дневной лимит убытков: ${self.daily_pnl:.2f}")
                return
            logger.info(f"✅ Дневной лимит убытков не достигнут")
            
            exchanges = ['bybit', 'gate', 'mexc', 'bingx']
            active_exchanges = [ex for ex in exchanges if self.exchange_errors[ex] < self.max_errors_before_disable]
            
            logger.info(f"🔍 [EXCHANGES] Проверка доступности бирж:")
            logger.info(f"   - Все биржи: {exchanges}")
            logger.info(f"   - Активные биржи: {active_exchanges}")
            logger.info(f"   - Ошибки по биржам: {dict(self.exchange_errors)}")
            logger.info(f"   - Лимит ошибок перед отключением: {self.max_errors_before_disable}")
            
            if len(active_exchanges) < 2:
                logger.warning(f"❌ [EXCHANGES] ПРОПУСК: недостаточно активных бирж для арбитража (активно: {len(active_exchanges)}, требуется: 2)")
                logger.warning(f"   Доступные биржи: {active_exchanges}")
                return
            
            # Получаем контракты токена (если доступны) для точного сопоставления на биржах
            contracts = None
            try:
                # 1) Try local DB first
                contracts = self.token_db.get_contracts(
                    symbol,
                    reference_price=reference_price,
                    tolerance_percent=10.0,
                )
                if contracts:
                    logger.info(f"💾 TokenDB hit: контракты {symbol}: {contracts}")
                else:
                    logger.info(f"💾 TokenDB miss для {symbol}, обращаемся к CMC")
                    self.token_db.mark_api_call()
                    contracts = self.cmc_client.get_token_contracts(
                        symbol,
                        reference_price=reference_price,
                        tolerance_percent=10.0,
                    )
                    if contracts:
                        logger.info(f"🔗 Контракты {symbol} из CMC: {contracts} — сохраняем в локальную базу")
                        # exchanges_found заполним после получения цен
                    else:
                        logger.info(f"🔗 Контракты {symbol} не найдены в CMC — используем резервный поиск по символу")
            except Exception as e:
                logger.debug(f"⚠️ Ошибка получения контрактов {symbol} из CMC: {e}")

            # ПРЕДЗАГРУЗКА СИМВОЛОВ: Параллельный поиск символа на всех биржах
            contract_address = None
            contract_network = 'BSC'  # По умолчанию BSC
            
            if contracts:
                # Приоритет: ETHEREUM > BSC > POLYGON
                if contracts.get('ETHEREUM'):
                    contract_address = contracts.get('ETHEREUM')
                    contract_network = 'ETHEREUM'
                elif contracts.get('BSC'):
                    contract_address = contracts.get('BSC')
                    contract_network = 'BSC'
                elif contracts.get('POLYGON'):
                    contract_address = contracts.get('POLYGON')
                    contract_network = 'POLYGON'
            
            logger.info(f"🔄 [PRELOAD] Предзагрузка символа {symbol} на всех биржах...")
            if contract_address:
                logger.info(f"🔗 [PRELOAD] Используем контракт для поиска: {contract_address[:10]}... ({contract_network})")
            
            # Параллельный поиск символов на всех биржах
            preload_results = await self.price_fetcher.parallel_symbol_search(symbol, contract_address, contract_network)
            logger.info(f"📊 [PRELOAD] Результаты предзагрузки: {len(preload_results)}/{len(active_exchanges)} бирж")
            for ex_name, found_sym in preload_results.items():
                logger.info(f"   ✅ {ex_name.upper()}: {found_sym}")
            
            # Параллельное получение цен со всех бирж (оптимизация скорости)
            # Добавляем таймаут для каждого запроса, чтобы не ждать бесконечно
            price_tasks = {
                ex: asyncio.wait_for(
                    self.price_fetcher.get_symbol_price_with_cmc(ex, symbol, contracts=contracts),
                    timeout=15.0  # Увеличиваем таймаут до 15 секунд
                )
                for ex in active_exchanges
            }
            
            price_results = await asyncio.gather(
                *price_tasks.values(),
                return_exceptions=True
            )
            
            available_prices = {}
            for exchange, result in zip(active_exchanges, price_results):
                if isinstance(result, Exception):
                    error_type = type(result).__name__
                    error_msg = str(result)
                    if isinstance(result, asyncio.TimeoutError):
                        logger.warning(f"⏱️ ТАЙМАУТ получения цены {symbol} с {exchange} (превышен лимит {ASYNC_TIMEOUT} сек)")
                        logger.warning(f"   Это может означать, что биржа {exchange} медленно отвечает или символ не найден")
                    else:
                        logger.debug(f"⚠️ Ошибка получения цены {symbol} с {exchange} ({error_type}): {error_msg}")
                    self.exchange_errors[exchange] += 1
                    continue
                    
                try:
                    price, found_symbol, market_type = result
                    if price and price > 0:
                        available_prices[exchange] = {
                            'price': price,
                            'symbol': found_symbol,
                            'market_type': market_type
                        }
                        logger.info(f"💰 {exchange.upper()} {symbol}: ${price:.6f} ({found_symbol})")
                    else:
                        logger.debug(f"⚠️ {exchange.upper()} не поддерживает {symbol}")
                except Exception as e:
                    logger.debug(f"⚠️ Ошибка обработки цены {symbol} с {exchange}: {e}")
                    self.exchange_errors[exchange] += 1
                    continue

            if len(available_prices) < 2:
                logger.warning(f"🚫 ПРОПУСК: Недостаточно бирж с символом {symbol}. Найдено: {len(available_prices)}")
                logger.warning(f"   Доступные биржи: {list(available_prices.keys())}")
                # Если контракт пришел из CMC и мы нашли хотя бы одну биржу — всё равно зафиксируем exchanges_found в DB
                try:
                    if contracts:
                        self.token_db.upsert_token(
                            symbol,
                            contracts,
                            exchanges_found=list(available_prices.keys()),
                            reference_price=reference_price,
                        )
                except Exception:
                    pass
                return

            logger.info(f"✅ Найдено {len(available_prices)} бирж с символом {symbol}: {list(available_prices.keys())}")
            # Сохраняем/обновляем запись в локальной базе при наличии контрактов
            try:
                if contracts:
                    self.token_db.upsert_token(
                        symbol,
                        contracts,
                        exchanges_found=list(available_prices.keys()),
                        reference_price=reference_price,
                    )
            except Exception:
                pass
            logger.info(f"🔍 [SPREAD] Поиск лучшей арбитражной возможности...")
            logger.info(f"   - Минимальный спред (MIN_SPREAD): {MIN_SPREAD}%")
            logger.info(f"   - Доступные биржи с ценами: {list(available_prices.keys())}")
            num_pairs = len(available_prices) * (len(available_prices) - 1)
            logger.info(f"   - Количество пар для проверки: {num_pairs}")

            best_opportunity = None
            best_spread = 0
            all_spreads = []
            
            for long_ex, long_data in available_prices.items():
                for short_ex, short_data in available_prices.items():
                    if long_ex != short_ex:
                        long_price = long_data['price']
                        short_price = short_data['price']
                        
                        if long_price > 0 and short_price > 0:
                            spread = (short_price - long_price) / long_price * 100
                            all_spreads.append({
                                'long_ex': long_ex,
                                'short_ex': short_ex,
                                'spread': spread
                            })
                            
                            logger.info(f"📊 [SPREAD] {symbol}: {long_ex.upper()} ${long_price:.6f} -> {short_ex.upper()} ${short_price:.6f} | Спред: {spread:.2f}%")
                            
                            if spread >= MIN_SPREAD and spread > best_spread:
                                logger.info(f"🔍 [SPREAD] Проверяю возможность: {long_ex.upper()} -> {short_ex.upper()} (спред: {spread:.2f}% >= {MIN_SPREAD}%)")
                                should_trade, _ = await self.should_trade_symbol(symbol, long_ex, short_ex)
                                if should_trade:
                                    logger.info(f"✅ [SPREAD] Возможность прошла упрощенную проверку: {long_ex.upper()} -> {short_ex.upper()}")
                                    best_spread = spread
                                    best_opportunity = {
                                        'long_exchange': long_ex,
                                        'short_exchange': short_ex,
                                        'long_price': long_price,
                                        'short_price': short_price,
                                        'spread': spread,
                                        'long_symbol': long_data['symbol'],
                                        'short_symbol': short_data['symbol']
                                    }
                                else:
                                    logger.warning(f"⚠️ [SPREAD] Возможность {long_ex.upper()} -> {short_ex.upper()} не прошла упрощенную проверку")
                            elif spread < MIN_SPREAD:
                                logger.debug(f"   [SPREAD] Спред {spread:.2f}% < минимального {MIN_SPREAD}% - пропуск")

            if best_opportunity and best_spread >= MIN_SPREAD:
                logger.info(f"🎯 ========== НАЙДЕНА АРБИТРАЖНАЯ ВОЗМОЖНОСТЬ {symbol} ==========")
                logger.info(f"   LONG: {best_opportunity['long_exchange'].upper()} @ ${best_opportunity['long_price']:.6f} ({best_opportunity.get('long_symbol', symbol)})")
                logger.info(f"   SHORT: {best_opportunity['short_exchange'].upper()} @ ${best_opportunity['short_price']:.6f} ({best_opportunity.get('short_symbol', symbol)})")
                logger.info(f"   СПРЕД: {best_spread:.2f}% (требуется: {MIN_SPREAD}%)")
                logger.info(f"   🚀 Запускаем выполнение сделки...")
                await self.execute_arbitrage_trade(symbol, best_opportunity)
            else:
                logger.warning(f"❌ ========== ПРОПУСК СДЕЛКИ ДЛЯ {symbol} ==========")
                if all_spreads:
                    logger.warning(f"   Все найденные спреды:")
                    for spread_info in all_spreads:
                        logger.warning(f"     {spread_info['long_ex'].upper()} -> {spread_info['short_ex'].upper()}: {spread_info['spread']:.2f}%")
                if best_spread > 0:
                    logger.warning(f"   Лучший спред: {best_spread:.2f}% < минимального {MIN_SPREAD}%")
                else:
                    logger.warning(f"   Причина: не найдено подходящих пар бирж или спреды < {MIN_SPREAD}%")
                logger.warning(f"   ==========================================")
                
        except Exception as e:
            logger.error(f"❌ Ошибка обработки сигнала: {e}")

    async def maintain_session_activity(self):
        """Поддержание активности Telegram сессии"""
        while True:
            try:
                if self.client and self.client.is_connected():
                    await self.client.get_me()
                    logger.info("🔄 Активность сессии поддержана")
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"❌ Ошибка поддержания активности: {e}")
                await asyncio.sleep(60)

    async def maintain_activity_with_chats(self):
        """Поддержание активности через чтение диалогов"""
        while True:
            try:
                if self.client and self.client.is_connected():
                    async for _ in self.client.iter_dialogs(limit=10):
                        pass
                await asyncio.sleep(600)
            except Exception as e:
                logger.debug(f"⚠️ Ошибка поддержания активности через диалоги: {e}")
                await asyncio.sleep(120)

    async def force_reconnect_telegram(self):
        """Принудительное переподключение Telegram"""
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                if self.client:
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                await self.initialize_telegram_client()
                if self.client and self.client.is_connected():
                    logger.info("✅ Telegram переподключен успешно")
                    return True
            except Exception as e:
                logger.error(f"❌ Попытка {attempt + 1}/{max_attempts} не удалась: {e}")
                await asyncio.sleep(2 ** attempt)
        logger.error("🚫 Не удалось переподключиться к Telegram")
        return False

    async def aggressive_connection_watchdog(self):
        """Агрессивный мониторинг и восстановление соединения"""
        while True:
            try:
                if not self.client or not self.client.is_connected():
                    logger.warning("🔌 Соединение потеряно - принудительное переподключение")
                    await self.force_reconnect_telegram()
                else:
                    try:
                        await asyncio.wait_for(self.client.get_me(), timeout=10)
                    except Exception:
                        logger.warning("⏱️ Таймаут проверки соединения - переподключение")
                        await self.force_reconnect_telegram()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"❌ Ошибка в watchdog: {e}")
                await asyncio.sleep(60)

    async def start_activity_maintenance(self):
        """Запуск всех механизмов поддержания активности"""
        try:
            asyncio.create_task(self.maintain_session_activity())
            asyncio.create_task(self.aggressive_connection_watchdog())
            asyncio.create_task(self.maintain_activity_with_chats())
            logger.info("🛠️ Запущены механизмы поддержания активности сессии")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска механизмов активности: {e}")
    
    async def force_process_recent_messages(self):
        """Принудительная обработка последних сообщений для тестирования"""
        try:
            if not self.client or not self.client.is_connected():
                logger.warning("⚠️ Telegram клиент не подключен для принудительной обработки")
                return
            
            logger.info("🔄 [FORCE] Принудительная обработка последних сообщений...")
            
            # Определяем обработчик внутри функции для доступа
            async def process_message(message_text, channel):
                """Внутренняя функция для обработки сообщения"""
                # Жесткая фильтрация
                required_keywords = ['Spread:', 'Long', 'Short', '📈📈#', '🟢', '🔴']
                if not any(keyword in message_text for keyword in required_keywords):
                    return
                if 'aligned' in message_text.lower():
                    return
                
                # Парсинг и обработка
                symbol = self.parse_symbol_enhanced(message_text)
                if symbol:
                    logger.info(f"🔄 [FORCE] Найден символ '{symbol}' - запуск обработки")
                    await self.handle_arbitrage_signal(message_text)
            
            for channel_name in MONITOR_CHANNELS:
                try:
                    clean_name = channel_name.replace('@', '')
                    channel = await self.client.get_entity(clean_name)
                    messages = await self.client.get_messages(channel, limit=10)
                    
                    logger.info(f"📨 [FORCE] Найдено {len(messages)} сообщений в канале '{channel.title}'")
                    
                    for message in messages:
                        if message.text:
                            await process_message(message.text, channel)
                            await asyncio.sleep(0.5)  # Небольшая задержка между сообщениями
                            
                except Exception as channel_error:
                    logger.warning(f"⚠️ [FORCE] Ошибка обработки канала {channel_name}: {channel_error}")
                    
        except Exception as e:
            logger.error(f"❌ [FORCE] Ошибка принудительной обработки: {e}")

    async def start_monitoring(self):
        """Надежный автономный мониторинг Telegram с авто-реконнектом и перерегистрацией хендлеров."""
        logger.info("📱 Старт автономного Telegram мониторинга...")
        logger.info(f"📡 Мониторим каналы: {', '.join(MONITOR_CHANNELS)}") if MONITOR_CHANNELS else logger.info("📡 Каналы не заданы — обрабатываем все входящие")
       
        async def on_new_message(event):
            try:
                logger.info("=" * 80)
                logger.info("🔔 ========== СОБЫТИЕ NewMessage ПОЛУЧЕНО ==========")
                logger.info("=" * 80)
                
                # ПРОПУСТИТЬ служебные сообщения
                if not event.message or not event.message.text:
                    logger.debug("⏩ Пропуск: нет текста в сообщении")
                    return
                
                # Извлекаем текст сообщения
                message = event.message.text
                logger.info(f"📨 [STEP 1] Получено сообщение (длина: {len(message)} символов)")
                logger.info(f"📨 [STEP 1] Полный текст: {message[:100]}...")
                
                if not message:
                    logger.warning("⚠️ [STEP 1] Сообщение пустое — пропуск")
                    return
                
                # ЖЕСТКАЯ ФИЛЬТРАЦИЯ: только арбитражные сигналы
                required_keywords = ['Spread:', 'Long', 'Short', '📈📈#', '🟢', '🔴']
                if not any(keyword in message for keyword in required_keywords):
                    logger.info("⏩ [FILTER] ПРОПУСК: отсутствуют ключевые слова арбитража")
                    return
                
                # Дополнительная проверка: пропускать сообщения о выравнивании
                if 'aligned' in message.lower():
                    logger.info("⏩ [FILTER] ПРОПУСК: сообщение о выравнивании")
                    return
                
                # Контекст чата
                chat_username = getattr(event.chat, 'username', None) if event and getattr(event, 'chat', None) else None
                chat_title = getattr(event.chat, 'title', None) if event and getattr(event, 'chat', None) else None
                chat_id = getattr(event.chat, 'id', None) if event and getattr(event, 'chat', None) else None
                logger.info(f"👤 [STEP 2] Контекст чата:")
                logger.info(f"   - ID: {chat_id}")
                logger.info(f"   - Username: @{chat_username}" if chat_username else "   - Username: (нет)")
                logger.info(f"   - Title: '{chat_title}'" if chat_title else "   - Title: (нет)")
                
                # Фильтр по каналам
                logger.info(f"🪪 [STEP 3] Проверка фильтра каналов...")
                logger.info(f"   - Мониторируемые каналы: {MONITOR_CHANNELS}")
                
                if MONITOR_CHANNELS and event.chat:
                    ok = False
                    uname = chat_username
                    title = chat_title
                    
                    if uname and f"@{uname}" in MONITOR_CHANNELS:
                        ok = True
                        logger.info(f"   ✅ Канал принят по username: @{uname}")
                    if not ok and title:
                        ok = any(ch.lower() == title.lower() for ch in MONITOR_CHANNELS if isinstance(ch, str))
                        if ok:
                            logger.info(f"   ✅ Канал принят по title: '{title}'")
                    
                    if not ok:
                        logger.warning(f"   ❌ Канал отклонен: username=@{uname}, title='{title}' не в списке мониторинга")
                        return
                else:
                    logger.info(f"   ⚠️ Фильтр каналов отключен или чат не определен")
                
                # Парсинг символа
                logger.info(f"🎯 [STEP 4] Начало парсинга символа...")
                symbol = self.parse_symbol_enhanced(message)
                logger.info(f"🎯 [STEP 4] Результат парсинга символа: '{symbol}'")
                
                if symbol:
                    logger.info("🚀 [STEP 5] Символ найден — запуск обработки арбитражного сигнала в фоне")
                    asyncio.create_task(self.handle_arbitrage_signal(message))
                else:
                    logger.warning("🔎 [STEP 5] Символ не распознан — сообщение пропущено")
                    logger.warning(f"   Полный текст сообщения для анализа: {message[:300]}")
                    
                logger.info("=" * 80)
            except Exception as e:
                logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в обработчике сообщений: {e}")
                try:
                    import traceback as _tb
                    logger.error(f"📋 Трассировка обработчика:\n{_tb.format_exc()}")
                except Exception:
                    pass

        backoff = 1
        backoff_max = 300
        while True:
            try:
                await self.initialize_telegram_client()
                # Перерегистрируем хендлер
                try:
                    self.client.remove_event_handler(on_new_message, events.NewMessage)
                except Exception:
                    pass
                self.client.add_event_handler(on_new_message, events.NewMessage)
                logger.info("✅ Обработчик событий NewMessage зарегистрирован")
                
                # Проверка регистрации обработчиков
                try:
                    handlers = self.client.list_event_handlers()
                    logger.info(f"📋 Проверка: зарегистрировано обработчиков: {len(handlers)}")
                    if handlers:
                        for idx, handler in enumerate(handlers, 1):
                            logger.info(f"   ✅ Обработчик {idx}: {handler}")
                    else:
                        logger.error("🚨 КРИТИЧЕСКАЯ ОШИБКА: обработчики не зарегистрированы!")
                        logger.error("🔄 Попытка переподключения и перерегистрации...")
                        await self.client.disconnect()
                        await asyncio.sleep(2)
                        await self.client.connect()
                        self.client.add_event_handler(on_new_message, events.NewMessage)
                        logger.info("🔄 Обработчик перерегистрирован после переподключения")
                        
                        # Повторная проверка
                        handlers = self.client.list_event_handlers()
                        logger.info(f"📋 После перерегистрации: {len(handlers)} обработчиков")
                except Exception as handler_error:
                    logger.warning(f"⚠️ Не удалось получить список обработчиков: {handler_error}")
                
                logger.info("🧭 Готов принимать входящие сообщения от Telegram")
                logger.info("=" * 80)
                
                # Принудительная обработка последних сообщений для тестирования
                try:
                    await self.force_process_recent_messages()
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка принудительной обработки сообщений: {e}")
                
                backoff = 1
                try:
                    await self.client.run_until_disconnected()
                except Exception as e:
                    logger.error(f"❌ Ошибка в run_until_disconnected: {e}")
                logger.warning("🔌 Соединение разорвано — переподключение")
            except Exception as e:
                logger.error(f"Мониторинг упал: {e} — повтор через {backoff} сек")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, backoff_max)

    def get_trading_stats(self) -> Dict:
        """Возвращает статистику торговли"""
        closed_trades = []
        
        for trade in self.trade_history:
            if trade.get('status') == 'closed':
                closed_trades.append(trade)
        
        for trade in self.active_trades.values():
            if trade.get('status') == 'closed':
                closed_trades.append(trade)
        
        total_trades = len(self.trade_history)
        win_trades = len([t for t in closed_trades if t.get('net_pnl', 0) > 0])
        total_closed_trades = len(closed_trades)
        win_rate = (win_trades / total_closed_trades * 100) if total_closed_trades > 0 else 0
        
        total_profit = sum(t.get('net_pnl', 0) for t in closed_trades)
        
        return {
            'daily_pnl': self.daily_pnl,
            'daily_trades': self.daily_trade_count,
            'total_trades': total_trades,
            'win_rate': win_rate,
            'total_profit': total_profit,
            'active_trades': len([t for t in self.active_trades.values() if t.get('status') != 'closed']),
            'active_symbols': list(set(t['symbol'] for t in self.active_trades.values() if t.get('status') != 'closed'))
        }

    async def run_interface(self):
        """Запускает Rich интерфейс"""
        try:
            with Live(self.create_layout(), refresh_per_second=4, screen=True) as live:
                while True:
                    try:
                        live.update(self.create_layout())
                        await asyncio.sleep(0.25)
                    except Exception as e:
                        logger.error(f"❌ Ошибка обновления интерфейса: {e}")
                        await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"❌ Ошибка интерфейса: {e}")

    def create_layout(self) -> Layout:
        """Создает макет интерфейса"""
        layout = Layout()
        
        layout.split_row(
            Layout(name="left"),
            Layout(name="right")
        )
        
        layout["left"].split_column(
            Layout(name="header", size=3),
            Layout(name="stats"),
            Layout(name="active_trades")
        )
        
        layout["right"].split_column(
            Layout(name="balances"),
            Layout(name="recent_trades"),
            Layout(name="network_logs")
        )
        
        try:
            layout["header"].update(self.create_header_panel())
            layout["stats"].update(self.create_stats_panel())
            layout["active_trades"].update(self.create_active_trades_panel())
            layout["balances"].update(self.create_balances_panel())
            layout["recent_trades"].update(self.create_recent_trades_panel())
            layout["network_logs"].update(self.create_network_logs_panel())
        except Exception as e:
            logger.error(f"❌ Ошибка создания макета: {e}")
            layout["header"].update(Panel("🚀 ARBITRAGE BOT - ОШИБКА ИНТЕРФЕЙСА", style="red"))
        
        return layout

    def create_header_panel(self) -> Panel:
        """Создает верхнюю панель с названием и статусом"""
        title = Text("🚀 ULTRA FAST ARBITRAGE BOT", style="bold cyan")
        status = Text("● РЕАЛЬНАЯ ТОРГОВЛЯ", style="bold green")
        
        current_time = datetime.now().strftime("%H:%M:%S")
        runtime = datetime.now() - self.last_reset
        
        balance_update_ago = (datetime.now() - self.last_balance_update).total_seconds()
        update_status = "🟢" if balance_update_ago < 5 else "🟡" if balance_update_ago < 10 else "🔴"
        
        time_sync_info = ""
        if self.last_time_sync:
            time_sync_ago = (datetime.now() - self.last_time_sync).total_seconds()
            time_status = "🟢" if time_sync_ago < 600 else "🟡" if time_sync_ago < 1800 else "🔴"
            time_sync_info = f" | 🕒 {time_status} offset:{self.time_offset}ms"
        
        header_table = Table(show_header=False, box=box.ROUNDED, show_edge=False)
        header_table.add_column("Title", justify="left")
        header_table.add_column("Status", justify="center")
        header_table.add_column("Time", justify="right")
        
        header_table.add_row(
            title,
            status,
            f"🕒 {current_time} | ⏱️ {str(runtime).split('.')[0]} | {update_status}{time_sync_info}"
        )
        
        return Panel(header_table, style="white")

    def create_stats_panel(self) -> Panel:
        """Создает панель статистики"""
        stats = self.get_trading_stats()
        
        table = Table(show_header=True, header_style="bold magenta", box=box.ROUNDED)
        table.add_column("Метрика", style="cyan", width=20)
        table.add_column("Значение", style="white", justify="right")
        
        daily_pnl_style = "green" if stats['daily_pnl'] >= 0 else "red"
        total_profit_style = "green" if stats['total_profit'] >= 0 else "red"
        
        total_unrealized = sum(bal.get('unrealized_pnl', 0) for bal in self.exchange_balances.values())
        unrealized_style = "green" if total_unrealized >= 0 else "red"
        
        active_real_orders = self.order_manager.get_active_orders_count()
        
        table.add_row("💰 Дневной PnL", f"[{daily_pnl_style}]{stats['daily_pnl']:+.2f}$[/]")
        table.add_row("📈 Всего PnL", f"[{total_profit_style}]{stats['total_profit']:+.2f}$[/]")
        table.add_row("💫 Нереализ. PnL", f"[{unrealized_style}]{total_unrealized:+.2f}$[/]")
        table.add_row("🎯 Винрейт", f"{stats['win_rate']:.1f}%")
        table.add_row("🔢 Сделок сегодня", str(stats['daily_trades']))
        table.add_row("📊 Всего сделок", str(stats['total_trades']))
        table.add_row("🔄 Активных сделок", f"[bold yellow]{stats['active_trades']}[/]")
        table.add_row("📤 Реальных ордеров", f"[bold cyan]{active_real_orders}[/]")
        table.add_row("📶 Обработано сигналов", str(self.signals_processed))
        
        exchange_status = []
        for exchange in ['bybit', 'gate', 'mexc', 'bingx']:
            errors = self.exchange_errors[exchange]
            status = "🟢" if errors == 0 else "🟡" if errors < 3 else "🔴"
            exchange_status.append(f"{exchange.upper()}{status}")
        
        table.add_row("🏦 Статус бирж", " ".join(exchange_status))
        
        if self.last_signal_time:
            last_signal = (datetime.now() - self.last_signal_time).total_seconds()
            signal_style = "green" if last_signal < 60 else "yellow" if last_signal < 300 else "red"
            table.add_row("⏰ Последний сигнал", f"[{signal_style}]{last_signal:.0f} сек назад[/]")
        else:
            table.add_row("⏰ Последний сигнал", "---")
            
        return Panel(table, title="📈 СТАТИСТИКА ТОРГОВЛИ", border_style="cyan")

    def create_active_trades_panel(self) -> Panel:
        """Создает панель активных сделок"""
        if not self.active_trades and self.order_manager.get_active_orders_count() == 0:
            return Panel(
                Align.center("📭 Нет активных сделок"), 
                title="🎯 АКТИВНЫЕ СДЕЛКИ", 
                border_style="yellow"
            )
        
        table = Table(show_header=True, header_style="bold green", box=box.ROUNDED)
        table.add_column("ID", style="cyan", width=12)
        table.add_column("Символ", width=8)
        table.add_column("LONG", style="blue", width=8)
        table.add_column("SHORT", style="red", width=8)
        table.add_column("Спред", justify="right", width=8)
        table.add_column("Объем", justify="right", width=10)
        table.add_column("Время", justify="right", width=8)
        table.add_column("PnL", justify="right", width=10)
        table.add_column("Статус", width=12)
        
        for trade in self.active_trades.values():
            duration = (datetime.now() - trade['entry_time']).total_seconds()
            
            try:
                current_pnl = self.calculate_simple_pnl(trade)
            except:
                current_pnl = 0.0
                    
            pnl_style = "green" if current_pnl > 0 else "red"
            pnl_text = f"{current_pnl:+.2f}$"
            
            status_text = "[green]активна[/]"
            if trade.get('long_order_id') or trade.get('short_order_id'):
                status_text = "[yellow]закрытие...[/]"
            
            table.add_row(
                trade['trade_id'][-8:],
                trade['symbol'],
                trade['long_exchange'],
                trade['short_exchange'],
                f"{trade['entry_spread']:.1f}%",
                f"{trade['quantity']:.4f}",
                f"{duration:.0f}с",
                f"[{pnl_style}]{pnl_text}[/]",
                status_text
            )
        
        for order_id, order_info in self.order_manager.active_orders.items():
            if order_info['status'] == 'open':
                table.add_row(
                    order_id[-8:],
                    order_info['symbol'],
                    order_info['exchange'],
                    order_info['side'],
                    "---",
                    f"{order_info['quantity']:.4f}",
                    f"{(datetime.now() - order_info['created_at']).total_seconds():.0f}с",
                    "---",
                    f"[cyan]{order_info['status']}[/]"
                )
        
        return Panel(table, title="🎯 АКТИВНЫЕ СДЕЛКИ", border_style="green")

    def create_balances_panel(self) -> Panel:
        """Создает панель балансов бирж с РЕАЛЬНЫМИ данными"""
        table = Table(show_header=True, header_style="bold blue", box=box.ROUNDED)
        table.add_column("Биржа", style="cyan", width=12)
        table.add_column("Общий баланс", justify="right", width=15)
        table.add_column("Доступно", justify="right", width=15)
        table.add_column("Занято", justify="right", width=15)
        table.add_column("Реал. PnL", justify="right", width=15)
        table.add_column("Нереал. PnL", justify="right", width=15)
        table.add_column("Статус", width=8)
        
        sorted_exchanges = sorted(self.exchange_balances.items(), 
                                key=lambda x: x[1]['total'], reverse=True)
        
        for exchange, balance in sorted_exchanges:
            total = balance['total']
            available = balance['available']
            locked = balance['locked']
            realized_today = balance['pnl_today']
            unrealized = balance.get('unrealized_pnl', 0.0)
            
            total_style = "green" if total > 0 else "red"
            available_style = "white" 
            locked_style = "yellow"
            realized_style = "green" if realized_today >= 0 else "red"
            unrealized_style = "green" if unrealized >= 0 else "yellow" if unrealized >= -1 else "red"
            
            balance_update_ago = (datetime.now() - self.last_balance_update).total_seconds()
            if balance_update_ago < 15:
                status = "🟢" if balance.get('real_data', True) else "🟡"
            else:
                status = "🔴"
            
            table.add_row(
                f"🏦 {exchange.upper()}",
                f"[{total_style}]{total:.2f}$[/]",
                f"[{available_style}]{available:.2f}$[/]",
                f"[{locked_style}]{locked:.2f}$[/]",
                f"[{realized_style}]{realized_today:+.2f}$[/]",
                f"[{unrealized_style}]{unrealized:+.2f}$[/]",
                status
            )
        
        total_balance = self.total_balance
        total_available = sum(bal['available'] for bal in self.exchange_balances.values())
        total_locked = sum(bal['locked'] for bal in self.exchange_balances.values())
        total_realized = sum(bal['pnl_today'] for bal in self.exchange_balances.values())
        total_unrealized = sum(bal.get('unrealized_pnl', 0) for bal in self.exchange_balances.values())
        
        total_style = "bold green" if total_balance > 0 else "bold red"
        total_realized_style = "bold green" if total_realized >= 0 else "bold red"
        total_unrealized_style = "bold green" if total_unrealized >= 0 else "bold yellow" if total_unrealized >= -2 else "bold red"
        
        table.add_row(
            "[bold]ВСЕГО[/bold]",
            f"[{total_style}]{total_balance:.2f}$[/]",
            f"[{total_style}]{total_available:.2f}$[/]",
            f"[bold yellow]{total_locked:.2f}$[/]",
            f"[{total_realized_style}]{total_realized:+.2f}$[/]",
            f"[{total_unrealized_style}]{total_unrealized:+.2f}$[/]",
            "📊"
        )
        
        update_ago = (datetime.now() - self.last_balance_update).total_seconds()
        update_status = "🟢 СВЕЖИЕ" if update_ago < 15 else "🟡 УСТАРЕЛИ" if update_ago < 60 else "🔴 НЕТ ДАННЫХ"
        
        info_text = f"🕒 Обновлено {update_ago:.0f} сек назад | {update_status}"
        
        main_layout = Layout()
        main_layout.split_column(
            Layout(table),
            Layout(Panel(Align.center(info_text), style="cyan"))
        )
        
        return Panel(main_layout, title="🏦 БАЛАНСЫ БИРЖ (РЕАЛЬНЫЕ ДАННЫХ)", border_style="blue")

    def create_recent_trades_panel(self) -> Panel:
        """Создает панель последних сделок"""
        closed_trades = []
        
        for trade in self.trade_history:
            if trade.get('status') == 'closed':
                closed_trades.append(trade)
        
        for trade in self.active_trades.values():
            if trade.get('status') == 'closed':
                closed_trades.append(trade)
        
        recent_trades = sorted(closed_trades, 
                             key=lambda x: x.get('exit_time', x.get('entry_time', datetime.min)), 
                             reverse=True)[:8]
        
        if not recent_trades:
            return Panel(
                Align.center("📭 Нет завершенных сделок"), 
                title="📋 ПОСЛЕДНИЕ СДЕЛКИ", 
                border_style="magenta"
            )
        
        table = Table(show_header=True, header_style="bold yellow", box=box.ROUNDED)
        table.add_column("Символ", style="cyan", width=8)
        table.add_column("Результат", width=12)
        table.add_column("Спред", justify="right", width=8)
        table.add_column("Длит.", justify="right", width=8)
        table.add_column("PnL", justify="right", width=12)
        table.add_column("Причина", width=12)
        
        for trade in recent_trades:
            pnl = trade.get('pnl', trade.get('net_pnl', 0))
            pnl_style = "green" if pnl > 0 else "red"
            result_style = "✅" if pnl > 0 else "❌"
            duration = trade.get('duration_seconds', 0)
            close_reason = trade.get('close_reason', 'unknown')
            
            if len(close_reason) > 10:
                close_reason = close_reason[:10] + "..."
            
            table.add_row(
                trade['symbol'],
                result_style,
                f"{trade.get('entry_spread', 0):.1f}%",
                f"{duration:.0f}с",
                f"[{pnl_style}]{pnl:+.2f}$[/]",
                close_reason
            )
        
        return Panel(table, title="📋 ПОСЛЕДНИЕ СДЕЛКИ", border_style="yellow")

    def create_network_logs_panel(self) -> Panel:
        """Создает панель сетевых логов"""
        try:
            network_logs = network_logger.get_network_logs(8)
        except Exception as e:
            network_logs = f"⚠️ Логи сети недоступны: {e}"
        
        log_text = Text()
        for line in network_logs.split('\n'):
            if 'ОШИБКА' in line or 'ERROR' in line:
                log_text.append(line + '\n', style="red")
            elif 'ПОВТОР' in line or 'RETRY' in line:
                log_text.append(line + '\n', style="yellow")
            elif 'ЗАПРОС' in line or 'REQUEST' in line:
                log_text.append(line + '\n', style="cyan")
            elif 'ОТВЕТ' in line or 'RESPONSE' in line:
                log_text.append(line + '\n', style="green")
            else:
                log_text.append(line + '\n', style="white")
        
        return Panel(log_text, title="🌐 СЕТЕВЫЕ ЗАПРОСЫ К БИРЖАМ", border_style="blue")

    def create_logs_panel(self) -> Panel:
        """Создает панель логов"""
        try:
            with open('arbitrage_bot_current.log', 'r', encoding='utf-8') as f:
                lines = f.readlines()[-8:]
        except:
            lines = ["Логи недоступны\n"]
        
        log_text = Text()
        for line in lines:
            if 'ERROR' in line:
                log_text.append(line, style="red")
            elif 'WARNING' in line:
                log_text.append(line, style="yellow")
            elif 'INFO' in line:
                if any(word in line for word in ['ОТКРЫТА', 'закрытие', 'PnL', 'ордер']):
                    log_text.append(line, style="green")
                else:
                    log_text.append(line, style="white")
            else:
                log_text.append(line, style="white")
        
        return Panel(log_text, title="📝 ПОСЛЕДНИЕ ЛОГИ", border_style="white")

async def main():
    """Основная функция запуска бота"""
    from logging_config import setup_logging
    setup_logging()
    
    bot = SmartArbitrageBot()
    try:
        await bot.initialize()
        
        # Бот инициализирован, все задачи запущены в фоне
        # Ждем бесконечно (бот работает в фоне)
        logger.info("🚀 Бот запущен и работает. Все задачи выполняются в фоновом режиме.")
        logger.info("💡 Для остановки нажмите Ctrl+C")
        
        # Бесконечный цикл для поддержания работы бота
        try:
            while True:
                await asyncio.sleep(60)  # Проверяем каждую минуту
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки (Ctrl+C)")
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка бота: {e}")
        import traceback
        logger.error(f"📋 Трассировка: {traceback.format_exc()}")
    finally:
        logger.info("🔄 Завершение работы бота...")
        if bot.client:
            try:
                if bot.client.is_connected():
                    await bot.client.disconnect()
                logger.info("✅ Telegram клиент отключен")
            except Exception as e:
                logger.error(f"⚠️ Ошибка при отключении Telegram клиента: {e}")
        bot.save_balances()
        logger.info("💾 Балансы сохранены при выходе")
        logger.info("✅ Бот завершил работу")

if __name__ == "__main__":
    asyncio.run(main())