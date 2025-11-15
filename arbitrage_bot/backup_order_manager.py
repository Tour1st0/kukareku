import ccxt
import asyncio
from typing import Dict, Optional, Tuple, List, Any
from datetime import datetime
import logging
import time
from logging_config import get_logger

# ✅ ИМПОРТИРУЕМ LEVERAGE ИЗ КОНФИГА
from config import LEVERAGE
from exchange_network_logger import network_logger

logger = get_logger(__name__)

class OrderManager:
    def __init__(self, exchanges_config: Dict):
        self.exchanges = {}
        self.active_orders = {}
        self.connection_status = {}
        self.setup_exchanges(exchanges_config)
        
    def setup_exchanges(self, exchanges_config: Dict):
        """Настройка подключений к биржам в РЕАЛЬНОМ режиме с валидацией"""
        exchange_classes = {
            'bybit': ccxt.bybit,
            'bingx': ccxt.bingx,
            'gate': ccxt.gateio,
            'mexc': ccxt.mexc
        }
        
        for exchange_name, config in exchanges_config.items():
            if exchange_name not in exchange_classes:
                logger.warning(f"⚠️ Неизвестная биржа в конфигурации: {exchange_name}")
                continue

            if not config.get('enabled', False):
                logger.info(f"⏭️ {exchange_name.upper()} отключен в конфигурации")
                self.connection_status[exchange_name] = {'connected': False, 'error': 'Disabled in config'}
                continue

            api_key = (config.get('api_key') or '').strip()
            api_secret = (config.get('api_secret') or '').strip()
            password = (config.get('password') or '').strip()

            if not api_key or not api_secret:
                logger.error(f"❌ {exchange_name.upper()}: отсутствуют API ключи")
                self.connection_status[exchange_name] = {'connected': False, 'error': 'Missing API keys'}
                continue

                try:
                    exchange_class = exchange_classes[exchange_name]
                    
                    exchange_config = {
                'apiKey': api_key,
                'secret': api_secret,
                        'enableRateLimit': True,
                'timeout': 60000,
                        'rateLimit': 1000,
                'options': {}
                    }
                    
                if password:
                    exchange_config['password'] = password

                if exchange_name == 'bybit':
                        exchange_config.update({
                        'adjustForTimeDifference': True
                    })
                    exchange_config['options'].update({
                                'defaultType': 'unified',
                                'recvWindow': 60000,
                        'timeDifference': True,
                        'defaultSettle': 'USDT'
                        })
                elif exchange_name == 'bingx':
                        exchange_config.update({
                        'adjustForTimeDifference': True
                    })
                    exchange_config['options'].update({
                        'defaultType': 'swap',
                        'recvWindow': 60000
                    })
                elif exchange_name == 'gate':
                    exchange_config['options'].update({
                        'defaultType': 'swap',
                        'settle': 'USDT'
                        })
                    elif exchange_name == 'mexc':
                    exchange_config['options'].update({
                                'defaultType': 'swap',
                                'adjustForTimeDifference': True
                    })

                exchange = exchange_class(exchange_config)

                # Подтверждаем подключение попыткой получения времени сервера
                try:
                exchange.load_markets()
                logger.info(f"🌐 {exchange_name.upper()} рынки загружены ({len(exchange.markets)} инструментов)")
                except Exception as market_error:
                logger.debug(f"⚠️ {exchange_name.upper()}: не удалось загрузить рынки при инициализации: {market_error}")

                self.exchanges[exchange_name] = exchange
                self.connection_status[exchange_name] = {'connected': True, 'error': None}
                masked_key = f"{api_key[:6]}...{api_key[-4:]}" if len(api_key) >= 10 else "***"
                logger.info(f"✅ {exchange_name.upper()} инициализирован успешно (API Key: {masked_key})")
                    
                except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Ошибка инициализации {exchange_name.upper()}: {error_msg}")
                logger.error(f"   Тип ошибки: {type(e).__name__}")
                import traceback
                logger.debug(f"   Трассировка: {traceback.format_exc()}")
                self.connection_status[exchange_name] = {'connected': False, 'error': error_msg}
    
    async def test_connection(self, exchange_name: str) -> bool:
        """Тестирует подключение к бирже"""
        if exchange_name not in self.exchanges:
            logger.error(f"🚫 {exchange_name.upper()} не инициализирован")
            return False
        
        exchange = self.exchanges[exchange_name]
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔍 Тестирование подключения к {exchange_name.upper()}...")
                
                # Пробуем получить баланс для проверки подключения
                balance_params = self._get_balance_params(exchange_name)
                
                balance = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.fetch_balance,
                    balance_params
                )
                
                if balance:
                    logger.info(f"✅ {exchange_name.upper()} подключен успешно")
                    self.connection_status[exchange_name]['connected'] = True
                    self.connection_status[exchange_name]['error'] = None
                    return True
                else:
                    logger.warning(f"⚠️ {exchange_name.upper()} вернул пустой баланс")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)
                    continue
                    
            except ccxt.AuthenticationError as e:
                error_msg = f"Ошибка аутентификации: {str(e)}"
                logger.error(f"🔑 {exchange_name.upper()} {error_msg}")
                logger.error(f"   Проверьте правильность API ключей и секретов")
                self.connection_status[exchange_name]['connected'] = False
                self.connection_status[exchange_name]['error'] = error_msg
                return False
                
            except ccxt.PermissionDenied as e:
                error_msg = f"Недостаточно прав: {str(e)}"
                logger.error(f"🚫 {exchange_name.upper()} {error_msg}")
                logger.error(f"   Убедитесь что API ключ имеет права на фьючерсную торговлю")
                self.connection_status[exchange_name]['connected'] = False
                self.connection_status[exchange_name]['error'] = error_msg
                return False
                
            except ccxt.NetworkError as e:
                error_msg = f"Сетевая ошибка: {str(e)}"
                error_str = str(e)
                logger.warning(f"🌐 {exchange_name.upper()} {error_msg} (попытка {attempt + 1}/{max_retries})")
                
                # Для Gate.io сетевые ошибки могут быть временными
                # Проверяем, не связана ли ошибка с неправильным endpoint
                if exchange_name == 'gate' and 'spot/currencies' in error_str:
                    logger.debug(f"🔍 Gate.io: пропуск ошибки spot API (это нормально для фьючерсов)")
                    # Пробуем получить баланс напрямую через фьючерсный API
                    try:
                        # Пробуем альтернативный способ получения баланса
                        balance_params_alt = {'type': 'future', 'settle': 'USDT'}
                        balance = await asyncio.get_event_loop().run_in_executor(
                            None,
                            exchange.fetch_balance,
                            balance_params_alt
                        )
                        if balance:
                            logger.info(f"✅ {exchange_name.upper()} подключен через альтернативный метод")
                            self.connection_status[exchange_name]['connected'] = True
                            self.connection_status[exchange_name]['error'] = None
                            return True
                    except Exception as alt_error:
                        logger.debug(f"⚠️ Альтернативный метод не сработал: {alt_error}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                continue
                
            except ccxt.ExchangeError as e:
                error_msg = f"Ошибка биржи: {str(e)}"
                logger.error(f"🏦 {exchange_name.upper()} {error_msg}")
                self.connection_status[exchange_name]['connected'] = False
                self.connection_status[exchange_name]['error'] = error_msg
                return False
            
            except Exception as e:
                error_msg = f"Неизвестная ошибка: {type(e).__name__}: {str(e)}"
                logger.error(f"❌ {exchange_name.upper()} {error_msg}")
                import traceback
                logger.debug(f"   Трассировка: {traceback.format_exc()}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                continue
        
        logger.error(f"🚫 {exchange_name.upper()} не прошел тест подключения после {max_retries} попыток")
        self.connection_status[exchange_name]['connected'] = False
        return False
    
    async def test_all_connections(self) -> Dict[str, bool]:
        """Тестирует подключения ко всем биржам"""
        results = {}
        logger.info("🔍 Тестирование подключений ко всем биржам...")
        
        for exchange_name in self.exchanges.keys():
            results[exchange_name] = await self.test_connection(exchange_name)
            await asyncio.sleep(0.5)  # Небольшая задержка между запросами
        
        connected_count = sum(1 for v in results.values() if v)
        logger.info(f"📊 Результаты тестирования: {connected_count}/{len(results)} бирж подключено")
        
        for exchange_name, connected in results.items():
            status = "✅" if connected else "❌"
            logger.info(f"   {status} {exchange_name.upper()}: {'Подключено' if connected else 'Ошибка'}")
            if not connected and exchange_name in self.connection_status:
                error = self.connection_status[exchange_name].get('error', 'Unknown error')
                logger.info(f"      Ошибка: {error}")
        
        return results
    
    def log_order_request(self, exchange_name: str, method: str, symbol: str, 
                         params: Dict = None):
        """Логирует запрос на создание/управление ордером"""
        try:
            network_logger.log_request(exchange_name, method, symbol, params)
            return time.time()
        except Exception as e:
            logger.debug(f"⚠️ Ошибка логирования ордер-запроса: {e}")
            return time.time()

    def log_order_response(self, exchange_name: str, method: str, symbol: str, 
                          response_data: Any, start_time: float = None, 
                          error: str = None):
        """Логирует ответ по ордеру"""
        try:
            duration = None
            if start_time:
                duration = time.time() - start_time
                
            if error:
                network_logger.log_error(exchange_name, method, symbol, 
                                       'OrderError', error)
            else:
                network_logger.log_response(exchange_name, method, symbol, 
                                          200, response_data, duration=duration)
        except Exception as e:
            logger.debug(f"⚠️ Ошибка логирования ордер-ответа: {e}")

    async def create_limit_order(self, exchange_name: str, symbol: str, side: str, 
                               quantity: float, price: float, found_symbol: str = None) -> Optional[Dict]:
        """Создает лимитный ордер на реальной бирже С ЛОГИРОВАНИЕМ"""
        if exchange_name not in self.exchanges:
            logger.error(f"🚫 Биржа {exchange_name} не инициализирована")
            return None
        
        # Проверяем статус подключения
        if exchange_name in self.connection_status:
            status = self.connection_status[exchange_name]
            if not status.get('connected', False):
                error = status.get('error', 'Unknown error')
                logger.error(f"🚫 {exchange_name.upper()} не подключен: {error}")
            return None
            
        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        # Используем найденный символ если предоставлен, иначе исходный
        actual_symbol = found_symbol if found_symbol else symbol
        
        for attempt in range(max_retries):
            start_time = None
            try:
                logger.info(f"📤 Создание ордера на {exchange_name}: {side} {quantity} {actual_symbol} @ ${price:.6f}")
                
                # Подготавливаем параметры ордера в зависимости от биржи
                order_params = {}
                
                # Базовые параметры для всех бирж
                if exchange_name == 'bybit':
                order_params = {
                    'reduceOnly': False,
                        'positionIdx': 0,  # One-way mode
                        'timeInForce': 'GTC'  # Good Till Cancel
                }
                elif exchange_name == 'gate':
                order_params = {
                        'reduce_only': False,
                        'auto_borrow': False,
                        'settle': 'USDT'
                    }
                elif exchange_name == 'mexc':
                    order_params = {
                        'reduceOnly': False,
                        'leverage': LEVERAGE
                    }
                elif exchange_name == 'bingx':
                    order_params = {
                        'reduceOnly': False,
                        'leverage': LEVERAGE
                    }
                
                # Логируем запрос
                log_params = {
                    'symbol': actual_symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': price,
                    'leverage': LEVERAGE,
                    'marginMode': 'isolated',
                    **order_params
                }
                start_time = self.log_order_request(exchange_name, 'create_limit_order', actual_symbol, log_params)
                
                # Асинхронный вызов создания ордера
                order = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.create_order,
                    actual_symbol,  # Используем правильный символ
                    'limit',
                    side,
                    quantity,
                    price,
                    order_params
                )
                
                order_id = order.get('id') or order.get('orderId') or 'unknown'
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, 
                                      {'order_id': order_id, 'status': order.get('status', 'unknown')}, 
                                      start_time)
                
                logger.info(f"✅ Ордер создан: {order_id} на {exchange_name} для {actual_symbol}")
                
                self.active_orders[order_id] = {
                    'exchange': exchange_name,
                    'symbol': actual_symbol,
                    'side': side,
                    'quantity': quantity,
                    'price': price,
                    'status': order.get('status', 'open'),
                    'created_at': datetime.now(),
                    'raw_order': order
                }
                
                return order
                
            except ccxt.InsufficientFunds as e:
                error_msg = f"Недостаточно средств: {str(e)}"
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, None, start_time, error_msg)
                logger.error(f"❌ Недостаточно средств на {exchange_name} для {side} {quantity} {actual_symbol}: {str(e)}")
                # Попробуем получить баланс для диагностики
                try:
                    balance = await self.fetch_balance(exchange_name)
                    if balance:
                        logger.error(f"   Доступный баланс: ${balance.get('free', 0):.2f} USDT")
                except:
                    pass
                return None
            except ccxt.NetworkError as e:
                error_msg = f"Сетевая ошибка: {str(e)}"
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, None, start_time, error_msg)
                logger.error(f"🌐 Сетевая ошибка {exchange_name} (попытка {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'create_limit_order', actual_symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except ccxt.ExchangeError as e:
                error_msg = f"Ошибка биржи: {str(e)}"
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, None, start_time, error_msg)
                logger.error(f"🏦 Ошибка биржи {exchange_name}: {str(e)}")
                # Детализируем ошибку биржи
                error_str = str(e).lower()
                if 'insufficient' in error_str or 'balance' in error_str:
                    logger.error(f"💸 Недостаточно средств или маржи на {exchange_name}")
                    try:
                        balance = await self.fetch_balance(exchange_name)
                        if balance:
                            logger.error(f"   Доступный баланс: ${balance.get('free', 0):.2f} USDT")
                    except:
                        pass
                elif 'rate limit' in error_str or 'too many requests' in error_str:
                    logger.error(f"⏰ Превышен лимит запросов на {exchange_name}")
                    await asyncio.sleep(5)
                elif 'symbol' in error_str or 'invalid' in error_str:
                    logger.error(f"📛 Неверный символ {actual_symbol} на {exchange_name}")
                    logger.error(f"   Попробуйте использовать другой формат символа")
                elif 'leverage' in error_str or 'margin' in error_str:
                    logger.error(f"⚠️ Проблема с плечом или маржой на {exchange_name}")
                return None
            except ccxt.AuthenticationError as e:
                error_msg = f"Ошибка аутентификации: {str(e)}"
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, None, start_time, error_msg)
                logger.error(f"🔑 Ошибка аутентификации на {exchange_name}: {str(e)}")
                logger.error(f"   Проверьте правильность API ключей")
                self.connection_status[exchange_name]['connected'] = False
                self.connection_status[exchange_name]['error'] = error_msg
                return None
            except ccxt.RequestTimeout as e:
                error_msg = f"Таймаут запроса: {str(e)}"
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, None, start_time, error_msg)
                logger.error(f"⏰ Таймаут запроса на {exchange_name}: {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'create_limit_order', actual_symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except Exception as e:
                error_msg = f"Неизвестная ошибка: {type(e).__name__}: {str(e)}"
                self.log_order_response(exchange_name, 'create_limit_order', actual_symbol, None, start_time, error_msg)
                logger.error(f"❌ Неизвестная ошибка при создании ордера на {exchange_name}: {type(e).__name__}: {str(e)}")
                import traceback
                logger.error(f"📋 Трассировка: {traceback.format_exc()}")
                return None
        
        logger.error(f"🚫 Не удалось создать ордер на {exchange_name} после {max_retries} попыток")
        return None
    
    async def get_order_status(self, exchange_name: str, order_id: str) -> Optional[str]:
        """Проверяет статус ордера с детальным логированием"""
        if exchange_name not in self.exchanges:
            logger.error(f"🚫 Биржа {exchange_name} не инициализирована для проверки ордера")
            return None
            
        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'get_order_status', order_id)
                
                # Асинхронный вызов
                order = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.fetch_order,
                    order_id
                )
                
                status = order.get('status', 'unknown')
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'get_order_status', order_id, 
                                      {'status': status}, 
                                      start_time)
                
                if order_id in self.active_orders:
                    self.active_orders[order_id]['status'] = status
                    self.active_orders[order_id]['raw_order'] = order
                
                logger.debug(f"📊 Статус ордера {order_id} на {exchange_name}: {status}")
                return status
                
            except ccxt.OrderNotFound as e:
                error_msg = f"Ордер не найден: {str(e)}"
                self.log_order_response(exchange_name, 'get_order_status', order_id, None, start_time, error_msg)
                logger.error(f"📭 Ордер {order_id} не найден на {exchange_name}: {str(e)}")
                return 'not_found'
            except ccxt.NetworkError as e:
                error_msg = f"Сетевая ошибка: {str(e)}"
                self.log_order_response(exchange_name, 'get_order_status', order_id, None, start_time, error_msg)
                logger.error(f"🌐 Сетевая ошибка при проверке ордера {order_id} на {exchange_name} (попытка {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'get_order_status', order_id, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except ccxt.ExchangeError as e:
                error_msg = f"Ошибка биржи: {str(e)}"
                self.log_order_response(exchange_name, 'get_order_status', order_id, None, start_time, error_msg)
                logger.error(f"🏦 Ошибка биржи при проверке ордера {order_id} на {exchange_name}: {str(e)}")
                return None
            except Exception as e:
                error_msg = f"Неизвестная ошибка: {type(e).__name__}: {str(e)}"
                self.log_order_response(exchange_name, 'get_order_status', order_id, None, start_time, error_msg)
                logger.error(f"❌ Неизвестная ошибка при проверке ордера {order_id} на {exchange_name}: {type(e).__name__}: {str(e)}")
                return None
        
        return None
    
    async def cancel_order(self, exchange_name: str, order_id: str) -> bool:
        """Отменяет ордер"""
        if exchange_name not in self.exchanges:
            return False
            
        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'cancel_order', order_id)
                
                # Асинхронный вызов
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.cancel_order,
                    order_id
                )
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'cancel_order', order_id, 
                                      {'result': 'success'}, 
                                      start_time)
                
                logger.info(f"✅ Ордер {order_id} отменен на {exchange_name}")
                
                if order_id in self.active_orders:
                    self.active_orders[order_id]['status'] = 'canceled'
                
                return True
                
            except Exception as e:
                error_msg = f"Ошибка отмены ордера: {str(e)}"
                self.log_order_response(exchange_name, 'cancel_order', order_id, None, start_time, error_msg)
                logger.error(f"❌ Ошибка отмены ордера {order_id} на {exchange_name} (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'cancel_order', order_id, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return False
    
    async def fetch_balance(self, exchange_name: str) -> Optional[Dict]:
        """УНИФИЦИРОВАННЫЙ метод получения фьючерсного баланса с улучшенным логированием"""
        if exchange_name not in self.exchanges:
            logger.error(f"🚫 Биржа {exchange_name} не инициализирована для запроса баланса")
            return None
            
        exchange = self.exchanges[exchange_name]
        max_retries = 3
        default_balance = {'free': 0.0, 'used': 0.0, 'total': 0.0}
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос с ДЕТАЛЬНЫМИ параметрами
                balance_params = self._get_balance_params(exchange_name)
                start_time = self.log_order_request(exchange_name, 'fetch_balance', 'balance', balance_params)
                
                logger.info(f"🔍 Запрос баланса {exchange_name} с параметрами: {balance_params}")
                
                # Асинхронный вызов с обработкой разных типов бирж
                balance = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.fetch_balance,
                    balance_params
                )
                
                # УНИВЕРСАЛЬНАЯ ОБРАБОТКА БАЛАНСА С ДЕТАЛЬНЫМ ЛОГИРОВАНИЕМ
                logger.info(f"📊 СЫРОЙ ОТВЕТ ОТ {exchange_name}: {self._safe_balance_log(balance)}")
                
                usdt_balance = await self._parse_universal_balance(exchange_name, balance)
                
                free_balance = usdt_balance.get('free', 0)
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'fetch_balance', 'balance', 
                                      {'free_balance': free_balance, 'parsed_balance': usdt_balance}, 
                                      start_time)
                
                # Логируем баланс
                if float(free_balance) == 0:
                    logger.info(f"💰 ФЬЮЧЕРСНЫЙ баланс {exchange_name}: 0.00 USDT")
                else:
                    logger.info(f"💰 ФЬЮЧЕРСНЫЙ баланс {exchange_name}: {free_balance:.2f} USDT")
                    
                return usdt_balance
                
            except ccxt.NetworkError as e:
                error_msg = f"Сетевая ошибка: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_balance', 'balance', None, start_time, error_msg)
                logger.error(f"🌐 Сетевая ошибка при запросе баланса {exchange_name} (попытка {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'fetch_balance', 'balance', attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except ccxt.ExchangeError as e:
                error_msg = f"Ошибка биржи: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_balance', 'balance', None, start_time, error_msg)
                logger.error(f"🏦 Ошибка биржи {exchange_name} (попытка {attempt + 1}/{max_retries}): {type(e).__name__}: {str(e)}")
                
                # ДЕТАЛЬНАЯ ДИАГНОСТИКА ДЛЯ MEXC
                if exchange_name == 'mexc':
                    logger.error(f"🔧 ДИАГНОСТИКА MEXC: Проверьте:")
                    logger.error(f"   - Доступ к фьючерсному трейдингу на MEXC")
                    logger.error(f"   - API ключи с правами на фьючерсы")
                    logger.error(f"   - Тип аккаунта (возможно нужен основной аккаунт)")
                
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'fetch_balance', 'balance', attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except ccxt.AuthenticationError as e:
                error_msg = f"Ошибка аутентификации: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_balance', 'balance', None, start_time, error_msg)
                logger.error(f"🔑 Ошибка аутентификации {exchange_name}: {str(e)}")
                return default_balance
            except Exception as e:
                error_msg = f"Неизвестная ошибка: {type(e).__name__}: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_balance', 'balance', None, start_time, error_msg)
                logger.error(f"❌ Неизвестная ошибка {exchange_name} (попытка {attempt + 1}/{max_retries}): {type(e).__name__}: {str(e)}")
                
                # ДОБАВЛЯЕМ TRACEBACK ДЛЯ ДИАГНОСТИКИ
                import traceback
                logger.error(f"📋 Трассировка ошибки: {traceback.format_exc()}")
                
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'fetch_balance', 'balance', attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        logger.error(f"🚫 Не удалось получить фьючерсный баланс с {exchange_name} после {max_retries} попыток")
        return default_balance

    def _get_balance_params(self, exchange_name: str) -> Dict:
        """ИСПРАВЛЕННЫЕ параметры для запроса баланса фьючерсов"""
        params = {}
        
        if exchange_name == 'bybit':
            # Bybit: используем unified для unified account (обязательно)
            params = {'type': 'unified'}  # Bybit требует UNIFIED account type
        elif exchange_name == 'gate':
            # Gate.io: фьючерсы с USDT расчетом
            params = {
                'type': 'swap',  # Используем 'swap' для Gate.io фьючерсов
                'settle': 'USDT'
            }
        elif exchange_name == 'mexc':
            params = {'type': 'swap'}
        elif exchange_name == 'bingx':
            params = {'type': 'swap'}
            
        return params

    def _safe_balance_log(self, balance: Any) -> str:
        """Безопасное логирование баланса с улучшенной информацией"""
        if not balance:
            return "None"
        
        try:
            if isinstance(balance, dict):
                # Для Bybit выводим больше информации
                if 'info' in balance and isinstance(balance['info'], dict):
                    info = balance['info']
                    if 'result' in info and 'list' in info['result'] and info['result']['list']:
                        account = info['result']['list'][0]
                        safe_info = {
                            'availableBalance': account.get('availableBalance'),
                            'walletBalance': account.get('walletBalance'), 
                            'totalWalletBalance': account.get('totalWalletBalance'),
                            'totalEquity': account.get('totalEquity')
                        }
                        return f"Bybit detailed: {safe_info}"
                
                # Стандартное логирование для других бирж
                safe_balance = {}
                for key, value in balance.items():
                    if key in ['info', 'free', 'used', 'total', 'USDT']:
                        if isinstance(value, dict):
                            safe_balance[key] = {k: '***' if 'secret' in k.lower() or 'key' in k.lower() else v 
                                               for k, v in value.items()}
                        else:
                            safe_balance[key] = value
                
                return str(safe_balance)[:1000]
            else:
                return str(balance)[:500]
        except Exception:
            return "Error logging balance"

    async def _parse_universal_balance(self, exchange_name: str, balance: Dict) -> Dict:
        """Универсальный парсинг баланса для всех бирж с улучшенной диагностикой"""
        default_balance = {'free': 0.0, 'used': 0.0, 'total': 0.0}
        
        try:
            # ПРИОРИТЕТ 1: Специальная обработка для Bybit (до стандартной структуры CCXT)
            # Это важно, т.к. стандартная структура может вернуть free=0 даже когда есть баланс
            if exchange_name == 'bybit' and balance and 'info' in balance:
                info = balance['info']
                logger.debug(f"🔍 BYBIT balance info keys: {list(info.keys()) if isinstance(info, dict) else 'not dict'}")
                
                # Поиск в структуре ответа Bybit
                if isinstance(info, dict):
                    # Вариант 1: Unified account structure через result.list
                    if 'result' in info:
                        result_data = info['result']
                        if isinstance(result_data, dict):
                            # Unified account structure
                            if 'list' in result_data and result_data['list']:
                                account = result_data['list'][0]
                                logger.debug(f"📊 BYBIT account keys: {list(account.keys())}")
                    
                                # Для unified account нужно использовать другие поля
                                # Попробуем найти доступный баланс в разных полях
                                available = None
                                
                                # Варианты для available balance:
                                # 1. availableBalance (может быть None для unified)
                                # 2. crossMarginAvailable (для cross margin)
                                # 3. accountMargin (для isolated margin)
                                # 4. totalWalletBalance - totalUsedBalance
                                
                                available_balance = account.get('availableBalance')
                                if available_balance is not None:
                                    available = float(available_balance)
                                else:
                                    # Пробуем crossMarginAvailable
                                    cross_margin_available = account.get('crossMarginAvailable')
                                    if cross_margin_available is not None:
                                        available = float(cross_margin_available)
                                    else:
                                        # Пробуем вычислить: total - used
                                        total_wallet_str = account.get('totalWalletBalance')
                                        total_used_str = account.get('totalUsedBalance') or account.get('totalUsedMargin') or '0'
                                        
                                        if total_wallet_str:
                                            try:
                                                total_wallet = float(total_wallet_str)
                                                total_used = float(total_used_str) if total_used_str else 0.0
                                                available = max(0, total_wallet - total_used)
                                                logger.debug(f"📊 Bybit: вычислен available = {total_wallet} - {total_used} = {available}")
                                            except (ValueError, TypeError):
                                                pass
                                
                                # Если available все еще None, используем totalWalletBalance как available
                                # Это нормально для unified account когда нет открытых позиций
                                if available is None:
                                    total_wallet_str = account.get('totalWalletBalance')
                                    if total_wallet_str:
                                        try:
                                            available = float(total_wallet_str)
                                            logger.debug(f"📊 Bybit: используем totalWalletBalance как available (нет открытых позиций): {available}")
                                        except (ValueError, TypeError):
                                            available = 0.0
                                    else:
                                        # Последняя попытка - используем totalEquity
                                        total_equity_str = account.get('totalEquity')
                                        if total_equity_str:
                                            try:
                                                available = float(total_equity_str)
                                                logger.debug(f"📊 Bybit: используем totalEquity как available: {available}")
                                            except (ValueError, TypeError):
                                                available = 0.0
                                        else:
                                            available = 0.0
                                
                                # Общий баланс кошелька
                                total_wallet_str = account.get('totalWalletBalance', '0')
                                total_equity_str = account.get('totalEquity', total_wallet_str)
                                
                                try:
                                    total_wallet = float(total_wallet_str) if total_wallet_str else 0.0
                                    total_equity = float(total_equity_str) if total_equity_str else total_wallet
                                except (ValueError, TypeError):
                                    total_wallet = 0.0
                                    total_equity = 0.0
                                
                                # Используем totalEquity или totalWalletBalance как total
                                total = total_equity if total_equity > 0 else total_wallet
                                
                                # Used = total - free (available)
                                used = max(0, total - available)
                                
                result = {
                                    'free': available,
                                    'used': used,
                                    'total': total
                                }
                                logger.info(f"✅ Bybit баланс (unified): free={available:.2f}, used={used:.2f}, total={total:.2f}")
                return result
            
                # Прямой доступ к балансу в результате (если нет list)
                if 'availableBalance' in result_data:
                    available_str = result_data.get('availableBalance')
                    total_wallet_str = result_data.get('totalWalletBalance', '0')
                    total_equity_str = result_data.get('totalEquity', total_wallet_str)
                    
                    try:
                        available = float(available_str) if available_str else 0.0
                        total = float(total_equity_str) if total_equity_str else float(total_wallet_str) if total_wallet_str else 0.0
                        used = max(0, total - available)
                    except (ValueError, TypeError):
                        available = 0.0
                        total = 0.0
                        used = 0.0
                    
                    result = {'free': available, 'used': used, 'total': total}
                    logger.info(f"✅ Bybit баланс (direct): free={available:.2f}, used={used:.2f}, total={total:.2f}")
                return result
            
            # ПРИОРИТЕТ 2: Специальная обработка для Gate.io (до стандартной структуры CCXT)
            if exchange_name == 'gate' and balance:
                # Gate.io может возвращать баланс в списке info
                if 'info' in balance:
                info = balance['info']
                    
                    # Вариант 1: info - это список (реальный формат Gate.io)
                    if isinstance(info, list) and len(info) > 0:
                        gate_account = info[0]
                        logger.debug(f"📊 Gate.io account keys: {list(gate_account.keys())[:15]}")
                        
                        # Извлекаем баланс из структуры Gate.io
                        available_str = gate_account.get('available') or gate_account.get('cross_available') or '0'
                        total_str = gate_account.get('total') or gate_account.get('cross_margin_balance') or available_str
                        
                        try:
                            available = float(available_str) if available_str else 0.0
                            total = float(total_str) if total_str else available
                            used = max(0, total - available)
                            
                            result = {'free': available, 'used': used, 'total': total}
                            logger.info(f"✅ Gate баланс (list format): free={available:.2f}, used={used:.2f}, total={total:.2f}")
                    return result
                        except (ValueError, TypeError) as e:
                            logger.debug(f"⚠️ Ошибка парсинга Gate.io списка: {e}")
                    
                    # Вариант 2: info - это словарь
                    elif isinstance(info, dict):
                        if 'available' in info:
                            available = float(info.get('available', 0) or 0)
                            total = float(info.get('total', 0) or available)
                            used = max(0, total - available)
                            result = {'free': available, 'used': used, 'total': total}
                            logger.info(f"✅ Gate баланс (info dict): free={available:.2f}, used={used:.2f}, total={total:.2f}")
                    return result
            
            # ПРИОРИТЕТ 3: Стандартная структура CCXT (после специальных обработок)
            if balance and isinstance(balance, dict):
                # Проверяем стандартную структуру CCXT
                if 'total' in balance and isinstance(balance['total'], dict):
                    if 'USDT' in balance['total']:
                        free = float(balance.get('free', {}).get('USDT', 0) or 0)
                        used = float(balance.get('used', {}).get('USDT', 0) or 0)
                        total = float(balance.get('total', {}).get('USDT', 0) or 0)
                        
                        # Для Bybit: если free=0 но total>0, это может быть проблема парсинга
                        # Но мы уже обработали Bybit выше, так что это для других бирж
                        result = {'free': free, 'used': used, 'total': total}
                        logger.info(f"📊 {exchange_name}: баланс через стандартную структуру CCXT: free={free:.2f}, used={used:.2f}, total={total:.2f}")
                        return result
                
                # Прямой доступ к USDT
                if 'USDT' in balance and isinstance(balance['USDT'], dict):
                    free = float(balance['USDT'].get('free', 0) or 0)
                    used = float(balance['USDT'].get('used', 0) or 0)
                    total = float(balance['USDT'].get('total', 0) or 0)
                    result = {'free': free, 'used': used, 'total': total}
                    logger.info(f"📊 {exchange_name}: баланс через прямой USDT доступ: free={free:.2f}, used={used:.2f}, total={total:.2f}")
                    return result
            
            # СПОСОБ 4: Для MEXC - специальная обработка
            if exchange_name == 'mexc':
                result = await self._parse_mexc_balance(balance)
                if result['total'] > 0:
                    logger.info(f"✅ {exchange_name}: баланс через MEXC-специфичный парсинг: {result}")
                    return result
            
            # СПОСОБ 5: Поиск любого ключа с USDT в верхнем уровне
            for key in balance.keys():
                if key and 'USDT' in str(key).upper():
                    if isinstance(balance[key], dict):
                        usdt_data = balance[key]
                        free = float(usdt_data.get('free', 0) or 0)
                        used = float(usdt_data.get('used', 0) or 0)
                        total = float(usdt_data.get('total', 0) or 0)
                        if total > 0 or free > 0:
                            result = {'free': free, 'used': used, 'total': total}
                            logger.info(f"✅ {exchange_name}: баланс через поиск ключа {key}: free={free:.2f}, used={used:.2f}, total={total:.2f}")
                    return result
            
            # Логируем структуру баланса для отладки
            logger.warning(f"⚠️ Неизвестная структура баланса для {exchange_name}")
            logger.debug(f"   Тип balance: {type(balance)}")
            if isinstance(balance, dict):
                logger.debug(f"   Ключи баланса: {list(balance.keys())[:10]}")
                if 'info' in balance:
                    logger.debug(f"   Тип info: {type(balance['info'])}")
                    if isinstance(balance['info'], dict):
                        logger.debug(f"   Ключи info: {list(balance['info'].keys())[:10]}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга баланса для {exchange_name}: {e}")
            import traceback
            logger.error(f"📋 Трассировка парсинга: {traceback.format_exc()}")
        
        return default_balance

    async def _parse_mexc_balance(self, balance: Dict) -> Dict:
        """Специальный парсинг для MEXC баланса"""
        default_balance = {'free': 0.0, 'used': 0.0, 'total': 0.0}
        
        try:
            # MEXC может возвращать баланс в разных структурах
            if 'data' in balance and isinstance(balance['data'], dict):
                data = balance['data']
                if 'availableBalance' in data:
                    # Структура для фьючерсов MEXC
                    available = float(data.get('availableBalance', 0) or 0)
                    total = float(data.get('totalBalance', 0) or available)
                    used = total - available
                    return {'free': available, 'used': used, 'total': total}
            
            # Дополнительные попытки для MEXC
            for key in ['available', 'avail', 'free', 'walletBalance']:
                if key in balance:
                    value = float(balance[key] or 0)
                    if value > 0:
                        return {'free': value, 'used': 0.0, 'total': value}
                        
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга MEXC баланса: {e}")
            
        return default_balance

    async def close_all_positions(self, exchange_name: str, symbol: str) -> bool:
        """Закрывает все позиции по символу"""
        if exchange_name not in self.exchanges:
            return False
            
        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'close_all_positions', symbol)
                
                # Асинхронный вызов
                positions = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.fetch_positions,
                    [symbol]
                )
                
                for position in positions:
                    if position['contracts'] > 0:
                        side = 'sell' if position['side'] == 'long' else 'buy'
                        await self.create_limit_order(
                            exchange_name,
                            symbol,
                            side,
                            position['contracts'],
                            position['entryPrice']
                        )
                        logger.info(f"🆘 Закрываю позицию {position['side']} {position['contracts']} {symbol} на {exchange_name}")
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'close_all_positions', symbol, 
                                      {'closed_positions': len(positions)}, 
                                      start_time)
                
                return True
                
            except Exception as e:
                error_msg = f"Ошибка закрытия позиций: {str(e)}"
                self.log_order_response(exchange_name, 'close_all_positions', symbol, None, start_time, error_msg)
                logger.error(f"❌ Ошибка закрытия позиций на {exchange_name} (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'close_all_positions', symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return False
    
    async def set_margin_mode(self, exchange_name: str, symbol: str, margin_mode: str = 'isolated') -> bool:
        """Устанавливает тип маржи для символа"""
        if exchange_name not in self.exchanges:
            return False

        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'set_margin_mode', symbol, {'margin_mode': margin_mode})
                
                # Устанавливаем тип маржи
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.set_margin_mode,
                    margin_mode,
                    symbol,
                    {'leverage': LEVERAGE}
                )
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'set_margin_mode', symbol, 
                                      {'result': 'success'}, 
                                      start_time)
                
                logger.info(f"✅ Установлен режим маржи {margin_mode} для {symbol} на {exchange_name}")
                return True
            except Exception as e:
                error_msg = f"Ошибка установки режима маржи: {str(e)}"
                self.log_order_response(exchange_name, 'set_margin_mode', symbol, None, start_time, error_msg)
                logger.warning(f"⚠️ Не удалось установить режим маржи для {symbol} на {exchange_name} (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'set_margin_mode', symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return False

    async def set_leverage(self, exchange_name: str, symbol: str, leverage: int = LEVERAGE) -> bool:
        """Устанавливает плечо для символа"""
        if exchange_name not in self.exchanges:
            return False

        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'set_leverage', symbol, {'leverage': leverage})
                
                # Устанавливаем плечо
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    exchange.set_leverage,
                    leverage,
                    symbol
                )
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'set_leverage', symbol, 
                                      {'result': 'success'}, 
                                      start_time)
                
                logger.info(f"✅ Установлено плечо {leverage}x для {symbol} на {exchange_name}")
                return True
            except Exception as e:
                error_msg = f"Ошибка установки плеча: {str(e)}"
                self.log_order_response(exchange_name, 'set_leverage', symbol, None, start_time, error_msg)
                logger.warning(f"⚠️ Не удалось установить плечо для {symbol} на {exchange_name} (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'set_leverage', symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return False

    async def fetch_positions(self, exchange_name: str, symbols: List[str] = None) -> List[Dict]:
        """Получает позиции с биржи с детальным логированием"""
        if exchange_name not in self.exchanges:
            logger.error(f"🚫 Биржа {exchange_name} не инициализирована для запроса позиций")
            return []

        exchange = self.exchanges[exchange_name]
        max_retries = 3
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all')
                
                # Асинхронный вызов
                if symbols:
                    positions = await asyncio.get_event_loop().run_in_executor(
                        None,
                        exchange.fetch_positions,
                        symbols
                    )
                else:
                    positions = await asyncio.get_event_loop().run_in_executor(
                        None,
                        exchange.fetch_positions
                    )
                
                # Фильтруем только открытые позиции
                open_positions = []
                for position in positions:
                    if position and 'contracts' in position and float(position.get('contracts', 0)) > 0:
                        open_positions.append(position)
                
                # Логируем успешный ответ
                self.log_order_response(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', 
                                      {'open_positions': len(open_positions)}, 
                                      start_time)
                
                logger.debug(f"📊 Получено {len(open_positions)} открытых позиций с {exchange_name}")
                return open_positions
                
            except ccxt.NetworkError as e:
                error_msg = f"Сетевая ошибка: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', None, start_time, error_msg)
                logger.error(f"🌐 Сетевая ошибка при запросе позиций {exchange_name} (попытка {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except ccxt.ExchangeError as e:
                error_msg = f"Ошибка биржи: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', None, start_time, error_msg)
                logger.error(f"🏦 Ошибка биржи при запросе позиций {exchange_name} (попытка {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
            except Exception as e:
                error_msg = f"Неизвестная ошибка: {type(e).__name__}: {str(e)}"
                self.log_order_response(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', None, start_time, error_msg)
                logger.error(f"❌ Неизвестная ошибка при запросе позиций {exchange_name} (попытка {attempt + 1}/{max_retries}): {type(e).__name__}: {str(e)}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'fetch_positions', str(symbols) if symbols else 'all', attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return []

    async def check_position_exists(self, exchange_name: str, symbol: str, side: str) -> bool:
        """Проверяет, существует ли позиция по символу и направлению"""
        max_retries = 2
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'check_position_exists', symbol, {'side': side})
                
                positions = await self.fetch_positions(exchange_name, [symbol])
                
                for position in positions:
                    if (position['symbol'] == symbol and 
                        position['side'] == side and 
                        float(position['contracts']) > 0):
                        
                        # Логируем успешный ответ
                        self.log_order_response(exchange_name, 'check_position_exists', symbol, 
                                              {'exists': True}, 
                                              start_time)
                        return True
                
                # Логируем успешный ответ (позиция не найдена)
                self.log_order_response(exchange_name, 'check_position_exists', symbol, 
                                      {'exists': False}, 
                                      start_time)
                return False
                
            except Exception as e:
                error_msg = f"Ошибка проверки позиции: {str(e)}"
                self.log_order_response(exchange_name, 'check_position_exists', symbol, None, start_time, error_msg)
                logger.error(f"❌ Ошибка проверки позиции на {exchange_name} (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'check_position_exists', symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return False

    async def get_exchange_limits(self, exchange_name: str, symbol: str) -> Dict:
        """Получает реальные лимиты и настройки биржи"""
        if exchange_name not in self.exchanges:
            return {}

        exchange = self.exchanges[exchange_name]
        max_retries = 2
        
        for attempt in range(max_retries):
            start_time = None
            try:
                # Логируем запрос
                start_time = self.log_order_request(exchange_name, 'get_exchange_limits', symbol)
                
                market_info = await asyncio.get_event_loop().run_in_executor(
                    None, exchange.market, symbol
                )
                
                if market_info:
                    limits = {
                        'min_quantity': market_info.get('limits', {}).get('amount', {}).get('min', 0),
                        'max_quantity': market_info.get('limits', {}).get('amount', {}).get('max', 0),
                        'quantity_step': market_info.get('precision', {}).get('amount', 0.001),
                        'price_precision': market_info.get('precision', {}).get('price', 0.01),
                        'min_notional': market_info.get('limits', {}).get('cost', {}).get('min', 0)
                    }
                    
                    # Логируем успешный ответ
                    self.log_order_response(exchange_name, 'get_exchange_limits', symbol, 
                                          limits, 
                                          start_time)
                    
                    return limits
            except Exception as e:
                error_msg = f"Ошибка получения лимитов: {str(e)}"
                self.log_order_response(exchange_name, 'get_exchange_limits', symbol, None, start_time, error_msg)
                logger.warning(f"⚠️ Не удалось получить лимиты для {symbol} на {exchange_name} (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    network_logger.log_retry(exchange_name, 'get_exchange_limits', symbol, attempt + 1, max_retries, 2 ** attempt)
                    await asyncio.sleep(2 ** attempt)
                continue
        
        return {}

    def get_active_orders_count(self) -> int:
        """Возвращает количество активных ордеров"""
        return len([o for o in self.active_orders.values() if o['status'] == 'open'])