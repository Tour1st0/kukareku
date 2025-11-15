import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
import json
import os

class ExchangeNetworkLogger:
    def __init__(self):
        self.logger = logging.getLogger('exchange_network')
        self.logger.setLevel(logging.DEBUG)
        
        # Убедимся, что нет дублирующихся обработчиков
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Настройка формата с БОЛЬШЕЙ ИНФОРМАЦИЕЙ
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Файловый обработчик для детальных логов
        file_handler = logging.FileHandler('exchange_network_detailed.log', encoding='utf-8', mode='w')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Также сохраняем в старый файл для обратной совместимости
        legacy_file_handler = logging.FileHandler('exchange_network.log', encoding='utf-8', mode='w')
        legacy_file_handler.setLevel(logging.DEBUG)
        legacy_file_handler.setFormatter(formatter)
        self.logger.addHandler(legacy_file_handler)
        
        # Консольный обработчик для ошибок
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        self.logger.info("🔧 ExchangeNetworkLogger инициализирован с ДЕТАЛЬНЫМ логированием")

    def log_request(self, exchange: str, method: str, symbol: str, params: Dict = None):
        """Логирует запрос с ДЕТАЛЬНЫМИ параметрами"""
        try:
            timestamp = datetime.now().isoformat()
            message = f"📤 ЗАПРОС | {exchange.upper()} | {method} {symbol}"
            message += f"\nВременная метка: {timestamp}"
            
            if params:
                message += f"\nПараметры: {json.dumps(params, indent=2, ensure_ascii=False)}"
            else:
                message += f"\nПараметры: {{}}"
            
            # Добавляем разделитель для читаемости
            message += "\n" + "─" * 80
            
            self.logger.debug(message)
            return time.time()  # Возвращаем время начала для расчета длительности
            
        except Exception as e:
            self.logger.error(f"Ошибка логирования запроса: {e}")
            return time.time()

    def log_response(self, exchange: str, method: str, symbol: str, 
                    status_code: int, data: Any, duration: float = None,
                    headers: Dict = None):
        """Логирует ответ с ДЕТАЛЬНЫМИ данными"""
        try:
            timestamp = datetime.now().isoformat()
            status_emoji = "✅" if status_code == 200 else "⚠️" if 400 <= status_code < 500 else "❌"
            
            message = f"📥 ОТВЕТ | {exchange.upper()} | {method} {symbol} | {status_emoji} {status_code}"
            message += f"\nВременная метка: {timestamp}"
            
            if data:
                # Логируем структуру ответа для диагностики
                if isinstance(data, dict):
                    message += f"\nСтруктура ответа: {list(data.keys())}"
                    
                    # Безопасное логирование данных (убираем чувствительную информацию)
                    safe_data = self._sanitize_data(data)
                    message += f"\nДанные: {json.dumps(safe_data, indent=2, ensure_ascii=False, default=str)}"
                else:
                    safe_data = self._sanitize_data(data)
                    message += f"\nДанные: {str(safe_data)}"
            else:
                message += f"\nДанные: None"
            
            if headers:
                safe_headers = {k: '***' if any(sensitive in k.lower() for sensitive in ['key', 'secret', 'token', 'signature']) else v 
                              for k, v in headers.items()}
                message += f"\nЗаголовки: {json.dumps(safe_headers, indent=2, ensure_ascii=False)}"
            else:
                message += f"\nЗаголовки: {{}}"
            
            if duration is not None:
                message += f"\nВремя выполнения: {duration:.2f} мс"
                
            # Добавляем разделитель для читаемости
            message += "\n" + "─" * 80
                
            self.logger.debug(message)
            
        except Exception as e:
            self.logger.error(f"Ошибка логирования ответа: {e}")

    def log_error(self, exchange: str, method: str, symbol: str, 
                 error_type: str, error_message: str, response_body: str = None,
                 request_params: Dict = None, http_status: int = None):
        """Логирует ошибку с ДЕТАЛЬНОЙ информацией"""
        try:
            timestamp = datetime.now().isoformat()
            message = f"💥 ОШИБКА | {exchange.upper()} | {method} {symbol} | {error_type}"
            message += f"\nВременная метка: {timestamp}"
            message += f"\nСообщение: {error_message}"
            
            if http_status:
                message += f"\nHTTP статус: {http_status}"
            
            if response_body:
                safe_body = self._sanitize_data(response_body)
                message += f"\nТело ответа: {safe_body}"
                
            if request_params:
                safe_params = self._sanitize_data(request_params)
                message += f"\nПараметры запроса: {json.dumps(safe_params, indent=2, ensure_ascii=False)}"
                
            # Добавляем диагностическую информацию
            message += f"\nДиагностика:"
            message += f"\n- Время: {datetime.now().isoformat()}"
            message += f"\n- Биржа: {exchange}"
            message += f"\n- Метод: {method}"
            message += f"\n- Символ: {symbol}"
            message += f"\n- Тип ошибки: {error_type}"
            
            # Добавляем рекомендации по устранению
            message += f"\nРекомендации:"
            if "mexc" in exchange.lower():
                message += f"\n- Проверьте доступ к фьючерсному трейдингу на MEXC"
                message += f"\n- Убедитесь, что API ключи имеют права на фьючерсы"
                message += f"\n- Проверьте тип аккаунта (возможно нужен основной аккаунт)"
            elif "gate" in exchange.lower():
                message += f"\n- Проверьте сетевое соединение с Gate.io"
                message += f"\n- Возможны временные проблемы с API Gate.io"
            elif "authentication" in error_type.lower():
                message += f"\n- Проверьте корректность API ключей и секретов"
                message += f"\n- Убедитесь, что API ключи не истекли"
                message += f"\n- Проверьте права доступа API ключей"
            
            # Добавляем разделитель для читаемости
            message += "\n" + "─" * 80
            
            self.logger.error(message)
            
        except Exception as e:
            self.logger.error(f"Ошибка логирования ошибки: {e}")

    def log_retry(self, exchange: str, method: str, symbol: str, 
                 attempt: int, max_attempts: int, delay: float, last_error: str = None):
        """Логирует повторную попытку с дополнительной информацией"""
        try:
            timestamp = datetime.now().isoformat()
            message = (f"🔄 ПОВТОР | {exchange.upper()} | {method} {symbol} | "
                      f"Попытка {attempt}/{max_attempts} | Задержка: {delay:.0f}с")
            message += f"\nВременная метка: {timestamp}"
            
            if last_error:
                message += f"\nПредыдущая ошибка: {last_error}"
                
            # Добавляем разделитель для читаемости
            message += "\n" + "─" * 80
            
            self.logger.warning(message)
            
        except Exception as e:
            self.logger.error(f"Ошибка логирования повтора: {e}")

    def _sanitize_data(self, data: Any) -> Any:
        """Очищает данные от конфиденциальной информации"""
        try:
            if isinstance(data, str):
                # Пытаемся распарсить JSON строку
                try:
                    parsed = json.loads(data)
                    return self._sanitize_data(parsed)
                except:
                    # Если не JSON, проверяем на чувствительные данные
                    sensitive_keywords = ['api_key', 'api_secret', 'secret', 'key', 'signature', 'token', 'password']
                    if any(keyword in data.lower() for keyword in sensitive_keywords):
                        return "***SENSITIVE_DATA***"
                    return data[:1000]  # Ограничиваем длину
                    
            elif isinstance(data, dict):
                sanitized = {}
                for key, value in data.items():
                    # Проверяем ключи на чувствительность
                    if any(sensitive in str(key).lower() for sensitive in ['key', 'secret', 'token', 'password', 'signature']):
                        sanitized[key] = '***'
                    else:
                        sanitized[key] = self._sanitize_data(value)
                return sanitized
                
            elif isinstance(data, list):
                return [self._sanitize_data(item) for item in data]
                
            else:
                return data
                
        except Exception as e:
            return f"Error sanitizing data: {str(e)}"

    def get_network_logs(self, lines: int = 10) -> str:
        """Возвращает последние логи сети из детального файла"""
        try:
            log_file = 'exchange_network_detailed.log'
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    all_lines = f.readlines()
                    return ''.join(all_lines[-lines:])
            else:
                return "Файл детальных логов не найден"
        except Exception as e:
            return f"Ошибка чтения логов: {e}"

    def log_exchange_initialization(self, exchange: str, status: str, details: str = ""):
        """Логирует инициализацию биржи"""
        try:
            timestamp = datetime.now().isoformat()
            status_emoji = "✅" if status == "success" else "❌" if status == "error" else "⚠️"
            
            message = f"🏦 ИНИЦИАЛИЗАЦИЯ БИРЖИ | {exchange.upper()} | {status_emoji} {status.upper()}"
            message += f"\nВременная метка: {timestamp}"
            
            if details:
                message += f"\nДетали: {details}"
                
            # Добавляем разделитель для читаемости
            message += "\n" + "─" * 80
                
            if status == "success":
                self.logger.info(message)
            elif status == "error":
                self.logger.error(message)
            else:
                self.logger.warning(message)
                
        except Exception as e:
            self.logger.error(f"Ошибка логирования инициализации: {e}")

    def log_rate_limit(self, exchange: str, method: str, symbol: str, 
                      retry_after: float = None, limit_info: Dict = None):
        """Логирует информацию о лимитах запросов"""
        try:
            timestamp = datetime.now().isoformat()
            message = f"⏰ ЛИМИТ ЗАПРОСОВ | {exchange.upper()} | {method} {symbol}"
            message += f"\nВременная метка: {timestamp}"
            
            if retry_after:
                message += f"\nПовтор через: {retry_after} сек"
                
            if limit_info:
                message += f"\nИнформация о лимитах: {json.dumps(limit_info, indent=2, ensure_ascii=False)}"
                
            # Добавляем разделитель для читаемости
            message += "\n" + "─" * 80
                
            self.logger.warning(message)
            
        except Exception as e:
            self.logger.error(f"Ошибка логирования лимитов: {e}")

# Глобальный экземпляр логгера
network_logger = ExchangeNetworkLogger()