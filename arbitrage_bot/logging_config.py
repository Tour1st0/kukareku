import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
import threading
from datetime import datetime, timedelta

class SmartLogging:
    def __init__(self):
        self.log_file = 'arbitrage_bot_current.log'
        self.rotation_hours = 3
        self.setup_logging()
        
    def setup_logging(self):
        """Настройка логирования с автоматической ротацией"""
        if os.path.exists(self.log_file):
            try:
                os.remove(self.log_file)
                print(f"🗑️ Удален старый файл логов: {self.log_file}")
            except Exception as e:
                print(f"⚠️ Не удалось удалить старый файл логов: {e}")
        
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)
        
        file_handler = TimedRotatingFileHandler(
            filename=self.log_file,
            when='H',
            interval=self.rotation_hours,
            backupCount=0,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
        logging.getLogger('telethon').setLevel(logging.WARNING)
        logging.getLogger('ccxt').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        
        print(f"✅ Логирование настроено. Файл: {self.log_file}, ротация каждые {self.rotation_hours} часа")
        
        self.start_log_monitor()
    
    def start_log_monitor(self):
        """Запускает мониторинг размера лог-файла"""
        def monitor():
            while True:
                try:
                    if os.path.exists(self.log_file):
                        size = os.path.getsize(self.log_file) / 1024 / 1024
                        if size > 10:
                            logging.warning(f"📏 Файл логов большой: {size:.1f} МБ. Следующая ротация через 3 часа.")
                except Exception as e:
                    print(f"⚠️ Ошибка мониторинга логов: {e}")
                
                time.sleep(300)
        
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
    
    def force_rotation(self):
        """Принудительная ротация логов"""
        try:
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                if isinstance(handler, TimedRotatingFileHandler):
                    handler.doRollover()
                    logging.info("🔄 Принудительная ротация логов выполнена")
                    break
        except Exception as e:
            print(f"❌ Ошибка принудительной ротации: {e}")

log_manager = SmartLogging()

def setup_logging():
    return log_manager

def get_logger(name):
    return logging.getLogger(name)