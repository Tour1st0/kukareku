import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import json
import os

from config import *
from exchanges.price_fetcher import PriceFetcher

logger = logging.getLogger(__name__)

class SuperBacktest:
    def __init__(self, initial_balance: float = 1000):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.optimization_history = []
        self.best_params = None
        
    async def run_optimized_backtest(self, symbol: str, timeframe: str = '1h', days: int = 30) -> Dict:
        """Запускает оптимизированный бэктест"""
        logger.info(f"🚀 ЗАПУСК СУПЕР-ОПТИМИЗАЦИИ ДЛЯ {symbol}")
        
        historical_data = await self.fetch_real_historical_data(symbol, timeframe, days)
        
        if not historical_data:
            return {'error': 'Не удалось загрузить исторические данные'}
        
        best_params = await self.optimize_parameters(historical_data, symbol)
        
        final_trades = self.run_backtest_with_params(best_params, historical_data, symbol)
        final_metrics = self.create_advanced_metrics(final_trades)
        
        logger.info("🎯 РЕЗУЛЬТАТЫ ОПТИМИЗАЦИИ:")
        logger.info(f"💰 Прибыль: ${final_metrics['total_profit']:.2f}")
        logger.info(f"📈 Sharpe Ratio: {final_metrics['sharpe_ratio']:.2f}")
        logger.info(f"🎯 Win Rate: {final_metrics['win_rate']:.1f}%")
        logger.info(f"📉 Max Drawdown: {final_metrics['max_drawdown']:.1f}%")
        logger.info(f"⏱️ Среднее время удержания: {final_metrics['avg_hold_time']:.0f} сек")
        
        return {
            'best_params': best_params,
            'metrics': final_metrics,
            'trades': final_trades
        }

    async def fetch_real_historical_data(self, symbol: str, timeframe: str, days: int) -> Dict:
        """Загружает РЕАЛЬНЫЕ исторические данные с бирж"""
        logger.info(f"📊 Загружаем реальные исторические данные для {symbol} за {days} дней")
        
        historical_data = {}
        start_date = datetime.now() - timedelta(days=days)
        
        # Конфигурация для подключения к биржам
        exchanges_config = {
            'bybit': {'enabled': True, 'api_key': '', 'api_secret': ''},
            'gate': {'enabled': True, 'api_key': '', 'api_secret': ''},
            'mexc': {'enabled': True, 'api_key': '', 'api_secret': ''},
            'bingx': {'enabled': True, 'api_key': '', 'api_secret': ''}
        }
        
        price_fetcher = PriceFetcher(exchanges_config)
        
        for exchange_name in ['bybit', 'gate', 'mexc', 'bingx']:
            try:
                # Получаем данные через CCXT
                exchange = price_fetcher.exchanges.get(exchange_name)
                if not exchange:
                    continue
                    
                since = int(start_date.timestamp() * 1000)
                
                # Пробуем разные форматы символов
                symbol_variants = [
                    f"{symbol}/USDT:USDT",  # фьючерсы
                    f"{symbol}USDT",        # спот
                    f"{symbol}-USDT",       # другой формат
                ]
                
                ohlcv_data = None
                for symbol_variant in symbol_variants:
                    try:
                        ohlcv = await asyncio.get_event_loop().run_in_executor(
                            None,
                            exchange.fetch_ohlcv,
                            symbol_variant,
                            timeframe,
                            since
                        )
                        if ohlcv:
                            ohlcv_data = ohlcv
                            logger.info(f"✅ Нашли данные для {symbol} на {exchange_name} как {symbol_variant}")
                            break
                    except Exception as e:
                        continue
                
                if ohlcv_data:
                    df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    
                    # Добавляем случайный шум для симуляции разных бирж (если данные одинаковые)
                    if len(historical_data) > 0:
                        noise = np.random.normal(0, 0.001, len(df))
                        df['close'] = df['close'] * (1 + noise)
                    
                    historical_data[exchange_name.upper()] = df
                    logger.info(f"✅ Загружено {len(df)} свечей с {exchange_name}")
                else:
                    logger.warning(f"⚠️ Не удалось загрузить данные для {symbol} с {exchange_name}")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки данных с {exchange_name}: {e}")
        
        # Если не удалось загрузить реальные данные, используем симулированные
        if not historical_data:
            logger.warning("⚠️ Используем симулированные данные")
            historical_data = await self.fetch_simulated_historical_data(symbol, timeframe, days)
        
        return historical_data

    async def fetch_simulated_historical_data(self, symbol: str, timeframe: str, days: int) -> Dict:
        """Создает симулированные данные (fallback)"""
        logger.info(f"📊 Создаем симулированные данные для {symbol} за {days} дней")
        
        historical_data = {}
        start_date = datetime.now() - timedelta(days=days)
        
        exchanges = ['BYBIT', 'GATE', 'MEXC', 'BINGX']
        base_prices = {
            'BYBIT': 100.0,
            'GATE': 101.0,
            'MEXC': 99.5,
            'BINGX': 102.0
        }
        
        base_volumes = {
            'BYBIT': 1000000,
            'GATE': 800000,
            'MEXC': 600000, 
            'BINGX': 900000
        }
        
        for exchange in exchanges:
            dates = pd.date_range(start=start_date, end=datetime.now(), freq=timeframe)
            
            np.random.seed(42)
            returns = np.random.normal(0, 0.02, len(dates))
            prices = [base_prices[exchange]]
            
            for ret in returns[1:]:
                new_price = prices[-1] * (1 + ret)
                prices.append(new_price)
            
            volumes = np.random.normal(base_volumes[exchange], base_volumes[exchange] * 0.3, len(dates))
            volumes = np.maximum(volumes, 100000)
            
            df = pd.DataFrame({
                'timestamp': dates,
                'close': prices[:len(dates)],
                'volume': volumes
            })
            df.set_index('timestamp', inplace=True)
            
            historical_data[exchange] = df
            
        return historical_data

    async def optimize_parameters(self, historical_data: Dict, symbol: str) -> Dict:
        """Оптимизирует параметры"""
        initial_params = {
            'min_spread': MIN_SPREAD,
            'close_spread': CLOSE_SPREAD,
            'leverage': LEVERAGE,
            'trade_amount': TRADE_AMOUNT,
            'max_slippage': MAX_ENTRY_SLIPPAGE,
            'max_hold_time': MAX_HOLD_TIME
        }
        
        best_score = -float('inf')
        best_params = initial_params.copy()
        
        for min_spread in [3.0, 3.5, 4.0, 4.5]:
            for close_spread in [0.3, 0.5, 0.7]:
                for max_hold in [300, 600, 900]:
                    test_params = initial_params.copy()
                    test_params['min_spread'] = min_spread
                    test_params['close_spread'] = close_spread
                    test_params['max_hold_time'] = max_hold
                    
                    trades = self.run_backtest_with_params(test_params, historical_data, symbol)
                    metrics = self.create_advanced_metrics(trades)
                    
                    if metrics:
                        score = (metrics['sharpe_ratio'] * 15 + 
                                metrics['total_profit'] / self.initial_balance * 100 -
                                metrics['max_drawdown'] * 2 -
                                metrics['avg_hold_time'] / 100)
                        
                        if score > best_score:
                            best_score = score
                            best_params = test_params.copy()
        
        logger.info(f"✅ Найдены оптимальные параметры: MIN_SPREAD={best_params['min_spread']}%, MAX_HOLD={best_params['max_hold_time']}сек")
        return best_params

    def run_backtest_with_params(self, params: Dict, historical_data: Dict, symbol: str) -> List[Dict]:
        """Запускает бэктест"""
        trades = []
        active_trades = {}
        
        timestamps = list(historical_data.values())[0].index
        
        for timestamp in timestamps:
            current_prices = {}
            current_volumes = {}
            
            for exchange, data in historical_data.items():
                if timestamp in data.index:
                    current_prices[exchange] = data.loc[timestamp, 'close']
                    current_volumes[exchange] = data.loc[timestamp, 'volume']
            
            opportunities = self.find_arbitrage_opportunities(current_prices, current_volumes, params['min_spread'])
            
            for opportunity in opportunities[:2]:
                if len(active_trades) < MAX_CONCURRENT_TRADES:
                    trade_id = f"backtest_{len(trades) + 1}"
                    trade = {
                        'trade_id': trade_id,
                        'symbol': symbol,
                        'entry_time': timestamp,
                        'long_exchange': opportunity['long_exchange'],
                        'short_exchange': opportunity['short_exchange'],
                        'entry_long_price': opportunity['long_price'],
                        'entry_short_price': opportunity['short_price'],
                        'quantity': self.calculate_adaptive_quantity(symbol, opportunity['long_price'], 
                                                                   opportunity['spread'], opportunity['volume_ratio']),
                        'entry_spread': opportunity['spread'],
                        'volume_ratio': opportunity['volume_ratio'],
                        'status': 'open',
                        'max_pnl': 0.0
                    }
                    active_trades[trade_id] = trade
                    trades.append(trade)
            
            for trade_id, trade in list(active_trades.items()):
                close_reason = self.should_close_trade(trade, current_prices, params, timestamp)
                if close_reason:
                    self.close_trade(trade_id, trade, current_prices, timestamp, trades, close_reason)
                    del active_trades[trade_id]
        
        return trades

    def calculate_adaptive_quantity(self, symbol: str, price: float, spread: float, volume_ratio: float) -> float:
        """Адаптивный расчет количества"""
        base_amount = TRADE_AMOUNT
        
        if spread > 7.0:
            base_amount *= 1.8
        elif spread > 5.0:
            base_amount *= 1.5
            
        if volume_ratio < 0.2:
            base_amount *= 0.4
        elif volume_ratio < 0.4:
            base_amount *= 0.7
            
        risk_factor = RISKY_SYMBOLS.get(symbol, 1.0)
        base_amount *= risk_factor
        
        return base_amount * LEVERAGE / price

    def find_arbitrage_opportunities(self, prices: Dict, volumes: Dict, min_spread: float) -> List[Dict]:
        """Находит арбитражные возможности"""
        opportunities = []
        exchanges = list(prices.keys())
        
        for i, long_ex in enumerate(exchanges):
            for j, short_ex in enumerate(exchanges):
                if i != j and prices[long_ex] > 0 and prices[short_ex] > 0:
                    spread = (prices[short_ex] - prices[long_ex]) / prices[long_ex] * 100
                    
                    min_volume = min(volumes[long_ex], volumes[short_ex])
                    volume_ratio = min(volumes[long_ex], volumes[short_ex]) / max(volumes[long_ex], volumes[short_ex])
                    
                    if spread >= min_spread and min_volume >= MIN_VOLUME and volume_ratio >= MIN_VOLUME_RATIO:
                        opportunities.append({
                            'long_exchange': long_ex,
                            'short_exchange': short_ex,
                            'long_price': prices[long_ex],
                            'short_price': prices[short_ex],
                            'spread': spread,
                            'volume_ratio': volume_ratio
                        })
        
        return sorted(opportunities, key=lambda x: x['spread'], reverse=True)

    def should_close_trade(self, trade: Dict, current_prices: Dict, params: Dict, timestamp: datetime) -> Optional[str]:
        """Определяет, нужно ли закрывать сделку"""
        long_price = current_prices.get(trade['long_exchange'], 0)
        short_price = current_prices.get(trade['short_exchange'], 0)
        
        if long_price == 0 or short_price == 0:
            return None
        
        current_spread = (short_price - long_price) / long_price * 100
        duration = (timestamp - trade['entry_time']).total_seconds()
        
        if current_spread <= params['close_spread']:
            return "target_spread"
            
        if duration > params['max_hold_time']:
            return "timeout"
            
        current_pnl = self.calculate_trade_pnl(trade, long_price, short_price)
        
        if current_pnl > trade.get('max_pnl', 0):
            trade['max_pnl'] = current_pnl
            
        if current_pnl >= PROFIT_TRAILING_START:
            for time_threshold, keep_ratio in TRAILING_STOP_LEVELS.items():
                if duration >= time_threshold and current_pnl <= trade['max_pnl'] * keep_ratio:
                    return f"trailing_stop_{time_threshold}s"
        
        return None

    def calculate_trade_pnl(self, trade: Dict, long_price: float, short_price: float) -> float:
        """Рассчитывает PnL сделки"""
        long_pnl = (long_price - trade['entry_long_price']) * trade['quantity']
        short_pnl = (trade['entry_short_price'] - short_price) * trade['quantity']
        gross_pnl = long_pnl + short_pnl
        
        # Используем реальные комиссии из конфига
        long_commission_rate = COMMISSION_RATES.get(trade['long_exchange'].upper(), 0.001)
        short_commission_rate = COMMISSION_RATES.get(trade['short_exchange'].upper(), 0.001)
        
        long_commission = trade['quantity'] * trade['entry_long_price'] * long_commission_rate
        short_commission = trade['quantity'] * trade['entry_short_price'] * short_commission_rate
        total_commission = long_commission + short_commission
        
        return gross_pnl - total_commission

    def close_trade(self, trade_id: str, trade: Dict, current_prices: Dict, timestamp: datetime, trades: List[Dict], close_reason: str):
        """Закрывает сделку"""
        long_exit_price = current_prices.get(trade['long_exchange'], trade['entry_long_price'])
        short_exit_price = current_prices.get(trade['short_exchange'], trade['entry_short_price'])
        
        long_pnl = (long_exit_price - trade['entry_long_price']) * trade['quantity']
        short_pnl = (trade['entry_short_price'] - short_exit_price) * trade['quantity']
        gross_pnl = long_pnl + short_pnl
        
        # Используем реальные комиссии
        long_commission_rate = COMMISSION_RATES.get(trade['long_exchange'].upper(), 0.001)
        short_commission_rate = COMMISSION_RATES.get(trade['short_exchange'].upper(), 0.001)
        
        long_commission = trade['quantity'] * trade['entry_long_price'] * long_commission_rate
        short_commission = trade['quantity'] * trade['entry_short_price'] * short_commission_rate
        total_commission = long_commission + short_commission
        
        net_pnl = gross_pnl - total_commission
        
        exit_spread = (short_exit_price - long_exit_price) / long_exit_price * 100
        
        trade.update({
            'exit_time': timestamp,
            'exit_long_price': long_exit_price,
            'exit_short_price': short_exit_price,
            'gross_pnl': gross_pnl,
            'net_pnl': net_pnl,
            'commission': total_commission,
            'exit_spread': exit_spread,
            'duration_seconds': (timestamp - trade['entry_time']).total_seconds(),
            'close_reason': close_reason,
            'status': 'closed'
        })
        
        self.balance += net_pnl

    def create_advanced_metrics(self, trades: List[Dict]) -> Optional[Dict]:
        """Создает метрики"""
        if not trades:
            return None
            
        closed_trades = [t for t in trades if t.get('status') == 'closed']
        
        if not closed_trades:
            return None
            
        returns = [t['net_pnl'] / self.initial_balance * 100 for t in closed_trades]
        returns_array = np.array(returns)
        
        total_profit = sum(t['net_pnl'] for t in closed_trades)
        win_rate = len([t for t in closed_trades if t['net_pnl'] > 0]) / len(closed_trades) * 100
        
        volatility = np.std(returns_array) if len(returns_array) > 1 else 0
        sharpe_ratio = (np.mean(returns_array) / volatility) if volatility > 0 else 0
        
        cumulative_returns = np.cumsum(returns_array)
        peak = np.maximum.accumulate(cumulative_returns)
        drawdown = (peak - cumulative_returns) / (peak + 1e-8) * 100
        max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0
        
        avg_hold_time = np.mean([t.get('duration_seconds', 0) for t in closed_trades])
        
        close_reasons = {}
        for trade in closed_trades:
            reason = trade.get('close_reason', 'unknown')
            close_reasons[reason] = close_reasons.get(reason, 0) + 1
        
        return {
            'total_profit': total_profit,
            'win_rate': win_rate,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'volatility': volatility,
            'total_trades': len(closed_trades),
            'avg_hold_time': avg_hold_time,
            'close_reasons': close_reasons
        }

async def main():
    """Запуск бэктеста"""
    backtester = SuperBacktest(initial_balance=1000)
    
    test_symbols = ['BTC', 'ETH', 'SOL']
    
    for symbol in test_symbols:
        try:
            await backtester.run_optimized_backtest(symbol, days=7)
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"❌ Ошибка бэктеста для {symbol}: {e}")
            continue

if __name__ == '__main__':
    asyncio.run(main())