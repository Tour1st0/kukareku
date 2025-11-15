# dashboard.py
import asyncio
from datetime import datetime
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.text import Text
from rich import box

class Dashboard:
    def __init__(self, bot):
        self.bot = bot
        self.console = Console()
        self.start_time = datetime.utcnow()

    def render(self) -> Layout:
        layout = Layout(name="root")
        layout.split(
            Layout(name="header", size=5),
            Layout(name="main", ratio=1),
        )
        layout["main"].split_row(
            Layout(name="trades", ratio=2),
            Layout(name="prices", ratio=1),
            Layout(name="logs", ratio=3),
        )

        # === HEADER ===
        uptime = str(datetime.utcnow() - self.start_time).split('.')[0]
        connected = sum(1 for status in getattr(self.bot.order_manager, 'connection_status', {}).values() if status.get("connected"))
        total_exchanges = len(getattr(self.bot.order_manager, 'exchanges', {}))
        
        grid = Table.grid(expand=True, padding=(0, 1))
        for _ in range(4): grid.add_column()
        
        grid.add_row(
            f"[bold green]БОТ:[/][/] {'АКТИВЕН' if getattr(self.bot, 'is_running', False) else 'СТОП'}",
            f"[bold yellow]БИРЖИ:[/][/] {connected}/{total_exchanges}",
            f"[bold cyan]СДЕЛКИ:[/][/] {len(getattr(self.bot.order_manager, 'active_trades', {}))}/{getattr(self.bot.config, 'MAX_CONCURRENT_TRADES', 0)}",
            f"[bold magenta]ВРЕМЯ:[/][/] {uptime}",
        )
        grid.add_row(
            f"[bold blue]БАЛАНС:[/][/] ${getattr(self.bot, 'total_balance', 0.0):.2f}",
            f"[bold green]PNL:[/][/] ${getattr(self.bot.order_manager, 'daily_pnl', 0.0):+.2f}",
            f"[bold white]СИГНАЛЫ:[/][/] {getattr(self.bot, 'signal_count', 0)}",
            f"[bold red]ЛОГИ:[/][/] {len(getattr(self.bot, 'log_buffer', []))}",
        )
        layout["header"].update(Panel(grid, title="[bold]ULTRA ARBITRAGE BOT v2[/bold]", border_style="bright_blue", box=box.DOUBLE))

        # === TRADES ===
        t = Table(title="АКТИВНЫЕ СДЕЛКИ", box=box.ROUNDED, show_lines=True)
        for col in ["ID", "СИМВОЛ", "ПОЗИЦИИ", "QTY", "ПРИБЫЛЬ"]: t.add_column(col)
        
        active_trades = list(getattr(self.bot.order_manager, 'active_trades', {}).items())
        if not active_trades:
            t.add_row("—", "Нет активных сделок", "—", "—", "—")
        else:
            for tid, trade in active_trades[:3]:
                pnl = trade.get('net_pnl', 0)
                pos = f"BUY {trade.get('low_exchange','')}\nSELL {trade.get('high_exch','')}"
                t.add_row(tid[-6:], trade.get('symbol', '—'), pos, f"{trade.get('quantity', 0):.6f}", f"${p:+.4f}" if p else "В процессе")
        layout["trades"].update(Panel(t, border_style="green"))

        # === PRICES ===
        p = Table(title="ЦЕНЫ (ETH-USDT)", box=box.ROUNDED)
        p.add_column("БИРЖА"); p.add_column("ЦЕНА", justify="right")
        prices = getattr(self.bot.price_fetcher.ws_manager, 'prices', {}).get("ETH-USDT", {})
        for exch in ['bybit', 'mexc', 'gate', 'bingx']:
            val = prices.get(exch)
            text = f"${val:.2f}" if val else "—"
            p.add_row(exch.upper(), f"[{'green' if val else 'dim'}]{text}[/]")
        layout["prices"].update(Panel(p, border_style="yellow"))

        # === LOGS ===
        logs = getattr(self.bot, 'log_buffer', [])[-8:]
        log_text = ""
        for line in logs:
            line = str(line)
            if "ERROR" in line or "CRITICAL" in line: log_text += f"[bold red]{line}[/]\n"
            elif "WARNING" in line: log_text += f"[yellow]{line}[/]\n"
            elif "ОТКРЫТА СДЕЛКА" in line or "PNL" in line: log_text += f"[bold green]{line}[/]\n"
            else: log_text += f"[dim]{line}[/]\n"
        layout["logs"].update(Panel(log_text, title="ЛОГИ", border_style="white"))

        return layout

    async def run(self):
        with Live(console=self.console, screen=True, refresh_per_second=2, transient=True) as live:
            while getattr(self.bot, 'is_running', False):
                try:
                    live.update(self.render())
                except Exception:
                    pass  # Ignore rendering errors to keep dashboard alive
                await asyncio.sleep(0.5)
