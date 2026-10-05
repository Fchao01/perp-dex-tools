import asyncio
from decimal import Decimal
from types import SimpleNamespace

from exchanges.base import OrderResult
from trading_bot import TradingBot


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class DummyExchange:
    def __init__(self):
        self.placed = []
        self.canceled = []

    async def fetch_bbo_prices(self, contract_id):
        return Decimal("100"), Decimal("101")

    async def place_close_order(self, contract_id, quantity, price, side):
        self.placed.append((contract_id, quantity, price, side))
        return OrderResult(success=True, order_id="reconcile-1", price=price)

    async def cancel_order(self, order_id):
        self.canceled.append(order_id)
        return OrderResult(success=True, order_id=order_id)


def make_bot():
    bot = TradingBot.__new__(TradingBot)
    bot.config = SimpleNamespace(
        contract_id="BTC-USD",
        close_order_side="sell",
        take_profit=Decimal("0.02"),
        quantity=Decimal("0.001"),
    )
    bot.exchange_client = DummyExchange()
    bot.active_close_orders = []
    bot._last_reconciliation_time = 0
    bot.last_log_time = 123
    bot.logger = DummyLogger()
    return bot


def test_reconcile_adds_only_uncovered_position_size():
    bot = make_bot()
    bot.active_close_orders = [{"id": "existing", "size": Decimal("0.072"), "price": Decimal("101")}]

    ok = asyncio.run(bot._reconcile_close_orders(
        Decimal("0.075"), Decimal("0.072"), Decimal("0.003")
    ))

    assert ok is True
    assert bot.exchange_client.placed[0][1] == Decimal("0.003")
    assert bot.exchange_client.placed[0][3] == "sell"
    assert bot.exchange_client.canceled == []
    assert bot.last_log_time == 0


def test_reconcile_cancels_excess_close_inventory():
    bot = make_bot()
    bot.active_close_orders = [
        {"id": "order-1", "size": Decimal("0.001"), "price": Decimal("101")},
        {"id": "order-2", "size": Decimal("0.001"), "price": Decimal("102")},
    ]

    ok = asyncio.run(bot._reconcile_close_orders(
        Decimal("0.001"), Decimal("0.002"), Decimal("-0.001")
    ))

    assert ok is True
    assert bot.exchange_client.canceled == ["order-1"]
    assert bot.exchange_client.placed == []
