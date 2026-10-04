import json
import asyncio
from decimal import Decimal
from types import SimpleNamespace

from exchanges.arcus import ArcusClient


def make_client(monkeypatch):
    monkeypatch.setenv("ARCUS_ADDRESS", "0x0000000000000000000000000000000000000001")
    monkeypatch.setenv("ARCUS_API_SIGNING_KEY", "00" * 31 + "01")
    monkeypatch.delenv("ARCUS_API_KEY", raising=False)
    config = SimpleNamespace(ticker="BTC", contract_id="", tick_size=Decimal("0"))
    client = ArcusClient(config)
    client._market_id = 1
    client._top_tick_size = Decimal("0.1")
    client._step_size = Decimal("0.00000001")
    client._tick_tiers = [(Decimal("500000"), Decimal("0.1")), (None, Decimal("0.2"))]
    return client


def test_arcus_typed_order_payload_is_documented_shape(monkeypatch):
    client = make_client(monkeypatch)
    payload = client._order_payload(
        timestamp=1712345678000000000,
        price=Decimal("50000"),
        quantity=Decimal("0.01"),
        side="buy",
        tif="GTT",
        reduce_only=False,
        good_til_us=4102444800000000,
    )
    assert json.loads(payload) == {
        "ad": "0x0000000000000000000000000000000000000001",
        "ai": 0,
        "ct": 1712345678000000000,
        "g": 4102444800000000000,
        "m": 1,
        "op": 1,
        "p": 500000,
        "q": 1000000,
        "r": 0,
        "s": 0,
        "t": 0,
        "v": 1,
    }
    assert list(json.loads(payload)) == ["ad", "ai", "ct", "g", "m", "op", "p", "q", "r", "s", "t", "v"]


def test_arcus_tick_tiers_and_market_matching(monkeypatch):
    client = make_client(monkeypatch)
    assert client._price_tick(Decimal("499999.9")) == Decimal("0.1")
    assert client._price_tick(Decimal("500000.1")) == Decimal("0.2")
    client._markets = {
        "BTC-USD": {"marketDisplayName": "BTC-USD", "baseAsset": "BTC", "marketId": 1}
    }
    assert client._market_for_ticker("BTC")['marketId'] == 1
    assert client._market_for_ticker("BTC-USD")['marketId'] == 1


def test_arcus_order_info_uses_signed_size_and_filled_fields(monkeypatch):
    client = make_client(monkeypatch)
    info = client._order_info_from_data({
        "orderId": "abc",
        "side": "SELL",
        "status": "OPEN",
        "state": "PARTIALLY_FILLED",
        "price": "100.1",
        "originalSize": "2",
        "remainingSize": "0.75",
    })
    assert info.order_id == "abc"
    assert info.side == "sell"
    assert info.status == "PARTIALLY_FILLED"
    assert info.filled_size == Decimal("1.25")
    assert info.remaining_size == Decimal("0.75")


def test_arcus_place_order_uses_decimal_body_and_signed_integer_payload(monkeypatch):
    client = make_client(monkeypatch)
    captured = {}

    async def fake_request(method, path, **kwargs):
        captured.update(method=method, path=path, **kwargs)
        return {"orderId": "order-1", "status": "ACK"}

    client._request = fake_request
    result = asyncio.run(client._place_order(
        "BTC-USD", Decimal("0.01"), Decimal("50000.0"), "buy",
        reduce_only=False,
    ))
    assert result.success is True
    assert captured["body"]["quantity"] == "0.01"
    assert captured["body"]["price"] == "50000.0"
    assert captured["body"]["marketId"] == 1
    signed = json.loads(captured["signed_message"])
    assert signed["p"] == 500000
    assert signed["q"] == 1000000
    assert captured["body"]["timestamp"] == signed["ct"]
