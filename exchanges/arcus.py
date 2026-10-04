"""Arcus perpetuals client.

Arcus exposes a REST + multiplexed WebSocket API.  Order mutations use an
Ed25519 signature over the documented typed canonical payload; the HTTP body
continues to use decimal strings while the signed payload uses market ticks
and quantums.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
import websockets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from .base import BaseExchangeClient, OrderInfo, OrderResult, query_retry
from helpers.logger import TradingLogger


class ArcusClient(BaseExchangeClient):
    """Arcus perpetuals adapter for the common TradingBot interface."""

    MAINNET_REST = "https://api.arcus.xyz"
    TESTNET_REST = "https://api.testnet.arcus.xyz"
    MAINNET_WS = "wss://api.arcus.xyz/v1/ws"
    TESTNET_WS = "wss://api.testnet.arcus.xyz/v1/ws"

    _SIDE = {"buy": ("BUY", 0), "sell": ("SELL", 1)}
    _TIF = {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}

    def __init__(self, config: Any):
        self.logger = TradingLogger(
            exchange="arcus",
            ticker=getattr(config, "ticker", ""),
            log_to_console=False,
        )

        environment = os.getenv("ARCUS_ENVIRONMENT", "mainnet").strip().lower()
        if environment in ("test", "testnet"):
            default_rest, default_ws = self.TESTNET_REST, self.TESTNET_WS
        elif environment in ("main", "mainnet", "prod", "production"):
            default_rest, default_ws = self.MAINNET_REST, self.MAINNET_WS
        else:
            raise ValueError("ARCUS_ENVIRONMENT must be mainnet or testnet")

        self.base_url = os.getenv("ARCUS_BASE_URL", default_rest).rstrip("/")
        self.ws_url = os.getenv("ARCUS_WS_URL", default_ws)
        self.address = os.getenv("ARCUS_ADDRESS", "").strip()
        self.api_key = os.getenv("ARCUS_API_KEY", "").strip().lower()
        self.account_index = int(os.getenv("ARCUS_ACCOUNT_INDEX", "0"))
        self._signing_key = self._load_signing_key(os.getenv("ARCUS_API_SIGNING_KEY", ""))
        if self._signing_key is not None:
            derived = self._signing_key.public_key().public_bytes(
                serialization.Encoding.Raw, serialization.PublicFormat.Raw
            ).hex()
            if self.api_key and self.api_key != derived:
                raise ValueError("ARCUS_API_KEY does not match ARCUS_API_SIGNING_KEY")
            self.api_key = self.api_key or derived

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop: Optional[asyncio.Event] = None
        self._order_update_handler = None
        self._request_id = 0
        self._market_id: Optional[int] = None
        self._markets: Dict[str, Dict[str, Any]] = {}
        self._market_by_id: Dict[int, Dict[str, Any]] = {}
        self._step_size = Decimal("0")
        self._top_tick_size = Decimal("0")
        self._tick_tiers: List[Tuple[Optional[Decimal], Decimal]] = []

        super().__init__(config)

    @staticmethod
    def _load_signing_key(value: str) -> Optional[ed25519.Ed25519PrivateKey]:
        if not value:
            return None
        raw = value.strip().lower()
        if raw.startswith("0x"):
            raw = raw[2:]
        try:
            key_bytes = bytes.fromhex(raw)
        except ValueError as exc:
            raise ValueError("ARCUS_API_SIGNING_KEY must be hexadecimal Ed25519 private key") from exc
        if len(key_bytes) != 32:
            raise ValueError("ARCUS_API_SIGNING_KEY must contain exactly 32 bytes")
        return ed25519.Ed25519PrivateKey.from_private_bytes(key_bytes)

    def _validate_config(self) -> None:
        ticker = str(getattr(self.config, "ticker", "") or "").strip().upper()
        if not ticker:
            raise ValueError("Arcus requires a ticker")
        self.config.ticker = ticker
        if not self.address:
            raise ValueError("Missing ARCUS_ADDRESS")
        if not self.address.lower().startswith("0x") or len(self.address) != 42:
            raise ValueError("ARCUS_ADDRESS must be a 20-byte 0x-prefixed Ethereum address")
        try:
            int(self.address[2:], 16)
        except ValueError as exc:
            raise ValueError("ARCUS_ADDRESS must be hexadecimal") from exc
        self.address = "0x" + self.address[2:].lower()
        if not 0 <= self.account_index <= 9:
            raise ValueError("ARCUS_ACCOUNT_INDEX must be between 0 and 9")
        if self._signing_key is None:
            raise ValueError(
                "Arcus trading requires ARCUS_API_SIGNING_KEY; ARCUS_API_KEY is optional "
                "because the public key can be derived from it"
            )
        if len(self.api_key) != 64:
            raise ValueError("ARCUS_API_KEY must be a 32-byte (64 hex character) Ed25519 public key")
        try:
            bytes.fromhex(self.api_key)
        except ValueError as exc:
            raise ValueError("ARCUS_API_KEY must be hexadecimal") from exc

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Accept": "application/json", "Content-Type": "application/json"}
            )
        return self._session

    @staticmethod
    def _canonical_json(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)

    def _sign(self, message: str) -> str:
        if self._signing_key is None:
            raise ValueError("ARCUS_API_SIGNING_KEY is not configured")
        return self._signing_key.sign(message.encode("utf-8")).hex()

    def _timestamp_ns(self) -> int:
        return time.time_ns()

    def _headers(self, timestamp: int, signature: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "X-API-Key": self.api_key,
            "X-Timestamp": str(timestamp),
        }
        if signature is not None:
            headers["X-Signature"] = signature
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        signed_message: Optional[str] = None,
        require_signature: bool = False,
    ) -> Dict[str, Any]:
        session = await self._ensure_session()
        request_headers = dict(headers or {})
        if require_signature:
            if signed_message is None:
                raise ValueError("signed_message is required")
            timestamp = int(request_headers.get("X-Timestamp", self._timestamp_ns()))
            request_headers.update(self._headers(timestamp, self._sign(signed_message)))
        url = f"{self.base_url}/v1{path}"
        async with session.request(method, url, params=params, json=body, headers=request_headers) as response:
            try:
                data = await response.json(content_type=None)
            except Exception:
                data = {"error": await response.text()}
            if response.status == 429:
                retry_after = data.get("retryAfterMs") if isinstance(data, dict) else None
                if retry_after:
                    await asyncio.sleep(min(float(retry_after) / 1000, 10))
                raise RuntimeError(f"Arcus rate limit ({response.status}): {data}")
            if response.status >= 400:
                raise RuntimeError(f"Arcus API {response.status}: {data}")
            return data if isinstance(data, dict) else {}

    async def _load_markets(self) -> None:
        response = await self._request("GET", "/markets")
        markets = response.get("markets", [])
        self._markets = {}
        self._market_by_id = {}
        for market in markets:
            try:
                name = str(market["marketDisplayName"]).upper()
                market_id = int(market["marketId"])
                self._markets[name] = market
                self._market_by_id[market_id] = market
            except (KeyError, TypeError, ValueError):
                continue

    def _market_for_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        normalized = ticker.upper().strip()
        candidates = [normalized]
        if not normalized.endswith("-USD"):
            candidates.append(f"{normalized}-USD")
        for candidate in candidates:
            if candidate in self._markets:
                return self._markets[candidate]
        for market in self._markets.values():
            if str(market.get("baseAsset", "")).upper() == normalized:
                return market
        return None

    @staticmethod
    def _decimal_multiple(value: Decimal, unit: Decimal) -> int:
        if unit <= 0:
            raise ValueError("Arcus market unit must be positive")
        quotient = value / unit
        integral = quotient.to_integral_value()
        if quotient != integral:
            raise ValueError(f"{value} is not an exact multiple of {unit}")
        return int(integral)

    def _price_tick(self, price: Decimal) -> Decimal:
        for upper, tick in self._tick_tiers:
            if upper is None or price <= upper:
                return tick
        return self._top_tick_size

    def _snap_price(self, price: Decimal, side: str) -> Decimal:
        """Snap to the active tier while retaining the top-level signing unit."""
        price = Decimal(str(price))
        tick = self._price_tick(price)
        rounding = ROUND_FLOOR if side.lower() == "buy" else ROUND_CEILING
        snapped = (price / tick).to_integral_value(rounding=rounding) * tick
        # Arcus requires the price to be divisible by the top-level tick too.
        self._decimal_multiple(snapped, self._top_tick_size)
        return snapped

    def _good_til_time_us(self) -> int:
        # Arcus requires every order, including IOC/FOK, to expire at least one
        # month in the future. 40 days leaves margin for clock skew and retries.
        return time.time_ns() // 1_000 + 40 * 86_400 * 1_000_000

    def _order_payload(
        self,
        *,
        timestamp: int,
        price: Decimal,
        quantity: Decimal,
        side: str,
        tif: str,
        reduce_only: bool,
        good_til_us: int,
        op: int = 1,
        order_id: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> str:
        if self._market_id is None:
            raise ValueError("Arcus market has not been initialized")
        body: Dict[str, Any] = {
            "ad": self.address.lower(),
            "ai": self.account_index,
            "ct": timestamp,
            "g": good_til_us * 1000,
            "m": self._market_id,
            "op": op,
            "p": self._decimal_multiple(Decimal(str(price)), self._top_tick_size),
            "q": self._decimal_multiple(Decimal(str(quantity)), self._step_size),
            "r": 1 if reduce_only else 0,
            "s": self._SIDE[side.lower()][1],
            "t": self._TIF[tif],
            "v": 1,
        }
        if client_id:
            body["c"] = client_id
        if order_id:
            body["id"] = order_id
        return self._canonical_json(body)

    def _order_request_body(
        self,
        *,
        price: Decimal,
        quantity: Decimal,
        side: str,
        order_type: str,
        tif: str,
        reduce_only: bool,
        timestamp: int,
        good_til_us: int,
    ) -> Dict[str, Any]:
        return {
            "address": self.address,
            "accountIndex": self.account_index,
            "marketId": self._market_id,
            "orderSide": self._SIDE[side.lower()][0],
            "orderType": order_type,
            "quantity": format(Decimal(str(quantity)), "f"),
            "price": format(Decimal(str(price)), "f"),
            "timeInForce": tif,
            "goodTilTime": str(good_til_us),
            "reduceOnly": reduce_only,
            "timestamp": timestamp,
        }

    async def connect(self) -> None:
        await self._ensure_session()
        if self._order_update_handler and (self._ws_task is None or self._ws_task.done()):
            self._ws_stop = asyncio.Event()
            self._ws_task = asyncio.create_task(self._ws_reconnect_loop())

    async def _ws_reconnect_loop(self) -> None:
        backoff = 1.0
        while self._ws_stop and not self._ws_stop.is_set():
            try:
                async with websockets.connect(self.ws_url, open_timeout=10, ping_interval=30) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": "orders",
                        "id": self.address,
                        "accountIndex": self.account_index,
                        "market": self.config.contract_id,
                    }))
                    backoff = 1.0
                    while not self._ws_stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=35)
                        except asyncio.TimeoutError:
                            await ws.ping()
                            continue
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        self._handle_ws_message(json.loads(raw))
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._ws_stop and not self._ws_stop.is_set():
                    self.logger.log(f"[WS] Arcus connection error: {exc}", "WARNING")
                    try:
                        await asyncio.wait_for(self._ws_stop.wait(), timeout=backoff)
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff * 2, 30.0)
            finally:
                self._ws = None

    def _handle_ws_message(self, message: Dict[str, Any]) -> None:
        if not self._order_update_handler or not isinstance(message, dict):
            return
        if message.get("channel") != "orders":
            return
        contents = message.get("contents")
        if not isinstance(contents, dict):
            return
        if isinstance(contents.get("openOrders"), list):
            # Snapshot frames are intentionally not emitted as fills; the bot
            # obtains the authoritative open list through get_active_orders().
            return
        market_id = contents.get("marketId")
        if self._market_id is not None and market_id is not None and int(market_id) != self._market_id:
            return
        side = str(contents.get("side", "")).lower()
        status = str(contents.get("status") or contents.get("state") or "").upper()
        state = str(contents.get("state", "")).upper()
        if status == "PENDING":
            status = "OPEN"
        if state == "PARTIALLY_FILLED":
            status = "PARTIALLY_FILLED"
        original = Decimal(str(contents.get("originalSize", "0")))
        remaining = Decimal(str(contents.get("remainingSize", "0")))
        filled = max(Decimal("0"), original - remaining)
        order_type = "OPEN" if side == str(getattr(self.config, "direction", "buy")).lower() else "CLOSE"
        try:
            self._order_update_handler({
                "contract_id": contents.get("marketDisplayName") or self.config.contract_id,
                "order_id": str(contents.get("orderId", "")),
                "status": status,
                "side": side,
                "order_type": order_type,
                "filled_size": str(filled),
                "size": str(original),
                "price": str(contents.get("avgFillPrice") or contents.get("price") or "0"),
            })
        except Exception as exc:
            self.logger.log(f"[WS] Error handling Arcus order update: {exc}", "ERROR")

    async def disconnect(self) -> None:
        if self._ws_stop:
            self._ws_stop.set()
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        self._ws_task = None
        self._ws_stop = None
        self._ws = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    def get_exchange_name(self) -> str:
        return "arcus"

    def setup_order_update_handler(self, handler) -> None:
        self._order_update_handler = handler

    @query_retry(default_return=(Decimal("0"), Decimal("0")))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        market = quote(str(contract_id), safe="")
        data = await self._request("GET", f"/bbo/{market}")
        bid = data.get("bestBid") or {}
        ask = data.get("bestAsk") or {}
        return Decimal(str(bid.get("price", "0"))), Decimal(str(ask.get("price", "0")))

    async def get_order_price(self, direction: str) -> Decimal:
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("Arcus returned an empty order book")
        if direction.lower() == "buy":
            return self._snap_price(best_ask - self._price_tick(best_ask), "buy")
        if direction.lower() == "sell":
            return self._snap_price(best_bid + self._price_tick(best_bid), "sell")
        raise ValueError(f"Invalid direction: {direction}")

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        try:
            price = await self.get_order_price(direction)
            return await self._place_order(contract_id, quantity, price, direction, reduce_only=False)
        except Exception as exc:
            self.logger.log(f"[OPEN] Arcus order failed: {exc}", "ERROR")
            return OrderResult(success=False, error_message=str(exc))

    async def _place_order(
        self,
        contract_id: str,
        quantity: Decimal,
        price: Decimal,
        side: str,
        *,
        reduce_only: bool,
        order_type: str = "LIMIT",
        tif: str = "ALO",
    ) -> OrderResult:
        quantity = Decimal(str(quantity))
        if quantity <= 0:
            raise ValueError("Arcus quantity must be positive")
        self._decimal_multiple(quantity, self._step_size)
        price = self._snap_price(price, side)
        timestamp = self._timestamp_ns()
        good_til_us = self._good_til_time_us()
        payload = self._order_payload(
            timestamp=timestamp, price=price, quantity=quantity, side=side,
            tif=tif, reduce_only=reduce_only, good_til_us=good_til_us,
        )
        body = self._order_request_body(
            price=price, quantity=quantity, side=side, order_type=order_type,
            tif=tif, reduce_only=reduce_only, timestamp=timestamp, good_til_us=good_til_us,
        )
        data = await self._request(
            "POST", "/placeOrder", params={"address": self.address}, body=body,
            headers={"X-Timestamp": str(timestamp)}, signed_message=payload,
            require_signature=True,
        )
        order_id = data.get("orderId")
        status = str(data.get("status", "ACK")).upper()
        if not order_id:
            raise ValueError(f"Arcus did not return orderId: {data}")
        return OrderResult(
            success=True, order_id=str(order_id), side=side.lower(), size=quantity,
            price=price, status=status, filled_size=Decimal(str(data.get("filledSize", "0"))),
        )

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        try:
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
            if side.lower() == "sell" and price <= best_bid:
                price = best_bid + self._price_tick(best_bid)
            elif side.lower() == "buy" and price >= best_ask:
                price = best_ask - self._price_tick(best_ask)
            return await self._place_order(contract_id, quantity, price, side, reduce_only=True)
        except Exception as exc:
            self.logger.log(f"[CLOSE] Arcus order failed: {exc}", "ERROR")
            return OrderResult(success=False, error_message=str(exc))

    async def place_market_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an IOC market order with Arcus' mandatory protective price bound."""
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
        price = best_ask * Decimal("1.1") if direction.lower() == "buy" else best_bid * Decimal("0.9")
        price = self._snap_price(price, direction)
        return await self._place_order(
            contract_id, quantity, price, direction, reduce_only=True,
            order_type="MARKET", tif="IOC",
        )

    async def cancel_order(self, order_id: str) -> OrderResult:
        try:
            timestamp = self._timestamp_ns()
            payload: Dict[str, Any] = {
                "ad": self.address.lower(),
                "ai": self.account_index,
                "ct": timestamp,
                "id": str(order_id),
                "m": self._market_id,
                "op": 2,
                "v": 1,
            }
            signed = self._canonical_json(payload)
            body = {
                "address": self.address,
                "accountIndex": self.account_index,
                "marketId": self._market_id,
                "kind": "orderId",
                "orderId": str(order_id),
                "timestamp": timestamp,
            }
            data = await self._request(
                "POST", "/cancelOrder", params={"address": self.address}, body=body,
                headers={"X-Timestamp": str(timestamp)}, signed_message=signed,
                require_signature=True,
            )
            return OrderResult(success=True, order_id=str(order_id), status=str(data.get("status", "ACK")).upper())
        except Exception as exc:
            return OrderResult(success=False, order_id=str(order_id), error_message=str(exc))

    @query_retry(default_return=None)
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        try:
            data = await self._request(
                "GET", f"/order/{quote(str(order_id), safe='')}",
                params={"address": self.address, "accountIndex": self.account_index},
                headers={"X-API-Key": self.api_key},
            )
            return self._order_info_from_data(data)
        except Exception as exc:
            self.logger.log(f"Error getting Arcus order {order_id}: {exc}", "WARNING")
            return None

    def _order_info_from_data(self, order: Dict[str, Any]) -> OrderInfo:
        original = Decimal(str(order.get("originalSize", order.get("quantity", "0"))))
        remaining = Decimal(str(order.get("remainingSize", "0")))
        filled = Decimal(str(order.get("filledSize", original - remaining)))
        status = str(order.get("status", order.get("state", "UNKNOWN"))).upper()
        if status == "PENDING":
            status = "OPEN"
        if str(order.get("state", "")).upper() == "PARTIALLY_FILLED":
            status = "PARTIALLY_FILLED"
        return OrderInfo(
            order_id=str(order.get("orderId", "")),
            side=str(order.get("side", "")).lower(),
            size=original,
            price=Decimal(str(order.get("price", "0"))),
            status=status,
            filled_size=filled,
            remaining_size=remaining,
            cancel_reason=str(order.get("cancelReason", "")),
        )

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        try:
            data = await self._request(
                "GET", "/openOrders",
                params={
                    "address": self.address,
                    "accountIndex": self.account_index,
                    "market": contract_id,
                },
                headers={"X-API-Key": self.api_key},
            )
            result = []
            for order in data.get("orders", []):
                info = self._order_info_from_data(order)
                if info.status in ("OPEN", "UNTRIGGERED", "PARTIALLY_FILLED"):
                    result.append(info)
            return result
        except Exception as exc:
            self.logger.log(f"Error getting Arcus active orders: {exc}", "WARNING")
            return []

    @query_retry(default_return=Decimal("0"))
    async def get_account_positions(self) -> Decimal:
        data = await self._request(
            "GET", "/positions",
            params={"address": self.address, "accountIndex": self.account_index, "market": self.config.contract_id},
            headers={"X-API-Key": self.api_key},
        )
        positions = data.get("positions", {})
        if isinstance(positions, dict):
            positions = list(positions.values())
        elif not isinstance(positions, list):
            positions = []
        for position in positions:
            try:
                position_market_id = int(position.get("marketId", -1))
            except (TypeError, ValueError):
                position_market_id = -1
            if (
                str(position.get("marketDisplayName", "")).upper() == str(self.config.contract_id).upper()
                or position_market_id == self._market_id
            ):
                return Decimal(str(position.get("size", "0")))
        return Decimal("0")

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        await self._load_markets()
        market = self._market_for_ticker(self.config.ticker)
        if not market:
            raise ValueError(f"Arcus market not found for ticker: {self.config.ticker}")
        self._market_id = int(market["marketId"])
        self.config.contract_id = str(market["marketDisplayName"]).upper()
        self._top_tick_size = Decimal(str(market["tickSize"]))
        self._step_size = Decimal(str(market["stepSize"]))
        tiers: List[Tuple[Optional[Decimal], Decimal]] = []
        for tier in market.get("tickTiers", []) or []:
            tiers.append((
                Decimal(str(tier["upToPrice"])) if tier.get("upToPrice") is not None else None,
                Decimal(str(tier["tick"])),
            ))
        self._tick_tiers = tiers or [(None, self._top_tick_size)]
        return self.config.contract_id, self._top_tick_size
