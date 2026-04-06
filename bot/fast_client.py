"""
Fast order client for Bitget – bypasses ccxt entirely.

Uses:
  • aiohttp for REST (with a persistent TCP connector, no per-call handshake)
  • websockets for a pre-warmed WS connection (price feed + order confirmation)

The key optimisation for the entry order is _pre-signing_:
  If we know the listing symbol in advance (announcement path), we call
  `presign_market_buy()` at T-30s. At T=0 we only call `fire_presigned()`,
  which is a single aiohttp POST with an already-built payload. This eliminates
  HMAC computation from the hot path.

Measured overhead breakdown (VPS Singapore):
  HMAC sign          ~0.1 ms
  aiohttp POST RTT   ~2–10 ms  (persistent connection)
  Bitget processing  ~5–50 ms  (exchange-side, out of our control)
  ─────────────────────────────
  Total              ~8–60 ms
"""
import asyncio
import base64
import hashlib
import hmac
import json
import time
from typing import Optional

import aiohttp
from loguru import logger

from config import Config

# Bitget V2 REST base
_REST_BASE = "https://api.bitget.com"
_WS_PUBLIC  = "wss://ws.bitget.com/v2/ws/public"
_WS_PRIVATE = "wss://ws.bitget.com/v2/ws/private"


def _sign(secret: str, timestamp: str, method: str, path: str, body: str = "") -> str:
    """Bitget HMAC-SHA256 signature."""
    msg = timestamp + method.upper() + path + body
    return base64.b64encode(
        hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()


def _ts() -> str:
    return str(int(time.time() * 1000))


class FastOrderClient:
    """
    Persistent HTTP session + optional WebSocket price feed.
    Designed to be initialised at startup and kept alive throughout the bot run.
    """

    def __init__(self):
        # Persistent TCP connector: avoids DNS + TLS handshake on each request
        self._connector = aiohttp.TCPConnector(
            limit=10,
            ttl_dns_cache=300,
            ssl=True,
            force_close=False,   # keep-alive
        )
        self._session: Optional[aiohttp.ClientSession] = None

        # Pre-signed order cache: symbol -> (headers, body, path)
        self._presigned: dict[str, tuple[dict, str, str]] = {}

        # WebSocket connections
        self._ws_public = None
        self._ws_private = None
        self._ws_prices: dict[str, float] = {}   # symbol -> latest price
        self._ws_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Open the persistent HTTP session and authenticate the private WS."""
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            connector_owner=False,
            timeout=aiohttp.ClientTimeout(total=5),
        )
        # Pre-warm: make a cheap authenticated call to establish TLS session cache
        if Config.API_KEY:
            try:
                await self.get_balance("USDT")
                logger.info("[FastClient] HTTP session pre-warmed.")
            except Exception as e:
                logger.debug(f"[FastClient] Pre-warm failed (ok in dry-run): {e}")

    async def close(self):
        if self._session:
            await self._session.close()
        if self._connector:
            await self._connector.close()

    # ------------------------------------------------------------------
    # Pre-signing  (call this 30s before the listing!)
    # ------------------------------------------------------------------

    def presign_market_buy(self, symbol: str, usdt_amount: float):
        """
        Build and sign the market-buy payload NOW so the hot path only needs
        to send the pre-built request. Safe to call multiple times; last call wins.

        NOTE: Bitget timestamps are checked within ±30s, so presign no earlier
        than 25s before execution to stay within the validity window.
        """
        if Config.DRY_RUN:
            logger.info(f"[FastClient] [DRY RUN] Pre-signed order for {symbol}")
            return

        # Normalise: "NEWTOKEN/USDT" → "NEWTOKENUSDT"
        inst = symbol.replace("/", "")

        path   = "/api/v2/spot/trade/place-order"
        method = "POST"
        body_dict = {
            "symbol":     inst,
            "side":       "buy",
            "orderType":  "market",
            "force":      "gtc",
            "quoteSize":  str(usdt_amount),   # spend this many USDT
        }
        body_str = json.dumps(body_dict, separators=(",", ":"))
        ts       = _ts()
        sig      = _sign(Config.SECRET_KEY, ts, method, path, body_str)

        headers = {
            "Content-Type":          "application/json",
            "ACCESS-KEY":            Config.API_KEY,
            "ACCESS-SIGN":           sig,
            "ACCESS-TIMESTAMP":      ts,
            "ACCESS-PASSPHRASE":     Config.PASSPHRASE,
            "locale":                "en-US",
        }
        self._presigned[symbol] = (headers, body_str, path)
        logger.info(f"[FastClient] Order pre-signed for {symbol} (ts={ts})")

    async def fire_presigned(self, symbol: str) -> Optional[dict]:
        """
        Send the pre-signed order immediately. This is the fastest possible path:
        no signing, no serialisation – just one POST with pre-built bytes.
        """
        if Config.DRY_RUN:
            logger.info(f"[FastClient] [DRY RUN] Fired pre-signed order for {symbol}")
            return {"id": "dry-run", "symbol": symbol, "side": "buy", "status": "closed"}

        if symbol not in self._presigned:
            logger.warning(f"[FastClient] No pre-signed order for {symbol}, falling back to live-sign.")
            return await self.market_buy(symbol, Config.TRADE_AMOUNT_USDT)

        headers, body_str, path = self._presigned.pop(symbol)
        t0 = time.perf_counter()
        try:
            async with self._session.post(
                _REST_BASE + path,
                data=body_str.encode(),
                headers=headers,
            ) as resp:
                data = await resp.json()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.success(
                    f"[FastClient] Order fired in {elapsed_ms:.1f} ms | "
                    f"status={resp.status} | resp={data.get('msg','')}"
                )
                return data.get("data", data)
        except Exception as e:
            logger.error(f"[FastClient] fire_presigned failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Live-signed market order (fallback / api_poll path)
    # ------------------------------------------------------------------

    async def market_buy(self, symbol: str, usdt_amount: float) -> Optional[dict]:
        """Sign and send a market buy on the hot path (slower than presigned)."""
        if Config.DRY_RUN:
            logger.info(f"[FastClient] [DRY RUN] Market BUY {symbol} for {usdt_amount} USDT")
            return {"id": "dry-run", "symbol": symbol, "side": "buy", "status": "closed"}

        inst     = symbol.replace("/", "")
        path     = "/api/v2/spot/trade/place-order"
        body_dict = {
            "symbol":    inst,
            "side":      "buy",
            "orderType": "market",
            "force":     "gtc",
            "quoteSize": str(usdt_amount),
        }
        body_str = json.dumps(body_dict, separators=(",", ":"))
        ts       = _ts()
        sig      = _sign(Config.SECRET_KEY, ts, "POST", path, body_str)

        headers = {
            "Content-Type":      "application/json",
            "ACCESS-KEY":        Config.API_KEY,
            "ACCESS-SIGN":       sig,
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": Config.PASSPHRASE,
            "locale":            "en-US",
        }
        t0 = time.perf_counter()
        try:
            async with self._session.post(
                _REST_BASE + path,
                data=body_str.encode(),
                headers=headers,
            ) as resp:
                data = await resp.json()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.info(f"[FastClient] Live buy {symbol} in {elapsed_ms:.1f} ms")
                return data.get("data", data)
        except Exception as e:
            logger.error(f"[FastClient] market_buy error: {e}")
            return None

    async def market_sell(self, symbol: str, quantity: float) -> Optional[dict]:
        if Config.DRY_RUN:
            logger.info(f"[FastClient] [DRY RUN] Market SELL {symbol} qty={quantity}")
            return {"id": "dry-run", "symbol": symbol, "side": "sell", "status": "closed"}

        inst     = symbol.replace("/", "")
        path     = "/api/v2/spot/trade/place-order"
        body_dict = {
            "symbol":    inst,
            "side":      "sell",
            "orderType": "market",
            "force":     "gtc",
            "baseSize":  str(quantity),
        }
        body_str = json.dumps(body_dict, separators=(",", ":"))
        ts       = _ts()
        sig      = _sign(Config.SECRET_KEY, ts, "POST", path, body_str)

        headers = {
            "Content-Type":      "application/json",
            "ACCESS-KEY":        Config.API_KEY,
            "ACCESS-SIGN":       sig,
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": Config.PASSPHRASE,
            "locale":            "en-US",
        }
        t0 = time.perf_counter()
        try:
            async with self._session.post(
                _REST_BASE + path,
                data=body_str.encode(),
                headers=headers,
            ) as resp:
                data = await resp.json()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.info(f"[FastClient] Live sell {symbol} in {elapsed_ms:.1f} ms")
                return data.get("data", data)
        except Exception as e:
            logger.error(f"[FastClient] market_sell error: {e}")
            return None

    # ------------------------------------------------------------------
    # Balance
    # ------------------------------------------------------------------

    async def get_balance(self, asset: str = "USDT") -> float:
        path = "/api/v2/spot/account/assets"
        ts   = _ts()
        sig  = _sign(Config.SECRET_KEY, ts, "GET", path)
        headers = {
            "ACCESS-KEY":        Config.API_KEY,
            "ACCESS-SIGN":       sig,
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": Config.PASSPHRASE,
        }
        async with self._session.get(_REST_BASE + path, headers=headers) as resp:
            data = await resp.json()
        assets = data.get("data", [])
        for a in assets:
            if a.get("coin") == asset:
                return float(a.get("available", 0))
        return 0.0

    # ------------------------------------------------------------------
    # WebSocket price feed (replaces order-book REST polls)
    # ------------------------------------------------------------------

    async def subscribe_price_feed(self, symbol: str):
        """
        Subscribe to the Bitget public WS ticker for a symbol.
        Prices are stored in self._ws_prices for lock-free reads.
        """
        inst = symbol.replace("/", "")
        asyncio.create_task(self._ws_ticker_loop(inst, symbol))

    async def _ws_ticker_loop(self, inst: str, symbol: str):
        import websockets
        sub_msg = json.dumps({
            "op": "subscribe",
            "args": [{"instType": "SPOT", "channel": "ticker", "instId": inst}],
        })
        uri = _WS_PUBLIC
        while True:
            try:
                async with websockets.connect(uri, ping_interval=20) as ws:
                    await ws.send(sub_msg)
                    logger.info(f"[FastClient] WS ticker subscribed: {symbol}")
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", [])
                        if data:
                            last = float(data[0].get("lastPr") or data[0].get("last") or 0)
                            if last > 0:
                                self._ws_prices[symbol] = last
            except Exception as e:
                logger.warning(f"[FastClient] WS ticker error ({symbol}): {e}, reconnecting…")
                await asyncio.sleep(0.5)

    def get_ws_price(self, symbol: str) -> Optional[float]:
        """Lock-free read of the latest WS price. Returns None if not available."""
        return self._ws_prices.get(symbol)
