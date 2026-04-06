"""
Bitget exchange wrapper using ccxt for REST calls and native websockets
for ultra-low-latency price feeds during spike capture.
"""
import asyncio
import time
from typing import Optional

import ccxt.async_support as ccxt
from loguru import logger

from config import Config


class BitgetExchange:
    def __init__(self):
        self._exchange = ccxt.bitget({
            "apiKey": Config.API_KEY,
            "secret": Config.SECRET_KEY,
            "password": Config.PASSPHRASE,
            "enableRateLimit": False,  # We manage rate limits manually for speed
            "options": {
                "defaultType": "spot",
            },
        })
        self._known_symbols: set[str] = set()

    async def close(self):
        await self._exchange.close()

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    async def load_markets(self) -> dict:
        return await self._exchange.load_markets(reload=True)

    async def get_all_symbols(self) -> set[str]:
        markets = await self._exchange.load_markets(reload=True)
        return {s for s, m in markets.items() if m.get("active") and m.get("quote") == "USDT"}

    async def get_ticker(self, symbol: str) -> dict:
        return await self._exchange.fetch_ticker(symbol)

    async def get_order_book(self, symbol: str, limit: int = 5) -> dict:
        return await self._exchange.fetch_order_book(symbol, limit)

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def market_buy(self, symbol: str, usdt_amount: float) -> Optional[dict]:
        """Place a market buy order spending `usdt_amount` USDT."""
        if Config.DRY_RUN:
            logger.info(f"[DRY RUN] Market BUY {symbol} for {usdt_amount} USDT")
            return {"id": "dry-run", "symbol": symbol, "side": "buy", "amount": usdt_amount, "status": "closed"}

        try:
            # Bitget spot market buy using cost (quoteOrderQty)
            order = await self._exchange.create_market_buy_order(
                symbol,
                None,
                params={"quoteOrderQty": usdt_amount},
            )
            logger.info(f"BUY order placed: {order['id']} | {symbol} | {usdt_amount} USDT")
            return order
        except Exception as e:
            logger.error(f"Failed to place buy order for {symbol}: {e}")
            return None

    async def market_sell(self, symbol: str, quantity: float) -> Optional[dict]:
        """Place a market sell order for `quantity` units of the base asset."""
        if Config.DRY_RUN:
            logger.info(f"[DRY RUN] Market SELL {symbol} qty={quantity}")
            return {"id": "dry-run", "symbol": symbol, "side": "sell", "amount": quantity, "status": "closed"}

        try:
            order = await self._exchange.create_market_sell_order(symbol, quantity)
            logger.info(f"SELL order placed: {order['id']} | {symbol} | qty={quantity}")
            return order
        except Exception as e:
            logger.error(f"Failed to place sell order for {symbol}: {e}")
            return None

    async def get_balance(self, asset: str = "USDT") -> float:
        """Return free balance of a given asset."""
        balance = await self._exchange.fetch_balance()
        return balance.get("free", {}).get(asset, 0.0)

    async def get_open_orders(self, symbol: str) -> list:
        return await self._exchange.fetch_open_orders(symbol)

    async def cancel_all_orders(self, symbol: str):
        orders = await self.get_open_orders(symbol)
        for order in orders:
            await self._exchange.cancel_order(order["id"], symbol)
            logger.info(f"Cancelled order {order['id']} for {symbol}")

    # ------------------------------------------------------------------
    # Polling price (fast loop, no WS dependency)
    # ------------------------------------------------------------------

    async def poll_price(self, symbol: str, interval_ms: int = 100) -> float:
        """Fetch the latest ask price for a symbol."""
        ob = await self.get_order_book(symbol, limit=1)
        asks = ob.get("asks", [])
        if asks:
            return float(asks[0][0])
        bids = ob.get("bids", [])
        if bids:
            return float(bids[0][0])
        ticker = await self.get_ticker(symbol)
        return float(ticker.get("last", 0))
