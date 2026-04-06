import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Bitget API
    API_KEY: str = os.getenv("BITGET_API_KEY", "")
    SECRET_KEY: str = os.getenv("BITGET_SECRET_KEY", "")
    PASSPHRASE: str = os.getenv("BITGET_PASSPHRASE", "")

    # Trading parameters
    TRADE_AMOUNT_USDT: float = float(os.getenv("TRADE_AMOUNT_USDT", "10"))
    TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "150"))
    STOP_LOSS_PCT: float = float(os.getenv("STOP_LOSS_PCT", "30"))
    MAX_HOLD_SECONDS: int = int(os.getenv("MAX_HOLD_SECONDS", "10"))
    MAX_SLIPPAGE_PCT: float = float(os.getenv("MAX_SLIPPAGE_PCT", "20"))

    # Strategy options
    DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() == "true"
    POLL_INTERVAL_MS: int = int(os.getenv("POLL_INTERVAL_MS", "500"))

    @classmethod
    def validate(cls):
        if not cls.DRY_RUN:
            assert cls.API_KEY, "BITGET_API_KEY is required for live trading"
            assert cls.SECRET_KEY, "BITGET_SECRET_KEY is required for live trading"
            assert cls.PASSPHRASE, "BITGET_PASSPHRASE is required for live trading"
