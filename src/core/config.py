"""
Configuration management using pydantic-settings.
All settings can be overridden via .env file or environment variables.
"""
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    # --- Project paths ---
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    LOG_DIR: Path = BASE_DIR / "logs"

    # --- Tokocrypto WebSocket ---
    WS_BASE_URL: str = "wss://stream-cloud.tokocrypto.site/stream"
    REST_BASE_URL: str = "https://www.tokocrypto.com/open/v1"
    WS_RECONNECT_DELAY: float = 3.0
    WS_MAX_RECONNECT_DELAY: float = 60.0
    WS_STALE_THRESHOLD: float = 10.0
    WS_PING_INTERVAL: float = 180.0

    # --- Symbols (USDT pairs only) ---
    SYMBOLS: list[str] = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
        "ADAUSDT", "XRPUSDT", "DOGEUSDT", "AVAXUSDT",
    ]

    # --- Fees (Tokocrypto USDT pairs) ---
    TAKER_FEE: float = 0.001        # 0.10%
    MAKER_FEE: float = 0.001        # 0.10%
    EXTRA_FEE_USDT: float = 0.001544
    MIN_ORDER_USDT: float = 1.0

    # --- Paper Trading capital ---
    INITIAL_USDT_BALANCE: float = 100.0

    # --- SMC Position Management ---
    MAX_OPEN_POSITIONS: int = 4
    POSITION_SIZE_PCT: float = 0.10      # 10% of total equity per slot
    CUT_LOSS_PCT: float = 0.02           # 2% SL per position
    DEFAULT_TP_RATIO: str = "1:2"        # "1:1", "1:2", or "1:3"
    TRAILING_STOP_PCT: float = 0.015     # 1.5% trailing stop
    PARTIAL_CLOSE_AT_1R: bool = True     # close 50% at 1R target

    # --- SMC Analysis Timeframes ---
    HTF_CANDLES_NEEDED: int = 100        # min candles for HTF analysis
    LTF_CANDLES_NEEDED: int = 50         # min candles for LTF confirmation
    HTF_ANALYSIS_INTERVAL: float = 300.0 # re-run HTF every 5 min
    SWING_LOOKBACK: int = 5              # bars each side for swing detection

    # --- EMA ---
    EMA_FAST: int = 13
    EMA_SLOW: int = 21

    # --- Dashboard ---
    DASHBOARD_HOST: str = "0.0.0.0"
    DASHBOARD_PORT: int = 8080

    # --- Logging ---
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "trading_bot.log"
    TRADE_LOG_FILE: str = "trades.jsonl"
    PRICE_LOG_FILE: str = "prices.jsonl"
    PORTFOLIO_SNAPSHOT_FILE: str = "portfolio_snapshots.jsonl"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    def ensure_dirs(self) -> None:
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)

    @property
    def total_taker_fee(self) -> float:
        return self.TAKER_FEE + self.EXTRA_FEE_USDT

    @property
    def total_maker_fee(self) -> float:
        return self.MAKER_FEE + self.EXTRA_FEE_USDT


settings = Settings()
